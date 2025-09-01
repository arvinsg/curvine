// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::master::meta::inode::{InodePtr, InodeView};
use orpc::{err_box, CommonResult};
use std::collections::btree_map::{Entry, Values};
use std::collections::BTreeMap;
use std::slice::Iter;

#[derive(Debug, Clone)]
pub enum InodeChildren {
    List(Vec<Box<InodeView>>),
    Map(BTreeMap<String, Box<InodeView>>),
}

impl InodeChildren {
    pub fn new_list() -> Self {
        InodeChildren::List(vec![])
    }

    pub fn new_map() -> Self {
        InodeChildren::Map(BTreeMap::new())
    }

    // Search for whether the current inode name exists.
    fn search_by_name(list: &[Box<InodeView>], name: &str) -> Result<usize, usize> {
        list.binary_search_by(|f| f.name().cmp(name))
    }

    pub fn get_child(&self, name: &str) -> Option<&InodeView> {
        match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, name);
                match index {
                    Err(_) => None,
                    Ok(v) => Some(&list[v]),
                }
            }
            InodeChildren::Map(map) => map.get(name).map(|x| x.as_ref()),
        }
    }

    pub fn get_child_ptr(&self, name: &str) -> Option<InodePtr> {
        self.get_child(name).map(InodePtr::from_ref)
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        let removed = match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, child_name);
                match index {
                    Ok(v) => Some(list.remove(v)),
                    Err(_) => None,
                }
            }

            InodeChildren::Map(map) => map.remove(child_name),
        };

        match removed {
            None => err_box!("Child {} not exists", child_name),
            Some(r) => {
                if r.id() != child_id {
                    err_box!(
                        "Inode status error, expect id {}, actually delete {}",
                        child_id,
                        r.id()
                    )
                } else {
                    Ok(*r)
                }
            }
        }
    }

    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<InodePtr> {
        let inode = Box::new(inode);
        // Assert that it should not be FileEntry
        assert!(!inode.is_file_entry());
        match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, inode.name());
                match index {
                    Err(v) => {
                        list.insert(v, inode);
                        let cur_node = list[v].as_ref();
                        Ok(InodePtr::from_ref(cur_node))
                    }

                    Ok(_) => {
                        err_box!("Child {} already exists", inode.name())
                    }
                }
            }

            InodeChildren::Map(map) => match map.entry(inode.name().to_owned()) {
                Entry::Vacant(v) => {
                    if inode.is_file() {
                        // Store lightweight FileEntry in map
                        v.insert(Box::new(InodeView::FileEntry(
                            inode.name().to_string(),
                            inode.id(),
                        )));
                        // But return owned pointer to complete object
                        Ok(InodePtr::from_owned(*inode))
                    } else {
                        // Directory directly stores complete object and returns its reference
                        let inserted = v.insert(inode.clone());
                        Ok(InodePtr::from_ref(inserted.as_ref()))
                    }
                }

                Entry::Occupied(_) => {
                    err_box!("Child {} already exists", inode.name())
                }
            },
        }
    }

    pub fn iter(&self) -> ChildrenIter<'_> {
        match self {
            InodeChildren::List(list) => ChildrenIter {
                len: list.len(),
                inner: InnerIter::List(list.iter()),
            },

            InodeChildren::Map(map) => ChildrenIter {
                len: map.len(),
                inner: InnerIter::Map(map.values()),
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            InodeChildren::List(list) => list.len(),
            InodeChildren::Map(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for InodeChildren {
    fn default() -> Self {
        Self::new_map()
    }
}

enum InnerIter<'a> {
    List(Iter<'a, Box<InodeView>>),
    Map(Values<'a, String, Box<InodeView>>),
}

pub struct ChildrenIter<'a> {
    len: usize,
    inner: InnerIter<'a>,
}

impl ChildrenIter<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> Iterator for ChildrenIter<'a> {
    type Item = &'a InodeView;

    fn next(&mut self) -> Option<Self::Item> {
        let next = match &mut self.inner {
            InnerIter::List(list) => list.next(),
            InnerIter::Map(map) => map.next(),
        };
        next.map(|x| x.as_ref())
    }
}
