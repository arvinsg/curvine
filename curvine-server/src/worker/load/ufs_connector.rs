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

use crate::common::ufs_manager::UfsManager;
use axum::async_trait;
use bytes::{Bytes, BytesMut};
use curvine_client::file::{FsClient, FsWriter};
use curvine_client::unified::UnifiedReader;
use curvine_common::fs::{CurvineURI, Path, Reader, Writer};
use curvine_common::state::CreateFileOptsBuilder;
use curvine_common::FsResult;
use curvine_ufs::fs::{AsyncChunkReader, AsyncChunkWriter};
use orpc::sys::DataSlice;
use orpc::CommonResult;
use std::io::Error;
use std::io::Result as IoResult;
use std::sync::Arc;
use crate::worker::load::LoadTask;

pub struct UfsConnector {
    source_path: String,
    fs_client: Arc<FsClient>,
    ufs_manager: UfsManager,
}

impl UfsConnector {
    pub async fn new(source_path: String, fs_client: Arc<FsClient>) -> CommonResult<Self> {
        let mut ufs_manager = UfsManager::new(fs_client.clone());
        // ufs connector will be created by the worker side,
        // and all the mount table should be retrieved from master.
        ufs_manager.sync_mount_table().await?;
        Ok(Self {
            source_path,
            fs_client: fs_client.clone(),
            ufs_manager,
        })
    }

    pub(crate) async fn get_file_size(&mut self) -> CommonResult<u64> {
        let reader = self.create_reader().await?;
        Ok(reader.content_length())
    }

    pub async fn create_reader(&mut self) -> FsResult<Box<dyn AsyncChunkReader + Send + 'static>> {
        let uri = CurvineURI::new(&self.source_path)?;
        let ufs_client = self.ufs_manager.get_client(&uri).await?;
        let reader = ufs_client.open(&uri).await?;
        let status = ufs_client.get_file_status(&uri).await?;
        Ok(Box::new(S3ReaderWrapper::new(reader, status.mtime)))
    }
}

pub struct S3ReaderWrapper {
    reader: UnifiedReader,
    mtime: i64,
}

impl S3ReaderWrapper {
    pub fn new(reader: UnifiedReader, mtime: i64) -> Self {
        Self { reader, mtime }
    }
}

#[async_trait]
impl AsyncChunkReader for S3ReaderWrapper {
    async fn read_chunk(&mut self, buf: &mut BytesMut) -> IoResult<usize> {
        match self.reader.read_chunk0().await {
            Ok(DataSlice::Bytes(bytes)) => {
                let len = bytes.len();
                buf.extend_from_slice(&bytes);
                Ok(len)
            }
            Ok(DataSlice::Buffer(bytes_mut)) => {
                let len = bytes_mut.len();
                buf.extend_from_slice(&bytes_mut);
                Ok(len)
            }
            Ok(DataSlice::IOSlice(_io_slice)) => {
                // IOSlice doesn't support direct byte access, treat as empty for now
                // In a real implementation, you might need to handle this differently
                Ok(0)
            }
            Ok(DataSlice::MemSlice(mem_slice)) => {
                let data = mem_slice.as_slice();
                let len = data.len();
                buf.extend_from_slice(data);
                Ok(len)
            }
            Ok(DataSlice::Empty) => Ok(0),
            Err(e) => Err(Error::other(format!("S3 read error: {}", e))),
        }
    }

    fn content_length(&self) -> u64 {
        self.reader.len() as u64
    }

    fn mtime(&self) -> i64 {
        self.mtime
    }
    async fn read(&mut self, offset: u64, length: u64) -> IoResult<Bytes> {
        // Seek to the specified offset
        if let Err(e) = self.reader.seek(offset as i64).await {
            return Err(Error::other(format!("S3 seek error: {}", e)));
        }

        let mut result = BytesMut::new();
        let mut remaining = length;

        while remaining > 0 {
            match self.reader.read_chunk0().await {
                Ok(DataSlice::Bytes(bytes)) => {
                    let chunk_len = bytes.len() as u64;
                    if chunk_len <= remaining {
                        result.extend_from_slice(&bytes);
                        remaining -= chunk_len;
                    } else {
                        result.extend_from_slice(&bytes[..remaining as usize]);
                        remaining = 0;
                    }
                }
                Ok(DataSlice::Buffer(bytes_mut)) => {
                    let chunk_len = bytes_mut.len() as u64;
                    if chunk_len <= remaining {
                        result.extend_from_slice(&bytes_mut);
                        remaining -= chunk_len;
                    } else {
                        result.extend_from_slice(bytes_mut[..remaining as usize].as_ref());
                        remaining = 0;
                    }
                }
                Ok(DataSlice::IOSlice(_io_slice)) => {
                    // IOSlice doesn't support direct byte access, skip for now
                    // In a real implementation, you might need to handle this differently
                    break;
                }
                Ok(DataSlice::MemSlice(mem_slice)) => {
                    let data = mem_slice.as_slice();
                    let chunk_len = data.len() as u64;
                    if chunk_len <= remaining {
                        result.extend_from_slice(data);
                        remaining -= chunk_len;
                    } else {
                        result.extend_from_slice(&data[..remaining as usize]);
                        remaining = 0;
                    }
                }
                Ok(DataSlice::Empty) => break,
                Err(e) => return Err(Error::other(format!("S3 read error: {}", e))),
            }
        }

        Ok(result.freeze())
    }
}

pub struct CurvineFsWriter {
    writer: FsWriter,
}

impl CurvineFsWriter {
    pub async fn new(client: &FsClient, task: &LoadTask) -> FsResult<CurvineFsWriter> {
        let path = Path::from_str(&task.source_path)?;
        let opts = CreateFileOptsBuilder::new()
            .replicas(task.replicas)
            .block_size(task.block_size)
            .storage_type(task.storage_type)
            .ttl_ms(task.ttl_ms)
            .ttl_action(task.ttl_action)
            .build();
        let status = client.create_with_opts(&path, opts).await?;
        let writer = FsWriter::create(
            client.context(),
            path,
            status,
        );
        Ok(CurvineFsWriter { writer })
    }
}

#[async_trait]
impl AsyncChunkWriter for CurvineFsWriter {
    async fn write_chunk(&mut self, data: Bytes) -> IoResult<()> {
        self.writer
            .write(data.as_ref())
            .await
            .map_err(|e| Error::other(format!("Curvine write error: {}", e)))
    }

    async fn flush(&mut self) -> IoResult<()> {
        self.writer
            .complete()
            .await
            .map_err(|e| Error::other(format!("Curvine complete error: {}", e)))
    }
}
