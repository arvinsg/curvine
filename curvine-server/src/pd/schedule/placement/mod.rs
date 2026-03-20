pub mod fit;
pub mod rule;

pub use fit::{fit_bg, is_better_fit, BGFit};
pub use rule::{LabelConstraint, LabelOp, PlacementRule};
