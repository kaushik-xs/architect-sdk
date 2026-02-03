//! Safe SQL builder: identifiers from config only, values as parameters.

mod builder;
pub mod params;
pub use builder::*;
pub use params::*;
