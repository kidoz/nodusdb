//! Catalog: schema/RBAC descriptors, operation requests, the reader/writer/store
//! trait interfaces, and the in-memory implementation. Split into focused
//! modules and re-exported flat so `nodus_catalog::TableDescriptor` etc. resolve.

mod descriptors;
mod memory_catalog;
mod rbac_descriptors;
mod requests;
mod traits;

pub use descriptors::*;
pub use memory_catalog::MemoryCatalog;
pub use rbac_descriptors::*;
pub use requests::*;
pub use traits::*;
