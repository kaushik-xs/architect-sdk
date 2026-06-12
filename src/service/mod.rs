//! CrudService: generic CRUD using safe SQL builder.

mod crud;
mod validation;
pub use crud::{CrudService, GraphChild, TenantExecutor, TenantExecutorInner};
pub use validation::RequestValidator;
