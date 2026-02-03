//! CrudService: generic CRUD using safe SQL builder.

mod crud;
mod validation;
pub use crud::CrudService;
pub use validation::RequestValidator;
