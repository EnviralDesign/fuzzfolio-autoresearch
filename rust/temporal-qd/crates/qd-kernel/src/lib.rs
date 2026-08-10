//! One-shot native execution kernel for the Temporal QD coordinator front half.
//!
//! The public modules are admitted independently against the Python oracle
//! before the one-shot batch coordinator is allowed to compose them.

pub mod construction;
pub mod factory;
pub mod g0;
pub mod g0_funnel;
pub mod generation;
pub mod genome;
pub mod grammar;
pub mod identity;
pub mod indicator;
pub mod journal;
pub mod proposal;
pub mod protection;
pub mod publication;
pub mod schedule;
pub mod selector;

pub use temporal_qd_contract::{
    CONTRACT_VERSION, FOUNDATION_OPERATION, FOUNDATION_RESULT_PATH, MANIFEST_SCHEMA, RESULT_SCHEMA,
    VERSION_SCHEMA,
};

#[derive(Debug, thiserror::Error)]
pub enum KernelError {
    #[error("native foundation kernel is not initialized")]
    NotInitialized,
}
