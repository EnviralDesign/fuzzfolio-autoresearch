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
/// Native v5 evolvable-module proposal construction.  This is intentionally
/// separate from the legacy typed-fragment runtime: it never starts or calls
/// the Dashboard JSONL Python authority in production.
pub mod v5;
/// Typed durable-object inventory and strict offline reconstruction for native
/// v5 later-generation transactions.  Batch persists these objects but never
/// invents a shadow replay schema.
pub mod v5_evolved_durable;
/// Cap-free plan, one-pass fragment stream, and receipt-only adoption
/// verification for native v5 later-generation public artifacts.
pub mod v5_evolved_publication;
/// Write-neutral native v5 later-generation transaction shell.  Its sealed
/// materializer remains crate-visible until the complete evolved replay gate
/// is wired, so external callers cannot bypass the transaction boundary.
pub mod v5_evolved_transaction;
/// Durable compact funnel receipt and no-rich projection streams for native
/// v5 G0 bootstrap publication.
pub mod v5_g0_funnel;
/// Sealed, write-neutral v5 public-artifact plans and selected-only streams.
/// qd-batch remains solely responsible for filesystem/object-store sinks.
pub mod v5_publication;
/// Write-neutral, typed native v5 proposal transactions.  Batch owns all
/// filesystem/object-store publication; this module owns deterministic G0
/// construction, compact replay, and selection semantics.
pub mod v5_transaction;
// Kept crate-private until the v5 transaction adopts the complete
// authority-bound compiler/selection contract.  The core v5 compiler calls
// this only for evolved-program structural admission; no public caller can
// bypass the compact transaction boundary.
pub(crate) mod v5_operators;

pub use temporal_qd_contract::{
    CONTRACT_VERSION, FOUNDATION_OPERATION, FOUNDATION_RESULT_PATH, MANIFEST_SCHEMA, RESULT_SCHEMA,
    VERSION_SCHEMA,
};

#[derive(Debug, thiserror::Error)]
pub enum KernelError {
    #[error("native foundation kernel is not initialized")]
    NotInitialized,
}
