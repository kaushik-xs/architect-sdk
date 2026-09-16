//! HTTP middleware for the SDK.

pub mod trace;

pub use trace::{
    current_trace_id, outbound_traceparent, trace_id_layer, TraceId, TRACEPARENT_HEADER,
};
