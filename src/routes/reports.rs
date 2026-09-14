//! Report routes: read-only query execution plus report-definition CRUD.
//!
//! Data plane (any valid tenant, authorized per-run via authrs):
//!   GET  /reports                     list report metadata
//!   GET  /reports/:report_id          one report's metadata
//!   POST /reports/:report_id/run      execute a report
//!
//! Config plane (Platform Admin tenant only):
//!   GET    /config/reports            raw stored definitions (includes SQL)
//!   POST   /config/reports            replace the whole standalone report set
//!   PUT    /config/reports/:report_id upsert one report by id
//!   DELETE /config/reports/:report_id remove one report by id

use crate::handlers::reports::{
    delete_report_by_id, get_report, get_reports_config, list_reports, post_reports,
    put_report_by_id, run_report,
};
use crate::state::AppState;
use axum::{
    routing::{get, post, put},
    Router,
};

pub fn report_routes(state: AppState) -> Router {
    Router::new()
        .route("/reports", get(list_reports))
        .route("/reports/:report_id", get(get_report))
        .route("/reports/:report_id/run", post(run_report))
        .route(
            "/config/reports",
            get(get_reports_config).post(post_reports),
        )
        .route(
            "/config/reports/:report_id",
            put(put_report_by_id).delete(delete_report_by_id),
        )
        .with_state(state)
}
