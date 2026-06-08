//! MCP `ServerHandler` implementation + transport entrypoints.

use std::collections::HashMap;
use std::sync::Arc;

use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerInfo,
};
use rmcp::service::{MaybeSendFuture, RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler};
use serde_json::Value;
use tracing::{debug, error};

use crate::config::ResolvedEntity;
use crate::config::ResolvedModel;
use crate::error::AppError;
use crate::service::{CrudService, TenantExecutor};
use crate::sql::{parse_rsql, parse_sort};
use crate::state::AppState;
use crate::tenant::TenantStrategy;

use super::tools::{build_tool_list, McpToolSpec};

struct ToolRegistry {
    tools: Vec<McpToolSpec>,
    by_name: HashMap<String, usize>,
}

impl ToolRegistry {
    fn build(model: &ResolvedModel) -> Self {
        let tools = build_tool_list(model);
        let by_name = tools
            .iter()
            .enumerate()
            .map(|(i, s)| (s.tool.name.to_string(), i))
            .collect();
        ToolRegistry { tools, by_name }
    }
}

/// Architect MCP server — wraps `AppState` and implements `ServerHandler`.
#[derive(Clone)]
pub struct ArchitectMcpServer {
    state: AppState,
    registry: Arc<ToolRegistry>,
    default_tenant_id: Option<String>,
    /// Default user ID for authrs permission checks. Sourced from MCP_USER_ID env var.
    default_user_id: Option<String>,
}

impl ArchitectMcpServer {
    pub fn new(
        state: AppState,
        default_tenant_id: Option<String>,
        default_user_id: Option<String>,
    ) -> Self {
        let registry = {
            let model = state.model.read().expect("model lock poisoned");
            Arc::new(ToolRegistry::build(&model))
        };
        ArchitectMcpServer {
            state,
            registry,
            default_tenant_id,
            default_user_id,
        }
    }
}

impl ServerHandler for ArchitectMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new("architect-sdk", env!("CARGO_PKG_VERSION")),
        )
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + MaybeSendFuture + '_
    {
        let tools: Vec<_> = self.registry.tools.iter().map(|s| s.tool.clone()).collect();
        async move {
            Ok(ListToolsResult {
                tools,
                next_cursor: None,
                meta: None,
            })
        }
    }

    fn get_tool(&self, name: &str) -> Option<rmcp::model::Tool> {
        self.registry
            .by_name
            .get(name)
            .map(|&i| self.registry.tools[i].tool.clone())
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, McpError>> + MaybeSendFuture + '_
    {
        let state = self.state.clone();
        let registry = self.registry.clone();
        let default_tenant = self.default_tenant_id.clone();
        let default_user = self.default_user_id.clone();

        async move {
            let name = request.name.as_ref();
            let args = request.arguments.unwrap_or_default();

            let spec = registry
                .by_name
                .get(name)
                .map(|&i| &registry.tools[i])
                .ok_or_else(|| {
                    McpError::method_not_found::<rmcp::model::CallToolRequestMethod>()
                })?;

            let tenant_id = args
                .get("tenant_id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .or_else(|| default_tenant.clone())
                .ok_or_else(|| {
                    McpError::invalid_params(
                        "tenant_id is required (pass as tool argument or set MCP_TENANT_ID)",
                        None,
                    )
                })?;

            // user_id: per-call arg takes precedence over MCP_USER_ID default.
            // Optional when authrs is not configured; required when it is (enforced inside check_entity_permission_opt).
            let user_id = args
                .get("user_id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .or_else(|| default_user.clone());

            let entity = {
                let model = state
                    .model
                    .read()
                    .map_err(|_| McpError::internal_error("model lock poisoned", None))?;
                model
                    .entity_by_path(&spec.path_segment)
                    .cloned()
                    .ok_or_else(|| {
                        McpError::invalid_params(
                            format!("entity '{}' not found in model", spec.path_segment),
                            None,
                        )
                    })?
            };

            debug!(
                tool = %name,
                tenant = %tenant_id,
                user = ?user_id,
                operation = %spec.operation,
                "MCP tool call"
            );

            let result = dispatch_operation(
                &state,
                &entity,
                &spec.operation,
                &tenant_id,
                user_id.as_deref(),
                &args,
            )
            .await;

            match result {
                Ok(data) => {
                    let text =
                        serde_json::to_string_pretty(&data).unwrap_or_else(|_| data.to_string());
                    Ok(CallToolResult::success(vec![Content::text(text)]))
                }
                Err(e) => {
                    error!(tool = %name, error = %e, "MCP tool call failed");
                    Ok(CallToolResult::error(vec![Content::text(e.to_string())]))
                }
            }
        }
    }
}

/// Map an MCP operation name to the HTTP verb used by authrs action derivation.
fn operation_to_http_verb(op: &str) -> &'static str {
    match op {
        "list" | "read" => "get",
        "create" => "post",
        "update" => "patch",
        "delete" => "delete",
        _ => "get",
    }
}

async fn dispatch_operation(
    state: &AppState,
    entity: &ResolvedEntity,
    operation: &str,
    tenant_id: &str,
    user_id: Option<&str>,
    args: &rmcp::model::JsonObject,
) -> Result<Value, AppError> {
    // ── Permission check (no-op when authrs is not configured) ──────────────
    crate::authrs::check_entity_permission_opt(
        &state.authrs_client,
        Some(tenant_id),
        user_id,
        entity,
        operation_to_http_verb(operation),
    )
    .await?;

    let (strategy, pool) = {
        let entry = state
            .tenant_registry
            .get(tenant_id)
            .ok_or_else(|| AppError::BadRequest(format!("unknown tenant: {tenant_id}")))?;
        let pool = match entry.strategy {
            TenantStrategy::Database => {
                let url = entry
                    .database_url
                    .as_deref()
                    .ok_or_else(|| AppError::BadRequest("tenant missing database_url".into()))?;
                crate::handlers::entity::get_or_create_tenant_pool(state, tenant_id, url).await?
            }
            TenantStrategy::Rls => state.pool.clone(),
        };
        (entry.strategy.clone(), pool)
    };

    // For RLS, acquire a connection and set the tenant session variable
    let mut rls_conn = None;
    if strategy == TenantStrategy::Rls {
        let mut conn = pool.acquire().await?;
        if let Some(set_sql) = state.dialect.set_tenant_session_sql(tenant_id) {
            sqlx::query(&set_sql).execute(&mut *conn).await?;
        }
        rls_conn = Some(conn);
    }

    let mut executor = match rls_conn {
        Some(ref mut conn) => TenantExecutor::conn(conn, state.dialect.as_ref()),
        None => TenantExecutor::pool(&pool, state.dialect.as_ref()),
    };

    match operation {
        "list" => {
            let limit = args.get("limit").and_then(|v| v.as_u64()).map(|v| v as u32);
            let offset = args
                .get("offset")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32);
            let filter = args
                .get("filter")
                .and_then(|v| v.as_str())
                .and_then(|s| parse_rsql(s).ok());
            let sort = args
                .get("sort")
                .and_then(|v| v.as_str())
                .map(parse_sort)
                .unwrap_or_default();

            // Load the per-tenant extensible-field registry (cached) so `filter`/`sort` can
            // reference `<column>.<key>` keys on extensible JSON columns.
            let ext_registry =
                crate::handlers::entity::load_extensible_registry(state, entity, Some(tenant_id))
                    .await?;

            let rows = CrudService::list(
                &mut executor,
                entity,
                filter.as_ref(),
                &sort,
                limit,
                offset,
                &[],
                None,
                state.dialect.as_ref(),
                ext_registry.as_ref(),
            )
            .await?;

            let stripped: Vec<Value> = rows
                .into_iter()
                .map(|r| strip_sensitive(r, entity))
                .collect();
            Ok(Value::Array(stripped))
        }

        "read" => {
            let id_str = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AppError::BadRequest("id is required".into()))?;
            let id_val = Value::String(id_str.to_string());
            let row =
                CrudService::read(&mut executor, entity, &id_val, None, state.dialect.as_ref())
                    .await?
                    .ok_or_else(|| AppError::NotFound(format!("id {id_str}")))?;
            Ok(strip_sensitive(row, entity))
        }

        "create" => {
            let body = extract_body(args);
            let rls_tenant = if strategy == TenantStrategy::Rls {
                Some(tenant_id)
            } else {
                None
            };
            let row = CrudService::create(
                &mut executor,
                entity,
                &body,
                None,
                rls_tenant,
                user_id,
                state.dialect.as_ref(),
            )
            .await?;
            Ok(strip_sensitive(row, entity))
        }

        "update" => {
            let id_str = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AppError::BadRequest("id is required".into()))?;
            let id_val = Value::String(id_str.to_string());
            let body = extract_body(args);
            let row = CrudService::update(
                &mut executor,
                entity,
                &id_val,
                &body,
                None,
                user_id,
                state.dialect.as_ref(),
            )
            .await?
            .ok_or_else(|| AppError::NotFound(format!("id {id_str}")))?;
            Ok(strip_sensitive(row, entity))
        }

        "delete" => {
            let id_str = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AppError::BadRequest("id is required".into()))?;
            let id_val = Value::String(id_str.to_string());
            CrudService::delete(
                &mut executor,
                entity,
                &id_val,
                None,
                None,
                state.dialect.as_ref(),
            )
            .await?;
            Ok(serde_json::json!({ "deleted": true, "id": id_str }))
        }

        other => Err(AppError::BadRequest(format!(
            "unsupported operation: {other}"
        ))),
    }
}

fn extract_body(args: &rmcp::model::JsonObject) -> HashMap<String, Value> {
    const RESERVED: &[&str] = &[
        "tenant_id",
        "user_id",
        "id",
        "filter",
        "sort",
        "limit",
        "offset",
        "include",
    ];
    args.iter()
        .filter(|(k, _)| !RESERVED.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn strip_sensitive(mut row: Value, entity: &ResolvedEntity) -> Value {
    if let Value::Object(ref mut map) = row {
        for col in &entity.sensitive_columns {
            map.remove(col);
        }
    }
    row
}

// ─── Public entrypoints ───────────────────────────────────────────────────────

/// Start the MCP server. Transport is selected by the `MCP_TRANSPORT` env var:
/// - `stdio` (default): reads stdin / writes stdout (for Claude Desktop, Claude Code)
/// - `http`: serves SSE/HTTP on `MCP_PORT` (default 3001)
///
/// **Environment variables:**
/// - `MCP_TENANT_ID` — default tenant for all tool calls (can be overridden per call)
/// - `MCP_USER_ID`   — default user ID for authrs permission checks (can be overridden per call)
/// - `MCP_PORT`      — HTTP transport port (default 3001)
pub async fn serve(state: AppState) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let default_tenant = std::env::var("MCP_TENANT_ID").ok();
    let default_user = std::env::var("MCP_USER_ID").ok();
    let transport = std::env::var("MCP_TRANSPORT").unwrap_or_else(|_| "stdio".to_string());

    match transport.to_lowercase().as_str() {
        "http" => serve_http(state, default_tenant, default_user).await,
        _ => serve_stdio(state, default_tenant, default_user).await,
    }
}

async fn serve_stdio(
    state: AppState,
    default_tenant: Option<String>,
    default_user: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use rmcp::{serve_server, transport::stdio};

    let server = ArchitectMcpServer::new(state, default_tenant, default_user);
    let running = serve_server(server, stdio()).await?;
    running.waiting().await?;
    Ok(())
}

async fn serve_http(
    state: AppState,
    default_tenant: Option<String>,
    default_user: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use rmcp::transport::streamable_http_server::{
        session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    };

    let port: u16 = std::env::var("MCP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3001);
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));

    let state_clone = state.clone();
    let default_tenant_clone = default_tenant.clone();
    let default_user_clone = default_user.clone();

    let service = StreamableHttpService::new(
        move || {
            Ok(ArchitectMcpServer::new(
                state_clone.clone(),
                default_tenant_clone.clone(),
                default_user_clone.clone(),
            ))
        },
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default().with_allowed_hosts([
            "localhost",
            "127.0.0.1",
            "0.0.0.0",
        ]),
    );

    let router = axum::Router::new().route_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(port, "MCP HTTP server listening on /mcp");
    axum::serve(listener, router).await?;
    Ok(())
}
