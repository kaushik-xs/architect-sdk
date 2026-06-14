//! Builds parameterized INSERT, SELECT, UPDATE, DELETE from resolved entity.

use crate::config::{IncludeDirection, PkType, ResolvedEntity};
use crate::db::{type_category_from_cast, CanonicalType, Dialect, TypeCategory};
use crate::error::AppError;
use crate::extensible_fields::ExtensibleRegistry;
use crate::sql::rsql::{FilterNode, RsqlOp, SortSpec};
use serde_json::Value;
use std::collections::HashMap;

/// Describes one include for single-query list: name, direction, related entity, our key column, their key column.
pub struct IncludeSelect<'a> {
    pub name: &'a str,
    pub direction: IncludeDirection,
    pub related: &'a ResolvedEntity,
    pub our_key: &'a str,
    pub their_key: &'a str,
}

/// Quote identifier for PostgreSQL (safe: only from config).
fn quoted(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Full qualified table name.
fn qualified_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quoted(schema), quoted(table))
}

pub struct QueryBuf {
    pub sql: String,
    pub params: Vec<Value>,
}

impl QueryBuf {
    fn new() -> Self {
        QueryBuf {
            sql: String::new(),
            params: Vec::new(),
        }
    }

    fn push_param(&mut self, v: Value) -> u32 {
        let n = self.params.len() as u32 + 1;
        self.params.push(v);
        n
    }
}

/// SELECT list: each column as-is, except custom enum (schema.typename), numeric, time, and timetz
/// as col::text so sqlx returns String.
fn select_column_list(entity: &ResolvedEntity) -> String {
    entity
        .columns
        .iter()
        .map(|c| {
            let q = quoted(&c.name);
            let pg_type = c.pg_type.as_deref().unwrap_or("");
            if pg_type.contains('.')
                || pg_type == "numeric"
                || pg_type == "time"
                || pg_type == "timetz"
            {
                format!("{}::text", q)
            } else {
                q
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Resolve schema: override if present, else entity's schema.
fn resolve_schema<'a>(entity: &'a ResolvedEntity, schema_override: Option<&'a str>) -> &'a str {
    schema_override.unwrap_or(&entity.schema_name)
}

/// Postgres array columns: API accepts JSON `["a","b"]`; bind as array literal + `$n::varchar(255)[]` etc.
pub fn coerce_json_value_for_pg_array(val: Value, pg_type: Option<&str>) -> Value {
    if !pg_type.is_some_and(|t| t.ends_with("[]")) {
        return val;
    }
    match val {
        Value::Null => Value::Null,
        Value::Array(items) => {
            let mut out = String::from('{');
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                match v {
                    Value::Null => out.push_str("NULL"),
                    other => {
                        let elem = match other {
                            Value::String(s) => s.clone(),
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            _ => serde_json::to_string(other).unwrap_or_else(|_| "{}".to_string()),
                        };
                        out.push('"');
                        for ch in elem.chars() {
                            if ch == '"' || ch == '\\' {
                                out.push('\\');
                            }
                            out.push(ch);
                        }
                        out.push('"');
                    }
                }
            }
            out.push('}');
            Value::String(out)
        }
        // multipart/form-data sends every field as a scalar string, so an array column
        // arrives as a single comma-separated string (e.g. "id1, id2"). Split it into
        // elements — trimming whitespace and dropping empties — so it binds as a real
        // array. A string with no comma becomes a single-element array (clients can send
        // `"id"` instead of `["id"]`). JSON clients send a proper `Value::Array` and hit
        // the arm above, so their comma-containing values are never split.
        Value::String(s) => {
            let items: Vec<Value> = s
                .split(',')
                .map(|part| part.trim())
                .filter(|part| !part.is_empty())
                .map(|part| Value::String(part.to_string()))
                .collect();
            coerce_json_value_for_pg_array(Value::Array(items), pg_type)
        }
        // Other scalar JSON values (number, bool) → single-element array for convenience.
        other => coerce_json_value_for_pg_array(Value::Array(vec![other]), pg_type),
    }
}

/// Placeholder for PK in WHERE (e.g. $1, $1::uuid, $1::bigint) so the bound value — which
/// always travels over the wire as TEXT — is cast to the column type. Without this, a numeric
/// PK comparison fails with `operator does not exist: bigint = text`.
fn pk_placeholder(entity: &ResolvedEntity, param_num: usize, dialect: &dyn Dialect) -> String {
    let ph = dialect.placeholder(param_num);
    let canonical = match &entity.pk_type {
        PkType::Uuid => crate::db::CanonicalType::Uuid,
        PkType::BigInt => crate::db::CanonicalType::BigInt,
        PkType::Int => crate::db::CanonicalType::Int,
        PkType::Text => return ph,
    };
    match dialect.cast_name(&canonical) {
        Some(cast) => dialect.cast_expr(&ph, &cast),
        None => ph,
    }
}

// ─── RSQL → SQL ───────────────────────────────────────────────────────────────

fn op_valid_for_category(op: &RsqlOp, category: TypeCategory) -> bool {
    match category {
        TypeCategory::Text => matches!(
            op,
            RsqlOp::Eq
                | RsqlOp::Neq
                | RsqlOp::In
                | RsqlOp::Out
                | RsqlOp::Like
                | RsqlOp::Ilike
                | RsqlOp::Contains
                | RsqlOp::Starts
                | RsqlOp::Ends
                | RsqlOp::Null(_)
        ),
        TypeCategory::Int | TypeCategory::Float => matches!(
            op,
            RsqlOp::Eq
                | RsqlOp::Neq
                | RsqlOp::Gt
                | RsqlOp::Ge
                | RsqlOp::Lt
                | RsqlOp::Le
                | RsqlOp::Between
                | RsqlOp::In
                | RsqlOp::Out
                | RsqlOp::Null(_)
        ),
        TypeCategory::Bool => matches!(op, RsqlOp::Eq | RsqlOp::Neq | RsqlOp::Null(_)),
        TypeCategory::Uuid => matches!(
            op,
            RsqlOp::Eq | RsqlOp::Neq | RsqlOp::In | RsqlOp::Out | RsqlOp::Null(_)
        ),
        TypeCategory::Date | TypeCategory::Timestamp | TypeCategory::Time => matches!(
            op,
            RsqlOp::Eq
                | RsqlOp::Neq
                | RsqlOp::Gt
                | RsqlOp::Ge
                | RsqlOp::Lt
                | RsqlOp::Le
                | RsqlOp::Between
                | RsqlOp::In
                | RsqlOp::Out
                | RsqlOp::Null(_)
        ),
        // JSON, bytes, arrays, custom types: allow all operators.
        TypeCategory::Json | TypeCategory::Bytes | TypeCategory::Other => true,
    }
}

/// Dialect-independent SQL type name for a canonical type, suitable for both RSQL
/// operator-category classification (via `type_category_from_cast`) and Postgres placeholder
/// casts. Returns `None` for text-like types (which need no cast). Used for extensible-field keys
/// whose declared type comes from the KV registry rather than `ColumnInfo.pg_type`.
fn canonical_cast_str(t: &CanonicalType) -> Option<&'static str> {
    match t {
        CanonicalType::SmallInt => Some("smallint"),
        CanonicalType::Int | CanonicalType::Serial => Some("integer"),
        CanonicalType::BigInt | CanonicalType::BigSerial => Some("bigint"),
        CanonicalType::Real => Some("real"),
        CanonicalType::Double => Some("double precision"),
        CanonicalType::Decimal(_) => Some("numeric"),
        CanonicalType::Boolean => Some("boolean"),
        CanonicalType::Uuid => Some("uuid"),
        CanonicalType::Json | CanonicalType::Jsonb => Some("jsonb"),
        CanonicalType::Timestamp => Some("timestamptz"),
        CanonicalType::TimestampNtz => Some("timestamp"),
        CanonicalType::Date => Some("date"),
        CanonicalType::Time => Some("time"),
        CanonicalType::Timetz => Some("timetz"),
        _ => None,
    }
}

fn make_placeholder(n: usize, cast: Option<&str>, dialect: &dyn Dialect) -> String {
    let ph = dialect.placeholder(n);
    match cast {
        Some(t) => dialect.cast_expr(&ph, t),
        None => ph,
    }
}

/// Build the SQL fragment for a single RSQL leaf condition.
/// `qcol` is an already-quoted (and optionally qualified) column expression.
/// `pg_type` drives operator validation and placeholder casting.
/// `field_label` is used only in error messages (e.g. "bay" or "transport_unit.bay").
fn build_leaf_sql(
    qcol: &str,
    pg_type: Option<&str>,
    op: &RsqlOp,
    values: &[String],
    q: &mut QueryBuf,
    field_label: &str,
    dialect: &dyn Dialect,
) -> Result<String, AppError> {
    let category = type_category_from_cast(pg_type.unwrap_or("text"));
    if !op_valid_for_category(op, category) {
        return Err(AppError::Validation(format!(
            "operator {} is not valid for {:?} field '{}' (type: {})",
            op.display(),
            category,
            field_label,
            pg_type.unwrap_or("text")
        )));
    }
    let cast = if matches!(
        op,
        RsqlOp::Like | RsqlOp::Ilike | RsqlOp::Contains | RsqlOp::Starts | RsqlOp::Ends
    ) {
        None
    } else {
        pg_type
    };
    match op {
        RsqlOp::Null(is_null) => Ok(if *is_null {
            format!("{} IS NULL", qcol)
        } else {
            format!("{} IS NOT NULL", qcol)
        }),
        RsqlOp::Eq | RsqlOp::Neq | RsqlOp::Gt | RsqlOp::Ge | RsqlOp::Lt | RsqlOp::Le => {
            let v = values.first().cloned().unwrap_or_default();
            let n = q.push_param(Value::String(v));
            let ph = make_placeholder(n as usize, cast, dialect);
            let cmp = match op {
                RsqlOp::Eq => "=",
                RsqlOp::Neq => "!=",
                RsqlOp::Gt => ">",
                RsqlOp::Ge => ">=",
                RsqlOp::Lt => "<",
                RsqlOp::Le => "<=",
                _ => unreachable!(),
            };
            Ok(format!("{} {} {}", qcol, cmp, ph))
        }
        RsqlOp::Like => {
            let v = values.first().cloned().unwrap_or_default();
            let n = q.push_param(Value::String(v));
            Ok(format!("{} LIKE {}", qcol, dialect.placeholder(n as usize)))
        }
        RsqlOp::Ilike => {
            let v = values.first().cloned().unwrap_or_default();
            let n = q.push_param(Value::String(v));
            let ph = dialect.placeholder(n as usize);
            Ok(dialect.case_insensitive_like(qcol, &ph))
        }
        RsqlOp::Contains => {
            let v = values.first().cloned().unwrap_or_default();
            let n = q.push_param(Value::String(format!("%{}%", v)));
            let ph = dialect.placeholder(n as usize);
            Ok(dialect.case_insensitive_like(qcol, &ph))
        }
        RsqlOp::Starts => {
            let v = values.first().cloned().unwrap_or_default();
            let n = q.push_param(Value::String(format!("{}%", v)));
            let ph = dialect.placeholder(n as usize);
            Ok(dialect.case_insensitive_like(qcol, &ph))
        }
        RsqlOp::Ends => {
            let v = values.first().cloned().unwrap_or_default();
            let n = q.push_param(Value::String(format!("%{}", v)));
            let ph = dialect.placeholder(n as usize);
            Ok(dialect.case_insensitive_like(qcol, &ph))
        }
        RsqlOp::In => {
            if values.is_empty() {
                return Err(AppError::Validation(format!(
                    "=in= requires at least one value for field '{}'",
                    field_label
                )));
            }
            let phs: Vec<String> = values
                .iter()
                .map(|v| {
                    let n = q.push_param(Value::String(v.clone()));
                    make_placeholder(n as usize, cast, dialect)
                })
                .collect();
            Ok(format!("{} IN ({})", qcol, phs.join(", ")))
        }
        RsqlOp::Out => {
            if values.is_empty() {
                return Err(AppError::Validation(format!(
                    "=out= requires at least one value for field '{}'",
                    field_label
                )));
            }
            let phs: Vec<String> = values
                .iter()
                .map(|v| {
                    let n = q.push_param(Value::String(v.clone()));
                    make_placeholder(n as usize, cast, dialect)
                })
                .collect();
            Ok(format!("{} NOT IN ({})", qcol, phs.join(", ")))
        }
        RsqlOp::Between => {
            if values.len() != 2 {
                return Err(AppError::Validation(format!(
                    "=between= requires exactly 2 values for field '{}', got {}",
                    field_label,
                    values.len()
                )));
            }
            let n1 = q.push_param(Value::String(values[0].clone()));
            let n2 = q.push_param(Value::String(values[1].clone()));
            Ok(format!(
                "{} BETWEEN {} AND {}",
                qcol,
                make_placeholder(n1 as usize, cast, dialect),
                make_placeholder(n2 as usize, cast, dialect)
            ))
        }
        #[allow(unreachable_patterns)]
        RsqlOp::Null(_) => unreachable!(),
    }
}

/// Convert a `FilterNode` tree into a SQL WHERE fragment (no leading `WHERE`).
/// All values are pushed as parameters into `q`; identifiers come only from
/// config (never from user input) so SQL injection is structurally impossible.
///
/// `col_qualifier` is an optional table alias prefix, e.g. `"main."` for aliased queries.
///
/// `filter_includes` supplies the related-entity metadata needed to generate
/// EXISTS subqueries for dotted-field filters like `transport_unit.bay=contains=bay23`.
#[allow(clippy::too_many_arguments)]
pub fn rsql_to_sql(
    node: &FilterNode,
    entity: &ResolvedEntity,
    q: &mut QueryBuf,
    col_qualifier: Option<&str>,
    filter_includes: &[IncludeSelect<'_>],
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
    registry: Option<&ExtensibleRegistry>,
) -> Result<String, AppError> {
    match node {
        FilterNode::And(children) => {
            let parts: Result<Vec<_>, _> = children
                .iter()
                .map(|c| {
                    rsql_to_sql(
                        c,
                        entity,
                        q,
                        col_qualifier,
                        filter_includes,
                        schema_override,
                        dialect,
                        registry,
                    )
                })
                .collect();
            Ok(format!("({})", parts?.join(" AND ")))
        }
        FilterNode::Or(children) => {
            let parts: Result<Vec<_>, _> = children
                .iter()
                .map(|c| {
                    rsql_to_sql(
                        c,
                        entity,
                        q,
                        col_qualifier,
                        filter_includes,
                        schema_override,
                        dialect,
                        registry,
                    )
                })
                .collect();
            Ok(format!("({})", parts?.join(" OR ")))
        }
        FilterNode::Leaf { field, op, values } => {
            // Dotted field: first check for a extensible-fields bag (`<extensible_col>.<key>`),
            // then fall back to related-entity include semantics (`<include>.<field>`).
            if let Some(dot_pos) = field.find('.') {
                let head = &field[..dot_pos];
                let key = &field[dot_pos + 1..];

                if entity.extensible_columns.iter().any(|c| c == head) {
                    let def = registry.and_then(|r| r.field(head, key)).ok_or_else(|| {
                        AppError::Validation(format!(
                            "unknown extensible field '{}' (not declared in the registry)",
                            field
                        ))
                    })?;
                    if !def.filterable {
                        return Err(AppError::Validation(format!(
                            "extensible field '{}' is not filterable",
                            field
                        )));
                    }
                    let canonical = def.canonical();
                    // The canonical cast string drives both operator-category validation and the
                    // Postgres placeholder cast. It is dialect-independent on purpose: MySQL/SQLite
                    // `cast_name` is always None, which would otherwise misclassify a numeric
                    // extensible field as text and reject `=gt=`.
                    let cf_cast = canonical_cast_str(&canonical);
                    let base_col = match col_qualifier {
                        Some(pfx) => format!("{}{}", pfx, quoted(head)),
                        None => quoted(head),
                    };
                    let json_expr = dialect.json_extract_typed(&base_col, key, &canonical);
                    return build_leaf_sql(&json_expr, cf_cast, op, values, q, field, dialect);
                }

                let include_name = head;
                let sub_field = key;

                let inc = filter_includes
                    .iter()
                    .find(|i| i.name == include_name)
                    .ok_or_else(|| AppError::Validation(format!(
                        "filter on '{}': '{}' is not a known include — add it to the include= parameter or ensure the relationship is configured",
                        field, include_name
                    )))?;

                let col_info = inc
                    .related
                    .columns
                    .iter()
                    .find(|c| c.name == sub_field)
                    .ok_or_else(|| {
                        AppError::Validation(format!(
                            "unknown filter field '{}' on related entity '{}'",
                            sub_field, include_name
                        ))
                    })?;

                let rel_schema = schema_override.unwrap_or(inc.related.schema_name.as_str());
                let rel_table = qualified_table(rel_schema, &inc.related.table_name);

                // FK join condition: related.their_key = main.our_key
                let join_cond = match col_qualifier {
                    Some(pfx) => {
                        format!("{} = {}{}", quoted(inc.their_key), pfx, quoted(inc.our_key))
                    }
                    None => format!("{} = {}", quoted(inc.their_key), quoted(inc.our_key)),
                };

                let field_cond = build_leaf_sql(
                    &quoted(sub_field),
                    col_info.pg_type.as_deref(),
                    op,
                    values,
                    q,
                    field,
                    dialect,
                )?;

                return Ok(format!(
                    "EXISTS (SELECT 1 FROM {} WHERE {} AND {})",
                    rel_table, join_cond, field_cond
                ));
            }

            // Plain field: look up in main entity
            let col_info = entity
                .columns
                .iter()
                .find(|c| c.name == *field)
                .ok_or_else(|| AppError::Validation(format!("unknown filter field '{}'", field)))?;

            let qcol = match col_qualifier {
                Some(pfx) => format!("{}{}", pfx, quoted(field)),
                None => quoted(field),
            };

            build_leaf_sql(
                &qcol,
                col_info.pg_type.as_deref(),
                op,
                values,
                q,
                field,
                dialect,
            )
        }
    }
}

/// Build ORDER BY clause from sort specs, falling back to pk ASC when empty.
///
/// A sort field may be a plain column, or a extensible-field key via the `<extensible_col>.<key>`
/// syntax — resolved against the per-tenant `registry` and emitted as a typed JSON extraction.
/// Unknown plain columns are silently skipped (back-compatible); a extensible-field sort that is
/// unknown or not sortable is a hard error.
fn build_order_by(
    sort: &[SortSpec],
    entity: &ResolvedEntity,
    col_qualifier: Option<&str>,
    dialect: &dyn Dialect,
    registry: Option<&ExtensibleRegistry>,
) -> Result<String, AppError> {
    let pk = &entity.pk_columns[0];
    let col_names: std::collections::HashSet<&str> =
        entity.columns.iter().map(|c| c.name.as_str()).collect();

    let dir = |desc: bool| if desc { "DESC" } else { "ASC" };
    let qualify = |name: &str| match col_qualifier {
        Some(pfx) => format!("{}{}", pfx, quoted(name)),
        None => quoted(name),
    };

    let mut parts: Vec<String> = Vec::new();
    for s in sort {
        // Custom-field sort: `<extensible_col>.<key>`.
        if let Some(dot_pos) = s.field.find('.') {
            let head = &s.field[..dot_pos];
            let key = &s.field[dot_pos + 1..];
            if entity.extensible_columns.iter().any(|c| c == head) {
                let def = registry.and_then(|r| r.field(head, key)).ok_or_else(|| {
                    AppError::Validation(format!(
                        "unknown extensible field '{}' in sort (not declared in the registry)",
                        s.field
                    ))
                })?;
                if !def.sortable {
                    return Err(AppError::Validation(format!(
                        "extensible field '{}' is not sortable",
                        s.field
                    )));
                }
                let canonical = def.canonical();
                let json_expr = dialect.json_extract_typed(&qualify(head), key, &canonical);
                parts.push(format!("{} {}", json_expr, dir(s.desc)));
                continue;
            }
            // Not a extensible-fields column: fall through and let the plain-column filter drop it.
        }
        if col_names.contains(s.field.as_str()) {
            parts.push(format!("{} {}", qualify(&s.field), dir(s.desc)));
        }
    }

    if parts.is_empty() {
        Ok(format!(" ORDER BY {}", qualify(pk)))
    } else {
        Ok(format!(" ORDER BY {}", parts.join(", ")))
    }
}

/// SELECT by primary key (single column PK only). Caller adds id as sole param.
pub fn select_by_id(
    entity: &ResolvedEntity,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let cols = select_column_list(entity);
    let ph = pk_placeholder(entity, 1, dialect);
    q.sql = format!(
        "SELECT {} FROM {} WHERE {} = {}",
        cols,
        table,
        quoted(pk),
        ph
    );
    q
}

/// SELECT list with includes in a single query: main table aliased as "main", each include as a scalar subquery (json_agg for to_many, row_to_json for to_one).
/// `includes` drives the scalar subqueries (response data); `filter_includes` is the superset used
/// for EXISTS generation when the filter references dotted fields like `transport_unit.bay`.
#[allow(clippy::too_many_arguments)]
pub fn select_list_with_includes(
    entity: &ResolvedEntity,
    filter: Option<&FilterNode>,
    sort: &[SortSpec],
    limit: Option<u32>,
    offset: Option<u32>,
    includes: &[IncludeSelect<'_>],
    filter_includes: &[IncludeSelect<'_>],
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
    registry: Option<&ExtensibleRegistry>,
) -> Result<QueryBuf, AppError> {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    const MAIN_ALIAS: &str = "main";
    let main_qualifier = format!("{}.", MAIN_ALIAS);

    let main_cols: Vec<String> = entity
        .columns
        .iter()
        .map(|c| {
            let q = quoted(&c.name);
            let pg_type = c.pg_type.as_deref().unwrap_or("");
            let expr = if pg_type.contains('.')
                || pg_type == "numeric"
                || pg_type == "time"
                || pg_type == "timetz"
            {
                format!("{}.{}::text", MAIN_ALIAS, q)
            } else {
                format!("{}.{}", MAIN_ALIAS, q)
            };
            format!("{} AS {}", expr, q)
        })
        .collect();

    let mut select_parts = main_cols;
    for inc in includes {
        let rel_schema = resolve_schema(inc.related, schema_override);
        let rel_table = qualified_table(rel_schema, &inc.related.table_name);
        let sub_from = format!(
            "{} WHERE {} = {}.{}",
            rel_table,
            quoted(inc.their_key),
            MAIN_ALIAS,
            quoted(inc.our_key)
        );
        let rel_col_exprs: Vec<String> = inc
            .related
            .columns
            .iter()
            .map(|c| dialect.quote_ident(&c.name))
            .collect();
        let subquery = match inc.direction {
            IncludeDirection::ToOne => dialect.to_one_subquery(&rel_col_exprs, &sub_from),
            IncludeDirection::ToMany => dialect.to_many_subquery(&rel_col_exprs, &sub_from),
        };
        select_parts.push(format!("{} AS {}", subquery, quoted(inc.name)));
    }

    let where_clause = match filter {
        Some(node) => {
            let frag = rsql_to_sql(
                node,
                entity,
                &mut q,
                Some(&main_qualifier),
                filter_includes,
                schema_override,
                dialect,
                registry,
            )?;
            format!(" WHERE {}", frag)
        }
        None => String::new(),
    };
    let order_clause = build_order_by(sort, entity, Some(&main_qualifier), dialect, registry)?;
    let limit_clause = limit
        .map(|n| format!(" LIMIT {}", n.min(1000)))
        .unwrap_or_default();
    let offset_clause = offset.map(|n| format!(" OFFSET {}", n)).unwrap_or_default();

    q.sql = format!(
        "SELECT {} FROM {} {}{}{}{}{}",
        select_parts.join(", "),
        table,
        MAIN_ALIAS,
        where_clause,
        order_clause,
        limit_clause,
        offset_clause
    );
    Ok(q)
}

/// SELECT list with optional RSQL filter and sort specs.
/// `filter_includes` is needed when the filter contains dotted-field conditions
/// (e.g. `transport_unit.bay=contains=bay23`) that generate EXISTS subqueries.
/// Pass an empty slice when there are no such filters.
#[allow(clippy::too_many_arguments)]
pub fn select_list(
    entity: &ResolvedEntity,
    filter: Option<&FilterNode>,
    sort: &[SortSpec],
    limit: Option<u32>,
    offset: Option<u32>,
    filter_includes: &[IncludeSelect<'_>],
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
    registry: Option<&ExtensibleRegistry>,
) -> Result<QueryBuf, AppError> {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);

    let where_clause = match filter {
        Some(node) => {
            let frag = rsql_to_sql(
                node,
                entity,
                &mut q,
                None,
                filter_includes,
                schema_override,
                dialect,
                registry,
            )?;
            format!(" WHERE {}", frag)
        }
        None => String::new(),
    };
    let order_clause = build_order_by(sort, entity, None, dialect, registry)?;
    let limit_clause = limit
        .map(|n| format!(" LIMIT {}", n.min(1000)))
        .unwrap_or_default();
    let offset_clause = offset.map(|n| format!(" OFFSET {}", n)).unwrap_or_default();
    let cols = select_column_list(entity);
    q.sql = format!(
        "SELECT {} FROM {}{}{}{}{}",
        cols, table, where_clause, order_clause, limit_clause, offset_clause
    );
    Ok(q)
}

/// SELECT * FROM entity WHERE column IN ($1, $2, ...) ORDER BY pk. Used for batch-fetching related rows (to_many or to_one by key).
pub fn select_by_column_in(
    entity: &ResolvedEntity,
    column_name: &str,
    values: &[Value],
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    if values.is_empty() {
        let cols = select_column_list(entity);
        q.sql = format!("SELECT {} FROM {} WHERE 1 = 0", cols, table);
        return q;
    }
    let placeholders: Vec<String> = values
        .iter()
        .map(|v| {
            let n = q.push_param(v.clone());
            entity
                .columns
                .iter()
                .find(|c| c.name == column_name)
                .and_then(|c| c.pg_type.as_deref())
                .map(|t| dialect.cast_expr(&dialect.placeholder(n as usize), t))
                .unwrap_or_else(|| dialect.placeholder(n as usize))
        })
        .collect();
    let cols = select_column_list(entity);
    q.sql = format!(
        "SELECT {} FROM {} WHERE {} IN ({}) ORDER BY {}",
        cols,
        table,
        quoted(column_name),
        placeholders.join(", "),
        quoted(pk)
    );
    q
}

/// INSERT: columns and placeholders from entity; values from body. Excludes PK if has_default.
/// Omits columns with DB default when body does not provide a value (so DB uses default).
/// Uses SQL cast (e.g. $n::timestamptz) for timestamp columns so string values bind correctly.
/// When `rls_tenant_id` is Some, appends tenant_id column and value (for RLS strategy).
pub fn insert(
    entity: &ResolvedEntity,
    body: &HashMap<String, Value>,
    include_pk: bool,
    schema_override: Option<&str>,
    rls_tenant_id: Option<&str>,
    caller_user_id: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let mut cols = Vec::new();
    let mut placeholders = Vec::new();
    for c in &entity.columns {
        let name = &c.name;
        if c.pk_type.is_some() && !include_pk {
            continue;
        }
        // archive_field may only be written via the dedicated archive endpoint, never via POST/create.
        if entity.archive_field.as_deref().is_some_and(|af| name == af) {
            continue;
        }
        // updated_by is only meaningful on updates, leave NULL on insert.
        if name == "updated_by" {
            continue;
        }
        let val = if name == "created_by" {
            caller_user_id
                .map(|uid| Value::String(uid.to_string()))
                .or_else(|| body.get(name).cloned())
        } else {
            body.get(name).cloned()
        };
        if val.is_none() && c.has_default {
            continue;
        }
        let val = val.unwrap_or(Value::Null);
        let val = coerce_json_value_for_pg_array(val, c.pg_type.as_deref());
        let param_num = q.push_param(val);
        let ph = c
            .pg_type
            .as_deref()
            .map(|t| dialect.cast_expr(&dialect.placeholder(param_num as usize), t))
            .unwrap_or_else(|| dialect.placeholder(param_num as usize));
        cols.push(quoted(name));
        placeholders.push(ph);
    }
    if let Some(tid) = rls_tenant_id {
        let param_num = q.push_param(Value::String(tid.to_string()));
        cols.push(quoted("tenant_id"));
        placeholders.push(dialect.placeholder(param_num as usize));
    }
    let col_list = select_column_list(entity);
    let ret = dialect.returning_clause(&col_list);
    let suffix = if ret.is_empty() {
        String::new()
    } else {
        format!(" {}", ret)
    };
    q.sql = format!(
        "INSERT INTO {} ({}) VALUES ({}){}",
        table,
        cols.join(", "),
        placeholders.join(", "),
        suffix
    );
    q
}

/// UPDATE by id: SET only columns present in body (and in entity columns).
/// Uses SQL cast for timestamp columns so string values bind correctly.
pub fn update(
    entity: &ResolvedEntity,
    id: &Value,
    body: &HashMap<String, Value>,
    schema_override: Option<&str>,
    caller_user_id: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let col_by_name: std::collections::HashMap<_, _> = entity
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let mut sets = Vec::new();
    for (k, v) in body {
        if *k == *pk {
            continue;
        }
        if k == "tenant_id" {
            continue;
        }
        // archive_field may only be written via the dedicated archive endpoint, never via PATCH.
        if entity.archive_field.as_deref().is_some_and(|af| k == af) {
            continue;
        }
        let Some(c) = col_by_name.get(k.as_str()) else {
            continue;
        };
        let v = coerce_json_value_for_pg_array(v.clone(), c.pg_type.as_deref());
        let param_num = q.push_param(v);
        let rhs = c
            .pg_type
            .as_deref()
            .map(|t| dialect.cast_expr(&dialect.placeholder(param_num as usize), t))
            .unwrap_or_else(|| dialect.placeholder(param_num as usize));
        sets.push(format!("{} = {}", quoted(k), rhs));
    }
    sets.push(format!("{} = {}", quoted("updated_at"), dialect.now_fn()));
    if let Some(uid) = caller_user_id {
        if entity.columns.iter().any(|c| c.name == "updated_by") {
            let param_num = q.push_param(Value::String(uid.to_string()));
            sets.push(format!(
                "{} = {}",
                quoted("updated_by"),
                dialect.placeholder(param_num as usize)
            ));
        }
    }
    if sets.is_empty() {
        let cols = select_column_list(entity);
        let ph = pk_placeholder(entity, 1, dialect);
        q.sql = format!(
            "SELECT {} FROM {} WHERE {} = {}",
            cols,
            table,
            quoted(pk),
            ph
        );
        q.params.push(id.clone());
        return q;
    }
    let set_clause = sets.join(", ");
    let id_param = q.params.len() + 1;
    q.params.push(id.clone());
    let ph = pk_placeholder(entity, id_param, dialect);
    let col_list = select_column_list(entity);
    let ret = dialect.returning_clause(&col_list);
    let suffix = if ret.is_empty() {
        String::new()
    } else {
        format!(" {}", ret)
    };
    q.sql = format!(
        "UPDATE {} SET {} WHERE {} = {}{}",
        table,
        set_clause,
        quoted(pk),
        ph,
        suffix
    );
    q
}

/// DELETE by id.
pub fn delete(
    entity: &ResolvedEntity,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let ph = pk_placeholder(entity, 1, dialect);
    q.params.push(Value::Null);
    let col_list = select_column_list(entity);
    let ret = dialect.returning_clause(&col_list);
    let suffix = if ret.is_empty() {
        String::new()
    } else {
        format!(" {}", ret)
    };
    q.sql = format!(
        "DELETE FROM {} WHERE {} = {}{}",
        table,
        quoted(pk),
        ph,
        suffix
    );
    q
}

/// UPDATE by id: clear archive_field (set to NULL) where it is currently NOT NULL.
/// Returns the updated row or None (record not found or not archived).
pub fn unarchive(
    entity: &ResolvedEntity,
    archive_field: &str,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let ph = pk_placeholder(entity, 1, dialect);
    q.params.push(Value::Null); // placeholder; caller passes real id via execute_returning_one_with_params_exec
    let col_list = select_column_list(entity);
    let ret = dialect.returning_clause(&col_list);
    let suffix = if ret.is_empty() {
        String::new()
    } else {
        format!(" {}", ret)
    };
    q.sql = format!(
        "UPDATE {} SET {} = NULL WHERE {} = {} AND {} IS NOT NULL{}",
        table,
        quoted(archive_field),
        quoted(pk),
        ph,
        quoted(archive_field),
        suffix
    );
    q
}

// ─── Row Versioning Builders ──────────────────────────────────────────────────

/// INSERT INTO {table}_history: copy the current row from the main table before an update/delete.
/// Uses a single INSERT ... SELECT so the snapshot is atomic and never goes through the app layer.
/// Binds: $1 = operation text ("update" | "delete"), $2 = pk value.
pub fn insert_history_snapshot(
    entity: &ResolvedEntity,
    operation: &str,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let main_table = qualified_table(schema, &entity.table_name);
    let history_table = qualified_table(schema, &format!("{}_history", entity.table_name));
    let pk = &entity.pk_columns[0];

    // $1 = operation, $2 = pk id
    let op_ph = dialect.placeholder(1);
    let pk_ph = pk_placeholder(entity, 2, dialect);

    let col_names: Vec<String> = entity.columns.iter().map(|c| quoted(&c.name)).collect();
    let col_list = col_names.join(", ");

    q.sql = format!(
        "INSERT INTO {history} (\
            \"_version\", \"_operation\", \"_recorded_at\", \"_valid_from\", \"_valid_to\", {cols}\
        ) \
        SELECT \
            COALESCE(\"_version\", 1), {op_ph}, {now}, \"updated_at\", {now}, {cols} \
        FROM {main} \
        WHERE {pk_q} = {pk_ph}",
        history = history_table,
        cols = col_list,
        op_ph = op_ph,
        now = dialect.now_fn(),
        main = main_table,
        pk_q = quoted(pk),
        pk_ph = pk_ph,
    );
    q.params.push(Value::String(operation.to_string()));
    q.params.push(Value::Null); // placeholder; caller replaces with real id
    q
}

/// SELECT all history rows for a given pk, ordered newest first.
/// Binds: $1 = pk value.
pub fn select_history_list(
    entity: &ResolvedEntity,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let history_table = qualified_table(schema, &format!("{}_history", entity.table_name));
    let pk = &entity.pk_columns[0];
    let pk_ph = pk_placeholder(entity, 1, dialect);
    q.sql = format!(
        "SELECT * FROM {} WHERE {} = {} ORDER BY {} DESC",
        history_table,
        quoted(pk),
        pk_ph,
        quoted("_version")
    );
    q.params.push(Value::Null); // placeholder; caller passes real id
    q
}

/// SELECT a specific version from history for a given pk.
/// Binds: $1 = pk value, $2 = version (bigint).
pub fn select_history_by_version(
    entity: &ResolvedEntity,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let history_table = qualified_table(schema, &format!("{}_history", entity.table_name));
    let pk = &entity.pk_columns[0];
    let pk_ph = pk_placeholder(entity, 1, dialect);
    let v_ph = dialect.placeholder(2);
    q.sql = format!(
        "SELECT * FROM {} WHERE {} = {} AND {} = {}",
        history_table,
        quoted(pk),
        pk_ph,
        quoted("_version"),
        v_ph
    );
    q.params.push(Value::Null); // placeholder for pk
    q.params.push(Value::Null); // placeholder for version
    q
}

/// DELETE old history rows beyond keep_versions for a given pk.
/// Binds: $1 = pk value, $2 = keep_versions (bigint).
pub fn prune_history(
    entity: &ResolvedEntity,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let history_table = qualified_table(schema, &format!("{}_history", entity.table_name));
    let pk = &entity.pk_columns[0];
    let pk_ph = pk_placeholder(entity, 1, dialect);
    let keep_ph = dialect.placeholder(2);
    q.sql = format!(
        "DELETE FROM {tbl} WHERE {pk_q} = {pk_ph} \
         AND \"_history_id\" NOT IN (\
             SELECT \"_history_id\" FROM {tbl} WHERE {pk_q} = {pk_ph} \
             ORDER BY \"_version\" DESC LIMIT {keep_ph}\
         )",
        tbl = history_table,
        pk_q = quoted(pk),
        pk_ph = pk_ph,
        keep_ph = keep_ph,
    );
    q.params.push(Value::Null); // pk placeholder
    q.params.push(Value::Null); // keep_versions placeholder
    q
}

// ─── History builder unit tests ───────────────────────────────────────────────

#[cfg(test)]
mod versioning_tests {
    use super::*;
    use crate::config::resolved::{ColumnInfo, PkType, ResolvedEntity};
    use std::collections::{HashMap, HashSet};

    struct PgDialect;
    impl crate::db::Dialect for PgDialect {
        fn name(&self) -> &'static str {
            "postgres"
        }
        fn placeholder(&self, n: usize) -> String {
            format!("${}", n)
        }
        fn quote_ident(&self, s: &str) -> String {
            format!("\"{}\"", s)
        }
        fn ddl_type(&self, _: &crate::db::CanonicalType) -> String {
            "TEXT".into()
        }
        fn cast_name(&self, _: &crate::db::CanonicalType) -> Option<String> {
            None
        }
        fn type_category(&self, _: &crate::db::CanonicalType) -> crate::db::TypeCategory {
            crate::db::TypeCategory::Text
        }
        fn type_support(&self, _: &crate::db::CanonicalType) -> crate::db::TypeSupport {
            crate::db::TypeSupport::Native("text")
        }
        fn cast_expr(&self, expr: &str, _: &str) -> String {
            expr.to_string()
        }
        fn now_fn(&self) -> &'static str {
            "NOW()"
        }
        fn sys_timestamp_type(&self) -> &'static str {
            "TIMESTAMPTZ"
        }
        fn audit_timestamp_type(&self) -> &'static str {
            "TIMESTAMPTZ"
        }
        fn sys_bigserial_type(&self) -> &'static str {
            "BIGSERIAL"
        }
        fn sys_bytes_type(&self) -> &'static str {
            "BYTEA"
        }
        fn sys_json_type(&self) -> &'static str {
            "JSONB"
        }
        fn uuid_default_expr(&self) -> &'static str {
            "gen_random_uuid()"
        }
        fn returning_clause(&self, cols: &str) -> String {
            format!("RETURNING {}", cols)
        }
        fn upsert_conflict(&self, _: &[&str], _: &str) -> String {
            String::new()
        }
        fn to_one_subquery(&self, _col_exprs: &[String], from_clause: &str) -> String {
            format!("(SELECT row_to_json(t) FROM ({}) t)", from_clause)
        }
        fn to_many_subquery(&self, _col_exprs: &[String], from_clause: &str) -> String {
            format!("(SELECT json_agg(t) FROM ({}) t)", from_clause)
        }
        fn supports_schemas(&self) -> bool {
            true
        }
        fn supports_rls(&self) -> bool {
            true
        }
        fn supports_named_enum_types(&self) -> bool {
            true
        }
        fn supports_index_include(&self) -> bool {
            true
        }
        fn set_tenant_session_sql(&self, _: &str) -> Option<String> {
            None
        }
        fn json_extract_text(&self, col: &str, key: &str) -> String {
            format!("({} ->> '{}')", col, key.replace('\'', "''"))
        }
        fn json_extract_typed(
            &self,
            col: &str,
            key: &str,
            _t: &crate::db::CanonicalType,
        ) -> String {
            self.json_extract_text(col, key)
        }
        fn case_insensitive_like(&self, col: &str, placeholder: &str) -> String {
            format!("{} ILIKE {}", col, placeholder)
        }
    }

    fn make_entity() -> ResolvedEntity {
        ResolvedEntity {
            table_id: "t1".into(),
            schema_name: "myschema".into(),
            table_name: "users".into(),
            path_segment: "users".into(),
            pk_columns: vec!["id".into()],
            pk_type: PkType::Uuid,
            columns: vec![
                ColumnInfo {
                    name: "id".into(),
                    pk_type: Some(PkType::Uuid),
                    nullable: false,
                    has_default: true,
                    pg_type: Some("uuid".into()),
                    is_asset: false,
                    asset_is_array: false,
                    asset_config: None,
                },
                ColumnInfo {
                    name: "name".into(),
                    pk_type: None,
                    nullable: true,
                    has_default: false,
                    pg_type: None,
                    is_asset: false,
                    asset_is_array: false,
                    asset_config: None,
                },
                ColumnInfo {
                    name: "updated_at".into(),
                    pk_type: None,
                    nullable: false,
                    has_default: true,
                    pg_type: Some("timestamptz".into()),
                    is_asset: false,
                    asset_is_array: false,
                    asset_config: None,
                },
            ],
            operations: vec![],
            sensitive_columns: HashSet::new(),
            includes: vec![],
            validation: HashMap::new(),
            events: vec![],
            archive_field: None,
            package_id: String::new(),
            audit_log: false,
            global: false,
            parent_ref_column: None,
            versioning: None,
            mcp: None,
            extensible_columns: vec![],
        }
    }

    #[test]
    fn insert_history_snapshot_inserts_into_history_table() {
        let entity = make_entity();
        let d = PgDialect;
        let q = insert_history_snapshot(&entity, "update", None, &d);
        assert!(q.sql.contains("INSERT INTO"));
        assert!(q.sql.contains("_history"));
        assert!(q.sql.contains("_version"));
        assert!(q.sql.contains("_operation"));
        assert!(q.sql.contains("\"name\""));
        assert_eq!(q.params[0], Value::String("update".into()));
    }

    #[test]
    fn insert_history_snapshot_uses_select_not_application_values() {
        let entity = make_entity();
        let d = PgDialect;
        let q = insert_history_snapshot(&entity, "delete", None, &d);
        assert!(q.sql.contains("SELECT"));
        assert!(q.sql.contains("FROM"));
    }

    #[test]
    fn select_history_list_orders_by_version_desc() {
        let entity = make_entity();
        let d = PgDialect;
        let q = select_history_list(&entity, None, &d);
        assert!(q.sql.contains("ORDER BY"));
        assert!(q.sql.contains("_version"));
        assert!(q.sql.contains("DESC"));
        assert_eq!(q.params.len(), 1);
    }

    #[test]
    fn select_history_by_version_has_two_params() {
        let entity = make_entity();
        let d = PgDialect;
        let q = select_history_by_version(&entity, None, &d);
        assert!(q.sql.contains("$1"));
        assert!(q.sql.contains("$2"));
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn prune_history_contains_limit() {
        let entity = make_entity();
        let d = PgDialect;
        let q = prune_history(&entity, None, &d);
        assert!(q.sql.to_uppercase().contains("LIMIT"));
        assert!(q.sql.contains("$2"));
    }

    #[test]
    fn history_table_uses_entity_schema() {
        let entity = make_entity();
        let d = PgDialect;
        let q = select_history_list(&entity, None, &d);
        assert!(q.sql.contains("\"myschema\""));
        assert!(q.sql.contains("\"users_history\""));
    }

    #[test]
    fn schema_override_is_respected() {
        let entity = make_entity();
        let d = PgDialect;
        let q = select_history_list(&entity, Some("tenant1"), &d);
        assert!(q.sql.contains("\"tenant1\""));
        assert!(!q.sql.contains("\"myschema\""));
    }

    #[test]
    fn coerce_array_splits_comma_separated_string() {
        // multipart sends a single field as one comma-separated string.
        let v =
            coerce_json_value_for_pg_array(Value::String("id1, id2".to_string()), Some("uuid[]"));
        assert_eq!(v, Value::String("{\"id1\",\"id2\"}".to_string()));
    }

    #[test]
    fn coerce_array_single_string_is_one_element() {
        let v = coerce_json_value_for_pg_array(Value::String("id1".to_string()), Some("uuid[]"));
        assert_eq!(v, Value::String("{\"id1\"}".to_string()));
    }

    #[test]
    fn coerce_array_drops_empty_segments() {
        let v = coerce_json_value_for_pg_array(
            Value::String("id1, , id2,".to_string()),
            Some("text[]"),
        );
        assert_eq!(v, Value::String("{\"id1\",\"id2\"}".to_string()));
    }

    #[test]
    fn coerce_array_json_array_is_not_split() {
        // JSON clients send a real array; a comma inside an element is preserved.
        let v = coerce_json_value_for_pg_array(
            Value::Array(vec![Value::String("a,b".to_string())]),
            Some("text[]"),
        );
        assert_eq!(v, Value::String("{\"a,b\"}".to_string()));
    }

    #[test]
    fn coerce_array_noop_for_non_array_column() {
        let v = coerce_json_value_for_pg_array(Value::String("id1, id2".to_string()), Some("uuid"));
        assert_eq!(v, Value::String("id1, id2".to_string()));
    }

    #[cfg(feature = "postgres")]
    fn entity_with_pk(pk_type: PkType) -> ResolvedEntity {
        let mut e = make_entity();
        e.pk_type = pk_type;
        e
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn select_by_id_casts_uuid_pk() {
        let d = crate::db::PostgresDialect;
        let q = select_by_id(&entity_with_pk(PkType::Uuid), None, &d);
        assert!(q.sql.contains("\"id\" = $1::uuid"), "got: {}", q.sql);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn select_by_id_casts_bigint_pk() {
        // Auto-number (BIGSERIAL) PKs resolve to PkType::BigInt; bound values arrive as TEXT,
        // so the placeholder must be cast or Postgres errors with `bigint = text`.
        let d = crate::db::PostgresDialect;
        let q = select_by_id(&entity_with_pk(PkType::BigInt), None, &d);
        assert!(q.sql.contains("\"id\" = $1::bigint"), "got: {}", q.sql);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn select_by_id_casts_int_pk() {
        let d = crate::db::PostgresDialect;
        let q = select_by_id(&entity_with_pk(PkType::Int), None, &d);
        assert!(q.sql.contains("\"id\" = $1::integer"), "got: {}", q.sql);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn select_by_id_leaves_text_pk_uncast() {
        let d = crate::db::PostgresDialect;
        let q = select_by_id(&entity_with_pk(PkType::Text), None, &d);
        assert!(q.sql.contains("\"id\" = $1"), "got: {}", q.sql);
        assert!(
            !q.sql.contains("$1::"),
            "text PK should not be cast: {}",
            q.sql
        );
    }

    fn entity_with_bag() -> ResolvedEntity {
        let mut e = make_entity();
        e.extensible_columns = vec!["attributes".into()];
        e
    }

    fn ext_registry() -> ExtensibleRegistry {
        ExtensibleRegistry::from_value(serde_json::json!({
            "attributes": [
                {"key": "warrantyMonths", "type": "int"},
                {"key": "energyRating", "type": "text"},
                {"key": "notes", "type": "text", "filterable": false, "sortable": false}
            ]
        }))
        .unwrap()
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn rsql_filters_and_sorts_on_extensible_field() {
        use crate::sql::rsql::{parse_rsql, parse_sort};
        let d = crate::db::PostgresDialect;
        let e = entity_with_bag();
        let reg = ext_registry();
        let filter = parse_rsql("attributes.warrantyMonths=ge=12").unwrap();
        let sort = parse_sort("-attributes.warrantyMonths");
        let q = select_list(
            &e,
            Some(&filter),
            &sort,
            Some(10),
            Some(0),
            &[],
            None,
            &d,
            Some(&reg),
        )
        .unwrap();
        assert!(
            q.sql
                .contains("(\"attributes\" ->> 'warrantyMonths')::integer >= $1::integer"),
            "got: {}",
            q.sql
        );
        assert!(
            q.sql
                .contains("ORDER BY (\"attributes\" ->> 'warrantyMonths')::integer DESC"),
            "got: {}",
            q.sql
        );
        assert_eq!(q.params.len(), 1);
    }

    #[test]
    fn rsql_text_extensible_field_uses_case_insensitive_like() {
        let d = PgDialect;
        let e = entity_with_bag();
        let reg = ext_registry();
        let filter = crate::sql::rsql::parse_rsql("attributes.energyRating=contains=plus").unwrap();
        let q = select_list(
            &e,
            Some(&filter),
            &[],
            None,
            None,
            &[],
            None,
            &d,
            Some(&reg),
        )
        .unwrap();
        assert!(
            q.sql
                .contains("(\"attributes\" ->> 'energyRating') ILIKE $1"),
            "got: {}",
            q.sql
        );
    }

    #[test]
    fn rsql_unknown_extensible_field_is_rejected() {
        let d = PgDialect;
        let e = entity_with_bag();
        let reg = ext_registry();
        let filter = crate::sql::rsql::parse_rsql("attributes.bogus==1").unwrap();
        let r = select_list(
            &e,
            Some(&filter),
            &[],
            None,
            None,
            &[],
            None,
            &d,
            Some(&reg),
        );
        assert!(r.is_err());
    }

    #[test]
    fn rsql_non_filterable_extensible_field_is_rejected() {
        let d = PgDialect;
        let e = entity_with_bag();
        let reg = ext_registry();
        let filter = crate::sql::rsql::parse_rsql("attributes.notes==hi").unwrap();
        let r = select_list(
            &e,
            Some(&filter),
            &[],
            None,
            None,
            &[],
            None,
            &d,
            Some(&reg),
        );
        assert!(r.is_err());
    }

    #[test]
    fn sort_on_non_sortable_extensible_field_is_rejected() {
        let d = PgDialect;
        let e = entity_with_bag();
        let reg = ext_registry();
        let sort = crate::sql::rsql::parse_sort("attributes.notes");
        let r = select_list(&e, None, &sort, None, None, &[], None, &d, Some(&reg));
        assert!(r.is_err());
    }
}

/// UPDATE by id: stamp archive_field with NOW() where it is currently NULL.
/// Returns the updated row or None (record not found or already archived).
pub fn archive(
    entity: &ResolvedEntity,
    archive_field: &str,
    schema_override: Option<&str>,
    dialect: &dyn Dialect,
) -> QueryBuf {
    let mut q = QueryBuf::new();
    let schema = resolve_schema(entity, schema_override);
    let table = qualified_table(schema, &entity.table_name);
    let pk = &entity.pk_columns[0];
    let ph = pk_placeholder(entity, 1, dialect);
    q.params.push(Value::Null); // placeholder; caller passes real id via execute_returning_one_with_params_exec
    let col_list = select_column_list(entity);
    let ret = dialect.returning_clause(&col_list);
    let suffix = if ret.is_empty() {
        String::new()
    } else {
        format!(" {}", ret)
    };
    q.sql = format!(
        "UPDATE {} SET {} = {} WHERE {} = {} AND {} IS NULL{}",
        table,
        quoted(archive_field),
        dialect.now_fn(),
        quoted(pk),
        ph,
        quoted(archive_field),
        suffix
    );
    q
}
