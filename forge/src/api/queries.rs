//! Saved queries: create, read, delete, and run one through LENS.
//!
//! # `POST /queries`
//!
//! ```json
//! {"name": "transfers-by-mint",
//!  "sql": "SELECT tx_signature, amount_raw FROM solana.token_transfers WHERE mint_address = {{mint}} ORDER BY slot DESC LIMIT {{n}}",
//!  "params": [{"name": "mint", "kind": "string", "required": true},
//!             {"name": "n", "kind": "u64", "required": false, "default": "10"}]}
//! ```
//!
//! Checked with the registry's own validation ([`NewSavedQuery::validate`]):
//! the name, the SQL template, and the parameters against its placeholders.
//! LENS is not asked: without values for the parameters there is nothing to
//! run. A `default` is a string, parsed like a query-string value. `201`
//! with a `Location` header and the stored [`QueryView`]; a taken name is
//! `409`.
//!
//! # `GET /q/{name}?param=value`
//!
//! 1. The query string is decoded strictly into pairs; a repeated key is a
//!    `400` ([`super::parse_query_string`]).
//! 2. The saved query is loaded (`404` if there is none).
//! 3. [`SavedQuery::bind`] renders the SQL: an unknown key, a missing
//!    required parameter, or a value that does not parse as its kind is a
//!    `400 validation_error` naming the parameter.
//! 4. LENS runs it. Its result comes back unchanged, plus the query's name:
//!
//! ```json
//! {"query": "transfers-by-mint", "rows": [{"tx_signature": "5x…", "amount_raw": 1000000}],
//!  "row_count": 1, "duration_ms": 12, "truncated": false, "sql": "SELECT …"}
//! ```
//!
//! A LENS failure has the status in the [API docs](super#errors), with the
//! bound SQL in `sql`.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{ApiError, ApiJson, ApiPath, AppState, blocking, parse_query_string};
use crate::registry::{NewSavedQuery, ParamKind, ParamSpec, SavedQuery};

/// The body of `POST /queries`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateQueryRequest {
    pub name: String,
    pub sql: String,
    #[serde(default)]
    pub params: Vec<ParamRequest>,
}

/// One parameter in a [`CreateQueryRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParamRequest {
    pub name: String,
    pub kind: ParamKind,
    pub required: bool,
    /// Set exactly when `required` is false.
    #[serde(default)]
    pub default: Option<String>,
}

impl From<CreateQueryRequest> for NewSavedQuery {
    fn from(request: CreateQueryRequest) -> Self {
        Self {
            name: request.name,
            sql: request.sql,
            params: request
                .params
                .into_iter()
                .map(|param| ParamSpec {
                    name: param.name,
                    kind: param.kind,
                    required: param.required,
                    default: param.default,
                })
                .collect(),
        }
    }
}

/// A saved query, as every route returns it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryView {
    pub name: String,
    pub sql: String,
    pub params: Vec<ParamSpec>,
    /// Unix seconds.
    pub created_at: i64,
    /// Unix seconds.
    pub updated_at: i64,
}

impl From<SavedQuery> for QueryView {
    fn from(query: SavedQuery) -> Self {
        Self {
            name: query.name,
            sql: query.sql,
            params: query.params,
            created_at: query.created_at,
            updated_at: query.updated_at,
        }
    }
}

/// The body of `GET /queries`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryList {
    /// By name.
    pub queries: Vec<QueryView>,
}

/// The body of a successful `GET /q/{name}`: LENS's result and the name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResultEnvelope {
    pub query: String,
    pub rows: Vec<Map<String, Value>>,
    pub row_count: u64,
    pub duration_ms: u64,
    pub truncated: bool,
    /// The bound SQL, as LENS ran it.
    pub sql: String,
}

/// Handler for `POST /queries`.
pub async fn create_query(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<CreateQueryRequest>,
) -> Result<Response, ApiError> {
    let new = NewSavedQuery::from(request);
    new.validate().map_err(|err| ApiError::from_registry(&err))?;
    let registry = state.registry.clone();
    let query = blocking(move || registry.create_query(new)).await?;
    tracing::info!(query = %query.name, params = query.params.len(), "saved query created");
    let location = format!("/queries/{}", query.name);
    Ok((StatusCode::CREATED, [(header::LOCATION, location)], Json(QueryView::from(query))).into_response())
}

/// Handler for `GET /queries`.
pub async fn list_queries(State(state): State<AppState>) -> Result<Json<QueryList>, ApiError> {
    let registry = state.registry.clone();
    let queries = blocking(move || registry.list_queries()).await?;
    Ok(Json(QueryList { queries: queries.into_iter().map(QueryView::from).collect() }))
}

/// Handler for `GET /queries/{name}`.
pub async fn get_query(State(state): State<AppState>, ApiPath(name): ApiPath<String>) -> Result<Json<QueryView>, ApiError> {
    let registry = state.registry.clone();
    let query = blocking(move || registry.get_query(&name)).await?;
    Ok(Json(query.into()))
}

/// Handler for `DELETE /queries/{name}`.
pub async fn delete_query(State(state): State<AppState>, ApiPath(name): ApiPath<String>) -> Result<StatusCode, ApiError> {
    let registry = state.registry.clone();
    let deleted = name.clone();
    blocking(move || registry.delete_query(&deleted)).await?;
    tracing::info!(query = %name, "saved query deleted");
    Ok(StatusCode::NO_CONTENT)
}

/// Handler for `GET /q/{name}`.
pub async fn run_query(
    State(state): State<AppState>,
    ApiPath(name): ApiPath<String>,
    RawQuery(raw): RawQuery,
) -> Result<Json<QueryResultEnvelope>, ApiError> {
    let supplied: HashMap<String, String> = parse_query_string(raw.as_deref())?.into_iter().collect();
    let registry = state.registry.clone();
    let query = blocking(move || registry.get_query(&name)).await?;
    let sql = query.bind(&supplied).map_err(|err| ApiError::from_registry(&err))?;
    let response = state.lens.query(&sql).await.map_err(|err| ApiError::from_lens(&err, &sql))?;
    tracing::debug!(query = %query.name, rows = response.row_count, round_trip_ms = response.round_trip_ms, "saved query ran");
    Ok(Json(QueryResultEnvelope {
        query: query.name,
        rows: response.rows,
        row_count: response.row_count,
        duration_ms: response.duration_ms,
        truncated: response.truncated,
        sql: response.sql,
    }))
}
