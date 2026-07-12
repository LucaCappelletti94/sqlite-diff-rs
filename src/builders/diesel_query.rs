//! `diesel` feature: makes patchset operations executable Diesel queries.
//!
//! Identifiers are quoted per backend via [`AstPass::push_identifier`],
//! values travel through [`AstPass::push_bind_param`] with the diesel
//! `SqlType` the target column expects. No `CAST` wrappers are emitted.
//! `Value::Null` renders as the literal keyword `NULL`.
//!
//! # Example: native BOOLEAN bind through an [`Adapter`]
//!
//! The load-bearing case. The source patchset stores the value as
//! `Value::Integer(1)` (SQLite has no boolean type). The target Postgres
//! column is `BOOLEAN`. The adapter maps `("users", column 1, Integer)` to
//! a `BoolBinder` that calls
//! [`push_bind_param::<Bool, bool>`](AstPass::push_bind_param), so the wire
//! carries a native `bool` and no `CAST` appears in the emitted SQL.
//!
//! ```
//! use diesel::debug_query;
//! use diesel::pg::Pg;
//! use diesel::query_builder::AstPass;
//! use diesel::result::QueryResult;
//! use diesel::sql_types::Bool;
//! use sqlite_diff_rs::{
//!     Adapter, Binder, DefaultBinder, DiffOps, Insert, PatchSet, SimpleTable, Value,
//! };
//!
//! struct BoolBinder(bool);
//! impl Binder<Pg> for BoolBinder {
//!     fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
//!         out.push_bind_param::<Bool, bool>(&self.0)
//!     }
//! }
//!
//! struct UsersAdapter;
//! impl<S, B> Adapter<Pg, S, B> for UsersAdapter
//! where
//!     S: AsRef<str> + Sync,
//!     B: AsRef<[u8]> + Sync,
//! {
//!     fn column_name(&self, _table: &str, index: usize) -> &str {
//!         ["id", "active"][index]
//!     }
//!     fn bind<'a>(
//!         &self,
//!         table: &str,
//!         column_index: usize,
//!         value: &'a Value<S, B>,
//!     ) -> Box<dyn Binder<Pg> + Send + 'a> {
//!         match (table, column_index, value) {
//!             ("users", 1, Value::Integer(i)) => Box::new(BoolBinder(*i != 0)),
//!             _ => Box::new(DefaultBinder::from(value)),
//!         }
//!     }
//! }
//!
//! let table = SimpleTable::new("users", &["id", "active"], &[0]);
//! let patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(
//!     Insert::from(table.clone())
//!         .set(0, 1_i64)
//!         .unwrap()
//!         .set(1, 1_i64)
//!         .unwrap(),
//! );
//!
//! let op = patchset
//!     .iter()
//!     .next()
//!     .unwrap()
//!     .with_adapter::<Pg, _>(&UsersAdapter);
//! let sql = debug_query::<Pg, _>(&op).to_string();
//! assert!(sql.starts_with(r#"INSERT INTO "users" ("id", "active") VALUES ($1, $2)"#));
//! assert!(!sql.contains("CAST")); // native bind, no cast wrapper
//! assert!(sql.contains("true")); // bind rendered via bool's Debug
//! ```

use alloc::boxed::Box;
use alloc::vec::Vec;

use diesel::backend::Backend;
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::query_dsl::RunQueryDsl;
use diesel::result::{Error as DieselError, QueryResult};
use diesel::serialize::ToSql;
use diesel::sql_types::{BigInt, Binary, Double, HasSqlType, Text};

use super::sql_output::ColumnNames;
use super::view::{PatchsetOp, PatchsetUpdateEntry};
use crate::SchemaWithPK;
use crate::encoding::Value;

/// Reasons a `PatchsetOp` cannot render into valid SQL. Wrapped in
/// [`DieselError::QueryBuilderError`].
#[derive(Debug, thiserror::Error)]
enum RenderError {
    /// `ColumnNames::column_name(index)` returned `None`.
    #[error("missing column name for index {column_index}")]
    MissingColumnName { column_index: usize },
    /// UPDATE whose non-PK entries are all `None`; `SET` would be empty.
    #[error("patchset UPDATE has an empty SET clause")]
    EmptyUpdateSet,
    /// UPDATE/DELETE against a table with no primary-key columns.
    #[error("patchset UPDATE/DELETE targets a table with no primary key")]
    EmptyRowPredicate,
}

#[inline]
fn render_err(err: RenderError) -> DieselError {
    DieselError::QueryBuilderError(Box::new(err))
}

#[inline]
fn column_name<T: ColumnNames>(table: &T, index: usize) -> QueryResult<&str> {
    table.column_name(index).ok_or_else(|| {
        render_err(RenderError::MissingColumnName {
            column_index: index,
        })
    })
}

fn push_value<'b, S, B, DB>(
    out: &mut AstPass<'_, 'b, DB>,
    value: &'b Value<S, B>,
) -> QueryResult<()>
where
    S: AsRef<str>,
    B: AsRef<[u8]>,
    DB: Backend + HasSqlType<BigInt> + HasSqlType<Double> + HasSqlType<Text> + HasSqlType<Binary>,
    i64: ToSql<BigInt, DB>,
    f64: ToSql<Double, DB>,
    str: ToSql<Text, DB>,
    [u8]: ToSql<Binary, DB>,
{
    match value {
        Value::Null => {
            out.push_sql("NULL");
            Ok(())
        }
        Value::Integer(i) => out.push_bind_param::<BigInt, i64>(i),
        Value::Real(f) => out.push_bind_param::<Double, f64>(f),
        Value::Text(s) => out.push_bind_param::<Text, str>(s.as_ref()),
        Value::Blob(b) => out.push_bind_param::<Binary, [u8]>(b.as_ref()),
    }
}

fn walk_insert<'b, T, S, B, DB>(
    table: &T,
    values: &'b [Value<S, B>],
    out: &mut AstPass<'_, 'b, DB>,
) -> QueryResult<()>
where
    T: ColumnNames,
    S: AsRef<str>,
    B: AsRef<[u8]>,
    DB: Backend + HasSqlType<BigInt> + HasSqlType<Double> + HasSqlType<Text> + HasSqlType<Binary>,
    i64: ToSql<BigInt, DB>,
    f64: ToSql<Double, DB>,
    str: ToSql<Text, DB>,
    [u8]: ToSql<Binary, DB>,
{
    out.push_sql("INSERT INTO ");
    out.push_identifier(table.name())?;
    out.push_sql(" (");
    for index in 0..table.number_of_columns() {
        if index > 0 {
            out.push_sql(", ");
        }
        out.push_identifier(column_name(table, index)?)?;
    }
    out.push_sql(") VALUES (");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push_sql(", ");
        }
        push_value(out, value)?;
    }
    out.push_sql(")");
    Ok(())
}

fn walk_update<'b, T, S, B, DB>(
    table: &T,
    pk: &'b [Value<S, B>],
    entries: &'b [PatchsetUpdateEntry<S, B>],
    out: &mut AstPass<'_, 'b, DB>,
) -> QueryResult<()>
where
    T: ColumnNames,
    S: AsRef<str>,
    B: AsRef<[u8]>,
    DB: Backend + HasSqlType<BigInt> + HasSqlType<Double> + HasSqlType<Text> + HasSqlType<Binary>,
    i64: ToSql<BigInt, DB>,
    f64: ToSql<Double, DB>,
    str: ToSql<Text, DB>,
    [u8]: ToSql<Binary, DB>,
{
    out.push_sql("UPDATE ");
    out.push_identifier(table.name())?;
    out.push_sql(" SET ");

    let mut first_set = true;
    for (col_idx, ((), new)) in entries.iter().enumerate() {
        let Some(new_value) = new.as_ref() else {
            continue;
        };
        if table.primary_key_index(col_idx).is_some() {
            continue;
        }
        if !first_set {
            out.push_sql(", ");
        }
        first_set = false;
        out.push_identifier(column_name(table, col_idx)?)?;
        out.push_sql(" = ");
        push_value(out, new_value)?;
    }
    if first_set {
        return Err(render_err(RenderError::EmptyUpdateSet));
    }

    out.push_sql(" WHERE ");
    walk_pk_predicate(table, pk, out)
}

fn walk_delete<'b, T, S, B, DB>(
    table: &T,
    pk: &'b [Value<S, B>],
    out: &mut AstPass<'_, 'b, DB>,
) -> QueryResult<()>
where
    T: ColumnNames,
    S: AsRef<str>,
    B: AsRef<[u8]>,
    DB: Backend + HasSqlType<BigInt> + HasSqlType<Double> + HasSqlType<Text> + HasSqlType<Binary>,
    i64: ToSql<BigInt, DB>,
    f64: ToSql<Double, DB>,
    str: ToSql<Text, DB>,
    [u8]: ToSql<Binary, DB>,
{
    out.push_sql("DELETE FROM ");
    out.push_identifier(table.name())?;
    out.push_sql(" WHERE ");
    walk_pk_predicate(table, pk, out)
}

fn walk_pk_predicate<'b, T, S, B, DB>(
    table: &T,
    pk: &'b [Value<S, B>],
    out: &mut AstPass<'_, 'b, DB>,
) -> QueryResult<()>
where
    T: ColumnNames,
    S: AsRef<str>,
    B: AsRef<[u8]>,
    DB: Backend + HasSqlType<BigInt> + HasSqlType<Double> + HasSqlType<Text> + HasSqlType<Binary>,
    i64: ToSql<BigInt, DB>,
    f64: ToSql<Double, DB>,
    str: ToSql<Text, DB>,
    [u8]: ToSql<Binary, DB>,
{
    let pk_indices = table.pk_indices();
    if pk_indices.is_empty() {
        return Err(render_err(RenderError::EmptyRowPredicate));
    }
    for (pk_ordinal, &col_idx) in pk_indices.iter().enumerate() {
        if pk_ordinal > 0 {
            out.push_sql(" AND ");
        }
        out.push_identifier(column_name(table, col_idx)?)?;
        out.push_sql(" = ");
        push_value(out, &pk[pk_ordinal])?;
    }
    Ok(())
}

impl<T, S, B, DB> QueryFragment<DB> for PatchsetOp<'_, T, S, B>
where
    T: ColumnNames,
    S: AsRef<str>,
    B: AsRef<[u8]>,
    DB: Backend + HasSqlType<BigInt> + HasSqlType<Double> + HasSqlType<Text> + HasSqlType<Binary>,
    i64: ToSql<BigInt, DB>,
    f64: ToSql<Double, DB>,
    str: ToSql<Text, DB>,
    [u8]: ToSql<Binary, DB>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        // Identifiers are chosen at runtime, so the prepared-statement cache
        // must not retain this query.
        out.unsafe_to_cache_prepared();

        match *self {
            PatchsetOp::Insert { table, values, .. } => walk_insert(table, values, &mut out),
            PatchsetOp::Update {
                table, pk, entries, ..
            } => walk_update(table, pk, entries, &mut out),
            PatchsetOp::Delete { table, pk, .. } => walk_delete(table, pk, &mut out),
        }
    }
}

impl<T, S, B> QueryId for PatchsetOp<'_, T, S, B> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

// `RunQueryDsl<Conn>` has a blanket impl only for `T: Table`, and `PatchsetOp`
// is not a Table. We add an unconditional impl the same way `SqlQuery` and
// `BoxedSqlQuery` do; the trait's own methods carry their bounds.
impl<T, S, B, Conn> RunQueryDsl<Conn> for PatchsetOp<'_, T, S, B> {}

// ============================================================================
// Adapter / Binder: user-driven per-column native binding, no CAST wrappers.
// ============================================================================

/// Writes one column value onto the query AST.
///
/// Owns any parsed representation the target column needs (`bool`,
/// `uuid::Uuid`, ...) so the value survives [`AstPass::push_bind_param`]'s
/// bind-lifetime requirement.
pub trait Binder<DB: Backend> {
    /// Push the column's value onto the AST. Typically calls
    /// [`AstPass::push_bind_param`] with the diesel `SqlType` matching the
    /// target column, or emits a literal via [`AstPass::push_sql`].
    ///
    /// # Errors
    ///
    /// Propagates any [`DieselError`](diesel::result::Error) from
    /// [`AstPass`].
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, DB>) -> QueryResult<()>;
}

/// The naive binder used when nothing custom is required for a column.
///
/// Binds `Integer -> BigInt`, `Real -> Double`, `Text -> Text`,
/// `Blob -> Binary`; pushes literal `NULL` for `Value::Null`.
pub struct DefaultBinder<'a, S, B> {
    value: &'a Value<S, B>,
}

impl<'a, S, B> DefaultBinder<'a, S, B> {
    /// Construct a default binder that reads through to `value`.
    #[must_use]
    pub fn new(value: &'a Value<S, B>) -> Self {
        Self { value }
    }
}

impl<'a, S, B> From<&'a Value<S, B>> for DefaultBinder<'a, S, B> {
    fn from(value: &'a Value<S, B>) -> Self {
        Self { value }
    }
}

impl<S, B, DB> Binder<DB> for DefaultBinder<'_, S, B>
where
    S: AsRef<str>,
    B: AsRef<[u8]>,
    DB: Backend + HasSqlType<BigInt> + HasSqlType<Double> + HasSqlType<Text> + HasSqlType<Binary>,
    i64: ToSql<BigInt, DB>,
    f64: ToSql<Double, DB>,
    str: ToSql<Text, DB>,
    [u8]: ToSql<Binary, DB>,
{
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, DB>) -> QueryResult<()> {
        push_value(out, self.value)
    }
}

/// Downstream-implemented source of truth for column names and native
/// bindings.
///
/// One adapter per schema (set of tables); it dispatches internally on
/// `table_name`. The wire format carries table names and column positions
/// but not names or types, so the adapter fills that in.
///
/// Object-safe.
pub trait Adapter<DB, S, B> {
    /// Column identifier emitted via `push_identifier` for
    /// `(table_name, column_index)`.
    fn column_name(&self, table_name: &str, column_index: usize) -> &str;

    /// Binder for `(table_name, column_index, value)`. Match on
    /// `(table_name, column_index)`, parse `value` into any owned
    /// representation the target column needs, and box it. Fall through to
    /// [`DefaultBinder`] for columns that need no custom handling.
    fn bind<'a>(
        &self,
        table_name: &str,
        column_index: usize,
        value: &'a Value<S, B>,
    ) -> Box<dyn Binder<DB> + Send + 'a>;
}

/// Executable Diesel query built from one `PatchsetOp` + [`Adapter`].
///
/// Constructed via [`PatchsetOp::with_adapter`]. Implements
/// [`QueryFragment`], [`QueryId`], and [`RunQueryDsl`].
pub struct BoundPatchsetOp<'a, T, S, B, DB, A>
where
    DB: Backend,
{
    op: PatchsetOp<'a, T, S, B>,
    binders: Vec<Box<dyn Binder<DB> + Send + 'a>>,
    adapter: &'a A,
}

impl<'a, T, S, B, DB, A> BoundPatchsetOp<'a, T, S, B, DB, A>
where
    T: SchemaWithPK,
    S: AsRef<str>,
    B: AsRef<[u8]>,
    DB: Backend,
    A: Adapter<DB, S, B> + Send + Sync,
{
    /// Resolve every column of `op` through `adapter` up front.
    ///
    /// Order of the collected binders exactly matches the order in which
    /// [`Self::walk_ast`] emits placeholders, so the two iterate in lockstep
    /// at execute time.
    fn resolve(op: PatchsetOp<'a, T, S, B>, adapter: &'a A) -> Self {
        let table = op.table();
        let table_name = table.name();
        let mut binders: Vec<Box<dyn Binder<DB> + Send + 'a>> = Vec::new();

        match op {
            PatchsetOp::Insert { values, .. } => {
                for (col_idx, value) in values.iter().enumerate() {
                    binders.push(adapter.bind(table_name, col_idx, value));
                }
            }
            PatchsetOp::Update { pk, entries, .. } => {
                // SET clause: non-PK columns with a new value, in column order.
                for (col_idx, ((), new)) in entries.iter().enumerate() {
                    if let Some(new_val) = new
                        && table.primary_key_index(col_idx).is_none()
                    {
                        binders.push(adapter.bind(table_name, col_idx, new_val));
                    }
                }
                // WHERE clause: PK columns, in PK-ordinal order.
                for (pk_ordinal, col_idx) in pk_indices(table).into_iter().enumerate() {
                    binders.push(adapter.bind(table_name, col_idx, &pk[pk_ordinal]));
                }
            }
            PatchsetOp::Delete { pk, .. } => {
                for (pk_ordinal, col_idx) in pk_indices(table).into_iter().enumerate() {
                    binders.push(adapter.bind(table_name, col_idx, &pk[pk_ordinal]));
                }
            }
        }

        Self {
            op,
            binders,
            adapter,
        }
    }
}

/// Derive PK-ordinal-ordered column indices from a [`SchemaWithPK`].
///
/// Mirrors the default `ColumnNames::pk_indices` behavior, but works with
/// any schema (including `TableSchema<String>` parsed from binary, which
/// does not implement `ColumnNames`).
fn pk_indices<T: SchemaWithPK>(table: &T) -> Vec<usize> {
    let mut pk_cols: Vec<(usize, usize)> = Vec::new();
    for col_idx in 0..table.number_of_columns() {
        if let Some(pk_ordinal) = table.primary_key_index(col_idx) {
            pk_cols.push((pk_ordinal, col_idx));
        }
    }
    pk_cols.sort_by_key(|(ordinal, _)| *ordinal);
    pk_cols.into_iter().map(|(_, col_idx)| col_idx).collect()
}

impl<T, S, B, DB, A> QueryFragment<DB> for BoundPatchsetOp<'_, T, S, B, DB, A>
where
    T: SchemaWithPK,
    S: AsRef<str>,
    B: AsRef<[u8]>,
    DB: Backend,
    A: Adapter<DB, S, B> + Send + Sync,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        let mut binders = self.binders.iter();
        let bind_next = |binders: &mut core::slice::Iter<'b, Box<dyn Binder<DB> + Send + '_>>,
                         out: &mut AstPass<'_, 'b, DB>|
         -> QueryResult<()> {
            let binder = binders.next().ok_or_else(|| {
                DieselError::QueryBuilderError(alloc::boxed::Box::new(BinderResolutionError))
            })?;
            binder.walk(out)
        };

        let adapter = self.adapter;

        match self.op {
            PatchsetOp::Insert { table, .. } => {
                let table_name = table.name();
                out.push_sql("INSERT INTO ");
                out.push_identifier(table_name)?;
                out.push_sql(" (");
                let column_count = table.number_of_columns();
                for index in 0..column_count {
                    if index > 0 {
                        out.push_sql(", ");
                    }
                    out.push_identifier(adapter.column_name(table_name, index))?;
                }
                out.push_sql(") VALUES (");
                for index in 0..column_count {
                    if index > 0 {
                        out.push_sql(", ");
                    }
                    bind_next(&mut binders, &mut out)?;
                }
                out.push_sql(")");
                Ok(())
            }
            PatchsetOp::Update {
                table, pk, entries, ..
            } => {
                let table_name = table.name();
                out.push_sql("UPDATE ");
                out.push_identifier(table_name)?;
                out.push_sql(" SET ");

                let mut first_set = true;
                for (col_idx, ((), new)) in entries.iter().enumerate() {
                    if new.is_none() || table.primary_key_index(col_idx).is_some() {
                        continue;
                    }
                    if !first_set {
                        out.push_sql(", ");
                    }
                    first_set = false;
                    out.push_identifier(adapter.column_name(table_name, col_idx))?;
                    out.push_sql(" = ");
                    bind_next(&mut binders, &mut out)?;
                }
                if first_set {
                    return Err(render_err(RenderError::EmptyUpdateSet));
                }

                out.push_sql(" WHERE ");
                let pk_col_indices = pk_indices(table);
                if pk_col_indices.is_empty() {
                    return Err(render_err(RenderError::EmptyRowPredicate));
                }
                for (pk_ordinal, col_idx) in pk_col_indices.iter().copied().enumerate() {
                    if pk_ordinal > 0 {
                        out.push_sql(" AND ");
                    }
                    out.push_identifier(adapter.column_name(table_name, col_idx))?;
                    out.push_sql(" = ");
                    bind_next(&mut binders, &mut out)?;
                }
                // Silence unused for the pk slice; the actual PK values live
                // inside the pre-resolved binders.
                let _ = pk;
                Ok(())
            }
            PatchsetOp::Delete { table, pk, .. } => {
                let table_name = table.name();
                out.push_sql("DELETE FROM ");
                out.push_identifier(table_name)?;
                out.push_sql(" WHERE ");
                let pk_col_indices = pk_indices(table);
                if pk_col_indices.is_empty() {
                    return Err(render_err(RenderError::EmptyRowPredicate));
                }
                for (pk_ordinal, col_idx) in pk_col_indices.iter().copied().enumerate() {
                    if pk_ordinal > 0 {
                        out.push_sql(" AND ");
                    }
                    out.push_identifier(adapter.column_name(table_name, col_idx))?;
                    out.push_sql(" = ");
                    bind_next(&mut binders, &mut out)?;
                }
                let _ = pk;
                Ok(())
            }
        }
    }
}

impl<T, S, B, DB, A> QueryId for BoundPatchsetOp<'_, T, S, B, DB, A>
where
    DB: Backend,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<T, S, B, DB, A, Conn> RunQueryDsl<Conn> for BoundPatchsetOp<'_, T, S, B, DB, A> where
    DB: Backend
{
}

/// Internal error: binder count out of sync between resolve and walk_ast.
/// Only reachable if the two disagree about column bookkeeping, which is a
/// bug in this crate.
#[derive(Debug, thiserror::Error)]
#[error("internal: binder count mismatch between BoundPatchsetOp resolve and walk_ast")]
struct BinderResolutionError;

// ============================================================================
// Entry point on PatchsetOp
// ============================================================================

impl<'a, T, S, B> PatchsetOp<'a, T, S, B>
where
    T: SchemaWithPK,
    S: AsRef<str>,
    B: AsRef<[u8]>,
{
    /// Build an executable [`BoundPatchsetOp`] from this op and `adapter`.
    #[must_use]
    pub fn with_adapter<DB, A>(self, adapter: &'a A) -> BoundPatchsetOp<'a, T, S, B, DB, A>
    where
        A: Adapter<DB, S, B> + Send + Sync,
        DB: Backend,
    {
        BoundPatchsetOp::resolve(self, adapter)
    }
}

// ============================================================================
// ApplyOps: batch execute a patchset iterator without hand-rolled ceremony.
// ============================================================================

/// Extension trait: run every op in an iterator against a Diesel connection.
///
/// Works uniformly on the naive path (`patchset.iter()`) and the adapter
/// path (`patchset.iter().map(|op| op.with_adapter::<DB, _>(&adapter))`).
///
/// ```
/// use sqlite_diff_rs::ApplyOps;
/// // With `ApplyOps` in scope, on any iterator of Diesel-executable ops:
/// //
/// //   patchset.iter().apply_transactional(&mut conn)?;              // naive path
/// //   patchset.iter()                                                // adapter path
/// //       .map(|op| op.with_adapter::<Pg, _>(&adapter))
/// //       .apply_transactional(&mut conn)?;
/// ```
pub trait ApplyOps: Iterator + Sized {
    /// Execute every op in sequence, returning the summed affected-row
    /// count. No transaction; a mid-op failure leaves prior ops committed.
    ///
    /// # Errors
    ///
    /// Returns the first [`DieselError`](diesel::result::Error) any op
    /// produces; remaining ops are not run.
    fn apply<Conn>(self, conn: &mut Conn) -> QueryResult<usize>
    where
        Conn: diesel::Connection,
        Self::Item: QueryFragment<Conn::Backend> + QueryId + RunQueryDsl<Conn>;

    /// Same as [`Self::apply`] wrapped in [`Connection::transaction`], so a
    /// mid-op failure rolls the whole batch back.
    ///
    /// # Errors
    ///
    /// Returns the first [`DieselError`](diesel::result::Error) any op
    /// produces, after rollback.
    fn apply_transactional<Conn>(self, conn: &mut Conn) -> QueryResult<usize>
    where
        Conn: diesel::Connection,
        Self::Item: QueryFragment<Conn::Backend> + QueryId + RunQueryDsl<Conn>;
}

impl<I> ApplyOps for I
where
    I: Iterator + Sized,
{
    fn apply<Conn>(self, conn: &mut Conn) -> QueryResult<usize>
    where
        Conn: diesel::Connection,
        Self::Item: QueryFragment<Conn::Backend> + QueryId + RunQueryDsl<Conn>,
    {
        let mut total = 0_usize;
        for op in self {
            total = total.saturating_add(op.execute(conn)?);
        }
        Ok(total)
    }

    fn apply_transactional<Conn>(self, conn: &mut Conn) -> QueryResult<usize>
    where
        Conn: diesel::Connection,
        Self::Item: QueryFragment<Conn::Backend> + QueryId + RunQueryDsl<Conn>,
    {
        conn.transaction(|conn| self.apply(conn))
    }
}
