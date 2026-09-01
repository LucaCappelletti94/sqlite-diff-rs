//! Shared schema fixtures for CDC digestion integration tests.
//!
//! Provides a three-column `users` table (`id` INT PK, `name` TEXT, `active`
//! BOOL) with all required trait impls, reused by `digestion_pg_walstream`,
//! `digestion_wal2json`, and `digestion_maxwell`.

#![allow(dead_code, unused_imports)]

extern crate alloc;

use alloc::vec::Vec;

use sqlite_diff_rs::{
    DynTable, IndexableValues, NamedColumns, SchemaWithPK, SimpleTable, Value, WireColumnTypes,
    WireSchema, WireType,
};

/// A single-table schema holding the `users` test table.
#[derive(Debug, Clone)]
pub struct TestSchema {
    pub users: TestUsersTable,
}

#[derive(Debug, Clone)]
pub struct SourceScopedTestSchema {
    pub users: TestUsersTable,
    source_schema: &'static str,
}

/// The `users` table: `id INT PK`, `name TEXT`, `active BOOL`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TestUsersTable(pub SimpleTable);

impl DynTable for TestUsersTable {
    fn name(&self) -> &str {
        self.0.name()
    }
    fn number_of_columns(&self) -> usize {
        self.0.number_of_columns()
    }
    fn write_pk_flags(&self, buf: &mut [u8]) {
        self.0.write_pk_flags(buf);
    }
}

impl SchemaWithPK for TestUsersTable {
    fn extract_pk<S: Clone, B: Clone>(
        &self,
        values: &impl IndexableValues<Text = S, Binary = B>,
    ) -> Vec<Value<S, B>> {
        self.0.extract_pk(values)
    }
    fn number_of_primary_keys(&self) -> usize {
        self.0.number_of_primary_keys()
    }
    fn primary_key_index(&self, col: usize) -> Option<usize> {
        self.0.primary_key_index(col)
    }
}

impl NamedColumns for TestUsersTable {
    fn column_index(&self, name: &str) -> Option<usize> {
        self.0.column_index(name)
    }
}

impl WireColumnTypes for TestUsersTable {
    fn column_type(&self, column_index: usize) -> WireType {
        // id -> Int, name -> Text, active -> Bool
        match column_index {
            0 => WireType::Int,
            1 => WireType::Text,
            2 => WireType::Bool,
            _ => panic!("column {column_index} out of range"),
        }
    }
}

impl WireSchema for TestSchema {
    type Table = TestUsersTable;
    fn get(&self, _source_schema: Option<&str>, table_name: &str) -> Option<&Self::Table> {
        if table_name == "users" {
            Some(&self.users)
        } else {
            None
        }
    }
}

impl WireSchema for SourceScopedTestSchema {
    type Table = TestUsersTable;

    fn get(&self, source_schema: Option<&str>, table_name: &str) -> Option<&Self::Table> {
        (source_schema == Some(self.source_schema) && table_name == "users").then_some(&self.users)
    }
}

/// Constructs the canonical three-column `users` test schema.
pub fn test_schema() -> TestSchema {
    TestSchema {
        users: TestUsersTable(SimpleTable::new("users", &["id", "name", "active"], &[0])),
    }
}

pub fn source_scoped_test_schema(source_schema: &'static str) -> SourceScopedTestSchema {
    SourceScopedTestSchema {
        users: TestUsersTable(SimpleTable::new("users", &["id", "name", "active"], &[0])),
        source_schema,
    }
}
