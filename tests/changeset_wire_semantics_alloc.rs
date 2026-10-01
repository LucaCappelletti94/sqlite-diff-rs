//! Allocation contract tests for changeset wire helpers.

#![cfg(feature = "testing")]

use core::hint::black_box;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::alloc::{GlobalAlloc, Layout, System};

use sqlite_diff_rs::testing::session_changeset_and_patchset_with_setup;
use sqlite_diff_rs::{ChangesetOp, ChangesetUpdatePairExt, ParsedDiffSet, SchemaWithPK};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

// SAFETY: The wrapper preserves System's allocation and deallocation contract.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The original layout is forwarded unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from System.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[test]
fn wire_semantics_helpers_allocate_nothing() {
    let (bytes, _) = session_changeset_and_patchset_with_setup(
        &[
            "CREATE TABLE items (col0 INTEGER NOT NULL, value TEXT, col2 INTEGER NOT NULL, untouched TEXT, PRIMARY KEY(col2, col0))",
            "INSERT INTO items VALUES (10, 'before', 20, 'same')",
        ],
        &["UPDATE items SET value = 'after' WHERE col0 = 10 AND col2 = 20"],
    );
    let ParsedDiffSet::Changeset(changeset) = ParsedDiffSet::try_from(bytes.as_slice()).unwrap()
    else {
        panic!("expected changeset")
    };
    let operation = changeset.iter().next().unwrap();
    let ChangesetOp::Update { table, values, .. } = &operation else {
        panic!("expected update")
    };

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let mut checksum = 0;
    for index in table.primary_key_columns() {
        checksum ^= index;
    }
    for pair in *values {
        checksum ^= usize::from(pair.is_changed());
    }
    for index in operation.changed_column_indices() {
        checksum ^= index;
    }
    black_box(checksum);
    let after = ALLOCATIONS.load(Ordering::Relaxed);

    assert_eq!(after, before);
}
