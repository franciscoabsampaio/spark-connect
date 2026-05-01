//! End-to-end tests: annotate some dummy items, then iterate the inventory
//! and assert the expected entries are registered with correct metadata.

use api_parity_core::{inventory, api_parity, api_parity_impl, ParityEntry, Status};

struct Dummy;

#[api_parity_impl]
impl Dummy {
    #[api_parity(
        reference = "Dummy.foo",
        status = Implemented,
        since = "3.4",
    )]
    #[allow(dead_code)]
    fn foo(&self) {}

    #[api_parity(
        reference = "Dummy.bar",
        status = Partial,
        comment = "only handles the happy path",
        issue = 7,
    )]
    #[allow(dead_code)]
    fn bar(&self) {}
}

#[api_parity(
    reference = "module.free_fn",
    status = Implemented,
)]
#[allow(dead_code)]
fn free_fn() {}

fn find(reference: &str) -> Option<&'static ParityEntry> {
    inventory::iter::<ParityEntry>.into_iter().find(|e| e.reference == reference)
}

#[test]
fn impl_method_is_registered_with_type_prefix() {
    let entry = find("Dummy.foo").expect("Dummy.foo not registered");
    assert_eq!(entry.implementation, "Dummy::foo");
    assert_eq!(entry.status, Status::Implemented);
    assert_eq!(entry.since, Some("3.4"));
    assert_eq!(entry.comment, None);
}

#[test]
fn partial_entry_carries_comment_and_issue() {
    let entry = find("Dummy.bar").expect("Dummy.bar not registered");
    assert_eq!(entry.status, Status::Partial);
    assert_eq!(entry.comment, Some("only handles the happy path"));
    assert_eq!(entry.issue, Some(7));
}

#[test]
fn free_fn_uses_module_path() {
    let entry = find("module.free_fn").expect("module.free_fn not registered");
    assert!(
        entry.implementation.ends_with("::free_fn"),
        "expected implementation path to end with ::free_fn, got {}",
        entry.implementation,
    );
}
