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

struct WithRef;

#[api_parity_impl(
    reference = "ext.WithRef",
    status = Implemented,
)]
impl WithRef {
    #[api_parity(
        reference = ".relative",
        status = Implemented,
    )]
    #[allow(dead_code)]
    fn relative(&self) {}

    // Stub for an API we haven't built yet. The body never runs; the entry
    // exists purely so the parity report can surface it.
    #[api_parity(
        reference = ".missing",
        status = Unimplemented,
        comment = "not yet wired up to the gRPC client",
        issue = 42,
    )]
    #[allow(dead_code)]
    fn missing(&self) {
        unimplemented!()
    }
}

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

#[test]
fn impl_block_registers_class_level_entry() {
    let entry = find("ext.WithRef").expect("ext.WithRef not registered");
    assert_eq!(entry.implementation, "WithRef");
    assert_eq!(entry.status, Status::Implemented);
}

#[test]
fn relative_reference_is_prefixed_with_parent() {
    let entry = find("ext.WithRef.relative").expect("ext.WithRef.relative not registered");
    assert_eq!(entry.implementation, "WithRef::relative");
    assert_eq!(entry.status, Status::Implemented);
}

#[test]
fn unimplemented_stub_is_registered_without_being_called() {
    // The stub fn `WithRef::missing` is never invoked — but the entry still
    // exists in the inventory, with the mandatory comment and the issue.
    let entry = find("ext.WithRef.missing").expect("ext.WithRef.missing not registered");
    assert_eq!(entry.status, Status::Unimplemented);
    assert_eq!(entry.implementation, "WithRef::missing");
    assert_eq!(entry.comment, Some("not yet wired up to the gRPC client"));
    assert_eq!(entry.issue, Some(42));
}
