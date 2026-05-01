//! Attribute macros for registering API-parity metadata alongside implementation code.
//!
//! Domain-agnostic: the `reference` name can be a PySpark API, a REST endpoint,
//! a TypeScript type - anything you want to track an implementation against.
//!
//! Two forms:
//!
//! - `#[api_parity_impl(...)]` on an `impl` block. Its `reference` (if any)
//!   acts as a *prefix* for relative child references; if `reference` AND
//!   `status` are both present, an entry is also registered for the impl
//!   itself, with the implementation path set to the type name.
//! - `#[api_parity(...)]` on a method or free `fn`. Inside an
//!   `#[api_parity_impl]`, a leading `.` in the reference (e.g. `.builder`)
//!   is replaced at compile time with `parent_reference + child` (e.g.
//!   `pyspark.sql.session.SparkSession.builder`).
//!
//! Recognized arguments (both macros, all optional except where noted):
//! - `reference = "..."` (required on the child; required on the impl if
//!   any child uses a relative reference).
//! - `status = Implemented | Partial | Unimplemented` (required to emit an
//!   entry).
//! - `since = "..."`, `comment = "..."`, `issue = 42` (optional).
//! - `status = Unimplemented` requires a `comment`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TS2;
use quote::quote;
use syn::{
    parse_macro_input,
    spanned::Spanned,
    Attribute,
    Error,
    ImplItem,
    ItemFn,
    ItemImpl,
    LitInt,
    LitStr,
};

/// Parsed `#[api_parity(...)]` arguments. Used by both macros.
#[derive(Default)]
struct ParityArgs {
    reference: Option<LitStr>,
    status: Option<syn::Ident>,
    since: Option<LitStr>,
    comment: Option<LitStr>,
    issue: Option<LitInt>,
}

fn parse_parity_args(attr: &Attribute) -> Result<ParityArgs, Error> {
    let mut args = ParityArgs::default();
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("reference") {
            args.reference = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("status") {
            args.status = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("since") {
            args.since = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("comment") {
            args.comment = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("issue") {
            args.issue = Some(meta.value()?.parse()?);
        } else {
            return Err(meta.error(format!(
                "api_parity: unknown argument `{}` (expected one of: reference, status, since, comment, issue)",
                meta.path.get_ident().map(|i| i.to_string()).unwrap_or_default(),
            )));
        }
        Ok(())
    })?;
    Ok(args)
}

#[proc_macro_attribute]
pub fn api_parity_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item: ItemImpl = parse_macro_input!(input as ItemImpl);
    let self_ty = &item.self_ty;
    let self_ty_str = quote!(#self_ty).to_string().replace(' ', "");

    // Parse the impl-level args once. Wrap in a fake attribute so we can
    // reuse `parse_parity_args`.
    let args2: TS2 = args.into();
    let parent_attr: Attribute = syn::parse_quote!(#[api_parity(#args2)]);
    let parent_args = match parse_parity_args(&parent_attr) {
        Ok(a) => a,
        Err(e) => return e.into_compile_error().into(),
    };
    let parent_ref_str = parent_args.reference.as_ref().map(|r| r.value());

    let mut submits = TS2::new();

    // If the impl declares both reference and status, register the class
    // itself. The implementation path is the type name (e.g. `SparkSession`).
    if parent_args.reference.is_some() && parent_args.status.is_some() {
        let lit = LitStr::new(&self_ty_str, proc_macro2::Span::call_site());
        let tokens = build_submit(&parent_args, parent_attr.span(), quote!(#lit), None)
            .unwrap_or_else(Error::into_compile_error);
        submits.extend(tokens);
    }

    for impl_item in &mut item.items {
        if let ImplItem::Fn(method) = impl_item {
            let mut child_attrs = Vec::new();
            method.attrs.retain(|attr| {
                if attr.path().is_ident("api_parity") {
                    child_attrs.push(attr.clone());
                    false
                } else {
                    true
                }
            });

            for attr in child_attrs {
                let fn_name = method.sig.ident.to_string();
                let impl_path = format!("{}::{}", self_ty_str, fn_name);
                let lit = LitStr::new(&impl_path, proc_macro2::Span::call_site());
                let tokens = match parse_parity_args(&attr) {
                    Ok(child_args) => build_submit(
                        &child_args,
                        attr.span(),
                        quote!(#lit),
                        parent_ref_str.as_deref(),
                    )
                    .unwrap_or_else(Error::into_compile_error),
                    Err(e) => e.into_compile_error(),
                };
                submits.extend(tokens);
            }
        }
    }

    let out = quote! {
        #item
        #submits
    };
    out.into()
}

#[proc_macro_attribute]
pub fn api_parity(args: TokenStream, input: TokenStream) -> TokenStream {
    let item: ItemFn = parse_macro_input!(input as ItemFn);
    let fn_name = item.sig.ident.to_string();
    let impl_path_expr = quote! { concat!(module_path!(), "::", #fn_name) };

    let args2: TS2 = args.into();
    let attr: Attribute = syn::parse_quote!(#[api_parity(#args2)]);
    let submit = match parse_parity_args(&attr) {
        Ok(parsed) => build_submit(&parsed, attr.span(), impl_path_expr, None)
            .unwrap_or_else(Error::into_compile_error),
        Err(e) => e.into_compile_error(),
    };

    let out = quote! {
        #item
        #submit
    };
    out.into()
}

/// Emit `inventory::submit! { ParityEntry { ... } }`.
///
/// `parent_reference` is the enclosing impl's `reference` value (if any).
/// A child `reference` starting with `.` is rewritten as
/// `parent_reference + child` at expansion time, producing a single
/// `&'static str` literal in the generated code.
fn build_submit(
    args: &ParityArgs,
    span: proc_macro2::Span,
    impl_path_expr: TS2,
    parent_reference: Option<&str>,
) -> Result<TS2, Error> {
    let reference_lit = args.reference.as_ref().ok_or_else(|| {
        Error::new(span, "api_parity: missing required `reference = \"...\"`")
    })?;

    // Resolve relative references against the parent.
    let reference_value = reference_lit.value();
    let reference_lit = if let Some(suffix) = reference_value.strip_prefix('.') {
        match parent_reference {
            Some(parent) => LitStr::new(
                &format!("{parent}.{suffix}"),
                reference_lit.span(),
            ),
            None => {
                return Err(Error::new(
                    reference_lit.span(),
                    "api_parity: relative reference (leading `.`) requires the \
                     enclosing `#[api_parity_impl(...)]` to declare a `reference`",
                ));
            }
        }
    } else {
        reference_lit.clone()
    };

    let status = args.status.as_ref().ok_or_else(|| {
        Error::new(
            span,
            "api_parity: missing required `status = Implemented | Partial | Unimplemented`",
        )
    })?;

    if status != "Implemented" && status != "Partial" && status != "Unimplemented" {
        return Err(Error::new(
            status.span(),
            format!(
                "api_parity: `status` must be one of `Implemented`, `Partial`, or `Unimplemented` (got `{status}`)"
            ),
        ));
    }

    if status == "Unimplemented" && args.comment.is_none() {
        return Err(Error::new(
            span,
            "api_parity: `status = Unimplemented` requires a `comment = \"...\"` explaining why",
        ));
    }

    let since_tok = match &args.since {
        Some(s) => quote!(Some(#s)),
        None => quote!(None),
    };
    let comment_tok = match &args.comment {
        Some(s) => quote!(Some(#s)),
        None => quote!(None),
    };
    let issue_tok = match &args.issue {
        Some(i) => quote!(Some(#i)),
        None => quote!(None),
    };

    Ok(quote! {
        ::api_parity_core::inventory::submit! {
            ::api_parity_core::ParityEntry {
                reference: #reference_lit,
                implementation: #impl_path_expr,
                status: ::api_parity_core::Status::#status,
                since: #since_tok,
                comment: #comment_tok,
                issue: #issue_tok,
            }
        }
    })
}
