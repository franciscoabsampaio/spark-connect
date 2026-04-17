//! Attribute macros for registering PySpark-parity metadata alongside Rust code.
//!
//! Two forms:
//!
//! - `#[parity_impl]` on an `impl` block rewrites any `#[parity(...)]` attributes
//!   on its methods into `inventory::submit!` calls, prefixing the `rust` path
//!   with the type name (e.g. `SparkSession::sql`).
//! - `#[parity(...)]` directly on a free `fn` uses `module_path!()` + the fn name.
//!
//! Recognized arguments:
//! - `pyspark = "..."` (required) — canonical PySpark name (e.g. `"SparkSession.sql"`).
//! - `status = Implemented | Partial | Unimplemented` (required).
//! - `since = "3.4"` (optional) — first Spark version where this is claimed working.
//! - `comment = "..."` (optional; **required** when `status = Unimplemented`).
//! - `issue = 42` (optional) — GitHub issue number.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TS2;
use quote::quote;
use syn::{parse_macro_input, Attribute, ImplItem, ItemFn, ItemImpl, LitInt, LitStr};

#[proc_macro_attribute]
pub fn parity_impl(_args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item: ItemImpl = parse_macro_input!(input as ItemImpl);
    let self_ty = &item.self_ty;
    let self_ty_str = quote!(#self_ty).to_string().replace(' ', "");

    let mut submits = TS2::new();

    for impl_item in &mut item.items {
        if let ImplItem::Fn(method) = impl_item {
            let mut parity_attrs = Vec::new();
            method.attrs.retain(|attr| {
                if attr.path().is_ident("parity") {
                    parity_attrs.push(attr.clone());
                    false
                } else {
                    true
                }
            });

            for attr in parity_attrs {
                let fn_name = method.sig.ident.to_string();
                let rust_path = format!("{}::{}", self_ty_str, fn_name);
                let lit = LitStr::new(&rust_path, proc_macro2::Span::call_site());
                submits.extend(build_submit(&attr, quote!(#lit)));
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
pub fn parity(args: TokenStream, input: TokenStream) -> TokenStream {
    let item: ItemFn = parse_macro_input!(input as ItemFn);
    let fn_name = item.sig.ident.to_string();
    let rust_path_expr = quote! { concat!(module_path!(), "::", #fn_name) };

    let args2: TS2 = args.into();
    let fake_attr: Attribute = syn::parse_quote!(#[parity(#args2)]);
    let submit = build_submit(&fake_attr, rust_path_expr);

    let out = quote! {
        #item
        #submit
    };
    out.into()
}

fn build_submit(attr: &Attribute, rust_path_expr: TS2) -> TS2 {
    let mut pyspark: Option<LitStr> = None;
    let mut status: Option<syn::Ident> = None;
    let mut since: Option<LitStr> = None;
    let mut comment: Option<LitStr> = None;
    let mut issue: Option<LitInt> = None;

    let _ = attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("pyspark") {
            pyspark = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("status") {
            status = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("since") {
            since = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("comment") {
            comment = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("issue") {
            issue = Some(meta.value()?.parse()?);
        }
        Ok(())
    });

    let pyspark = pyspark.expect("parity: missing `pyspark = \"...\"`");
    let status = status.expect("parity: missing `status = Implemented|Partial|Unimplemented`");

    if status == "Unimplemented" && comment.is_none() {
        panic!("parity: `status = Unimplemented` requires a `comment = \"...\"` explaining why");
    }

    let since_tok = match since {
        Some(s) => quote!(Some(#s)),
        None => quote!(None),
    };
    let comment_tok = match comment {
        Some(s) => quote!(Some(#s)),
        None => quote!(None),
    };
    let issue_tok = match issue {
        Some(i) => quote!(Some(#i)),
        None => quote!(None),
    };

    quote! {
        ::parity_core::inventory::submit! {
            ::parity_core::ParityEntry {
                pyspark: #pyspark,
                rust: #rust_path_expr,
                status: ::parity_core::Status::#status,
                since: #since_tok,
                comment: #comment_tok,
                issue: #issue_tok,
                spark_versions: &[
                    #[cfg(feature = "spark-3-4")] "3.4",
                    #[cfg(feature = "spark-3-5")] "3.5",
                ],
            }
        }
    }
}
