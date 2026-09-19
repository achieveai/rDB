//! `#[retcd_test]` — wraps a test function in a root tracing span carrying
//! `testModule`, `testMethod`, `testRun` so every JSONL line produced during the test
//! (including from in-process cluster nodes) is attributable (ADR-0013).
//!
//! Usage:
//! ```ignore
//! #[retcd_test]                       // sync test
//! fn puts_then_gets() { ... }
//!
//! #[retcd_test]                       // async test -> #[tokio::test]
//! async fn cluster_forms() { ... }
//!
//! #[retcd_test(flavor = "multi_thread", worker_threads = 4)]   // args forwarded to tokio::test
//! async fn heavy() { ... }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn retcd_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr2: proc_macro2::TokenStream = attr.into();
    let input = parse_macro_input!(item as ItemFn);
    let vis = &input.vis;
    let sig = &input.sig;
    let name = &sig.ident;
    let name_str = name.to_string();
    let block = &input.block;
    let attrs = &input.attrs;
    let is_async = sig.asyncness.is_some();

    let expanded = if is_async {
        let test_attr = if attr2.is_empty() {
            quote! { #[tokio::test] }
        } else {
            quote! { #[tokio::test(#attr2)] }
        };
        quote! {
            #(#attrs)*
            #test_attr
            #vis #sig {
                let __retcd_span = ::config_log::testing::test_span(module_path!(), #name_str);
                let __retcd_fut = async move #block;
                ::config_log::testing::run_instrumented(__retcd_span, __retcd_fut).await
            }
        }
    } else {
        quote! {
            #(#attrs)*
            #[test]
            #vis #sig {
                let __retcd_span = ::config_log::testing::test_span(module_path!(), #name_str);
                let __retcd_guard = __retcd_span.enter();
                let __retcd_result = (move || #block)();
                ::config_log::testing::finish_sync(&__retcd_span);
                drop(__retcd_guard);
                __retcd_result
            }
        }
    };
    expanded.into()
}
