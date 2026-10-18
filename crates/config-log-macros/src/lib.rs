//! `#[retcd_test]` — wraps a test function in a root tracing span carrying
//! `testModule`, `testMethod`, `testRun` so every JSONL line produced during the test
//! (including from in-process cluster nodes) is attributable (ADR-0013).
//!
//! Usage:
//! ```ignore
//! #[retcd_test]                       // sync test
//! fn puts_then_gets() { ... }
//!
//! #[retcd_test]                       // async test, on a current-thread Tokio runtime
//! async fn cluster_forms() { ... }
//!
//! #[retcd_test(flavor = "multi_thread", worker_threads = 4)]   // args forwarded to the runtime
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

    // Every expansion makes `FlushTestLogsOnDrop` its first local, so it drops last. For an
    // async test that has to be after the runtime shuts down, and `#[tokio::test]` builds and
    // drops the runtime inside the function it generates, leaving nowhere to put it. So the
    // body runs in an inner `#[tokio::main]` function and the outer one is a plain `#[test]`.
    // `tokio::main` defaults to the multi-thread flavor where `tokio::test` defaults to
    // current-thread, so keep the test default unless the arguments name a flavor.
    let expanded = if is_async {
        let rt_args = if attr2.to_string().contains("flavor") {
            attr2
        } else if attr2.is_empty() {
            quote! { flavor = "current_thread" }
        } else {
            quote! { flavor = "current_thread", #attr2 }
        };
        let mut outer_sig = sig.clone();
        outer_sig.asyncness = None;
        let output = &sig.output;
        quote! {
            #(#attrs)*
            #[test]
            #vis #outer_sig {
                #[tokio::main(#rt_args)]
                async fn __retcd_body() #output {
                    let __retcd_span =
                        ::config_log::testing::test_span(module_path!(), #name_str);
                    let __retcd_fut = async move #block;
                    ::config_log::testing::run_instrumented(__retcd_span, __retcd_fut).await
                }
                let __retcd_flush = ::config_log::testing::FlushTestLogsOnDrop;
                __retcd_body()
            }
        }
    } else {
        quote! {
            #(#attrs)*
            #[test]
            #vis #sig {
                let __retcd_flush = ::config_log::testing::FlushTestLogsOnDrop;
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
