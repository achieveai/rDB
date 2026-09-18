//! The `#[retcd_test]` attribute must tag every line with testModule/testMethod/testRun and
//! route them to the per-test file under `target/test-logs`. Files append across runs, so
//! assertions filter on the current `testRun`.

use config_log::retcd_test;
use config_log::testing::{test_log_dir, test_run_id};
use serde_json::Value;

/// Lines of this test's file that belong to the current process run.
fn read_lines(module: &str, method: &str) -> Vec<Value> {
    let path = config_log::layer::test_file_path(&test_log_dir(), module, method);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing {}: {e}", path.display()));
    text.lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .filter(|l| l["testRun"] == test_run_id())
        .collect()
}

mod sync_case {
    use super::*;

    #[retcd_test]
    fn sync_test_is_tagged() {
        tracing::info!(step = 1, "hello from sync");
        let lines = read_lines(module_path!(), "sync_test_is_tagged");
        assert!(lines.iter().any(|l| l["@m"] == "test started"));
        let hello = lines
            .iter()
            .find(|l| l["@m"] == "hello from sync")
            .expect("line");
        assert_eq!(hello["testMethod"], "sync_test_is_tagged");
        assert_eq!(hello["testModule"], module_path!());
        assert_eq!(hello["testRun"], test_run_id());
        assert_eq!(hello["step"], 1);
    }
}

mod async_case {
    use super::*;

    #[retcd_test]
    async fn async_test_is_tagged_across_await_and_spawn() {
        tracing::info!("before await");
        tokio::task::yield_now().await;
        // spawned work must be instrumented explicitly to keep the test context
        let h = tokio::spawn(config_log::testing::in_current_span(async {
            tracing::info!("inside spawn");
        }));
        h.await.unwrap();
        tracing::info!("after await");
        let lines = read_lines(
            module_path!(),
            "async_test_is_tagged_across_await_and_spawn",
        );
        for m in ["before await", "inside spawn", "after await"] {
            let l = lines
                .iter()
                .find(|l| l["@m"] == m)
                .unwrap_or_else(|| panic!("missing {m}"));
            assert_eq!(
                l["testMethod"],
                "async_test_is_tagged_across_await_and_spawn"
            );
        }
    }

    #[retcd_test(flavor = "multi_thread", worker_threads = 2)]
    async fn tokio_args_are_forwarded() {
        tracing::info!("multi thread ok");
        let lines = read_lines(module_path!(), "tokio_args_are_forwarded");
        assert!(lines.iter().any(|l| l["@m"] == "multi thread ok"));
    }
}
