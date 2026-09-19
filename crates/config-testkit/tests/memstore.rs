//! Direct `MemStore` behavior not already covered by the conformance suite: capability
//! reporting and the `failing_with`/`stop_failing` toggle.

use std::sync::Arc;

use bytes::Bytes;
use config_core::{
    Authz, Capabilities, ConfigError, ConfigStore, Dedup, Durability, GetRequest, Pagination,
    TransportSecurity, WatchResumption,
};
use config_testkit::MemStore;

#[config_log::retcd_test]
fn memstore_reports_ephemeral_development_capabilities() {
    let store = MemStore::new();
    let caps = store.capabilities();
    assert_eq!(
        caps,
        Capabilities {
            durability: Durability::Ephemeral,
            watch_resumption: WatchResumption::Unsupported,
            authz: Authz::Development,
            transport_security: TransportSecurity::Insecure,
            pagination: Pagination::Unsupported,
            dedup: Dedup::Unsupported,
        }
    );
}

#[config_log::retcd_test]
async fn memstore_failing_with_toggles_every_call_then_stop_failing_restores_it() {
    let store = Arc::new(MemStore::new());
    let dyn_store: Arc<dyn ConfigStore> = store.clone();

    let key = Bytes::from_static(b"memstore_failing_with/k");
    let before = dyn_store
        .get(GetRequest { key: key.clone() })
        .await
        .expect("get succeeds before failing_with");
    assert!(before.record.is_none());

    store.failing_with(ConfigError::Unavailable {
        reason: "injected".into(),
    });
    let err = dyn_store
        .get(GetRequest { key: key.clone() })
        .await
        .expect_err("get must fail once failing_with is set");
    assert!(matches!(err, ConfigError::Unavailable { .. }));

    store.stop_failing();
    let after = dyn_store
        .get(GetRequest { key })
        .await
        .expect("get succeeds again after stop_failing");
    assert!(after.record.is_none());
}
