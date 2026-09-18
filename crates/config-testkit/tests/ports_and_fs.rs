//! `ports::ephemeral_listener` binds `127.0.0.1:0` and gets a real, non-zero port back;
//! `fs::temp_dir` is writable and cleans itself up on drop (TA-11).

#[config_log::retcd_test]
async fn ephemeral_listener_binds_loopback_with_a_real_port() {
    let (listener, addr) = config_testkit::ports::ephemeral_listener().await;
    assert_eq!(addr.ip().to_string(), "127.0.0.1");
    assert_ne!(addr.port(), 0, "the OS must have assigned a concrete port");
    assert_eq!(
        listener.local_addr().expect("listener has a local addr"),
        addr
    );
}

#[config_log::retcd_test]
fn temp_dir_is_writable_and_removed_on_drop() {
    let path = {
        let dir = config_testkit::fs::temp_dir();
        let file = dir.path().join("probe.txt");
        std::fs::write(&file, b"hello").expect("write into the temp dir");
        assert!(file.exists());
        dir.path().to_path_buf()
    };
    assert!(!path.exists(), "temp dir must be removed once dropped");
}
