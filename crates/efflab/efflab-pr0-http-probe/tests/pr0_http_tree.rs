#[test]
fn probe_package_is_workspace_member_named_efflab_pr0_http_probe() {
    let manifest = include_str!("../Cargo.toml");
    assert!(manifest.contains("name = \"efflab-pr0-http-probe\""));
    assert!(manifest.contains("version = \"0.13\""));
    assert!(manifest.contains("\"rustls\""));
    assert!(!manifest.contains("rustls-tls"));
    assert!(!manifest.contains("0.13.2"));
    assert!(!manifest.contains("auth"));
}
