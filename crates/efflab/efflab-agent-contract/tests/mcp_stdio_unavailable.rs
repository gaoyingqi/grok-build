//! MCP stdio 在最小 runtime 中必须明确拒绝，且不得触发进程启动。

use efflab_agent_contract::{ApprovedMcpConfig, McpServerSpec, deny_stdio_mcp};

#[test]
fn stdio_spec_is_unavailable_and_http_url_is_not_spawned() {
    let mut servers = ApprovedMcpConfig::default();
    servers.servers.insert(
        "demo".into(),
        McpServerSpec::Stdio {
            command: "/bin/echo".into(),
            args: vec![],
        },
    );
    let err = deny_stdio_mcp(&servers).unwrap_err();
    assert!(err.to_string().contains("stdio_mcp_unavailable"));
}

#[test]
fn command_toml_and_unc_strings_are_rejected_without_process_api() {
    for spec in [
        McpServerSpec::Stdio {
            command: r"\\?\C:\tool.exe".into(),
            args: vec![],
        },
        McpServerSpec::Stdio {
            command: "/tmp/symlink-tool".into(),
            args: vec![],
        },
    ] {
        let mut servers = ApprovedMcpConfig::default();
        servers.servers.insert("x".into(), spec);
        assert!(
            deny_stdio_mcp(&servers)
                .unwrap_err()
                .to_string()
                .contains("stdio_mcp_unavailable")
        );
    }
}

#[test]
fn http_spec_passes_through_unchanged() {
    let url = "http://127.0.0.1:8787/mcp".to_string();
    let mut servers = ApprovedMcpConfig::default();
    servers
        .servers
        .insert("demo".into(), McpServerSpec::Http { url: url.clone() });

    assert!(deny_stdio_mcp(&servers).is_ok());
    assert_eq!(
        servers.servers.get("demo"),
        Some(&McpServerSpec::Http { url })
    );
}
