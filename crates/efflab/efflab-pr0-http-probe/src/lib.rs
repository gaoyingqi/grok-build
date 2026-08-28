//! rmcp/reqwest HTTP 闭包探针。

/// 绑定 rmcp 的 HTTP transport 与 reqwest client 类型，验证最小依赖闭包。
pub type ProbeMcpTransport = rmcp::transport::StreamableHttpClientTransport<reqwest::Client>;

/// 暴露 reqwest client 类型，确保探针链接到目标 HTTP 客户端。
pub type ProbeHttpClient = reqwest::Client;

/// 保持探针无运行时行为。
pub fn probe_links() {}
