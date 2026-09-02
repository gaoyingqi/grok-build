//! sidecar 本地可观测性边界。
//!
//! 生命周期事件只发固定字段到 stderr。DEBUG 级别额外输出截断后的 ACP JSON-RPC
//! 预览，便于追踪 Host↔sidecar；binding、Authorization 和凭据字段仍必须脱敏。

use serde_json::Value;

use crate::model_client::truncate_for_debug;

/// DEBUG 日志中 ACP JSON 预览上限。
const ACP_WIRE_PREVIEW_BYTES: usize = 4096;
/// sidecar stdout 尚未成行的缓冲上限，避免无换行攻击撑爆内存。
const ACP_WIRE_PENDING_MAX: usize = 1_048_576;

/// 记录 ACP runtime 已开始接管 stdio。
pub(crate) fn runtime_started() {
    tracing::debug!(event = "acp_runtime_started", "ACP runtime 已启动");
}

/// 记录 stdin reader 已看到 EOF。
pub(crate) fn stdin_eof() {
    tracing::debug!(event = "stdin_eof", "sidecar stdin 已到 EOF");
}

/// 记录 ACP gateway 已完成 EOF 收尾。
pub(crate) fn acp_eof() {
    tracing::debug!(event = "acp_eof", "ACP gateway 已完成 EOF 收尾");
}

/// 记录 ACP I/O 发生非敏感传输错误。
pub(crate) fn acp_io_failed() {
    tracing::debug!(event = "acp_io_failed", "ACP gateway I/O 失败");
}

/// 记录 gateway receiver 已停止。
pub(crate) fn gateway_stopped() {
    tracing::debug!(event = "acp_gateway_stopped", "ACP gateway receiver 已停止");
}

/// 记录 stdin bridge 因下游关闭而停止。
pub(crate) fn stdin_bridge_stopped() {
    tracing::debug!(event = "stdin_bridge_stopped", "stdin bridge 已停止");
}

/// 记录 runtime 已释放 ACP transport 与本地状态。
pub(crate) fn runtime_cleanup() {
    tracing::debug!(event = "acp_runtime_cleanup", "ACP runtime 清理完成");
}

/// 记录收到 initialize，但不记录客户端字段。
pub(crate) fn initialize_received() {
    tracing::debug!(event = "acp_initialize", "收到 ACP initialize");
}

/// 记录创建了一个内存 session。
pub(crate) fn session_created() {
    tracing::debug!(event = "session_created", "内存 session 已创建");
}

/// 记录 session/load 的结果，不记录 session 标识。
pub(crate) fn session_loaded(found: bool) {
    tracing::debug!(event = "session_loaded", found, "处理 ACP session/load");
}

/// 记录 session/list 返回的数量。
pub(crate) fn sessions_listed(count: usize) {
    tracing::debug!(event = "sessions_listed", count, "处理 ACP session/list");
}

/// 记录 session/close 已删除内存与持久化状态，不记录 session 标识。
pub(crate) fn session_closed() {
    tracing::debug!(event = "session_closed", "处理 ACP session/close");
}

/// 记录最小 prompt 已完成，不记录 prompt 内容。
pub(crate) fn prompt_completed() {
    tracing::debug!(event = "prompt_completed", "最小 ACP prompt 已完成");
}

/// 记录取消通知是否对应已知 session，不记录 session 标识。
pub(crate) fn cancel_received(known_session: bool) {
    tracing::debug!(
        event = "session_cancel",
        known_session,
        "收到 ACP session/cancel"
    );
}

/// 记录扩展方法已返回最小 catalog。
pub(crate) fn extension_served() {
    tracing::debug!(event = "mcp_catalog_served", "返回最小 MCP catalog");
}

/// 记录未知扩展被拒绝，不回显方法名。
pub(crate) fn extension_rejected() {
    tracing::debug!(
        event = "extension_method_not_found",
        "拒绝未知 ACP extension"
    );
}

/// 识别不应进入 DEBUG 日志的凭据字段名。
fn is_sensitive_acp_key(key: &str) -> bool {
    let normalized = key.replace('-', "_").to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "authorization"
            | "api_key"
            | "apikey"
            | "access_token"
            | "app_key"
            | "password"
            | "secret"
            | "binding"
            | "token"
            | "env_key"
    ) || normalized.ends_with("_token")
        || normalized.ends_with("_secret")
        || normalized.ends_with("_password")
        || (normalized.ends_with("_key") && normalized.contains("api"))
}

/// 递归脱敏 JSON 对象中的凭据字段。
fn redact_acp_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut redacted = serde_json::Map::new();
            for (key, child) in map {
                if is_sensitive_acp_key(key) {
                    redacted.insert(key.clone(), Value::String("<redacted>".to_owned()));
                } else {
                    redacted.insert(key.clone(), redact_acp_json(child));
                }
            }
            Value::Object(redacted)
        }
        Value::Array(items) => Value::Array(items.iter().map(redact_acp_json).collect()),
        other => other.clone(),
    }
}

/// 解析并记录一条 ACP JSON-RPC 行，仅 DEBUG。
pub(crate) fn log_acp_wire_bytes(direction: &'static str, line: &[u8]) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    let text = String::from_utf8_lossy(line);
    let trimmed = text.trim_end_matches(['\n', '\r']);
    if trimmed.is_empty() {
        return;
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(value) => {
            let method = value.get("method").and_then(Value::as_str);
            let id = value.get("id").map(ToString::to_string);
            let kind = if method.is_some() {
                if value.get("id").is_some() {
                    "request"
                } else {
                    "notification"
                }
            } else if value.get("error").is_some() {
                "error_response"
            } else {
                "response"
            };
            let preview =
                truncate_for_debug(&redact_acp_json(&value).to_string(), ACP_WIRE_PREVIEW_BYTES);
            tracing::debug!(
                event = "acp_wire",
                direction,
                kind,
                method,
                id = id.as_deref(),
                payload_bytes = preview.len(),
                payload = %preview,
                "ACP Host↔sidecar 消息"
            );
        }
        Err(_) => {
            tracing::debug!(
                event = "acp_wire",
                direction,
                kind = "unparsed",
                payload = %truncate_for_debug(trimmed, ACP_WIRE_PREVIEW_BYTES),
                "ACP Host↔sidecar 非 JSON 行"
            );
        }
    }
}

/// 从 stdout 缓冲中拆出完整 JSON-RPC 行并记入 DEBUG。
pub(crate) fn drain_acp_stdout_lines(pending: &mut Vec<u8>) {
    while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
        let line: Vec<u8> = pending.drain(..=newline).collect();
        log_acp_wire_bytes("sidecar_to_host", &line);
    }
    if pending.len() > ACP_WIRE_PENDING_MAX {
        log_acp_wire_bytes("sidecar_to_host", pending);
        pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redact_acp_json_keeps_prompt_and_hides_credentials() {
        let value = json!({
            "method": "session/prompt",
            "params": {
                "prompt": [{"type": "text", "text": "hello"}],
                "api_key": "should-not-log",
                "access_token": "should-not-log",
            }
        });
        let redacted = redact_acp_json(&value);
        assert_eq!(redacted["method"], "session/prompt");
        assert_eq!(redacted["params"]["prompt"][0]["text"], "hello");
        assert_eq!(redacted["params"]["api_key"], "<redacted>");
        assert_eq!(redacted["params"]["access_token"], "<redacted>");
    }

    #[test]
    fn drain_acp_stdout_lines_consumes_complete_json_lines() {
        let mut pending = b"{\"jsonrpc\":\"2.0\",\"method\":\"session/update\"}\npartial".to_vec();
        drain_acp_stdout_lines(&mut pending);
        assert_eq!(pending, b"partial");
    }
}
