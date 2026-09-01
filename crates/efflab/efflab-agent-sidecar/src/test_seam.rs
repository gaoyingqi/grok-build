//! debug 构建专用的 sidecar 测试接缝。
//!
//! 该模块只使用测试进程传入的临时目录控制异步阶段，不参与 release 构建，也不承载
//! prompt、模型正文、工具参数或凭据。

use std::{fs, path::PathBuf, time::Duration};

/// 仅 debug 构建可用的文件型测试 seam。
#[derive(Clone)]
pub(crate) struct TestSeam {
    root: PathBuf,
}

impl TestSeam {
    /// 创建 seam 并初始化仅测试执行计数器。
    pub(crate) fn new(root: PathBuf) -> Self {
        if let Err(error) = fs::create_dir_all(&root) {
            tracing::debug!(event = "test_seam_init_failed", "测试 seam 目录不可用");
            let _ = error;
        }
        let counter = root.join("execution-count");
        if let Err(error) = fs::write(counter, b"0") {
            tracing::debug!(
                event = "test_seam_counter_init_failed",
                "测试 execution spy 不可用"
            );
            let _ = error;
        }
        Self { root }
    }

    /// 在测试可观测目录写入固定事件，不写入调用方数据。
    pub(crate) fn mark(&self, name: &str) {
        let Some(path) = self.path(name, "entered") else {
            tracing::debug!(event = "test_seam_name_rejected", "拒绝非法测试 seam 名称");
            return;
        };
        if let Err(error) = fs::write(path, b"entered") {
            tracing::debug!(event = "test_seam_mark_failed", "测试 seam 事件写入失败");
            let _ = error;
        }
    }

    /// 仅当测试显式创建 `.enabled` 文件时暂停在指定异步窗口。
    pub(crate) async fn wait_if_enabled(&self, name: &str) {
        let Some(enabled) = self.path(name, "enabled") else {
            return;
        };
        if !enabled.exists() {
            return;
        }
        self.mark(name);
        let Some(release) = self.path(name, "release") else {
            return;
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !release.exists() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        if !release.exists() {
            tracing::debug!(event = "test_seam_wait_timeout", "测试 seam 等待超时");
        }
    }

    /// 记录一次已经通过 permission 的 noop 执行点调用。
    pub(crate) fn record_execution(&self) {
        let path = self.root.join("execution-count");
        let current = fs::read_to_string(&path)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map_or(0, |parsed| parsed);
        let next = current.saturating_add(1);
        if let Err(error) = fs::write(path, next.to_string()) {
            tracing::debug!(
                event = "test_seam_counter_write_failed",
                "测试 execution spy 写入失败"
            );
            let _ = error;
        }
    }

    /// 只允许内部固定名称，避免 seam 路径被拼接出目录遍历。
    fn path(&self, name: &str, suffix: &str) -> Option<PathBuf> {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return None;
        }
        Some(self.root.join(format!("{name}.{suffix}")))
    }
}
