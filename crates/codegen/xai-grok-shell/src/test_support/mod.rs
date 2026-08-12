pub(crate) mod lsp_runtime;

pub(crate) const TEST_MODEL: &str = "test-model";

/// Keep this crate's unit-test binary from writing synthetic events into
/// the real unified log; pre-main so the redirect beats the lazily-opened
/// writer. Integration binaries under `tests/` isolate via `TestSandbox`
/// homes instead.
#[ctor::ctor]
fn redirect_unified_log_for_tests() {
    xai_grok_telemetry::unified_log::redirect_to_temp_for_tests();
}

/// jsonwebtoken 10.x 依赖 ring 的进程级 CryptoProvider。本仓库同时启用
/// `rust_crypto`（xai-grok-shell）与 `aws_lc_rs`（gcloud 链）两个 provider
/// feature，此时 `CryptoProvider::get_default()` 的 `get_or_init` 会落到
/// `from_crate_features()` 的 panic 分支并**永久污染**进程级 OnceLock——
/// 任何在 `install_default()` 之前触发 jsonwebtoken 的测试都会让后续所有
/// auth 测试崩溃。这里在测试进程启动（pre-main）即安装 rust_crypto provider，
/// 使全部测试行为与单测单独运行时一致（幂等：重复 install 被忽略）。
#[ctor::ctor]
fn install_crypto_provider_for_tests() {
    let _ = jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER.install_default();
}

/// rustls 0.23 与 jsonwebtoken 类似：workspace 中 shell 侧启用 `ring` feature、
/// hyper-rustls/tokio-rustls 链启用 `aws-lc-rs`，双 feature 并存时
/// `CryptoProvider::get_default()` 无法自动确定并同样永久污染 OnceLock，
/// 使 relay/TLS 相关测试在集合运行时崩溃。pre-main 安装 ring provider 兜底。
#[ctor::ctor]
fn install_rustls_crypto_provider_for_tests() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Prepend the hermetic git binary (via `GIT_BIN_PATH`) to `PATH` so that
/// `Command::new("git")` in test helpers resolves to the Bazel-provided
/// static binary instead of relying on system-installed git.
///
/// Safe to call multiple times — only the first call mutates `PATH`.
pub(crate) fn ensure_hermetic_git_on_path() {
    use std::path::PathBuf;
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        if let Ok(git_bin) = std::env::var("GIT_BIN_PATH") {
            let p = PathBuf::from(&git_bin);
            let p = if p.is_relative() {
                std::env::current_dir().unwrap().join(&p)
            } else {
                p
            };
            if let Some(dir) = p.parent() {
                let cur = std::env::var("PATH").unwrap_or_default();
                unsafe {
                    std::env::set_var("PATH", format!("{}:{}", dir.display(), cur));
                    // git-minimal spawns subcommands (`git stash` → `git
                    // update-index`) through its exec path, which is baked to
                    // a build-machine prefix. Helpers live next to the binary,
                    // so point the exec path there. Skip the host-fallback
                    // wrapper: host git must keep its own exec path.
                    if p.file_name().is_some_and(|name| name == "git") {
                        std::env::set_var("GIT_EXEC_PATH", dir);
                    }
                }
            }
        }
    });
}
