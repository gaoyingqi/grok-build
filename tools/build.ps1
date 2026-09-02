# tools/build.ps1 — Windows 编译脚本（Efflab Agent Kit）
# 职责：校验工具链 → 校验 workspace 成员 → 编译 efflab 三件套 → 可选验证
# 产物：target\{debug,release\}\efflab-agent-sidecar.exe（仅 sidecar 为可执行二进制）
# 约束：根 Cargo.toml 为生成物，成员注册由 scripts/fork-sync-apply.sh 维护；本脚本不改 workspace
# 用法示例见 Get-Help 或 tools/build.ps1 -Help

param(
  [switch]$Release,          # 以 --release 编译（默认）
  [switch]$Debug,            # 以 debug 编译
  [switch]$Dist,             # 以 --profile release-dist 编译
  [string]$Profile = "",     # 指定任意 profile（优先级高于开关）
  [string]$Target = "",      # 指定 --target（例如 x86_64-pc-windows-msvc）
  [switch]$Check,            # 编译后执行 cargo check + clippy
  [switch]$Test,             # 编译后执行 cargo test
  [switch]$Clean,            # 编译前执行 cargo clean
  [switch]$NoLocked,         # 不传 --locked
  [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------- 帮助 ----------
function Show-Usage {
  @"
用法: tools/build.ps1 [选项]

选项:
  -Release               以 --release 编译（默认）
  -Debug                 以 debug 编译
  -Dist                  以 --profile release-dist 编译（加固发布版）
  -Profile <name>        指定任意 Cargo profile（release / release-dist / x-prod 等）
  -Target <triple>       指定 --target（例如 x86_64-pc-windows-msvc）
  -Check                 编译后执行 cargo check + clippy（efflab 三件套）
  -Test                  编译后执行 cargo test（efflab 三件套）
  -Clean                 编译前执行 cargo clean
  -NoLocked              不传 --locked（不推荐，CI 必须 --locked）
  -Help                  显示本帮助

示例:
  tools/build.ps1                                  # 默认 release，本机架构
  tools/build.ps1 -Dist -Check -Test               # 加固发布 + 静态检查 + 单测
  tools/build.ps1 -Target aarch64-pc-windows-msvc -Release
  # 在 PowerShell 中以管理员或普通终端均可，需已安装 rustup/cargo

产物:
  target/release/efflab-agent-sidecar.exe
  target/debug/efflab-agent-sidecar.exe
  target/<triple>/release/efflab-agent-sidecar.exe  （指定 --target 时）
"@
}

if ($Help) { Show-Usage; exit 0 }

# ---------- 工具函数 ----------
function Info($msg) { Write-Host "[tools/build] $msg" }
function Die($msg) { Write-Host "[tools/build] ERROR: $msg" -ForegroundColor Red; exit 2 }

# 解析 profile 优先级：显式 -Profile > -Dist > -Debug > 默认 release
$EffectiveProfile = "release"
if ($Profile -ne "") { $EffectiveProfile = $Profile }
elseif ($Dist) { $EffectiveProfile = "release-dist" }
elseif ($Debug) { $EffectiveProfile = "debug" }
elseif ($Release) { $EffectiveProfile = "release" }

$Locked = if ($NoLocked) { "" } else { "--locked" }

# ---------- 定位仓库根 ----------
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = (Resolve-Path (Join-Path $ScriptDir "..")).Path
Set-Location $Root
Info "仓库根: $Root"
Info "模式: profile=$EffectiveProfile target=${Target:-本机}"

if (-not (Test-Path (Join-Path $Root "Cargo.toml"))) {
  Die "未找到 Cargo.toml: $(Join-Path $Root 'Cargo.toml')"
}

# ---------- 工具链检查 ----------
function Test-Toolchain {
  try { $null = Get-Command cargo -ErrorAction Stop } catch { Die "未找到 cargo；请先安装 rustup（https://rustup.rs）并执行 rustup show" }
  try { $null = Get-Command rustc -ErrorAction Stop } catch { Die "未找到 rustc" }

  $expected = ""
  if (Test-Path (Join-Path $Root "rust-toolchain.toml")) {
    $m = Select-String -Path (Join-Path $Root "rust-toolchain.toml") -Pattern 'channel\s*=\s*"([^"]+)"' -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($m) { $expected = $m.Matches[0].Groups[1].Value }
  }
  $actual = ""
  try { $actual = (rustc --version 2>$null) } catch {}
  Info "rustc: $actual（期望 channel=$expected，见 rust-toolchain.toml）"
  if ($expected -and $actual -notlike "*$expected*") {
    Info "提示: 当前 rustc 与 rust-toolchain.toml 不一致，cargo 会按 toolchain 文件自动切换"
  }
  # dotslash / protoc 仅提示
  try { $null = Get-Command dotslash -ErrorAction Stop; Info "dotslash: $((dotslash --help 2>&1 | Select-Object -First 1))" } catch {
    Info "提示: 未找到 dotslash，bin/protoc 将尝试回退到 PATH 上的 protoc；建议 cargo install dotslash"
  }
  try { $null = Get-Command protoc -ErrorAction Stop; Info "protoc: $((protoc --version 2>&1 | Select-Object -First 1))" } catch {
    Info "提示: PATH 上未找到 protoc，将由 bin/protoc 的 DotSlash 按需拉取（Windows 需自行保证 protoc 可用或使用 dotslash）"
  }
}

# ---------- workspace 成员检查 ----------
function Test-Workspace {
  $script = Join-Path $Root "scripts/fork-sync-apply.sh"
  if (Test-Path $script) {
    Info "检查 workspace 成员（scripts/fork-sync-apply.sh --check）"
    # Windows 上用 bash/sh 执行该脚本；若无 bash 则跳过
    $bash = $null
    foreach ($cand in @("bash", "sh")) {
      try { $null = Get-Command $cand -ErrorAction Stop; $bash = $cand; break } catch {}
    }
    if ($bash) {
      & $bash $script --check
      if ($LASTEXITCODE -ne 0) { Die "workspace 成员检查失败；请在 Git Bash/WSL 中执行 scripts/fork-sync-apply.sh --apply 或修复 Cargo.toml" }
    } else {
      Info "跳过 workspace 成员检查：未找到 bash/sh（可在 Git Bash 或 WSL 中手动执行 scripts/fork-sync-apply.sh --check）"
    }
  } else {
    Info "跳过 workspace 成员检查（未找到 scripts/fork-sync-apply.sh）"
  }
}

# ---------- 构建 ----------
function Invoke-CargoBuildOnce($profile, $target) {
  $argsList = @("build", "-p", "efflab-agent-contract", "-p", "efflab-agent-host", "-p", "efflab-agent-sidecar")
  if ($profile -eq "release") { $argsList += "--release" }
  elseif ($profile -eq "debug") { } # debug 不加 flag
  else { $argsList += @("--profile", $profile) }
  if ($Locked) { $argsList += $Locked }
  if ($target) { $argsList += @("--target", $target) }
  Info "执行: cargo $($argsList -join ' ')"
  & cargo @argsList
  if ($LASTEXITCODE -ne 0) { Die "cargo build 失败（exit $LASTEXITCODE）" }
}

if ($Clean) {
  Info "执行: cargo clean"
  & cargo clean
  if ($LASTEXITCODE -ne 0) { Die "cargo clean 失败" }
}

Test-Toolchain
Test-Workspace

# 自动安装缺失 target
if ($Target) {
  $installed = ""
  try { $installed = (rustup target list --installed 2>$null) } catch {}
  if ($installed -notlike "*$Target*") {
    Info "安装 target: $Target"
    try { & rustup target add $Target } catch { Info "警告: rustup target add 失败，尝试直接编译" }
  }
}

Invoke-CargoBuildOnce $EffectiveProfile $Target

# ---------- 产物提示 ----------
Info "编译完成，产物预览:"
$exeName = "efflab-agent-sidecar.exe"
if ($Target) {
  $candidates = @(
    (Join-Path $Root "target/$Target/$EffectiveProfile/$exeName"),
    (Join-Path $Root "target/$Target/release/$exeName"),
    (Join-Path $Root "target/$Target/debug/$exeName")
  )
  foreach ($p in $candidates) { if (Test-Path $p) { Info "产物: $p"; Get-Item $p | Format-List Length,LastWriteTime | Out-String | Write-Host; break } }
  $dir = Join-Path $Root "target/$Target/$EffectiveProfile"
  if (Test-Path $dir) { Get-ChildItem $dir | Select-Object -First 20 | Format-Table Name,Length,LastWriteTime | Out-String | Write-Host }
} else {
  if ($EffectiveProfile -eq "release") { $p = Join-Path $Root "target/release/$exeName" }
  elseif ($EffectiveProfile -eq "debug") { $p = Join-Path $Root "target/debug/$exeName" }
  else { $p = Join-Path $Root "target/$EffectiveProfile/$exeName" }
  if (Test-Path $p) { Info "产物: $p"; Get-Item $p | Format-List Length,LastWriteTime | Out-String | Write-Host }
  else {
    # 回退查找
    $fallback = Join-Path $Root "target/release/$exeName"
    if (Test-Path $fallback) { Info "产物: $fallback"; Get-Item $fallback | Format-List Length,LastWriteTime | Out-String | Write-Host }
  }
}

# ---------- 可选验证 ----------
if ($Check) {
  Info "执行静态检查: cargo check + clippy（efflab 三件套）"
  $checkArgs = @("check", "-p", "efflab-agent-contract", "-p", "efflab-agent-host", "-p", "efflab-agent-sidecar")
  if ($Locked) { $checkArgs += $Locked }
  if ($Target) { $checkArgs += @("--target", $Target) }
  Info "执行: cargo $($checkArgs -join ' ')"
  & cargo @checkArgs
  if ($LASTEXITCODE -ne 0) { Die "cargo check 失败" }

  # clippy
  $hasClippy = $true
  try { & cargo clippy --version 2>$null | Out-Null; if ($LASTEXITCODE -ne 0) { $hasClippy = $false } } catch { $hasClippy = $false }
  if ($hasClippy) {
    $clippyArgs = @("clippy", "-p", "efflab-agent-contract", "-p", "efflab-agent-host", "-p", "efflab-agent-sidecar", "--all-targets")
    if ($Locked) { $clippyArgs += $Locked }
    if ($Target) { $clippyArgs += @("--target", $Target) }
    Info "执行: cargo $($clippyArgs -join ' ')"
    & cargo @clippyArgs
    if ($LASTEXITCODE -ne 0) { Die "cargo clippy 失败" }
  } else {
    Info "跳过 clippy：未安装 clippy 组件（rustup component add clippy）"
  }

  Info "检查依赖边界: cargo tree -p efflab-agent-host"
  $treeOut = ""
  try { $treeOut = (& cargo tree -p efflab-agent-host 2>$null | Out-String) } catch {}
  if ($treeOut -match "efflab-agent-sidecar|xai-grok-shell|xai-grok-tools") {
    Die "依赖边界违规：efflab-agent-host 不应依赖 efflab-agent-sidecar / xai-grok-shell / xai-grok-tools"
  }
}

if ($Test) {
  Info "执行测试: cargo test -p efflab-agent-contract -p efflab-agent-host -p efflab-agent-sidecar"
  $testArgs = @("test", "-p", "efflab-agent-contract", "-p", "efflab-agent-host", "-p", "efflab-agent-sidecar")
  if ($Locked) { $testArgs += $Locked }
  if ($Target) { $testArgs += @("--target", $Target) }
  Info "执行: cargo $($testArgs -join ' ')"
  & cargo @testArgs
  if ($LASTEXITCODE -ne 0) { Die "cargo test 失败" }
}

Info "全部完成"
