#!/usr/bin/env python3
"""用于验证 sidecar MCP 失败隔离的最小 stdio fixture。

启动后故意输出非 JSON 协议文本并立即以非零状态退出。sidecar 必须将此 MCP
失败限制在该 server 内，而不是令 ACP stdio 主进程崩溃。
"""

import sys


# 故意污染 MCP 子进程 stdout；这不是 sidecar 自身 ACP stdout。
sys.stdout.write("not-json-mcp-fixture\n")
sys.stdout.flush()
sys.exit(1)
