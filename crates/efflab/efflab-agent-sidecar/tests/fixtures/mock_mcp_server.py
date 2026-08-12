#!/usr/bin/env python3
"""efflab-agent-sidecar 集成测试用的 mock MCP stdio server（server 名: echo）。

实现最小 MCP stdio 协议：initialize / notifications/initialized / tools/list /
tools/call（echo 回显）。每行一条 JSON-RPC（newline-delimited）。
"""
import json
import sys


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        method = msg.get("method")
        req_id = msg.get("id")
        if method == "initialize":
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "mock-echo", "version": "1.0.0"},
                },
            })
        elif method == "notifications/initialized":
            pass
        elif method == "tools/list":
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": [{
                        "name": "echo",
                        "description": "Echo the given text back",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                        },
                    }]
                },
            })
        elif method == "tools/call":
            args = msg.get("params", {}).get("arguments", {})
            text = args.get("text", "")
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": text}],
                    "isError": False,
                },
            })
        else:
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"mock: unknown method {method}"},
            })


if __name__ == "__main__":
    main()
