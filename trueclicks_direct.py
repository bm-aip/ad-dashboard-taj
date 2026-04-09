"""
Direct TrueClicks MCP client.

Protocol discovery from debug:
  GET {mcp_url} → SSE stream, first event:
    event: endpoint
    data: /messages?sessionId={id}   ← relative path

  Then POST to {base}/messages?sessionId={id}
  Response comes back in the POST response body (not via SSE).
"""

import json
import re
import requests
from urllib.parse import urlparse


def _base_url(mcp_url):
    p = urlparse(mcp_url)
    return f"{p.scheme}://{p.netloc}"


def _camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def _parse_trueclicks_rows(tc_result):
    """
    Convert TrueClicks result format to list of GAQL-style nested dicts.

    TrueClicks format:
      {"userLogin": null, "notification": {...}, "result": {"columns": [...], "rows": [...]}}
    """
    if isinstance(tc_result, list):
        return tc_result

    if not isinstance(tc_result, dict):
        return None

    # Navigate: outer result → inner result → columns/rows
    inner = tc_result
    if "result" in inner and isinstance(inner["result"], dict):
        inner = inner["result"]
    if "result" in inner and isinstance(inner["result"], dict):
        inner = inner["result"]

    columns = inner.get("columns", [])
    rows    = inner.get("rows",    [])

    if not columns:
        print(f"[TrueClicks Parse] No columns in result. Top keys: {list(tc_result.keys())}")
        return None

    print(f"[TrueClicks Parse] {len(rows)} rows, {len(columns)} columns: {columns}")

    result_list = []
    for row in rows:
        if isinstance(row, list):
            row_dict = dict(zip(columns, row))
        elif isinstance(row, dict):
            row_dict = row
        else:
            continue

        # Build nested dict from dotted column paths, storing both camelCase & snake_case
        nested = {}
        for col, val in row_dict.items():
            parts = col.split(".")
            d = nested
            for part in parts[:-1]:
                if part not in d or not isinstance(d[part], dict):
                    d[part] = {}
                d = d[part]
            leaf = parts[-1]
            snake = _camel_to_snake(leaf)
            d[leaf]  = val
            if snake != leaf:
                d[snake] = val

        result_list.append(nested)

    return result_list


def _get_endpoint_uri(mcp_url, timeout=8):
    """
    Connect to SSE endpoint, read the first endpoint event, return full POST URI.
    Closes the connection immediately after.
    """
    base = _base_url(mcp_url)
    try:
        resp = requests.get(
            mcp_url,
            stream=True,
            headers={"Accept": "text/event-stream", "Cache-Control": "no-cache"},
            timeout=timeout,
        )
        event_type = None
        for raw in resp.iter_lines():
            if raw is None:
                continue
            line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            if not line:
                event_type = None
                continue
            if line.startswith("event:"):
                event_type = line[6:].strip()
                continue
            if line.startswith("data:"):
                data_str = line[5:].strip()
                if event_type == "endpoint":
                    if data_str.startswith("/"):
                        resp.close()
                        return base + data_str
                    elif data_str.startswith("http"):
                        resp.close()
                        return data_str
                    else:
                        try:
                            d = json.loads(data_str)
                            uri = d.get("uri") or d.get("url")
                            if uri:
                                resp.close()
                                return uri
                        except Exception:
                            pass
        resp.close()
    except Exception as exc:
        # Timeout after reading endpoint is expected — just return what we have
        if "Read timed out" in str(exc) or "timeout" in str(exc).lower():
            pass  # Normal — TrueClicks keeps SSE open until POST arrives
        else:
            print(f"[TrueClicks] SSE connect error: {exc}")
    return None


def call_trueclicks_gaql(mcp_url, customer_id, login_customer_id, gaql_query, timeout=30):
    """
    Call TrueClicks MCP directly.
    1. GET SSE endpoint URI
    2. POST initialize   → read response from POST body
    3. POST tool call    → read response from POST body
    4. Parse result
    """
    # Step 1: Get endpoint URI
    endpoint_uri = _get_endpoint_uri(mcp_url, timeout=8)
    if not endpoint_uri:
        print("[TrueClicks] Could not get endpoint URI")
        return None
    print(f"[TrueClicks] Endpoint: {endpoint_uri}")

    headers = {"Content-Type": "application/json"}

    # Step 2: Initialize
    try:
        init_resp = requests.post(
            endpoint_uri,
            json={
                "jsonrpc": "2.0",
                "method":  "initialize",
                "params":  {
                    "protocolVersion": "2024-11-05",
                    "capabilities":    {"tools": {}},
                    "clientInfo":      {"name": "suncrest-dashboard", "version": "1.0"},
                },
                "id": 0,
            },
            headers=headers,
            timeout=15,
        )
        print(f"[TrueClicks] Initialize: HTTP {init_resp.status_code} body={init_resp.text[:200]}")
    except Exception as exc:
        print(f"[TrueClicks] Initialize error: {exc}")
        return None

    # Step 3: Initialized notification (no response expected)
    try:
        requests.post(
            endpoint_uri,
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            headers=headers,
            timeout=5,
        )
    except Exception:
        pass

    # Step 4: Tool call
    try:
        tool_resp = requests.post(
            endpoint_uri,
            json={
                "jsonrpc": "2.0",
                "method":  "tools/call",
                "params":  {
                    "name":      "google-ads-download-report",
                    "arguments": {
                        "customerId":      int(customer_id),
                        "loginCustomerId": int(login_customer_id),
                        "query":           gaql_query,
                    },
                },
                "id": 1,
            },
            headers=headers,
            timeout=timeout,
        )
        print(f"[TrueClicks] Tool call: HTTP {tool_resp.status_code} body={tool_resp.text[:400]}")
    except Exception as exc:
        print(f"[TrueClicks] Tool call error: {exc}")
        return None

    if tool_resp.status_code not in (200, 202):
        print(f"[TrueClicks] Unexpected status: {tool_resp.status_code}")
        return None

    # Parse response
    body = tool_resp.text.strip()
    if not body:
        print("[TrueClicks] Empty response body — TrueClicks may send result via SSE not POST body")
        return None

    try:
        data = json.loads(body)
    except Exception as exc:
        print(f"[TrueClicks] JSON parse error: {exc} — body: {body[:200]}")
        return None

    # data is either {"jsonrpc": "2.0", "id": 1, "result": {...}} or the result directly
    if isinstance(data, dict) and "result" in data:
        rows = _parse_trueclicks_rows(data["result"])
    else:
        rows = _parse_trueclicks_rows(data)

    print(f"[TrueClicks] Parsed {len(rows) if rows else 0} rows")
    return rows
