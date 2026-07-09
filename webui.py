"""生图注册机 Web UI — Flask + SSE 实时进度."""

from __future__ import annotations

import json
import logging
import queue
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import StringIO
from pathlib import Path

import yaml
from flask import Flask, Response, jsonify, render_template_string, request, stream_with_context

from register.registrar import register_worker

# ── Globals ───────────────────────────────────────────────────────────

app = Flask(__name__)
ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.yaml"
TOKENS_PATH = ROOT / "access_tokens.txt"

_task_state: dict = {
    "running": False,
    "total": 0,
    "succeeded": 0,
    "failed": 0,
    "results": [],      # list of {email, token, ok}
    "started_at": "",
    "stop_requested": False,
}

_log_queue: queue.Queue = queue.Queue()
_SSE_CLIENTS: list[queue.Queue] = []
_sse_lock = threading.Lock()


# ── Log handler → SSE ─────────────────────────────────────────────────

class SSELogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        _log_queue.put(msg)
        # Push to all SSE clients
        with _sse_lock:
            for q in _SSE_CLIENTS:
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    pass


handler = SSELogHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-5s] %(message)s", datefmt="%H:%M:%S"))
handler.setLevel(logging.INFO)
# Only attach to our logger, not root (avoids werkzeug log format conflicts)
_logger = logging.getLogger("webui")
_logger.addHandler(handler)
_logger.setLevel(logging.INFO)
_logger.propagate = False


# ── Config helpers ────────────────────────────────────────────────────

def load_config() -> dict:
    defaults = {
        "proxy": {"url": "", "flaresolverr_url": ""},
        "registration": {"threads": 2, "total": 10},
        "mail": {
            "providers": [{
                "type": "outlook_token", "enable": True, "mode": "graph",
                "mailboxes": "",
            }],
            "request_timeout": 30, "wait_timeout": 45, "wait_interval": 3,
        },
        "output_file": "access_tokens.txt",
    }
    if CONFIG_PATH.exists():
        raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            _deep_merge(defaults, raw)
    return defaults


def save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(
        yaml.dump(cfg, default_flow_style=False, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _deep_merge(base: dict, overlay: dict) -> None:
    for k, v in overlay.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def count_mailboxes() -> tuple[int, int, int]:
    """Return (total, available, used)."""
    try:
        cfg = load_config()
        raw = str(cfg.get("mail", {}).get("providers", [{}])[0].get("mailboxes", ""))
        lines = [l for l in raw.split("\n") if "@" in l]
        total = len(lines)
        # Check state file
        state_file = ROOT / "data" / "outlook_token_state.json"
        used = 0
        if state_file.exists():
            state = json.loads(state_file.read_text(encoding="utf-8"))
            used = len(state)
        return total, max(0, total - used), used
    except Exception:
        return 0, 0, 0


# ── Registration runner ───────────────────────────────────────────────

def _run_registration(total: int):
    """Run in background thread."""
    global _task_state
    cfg = load_config()
    proxy = str(cfg.get("proxy", {}).get("url", "")).strip()
    flaresolverr = str(cfg.get("proxy", {}).get("flaresolverr_url", "")).strip()
    mail_config = cfg.get("mail", {})
    threads = int(cfg.get("registration", {}).get("threads", 2))

    _task_state["running"] = True
    _task_state["total"] = total
    _task_state["succeeded"] = 0
    _task_state["failed"] = 0
    _task_state["results"] = []
    _task_state["started_at"] = datetime.now().strftime("%H:%M:%S")
    _task_state["stop_requested"] = False

    _logger.info(f"开始注册 {total} 个账号, {threads} 线程, 代理: {proxy or '(无)'}")

    # Clear old tokens file
    try:
        TOKENS_PATH.write_text("", encoding="utf-8")
    except Exception:
        pass

    with ThreadPoolExecutor(max_workers=max(1, threads)) as executor:
        futures = {
            executor.submit(
                register_worker,
                index=i,
                proxy=proxy,
                flaresolverr_url=flaresolverr,
                mail_config=mail_config,
            ): i
            for i in range(1, total + 1)
        }

        for future in as_completed(futures):
            if _task_state["stop_requested"]:
                _logger.warning("收到停止请求，取消剩余任务...")
                for f in futures:
                    f.cancel()
                break

            idx = futures[future]
            try:
                result = future.result()
            except Exception as e:
                _task_state["failed"] += 1
                _logger.warning(f"[{idx}/{total}] 异常: {e}")
                continue

            if result.get("ok"):
                _task_state["succeeded"] += 1
                at = result.get("result", {}).get("access_token", "")
                email = result.get("result", {}).get("email", "?")
                cost = result.get("cost_seconds", 0)
                _task_state["results"].append({"email": email, "token": at, "ok": True})
                if at:
                    try:
                        with open(TOKENS_PATH, "a", encoding="utf-8") as f:
                            f.write(at + "\n")
                    except Exception:
                        pass
                _logger.info(f"[{idx}/{total}] ✓ {email} ({cost:.1f}s)")
            else:
                _task_state["failed"] += 1
                _logger.warning(f"[{idx}/{total}] ✗ {result.get('error', '?')}")

    _task_state["running"] = False
    s, f_ = _task_state["succeeded"], _task_state["failed"]
    _logger.info(f"完成: 成功 {s}, 失败 {f_}")


# ── Routes — Page ─────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML_PAGE)


# ── Routes — API ──────────────────────────────────────────────────────

@app.route("/api/config", methods=["GET"])
def api_get_config():
    cfg = load_config()
    proxy_url = str(cfg.get("proxy", {}).get("url", "")).strip()
    threads = int(cfg.get("registration", {}).get("threads", 2))
    total = int(cfg.get("registration", {}).get("total", 10))
    return jsonify({"proxy_url": proxy_url, "threads": threads, "total": total})


@app.route("/api/config", methods=["POST"])
def api_set_config():
    data = request.get_json(force=True) or {}
    cfg = load_config()
    if "proxy_url" in data:
        cfg.setdefault("proxy", {})["url"] = str(data["proxy_url"]).strip()
    if "threads" in data:
        cfg.setdefault("registration", {})["threads"] = int(data["threads"])
    if "total" in data:
        cfg.setdefault("registration", {})["total"] = int(data["total"])
    save_config(cfg)
    return jsonify({"ok": True})


@app.route("/api/mailboxes", methods=["GET"])
def api_get_mailboxes():
    total, avail, used = count_mailboxes()
    try:
        cfg = load_config()
        raw = str(cfg.get("mail", {}).get("providers", [{}])[0].get("mailboxes", ""))
        lines = [l.strip() for l in raw.split("\n") if "@" in l]
    except Exception:
        lines = []
    return jsonify({"total": total, "available": avail, "used": used, "sample": lines[:5]})


@app.route("/api/mailboxes", methods=["POST"])
def api_set_mailboxes():
    data = request.get_json(force=True) or {}
    text = str(data.get("mailboxes", ""))
    if "@" not in text:
        return jsonify({"ok": False, "error": "邮箱格式不正确"}), 400
    cfg = load_config()
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    indented = "\n".join("      " + l for l in lines)
    # Rebuild mailboxes YAML
    old = str(cfg.get("mail", {}).get("providers", [{}])[0].get("mailboxes", ""))
    # Replace the entire mailboxes value
    cfg["mail"]["providers"][0]["mailboxes"] = indented.strip()
    save_config(cfg)
    # Reset pool state
    state_file = ROOT / "data" / "outlook_token_state.json"
    try:
        state_file.write_text("{}", encoding="utf-8")
    except Exception:
        pass
    return jsonify({"ok": True, "count": len(lines)})


@app.route("/api/start", methods=["POST"])
def api_start():
    if _task_state["running"]:
        return jsonify({"ok": False, "error": "任务已在运行中"}), 409
    data = request.get_json(force=True) or {}
    total = int(data.get("total") or load_config().get("registration", {}).get("total", 10))
    t = threading.Thread(target=_run_registration, args=(total,), daemon=True)
    t.start()
    return jsonify({"ok": True, "total": total})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    _task_state["stop_requested"] = True
    return jsonify({"ok": True})


@app.route("/api/status")
def api_status():
    """SSE endpoint for real-time status."""
    def stream():
        q: queue.Queue = queue.Queue(maxsize=200)
        with _sse_lock:
            _SSE_CLIENTS.append(q)
        try:
            # Send initial state
            yield f"data: {json.dumps(_status_payload())}\n\n"
            while True:
                try:
                    msg = q.get(timeout=2)
                    yield f"data: {json.dumps(_status_payload(log_line=msg))}\n\n"
                except queue.Empty:
                    # Send heartbeat with current state
                    yield f"data: {json.dumps(_status_payload())}\n\n"
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                if q in _SSE_CLIENTS:
                    _SSE_CLIENTS.remove(q)
    return Response(
        stream_with_context(stream()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _status_payload(log_line: str = "") -> dict:
    return {
        "running": _task_state["running"],
        "total": _task_state["total"],
        "succeeded": _task_state["succeeded"],
        "failed": _task_state["failed"],
        "started_at": _task_state["started_at"],
        "log": log_line,
    }


@app.route("/api/tokens")
def api_download_tokens():
    if not TOKENS_PATH.exists():
        return Response("", mimetype="text/plain")
    return Response(
        TOKENS_PATH.read_text(encoding="utf-8"),
        mimetype="text/plain",
        headers={"Content-Disposition": "attachment; filename=access_tokens.txt"},
    )


# ── HTML Page (inline) ────────────────────────────────────────────────

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>生图注册机</title>
<style>
:root {
  --bg: #0f172a; --card: #1e293b; --border: #334155;
  --text: #e2e8f0; --muted: #94a3b8;
  --accent: #38bdf8; --green: #4ade80; --red: #f87171;
  --orange: #fb923c;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: 'SF Mono', Consolas, monospace; padding: 20px; }
h1 { color: var(--accent); margin-bottom: 20px; font-size: 1.5rem; }
.card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 16px; }
.card h2 { font-size: 0.9rem; color: var(--muted); margin-bottom: 12px; text-transform: uppercase; letter-spacing: 1px; }
.row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
.col { display: flex; flex-direction: column; gap: 4px; flex: 1; min-width: 120px; }
label { font-size: 0.75rem; color: var(--muted); }
input, textarea { background: var(--bg); border: 1px solid var(--border); color: var(--text); padding: 6px 10px; border-radius: 4px; font-family: inherit; font-size: 0.85rem; width: 100%; }
textarea { resize: vertical; min-height: 100px; }
input:focus, textarea:focus { outline: none; border-color: var(--accent); }
.btn { padding: 8px 20px; border-radius: 6px; border: none; cursor: pointer; font-family: inherit; font-size: 0.85rem; font-weight: 600; transition: all 0.2s; }
.btn-start { background: var(--green); color: #000; }
.btn-stop { background: var(--red); color: #fff; }
.btn-dl { background: var(--accent); color: #000; }
.btn-set { background: var(--orange); color: #000; }
.btn:hover { opacity: 0.85; }
.btn:disabled { opacity: 0.4; cursor: not-allowed; }
.progress { display: flex; gap: 20px; margin: 12px 0; font-size: 1.2rem; }
.progress .ok { color: var(--green); }
.progress .fail { color: var(--red); }
.bar-wrap { height: 8px; background: var(--border); border-radius: 4px; overflow: hidden; margin: 8px 0; }
.bar-inner { height: 100%; background: var(--green); transition: width 0.3s; border-radius: 4px; }
.log-window { background: var(--bg); border: 1px solid var(--border); border-radius: 4px; height: 300px; overflow-y: auto; padding: 8px; font-size: 0.78rem; line-height: 1.5; }
.log-window div { white-space: pre-wrap; word-break: break-all; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 0.7rem; }
.badge-info { background: var(--accent); color: #000; }
.badge-ok { background: var(--green); color: #000; }
.badge-warn { background: var(--orange); color: #000; }
.tokens-link { margin-top: 12px; }
</style>
</head>
<body>

<h1>⚡ 生图注册机</h1>

<!-- Config -->
<div class="card">
  <h2>⚙ 配置</h2>
  <div class="row">
    <div class="col" style="flex:2">
      <label>代理地址</label>
      <input id="proxy-url" placeholder="http://127.0.0.1:7897">
    </div>
    <div class="col">
      <label>线程数</label>
      <input id="threads" type="number" value="2" min="1" max="10">
    </div>
    <div class="col">
      <label>注册数量</label>
      <input id="total" type="number" value="10" min="1" max="500">
    </div>
    <div class="col" style="justify-content:flex-end">
      <button class="btn btn-set" onclick="saveConfig()">保存配置</button>
    </div>
  </div>
</div>

<!-- Mailboxes -->
<div class="card">
  <h2>📧 邮箱池 <span id="mail-stats" class="badge badge-info">加载中...</span></h2>
  <textarea id="mailboxes" placeholder="email----password----client_id----refresh_token&#10;一行一个"></textarea>
  <div class="row" style="margin-top:8px">
    <button class="btn btn-set" onclick="saveMailboxes()">更新邮箱池</button>
  </div>
</div>

<!-- Control -->
<div class="card">
  <h2>🚀 任务控制</h2>
  <div class="progress">
    <span>进度: <span id="pct">0%</span></span>
    <span class="ok">✅ <span id="ok-count">0</span></span>
    <span class="fail">❌ <span id="fail-count">0</span></span>
    <span>⏱ <span id="elapsed">00:00</span></span>
  </div>
  <div class="bar-wrap"><div id="bar" class="bar-inner" style="width:0%"></div></div>
  <div class="row">
    <button id="btn-start" class="btn btn-start" onclick="startTask()">▶ 开始注册</button>
    <button id="btn-stop" class="btn btn-stop" onclick="stopTask()" disabled>⏹ 停止</button>
    <a id="dl-link" class="btn btn-dl" href="/api/tokens" style="display:none">⬇ 下载 Token</a>
  </div>
  <div class="log-window" id="log-window"><div style="color:var(--muted)">等待任务开始...</div></div>
</div>

<script>
const $ = id => document.getElementById(id);
let es = null, startTime = 0;

function log(msg) {
  const w = $('log-window');
  const d = document.createElement('div');
  const t = new Date().toLocaleTimeString('zh-CN', {hour12:false});
  if (msg.includes('[WARNING]')) d.style.color = 'var(--orange)';
  else if (msg.includes('✓')) d.style.color = 'var(--green)';
  else if (msg.includes('✗')) d.style.color = 'var(--red)';
  d.textContent = msg;
  w.appendChild(d);
  w.scrollTop = w.scrollHeight;
  // Keep max 500 lines
  while (w.children.length > 500) w.firstChild.remove();
}

async function loadConfig() {
  const r = await fetch('/api/config');
  const c = await r.json();
  $('proxy-url').value = c.proxy_url || '';
  $('threads').value = c.threads;
  $('total').value = c.total;
}

async function saveConfig() {
  await fetch('/api/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      proxy_url: $('proxy-url').value.trim(),
      threads: parseInt($('threads').value),
      total: parseInt($('total').value),
    }),
  });
  log('配置已保存');
}

async function loadMailboxes() {
  const r = await fetch('/api/mailboxes');
  const m = await r.json();
  $('mail-stats').textContent = `总数: ${m.total} | 可用: ${m.available} | 已用: ${m.used}`;
  if (m.sample && m.sample.length) $('mailboxes').placeholder = m.sample.slice(0,3).join('\n') + '\n...';
}

async function saveMailboxes() {
  const text = $('mailboxes').value.trim();
  if (!text.includes('@')) { log('邮箱格式错误'); return; }
  await fetch('/api/mailboxes', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({mailboxes: text}),
  });
  $('mailboxes').value = '';
  log('邮箱池已更新');
  loadMailboxes();
}

function startTask() {
  const total = parseInt($('total').value) || 10;
  fetch('/api/start', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({total}),
  }).then(r => r.json()).then(d => {
    if (d.ok) {
      $('btn-start').disabled = true;
      $('btn-stop').disabled = false;
      $('dl-link').style.display = 'none';
      startTime = Date.now();
      log('任务已启动: ' + d.total + ' 个账号');
      connectSSE();
    } else {
      log('启动失败: ' + (d.error || '未知错误'));
    }
  });
}

function stopTask() {
  fetch('/api/stop', {method:'POST'}).then(() => log('已发送停止请求...'));
}

function connectSSE() {
  if (es) es.close();
  es = new EventSource('/api/status');
  es.onmessage = e => {
    const d = JSON.parse(e.data);
    const total = d.total || 1;
    const done = d.succeeded + d.failed;
    const pct = Math.round(done / total * 100);
    $('ok-count').textContent = d.succeeded;
    $('fail-count').textContent = d.failed;
    $('pct').textContent = pct + '%';
    $('bar').style.width = pct + '%';
    if (startTime > 0) {
      const secs = Math.floor((Date.now() - startTime) / 1000);
      $('elapsed').textContent = String(Math.floor(secs/60)).padStart(2,'0') + ':' + String(secs%60).padStart(2,'0');
    }
    if (d.log) log(d.log);
    if (!d.running && done > 0) {
      $('btn-start').disabled = false;
      $('btn-stop').disabled = true;
      if (d.succeeded > 0) $('dl-link').style.display = 'inline-block';
      log('任务结束');
      es.close();
    }
  };
  es.onerror = () => { /* reconnect automatically */ };
}

window.onload = () => { loadConfig(); loadMailboxes(); };
</script>
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────────

def main():
    import argparse
    p = argparse.ArgumentParser(description="生图注册机 Web UI")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=5800)
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    print(f"\n  ⚡ 生图注册机 Web UI")
    print(f"  地址: http://{args.host}:{args.port}")
    print(f"  按 Ctrl+C 停止\n")

    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()
