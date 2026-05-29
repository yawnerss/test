#!/usr/bin/env python3
"""
Flask web interface for the raw HTTP GET stress tester.
Run on a port of your choice (default 5000).
"""

import asyncio
import ssl
import time
import threading
from flask import Flask, render_template_string, request, jsonify
from urllib.parse import urlparse

# ---------- Hardcoded settings ----------
DURATION_SECONDS = 9000           # Test runs for 9000 seconds when launched
CONCURRENCY = 100                 # Number of parallel workers
CONNECTION_TIMEOUT = 5.0          # Seconds per request timeout
# ----------------------------------------

# Global state for the running test
test_running = False
test_thread = None
stats = {
    "total": 0,
    "success": 0,
    "error": 0,
    "avg_latency_ms": 0,
    "current_rps": 0,
    "target": "",
    "running": False,
    "log": []                      # store last 200 log lines
}

app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Stress Tester – Raw HTTP GET</title>
    <style>
        body { font-family: monospace; margin: 2em; background: #f5f5f5; }
        .container { max-width: 900px; margin: auto; background: white; padding: 2em; border-radius: 8px; }
        input, button { padding: 0.5em; font-size: 1em; }
        input { width: 70%; }
        button { margin-left: 1em; cursor: pointer; background: #007bff; color: white; border: none; border-radius: 4px; }
        button:disabled { background: #aaa; cursor: not-allowed; }
        .stats { background: #e9ecef; padding: 1em; border-radius: 5px; margin: 1em 0; }
        .log { background: #212529; color: #0f0; padding: 1em; border-radius: 5px; height: 300px; overflow-y: scroll; font-size: 0.85em; }
        .error { color: red; }
        .ok { color: green; }
    </style>
    <script>
        function updateStats() {
            fetch('/api/status')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('total').innerText = data.total;
                    document.getElementById('success').innerText = data.success;
                    document.getElementById('error').innerText = data.error;
                    document.getElementById('rps').innerText = data.current_rps;
                    document.getElementById('latency').innerText = data.avg_latency_ms;
                    document.getElementById('target_display').innerText = data.target;
                    document.getElementById('running_status').innerHTML = data.running ? '<span class="ok">RUNNING</span>' : '<span class="error">IDLE</span>';
                    if (!data.running) {
                        document.getElementById('launchBtn').disabled = false;
                    } else {
                        document.getElementById('launchBtn').disabled = true;
                    }
                });
        }
        function updateLog() {
            fetch('/api/log')
                .then(response => response.json())
                .then(data => {
                    let logDiv = document.getElementById('log');
                    logDiv.innerHTML = data.log.map(l => l + '<br>').join('');
                    logDiv.scrollTop = logDiv.scrollHeight;
                });
        }
        function launchTest() {
            let target = document.getElementById('target_url').value;
            if (!target) {
                alert('Please enter a target URL (e.g., http://example.com/)');
                return;
            }
            fetch('/api/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({target: target})
            })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'started') {
                    document.getElementById('launchBtn').disabled = true;
                    updateStats();
                } else {
                    alert('Error: ' + data.message);
                }
            });
        }
        setInterval(updateStats, 1000);
        setInterval(updateLog, 800);
        window.onload = () => {
            updateStats();
            updateLog();
        };
    </script>
</head>
<body>
<div class="container">
    <h1>💥 Raw HTTP GET Stress Tester</h1>
    <p>Target: <input type="text" id="target_url" placeholder="http:// or https://..." style="width: 70%">
    <button id="launchBtn" onclick="launchTest()">Launch 9000s Test</button></p>
    <div class="stats">
        <strong>Status:</strong> <span id="running_status">IDLE</span><br>
        <strong>Target:</strong> <span id="target_display">-</span><br>
        <strong>Total requests:</strong> <span id="total">0</span><br>
        <strong>Successful:</strong> <span id="success">0</span> &nbsp;|&nbsp;
        <strong>Errors:</strong> <span id="error">0</span><br>
        <strong>Current RPS:</strong> <span id="rps">0.0</span> &nbsp;|&nbsp;
        <strong>Avg Latency:</strong> <span id="latency">0.0</span> ms
    </div>
    <div class="log" id="log">Waiting for test...</div>
    <p><small>⚠️ The test will run for exactly 9000 seconds (2.5 hours) once launched.<br>
    Concurrency: 100 workers, raw sockets (no certificate verification for HTTPS).</small></p>
</div>
</body>
</html>
"""

def add_log(msg):
    """Store log lines (last 200)"""
    stats["log"].append(f"[{time.strftime('%H:%M:%S')}] {msg}")
    if len(stats["log"]) > 200:
        stats["log"].pop(0)
    print(msg)  # also print to console

# -----------------------------------------------------------------
# Stress test engine (identical to previous, but adapted for logging)
# -----------------------------------------------------------------
async def raw_get(request_id, host, port, use_ssl, path, timeout):
    start = time.monotonic()
    reader = writer = None
    try:
        if use_ssl:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=ssl_ctx),
                timeout=timeout
            )
        else:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )
        request_line = f"GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n"
        writer.write(request_line.encode())
        await writer.drain()
        status_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        status_code = 0
        if status_line:
            parts = status_line.split()
            if len(parts) >= 2:
                status_code = int(parts[1])
        elapsed = time.monotonic() - start
        return elapsed, status_code
    except Exception:
        return None, 0
    finally:
        if writer:
            writer.close()
            await writer.wait_closed()

async def stress_worker(worker_id, stop_event, host, port, use_ssl, path, timeout, stats_local):
    """Worker that pushes updates into a shared stats dict"""
    while not stop_event.is_set():
        latency, status = await raw_get(worker_id, host, port, use_ssl, path, timeout)
        stats_local["total"] += 1
        if latency is not None and 200 <= status < 400:
            stats_local["success"] += 1
            # update moving average latency
            total_success = stats_local["success"]
            old_avg = stats_local["avg_latency_ms"]
            new_avg = old_avg + (latency*1000 - old_avg) / total_success
            stats_local["avg_latency_ms"] = new_avg
            add_log(f"Req #{stats_local['total']} -> {status} OK ({latency*1000:.1f}ms)")
        else:
            stats_local["error"] += 1
            add_log(f"Req #{stats_local['total']} -> FAILED (timeout/error)")
        await asyncio.sleep(0)

async def stress_runner(target_url, duration_sec, concurrency, timeout):
    """Main async routine that runs the test and updates RPS."""
    parsed = urlparse(target_url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    use_ssl = parsed.scheme == "https"
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query

    stop_event = asyncio.Event()
    stats_local = {
        "total": 0,
        "success": 0,
        "error": 0,
        "avg_latency_ms": 0.0,
    }

    # Launch workers
    workers = [
        asyncio.create_task(stress_worker(i, stop_event, host, port, use_ssl, path, timeout, stats_local))
        for i in range(concurrency)
    ]

    # RPS reporter (updates global stats every second)
    start_time = time.monotonic()
    last_total = 0
    last_time = start_time
    while time.monotonic() - start_time < duration_sec and not stop_event.is_set():
        await asyncio.sleep(1)
        now = time.monotonic()
        total_now = stats_local["total"]
        delta_req = total_now - last_total
        delta_time = now - last_time
        current_rps = delta_req / delta_time if delta_time > 0 else 0
        # Update global stats for web UI
        stats["total"] = stats_local["total"]
        stats["success"] = stats_local["success"]
        stats["error"] = stats_local["error"]
        stats["avg_latency_ms"] = stats_local["avg_latency_ms"]
        stats["current_rps"] = current_rps
        last_total = total_now
        last_time = now

    # Stop workers and wait
    stop_event.set()
    await asyncio.gather(*workers, return_exceptions=True)

    # Final update
    stats["total"] = stats_local["total"]
    stats["success"] = stats_local["success"]
    stats["error"] = stats_local["error"]
    stats["avg_latency_ms"] = stats_local["avg_latency_ms"]
    stats["current_rps"] = 0
    add_log(f"Test finished. Total: {stats_local['total']}, OK: {stats_local['success']}, ERR: {stats_local['error']}")

def run_stress_test(target_url):
    """Run the async stress test in a new event loop (called in a background thread)."""
    global test_running, stats
    stats["running"] = True
    stats["target"] = target_url
    stats["total"] = 0
    stats["success"] = 0
    stats["error"] = 0
    stats["avg_latency_ms"] = 0
    stats["current_rps"] = 0
    stats["log"] = []
    add_log(f"🚀 Launching stress test on {target_url} for {DURATION_SECONDS} seconds")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(stress_runner(target_url, DURATION_SECONDS, CONCURRENCY, CONNECTION_TIMEOUT))
    except Exception as e:
        add_log(f"Error: {e}")
    finally:
        loop.close()
        test_running = False
        stats["running"] = False
        add_log("Test stopped.")

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/status')
def api_status():
    return jsonify({
        "total": stats["total"],
        "success": stats["success"],
        "error": stats["error"],
        "avg_latency_ms": round(stats["avg_latency_ms"], 1),
        "current_rps": round(stats["current_rps"], 1),
        "target": stats["target"],
        "running": stats["running"]
    })

@app.route('/api/log')
def api_log():
    return jsonify({"log": stats["log"]})

@app.route('/api/start', methods=['POST'])
def api_start():
    global test_running, test_thread
    if test_running:
        return jsonify({"status": "error", "message": "Test already running"}), 400
    data = request.get_json()
    target = data.get("target", "").strip()
    if not target or (not target.startswith("http://") and not target.startswith("https://")):
        return jsonify({"status": "error", "message": "Invalid URL. Use http:// or https://"}), 400
    # Validate host resolution
    try:
        parsed = urlparse(target)
        if not parsed.hostname:
            raise ValueError
    except:
        return jsonify({"status": "error", "message": "Invalid URL format"}), 400

    test_running = True
    test_thread = threading.Thread(target=run_stress_test, args=(target,), daemon=True)
    test_thread.start()
    return jsonify({"status": "started", "message": f"Test launched on {target}"})

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
