#!/usr/bin/env python3
"""
Stress testing tool – raw HTTP GET requests only.
Hardcoded target, runs for 9000 seconds.
"""

import asyncio
import ssl
import time
import socket
from urllib.parse import urlparse

# ===== HARDCODED CONFIGURATION =====
TARGET_URL = "https://usl.edu.ph"   # CHANGE THIS TO YOUR TARGET
DURATION_SECONDS = 9000              # Run for 9000 seconds
CONCURRENCY = 100                    # Number of parallel workers
CONNECTION_TIMEOUT = 5.0             # Seconds per request timeout
# ===================================

parsed = urlparse(TARGET_URL)
HOST = parsed.hostname
PORT = parsed.port or (443 if parsed.scheme == "https" else 80)
USE_SSL = parsed.scheme == "https"
PATH = parsed.path or "/"
if parsed.query:
    PATH += "?" + parsed.query

# Request line – minimal HTTP/1.0 (closes after response, avoids chunked complexities)
REQUEST = f"GET {PATH} HTTP/1.0\r\nHost: {HOST}\r\nConnection: close\r\n\r\n"

# Global counters
total_requests = 0
success_count = 0
error_count = 0
latencies = []   # store last N latencies for reporting

async def raw_get(session_id: int):
    """Perform a single raw HTTP GET request, return latency or None on error."""
    start = time.monotonic()
    reader = writer = None
    try:
        loop = asyncio.get_running_loop()
        if USE_SSL:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(HOST, PORT, ssl=ssl_ctx),
                timeout=CONNECTION_TIMEOUT
            )
        else:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(HOST, PORT),
                timeout=CONNECTION_TIMEOUT
            )

        writer.write(REQUEST.encode())
        await writer.drain()

        # Read only the status line (raw GET – no body needed)
        status_line = await asyncio.wait_for(reader.readline(), timeout=CONNECTION_TIMEOUT)
        # Decode, check status code (e.g., b'HTTP/1.1 200 OK\r\n')
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

async def worker(worker_id: int, stop_event: asyncio.Event, stats_interval: float):
    """Worker that continuously sends requests until stop_event is set."""
    global total_requests, success_count, error_count, latencies
    while not stop_event.is_set():
        latency, status = await raw_get(worker_id)
        # Update global stats (non‑atomic but fine for a rough stress tool)
        total_requests += 1
        if latency is not None and 200 <= status < 400:
            success_count += 1
            latencies.append(latency)
            # Keep last 1000 latencies to avoid memory bloat
            if len(latencies) > 1000:
                latencies.pop(0)
        else:
            error_count += 1
        # Very small sleep to avoid event loop starvation (adjustable)
        await asyncio.sleep(0)

async def stats_reporter(stop_event: asyncio.Event):
    """Print running statistics every 10 seconds."""
    start_time = time.monotonic()
    last_total = 0
    last_time = start_time
    while not stop_event.is_set():
        await asyncio.sleep(10)
        if stop_event.is_set():
            break
        now = time.monotonic()
        elapsed = now - start_time
        total = total_requests
        successes = success_count
        errors = error_count
        req_since_last = total - last_total
        time_since_last = now - last_time
        current_rps = req_since_last / time_since_last if time_since_last > 0 else 0
        avg_latency = sum(latencies) / len(latencies) if latencies else 0

        print(f"[{elapsed:.0f}s] Total: {total}, OK: {successes}, ERR: {errors}, "
              f"RPS: {current_rps:.1f}, Avg Latency: {avg_latency*1000:.1f}ms")
        last_total = total
        last_time = now

async def main():
    stop_event = asyncio.Event()
    print(f"Stress test starting: {TARGET_URL}")
    print(f"Duration: {DURATION_SECONDS} seconds, Concurrency: {CONCURRENCY} workers\n")

    # Start reporter task
    reporter = asyncio.create_task(stats_reporter(stop_event))

    # Start worker tasks
    workers = [asyncio.create_task(worker(i, stop_event, 10)) for i in range(CONCURRENCY)]

    # Run for the specified duration
    await asyncio.sleep(DURATION_SECONDS)
    stop_event.set()

    # Wait for all workers and reporter to finish
    await asyncio.gather(*workers, reporter, return_exceptions=True)

    # Final summary
    total = total_requests
    successes = success_count
    errors = error_count
    elapsed = DURATION_SECONDS
    avg_rps = total / elapsed if elapsed > 0 else 0
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    print("\n========== FINAL REPORT ==========")
    print(f"Target:        {TARGET_URL}")
    print(f"Duration:      {elapsed:.0f} seconds")
    print(f"Total requests: {total}")
    print(f"Successful:     {successes}")
    print(f"Errors:         {errors}")
    print(f"Average RPS:    {avg_rps:.1f}")
    print(f"Avg latency:    {avg_latency*1000:.1f} ms")
    print("===================================")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
