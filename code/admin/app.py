from __future__ import annotations

import logging
import os
import sys

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

# allow `python -m admin.app` from /app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import DBManager  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
)
logger = logging.getLogger("admin")

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "/data/data.db")
db = DBManager(DB_PATH, init_schema=True)

app = FastAPI(title="DMScraper", docs_url="/api/docs", redoc_url=None)


@app.get("/api/health")
def health() -> dict:
    return {"ok": True}


@app.get("/api/mappings")
def mappings() -> JSONResponse:
    rows = db.all_dm_mappings()
    return JSONResponse([dict(r) for r in rows])


@app.get("/api/backfill")
def backfill() -> JSONResponse:
    rows = db.backfill_progress_summary()
    return JSONResponse([dict(r) for r in rows])


@app.get("/api/config")
def get_config() -> JSONResponse:
    return JSONResponse(db.get_all_config())


@app.put("/api/config/{key}")
def put_config(key: str, body: dict) -> dict:
    value = str(body.get("value", ""))
    db.set_config(key, value)
    return {"ok": True}


_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>DMScraper</title>
<style>
  :root { color-scheme: dark; }
  body { font-family: ui-sans-serif, system-ui, Segoe UI, Roboto, sans-serif;
         background:#0e1116; color:#e6edf3; margin:0; padding:24px; }
  h1 { margin:0 0 8px; }
  .sub { color:#8b949e; margin-bottom:24px; }
  table { border-collapse:collapse; width:100%; margin-bottom:32px;
          background:#161b22; border:1px solid #30363d; border-radius:6px; overflow:hidden; }
  th, td { padding:10px 12px; text-align:left; border-bottom:1px solid #21262d; font-size:13px; }
  th { background:#0d1117; color:#8b949e; font-weight:600; text-transform:uppercase; font-size:11px; letter-spacing:0.5px; }
  tr:last-child td { border-bottom:none; }
  .progress-bar { background:#21262d; border-radius:3px; height:6px; overflow:hidden; min-width:120px; }
  .progress-fill { background:#3fb950; height:100%; transition:width .3s; }
  .status-running { color:#d29922; }
  .status-completed { color:#3fb950; }
  .status-failed { color:#f85149; }
  code { background:#21262d; padding:2px 6px; border-radius:3px; font-size:12px; }
  .empty { color:#8b949e; padding:16px; text-align:center; }
</style>
</head>
<body>
  <h1>DMScraper</h1>
  <div class="sub">Mirroring DMs into Discord. Refreshes every 5s.</div>

  <h2>Backfill progress</h2>
  <table id="backfill-table">
    <thead><tr>
      <th>Partner</th><th>Status</th><th>Delivered</th><th>Expected</th>
      <th>Progress</th><th>Last message id</th><th>Started</th>
    </tr></thead>
    <tbody><tr><td colspan="7" class="empty">Loading…</td></tr></tbody>
  </table>

  <h2>DM mappings</h2>
  <table id="mappings-table">
    <thead><tr>
      <th>Partner</th><th>Type</th><th>Original channel</th>
      <th>Cloned channel</th><th>Webhook</th><th>Created</th>
    </tr></thead>
    <tbody><tr><td colspan="6" class="empty">Loading…</td></tr></tbody>
  </table>

<script>
function fmtDate(unix) {
  if (!unix) return '';
  return new Date(unix * 1000).toLocaleString();
}
function pct(d, t) {
  if (!t || t <= 0) return '';
  const p = Math.min(100, Math.round((d / t) * 100));
  return `<div class="progress-bar"><div class="progress-fill" style="width:${p}%"></div></div>`;
}
async function refresh() {
  try {
    const [bf, mp] = await Promise.all([
      fetch('/api/backfill').then(r => r.json()),
      fetch('/api/mappings').then(r => r.json()),
    ]);
    const bfBody = document.querySelector('#backfill-table tbody');
    bfBody.innerHTML = bf.length ? bf.map(r => `
      <tr>
        <td>${r.partner_label || '<unknown>'}</td>
        <td class="status-${r.status}">${r.status}</td>
        <td>${r.delivered}</td>
        <td>${r.expected_total ?? ''}</td>
        <td>${pct(r.delivered, r.expected_total)}</td>
        <td><code>${r.last_orig_message_id ?? ''}</code></td>
        <td>${fmtDate(r.started_at)}</td>
      </tr>`).join('') : '<tr><td colspan="7" class="empty">No backfills recorded yet.</td></tr>';

    const mpBody = document.querySelector('#mappings-table tbody');
    mpBody.innerHTML = mp.length ? mp.map(r => `
      <tr>
        <td>${r.partner_label || '<unknown>'}</td>
        <td>${r.is_group ? 'Group' : 'DM'}</td>
        <td><code>${r.original_channel_id}</code></td>
        <td><code>${r.cloned_channel_id ?? ''}</code></td>
        <td>${r.channel_webhook_url ? '✓' : '—'}</td>
        <td>${fmtDate(r.created_at)}</td>
      </tr>`).join('') : '<tr><td colspan="6" class="empty">No DMs mirrored yet.</td></tr>';
  } catch (e) {
    console.error(e);
  }
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(_HTML)


def main() -> None:
    import uvicorn
    port = int(os.getenv("ADMIN_PORT", "6767"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    main()
