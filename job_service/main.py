from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import aio_pika
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app

from common.database import connection, wait_for_postgres
from common.events import event
from common.messaging import (
    connect_with_retry,
    decode,
    declare_topology,
    publish,
)
from common.storage import upload_file, wait_for_storage
from common.text_merge import merge_overlapping_text


MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(200 * 1024 * 1024)))
MAX_ACTIVE_JOBS = int(os.getenv("MAX_ACTIVE_JOBS", "20"))
ALLOWED_EXTENSIONS = {".wav"}

HTTP_REQUESTS = Counter(
    "job_http_requests_total", "HTTP requests handled by Job Service", ["method", "path", "status"]
)
HTTP_LATENCY = Histogram(
    "job_http_request_duration_seconds", "Job Service HTTP latency", ["method", "path"]
)
JOBS_CREATED = Counter("jobs_created_total", "Jobs accepted for processing")
JOBS_COMPLETED = Counter("jobs_completed_total", "Jobs reaching a terminal state", ["status"])
ACTIVE_JOBS = Gauge("jobs_active", "Jobs currently queued or processing")
JOB_DURATION = Histogram("job_duration_seconds", "End-to-end job duration")


def initialise_database() -> None:
    wait_for_postgres()
    schema = Path(__file__).with_name("schema.sql").read_text()
    with connection() as conn:
        conn.execute(schema)
        conn.commit()


def active_job_count() -> int:
    with connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM jobs WHERE status IN ('QUEUED','TRANSCRIBING','TRANSLATING')"
        ).fetchone()
        return int(row["count"])


def insert_job(job: dict[str, Any]) -> None:
    uploaded = event(
        "job.uploaded",
        job_id=job["id"],
        object_name=job["object_name"],
        filename=job["filename"],
        source_language=job["source_language"],
        target_language=job["target_language"],
    )
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO jobs (id, filename, source_language, target_language, object_name, status)
            VALUES (%s, %s, %s, %s, %s, 'QUEUED')
            """,
            (
                job["id"],
                job["filename"],
                job["source_language"],
                job["target_language"],
                job["object_name"],
            ),
        )
        conn.execute(
            "INSERT INTO job_outbox (routing_key, payload) VALUES ('job.uploaded', %s)",
            (json.dumps(uploaded),),
        )
        conn.commit()


def get_job(job_id: str) -> dict[str, Any] | None:
    with connection() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        for key in ("created_at", "updated_at", "completed_at"):
            if result.get(key):
                result[key] = result[key].isoformat()
        return result


def get_segments(job_id: str) -> list[dict[str, Any]]:
    with connection() as conn:
        rows = conn.execute(
            "SELECT * FROM transcript_segments WHERE job_id = %s ORDER BY segment_number",
            (job_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def queue_translation_retry(job_id: str) -> bool:
    with connection() as conn:
        row = conn.execute(
            "SELECT transcript, source_language, target_language FROM jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
        if not row or not row["transcript"]:
            return False
        payload = event(
            "transcript.completed",
            job_id=job_id,
            transcript=row["transcript"],
            source_language=row["source_language"],
            target_language=row["target_language"],
        )
        conn.execute(
            "UPDATE jobs SET status='TRANSLATING', error=NULL, updated_at=NOW() WHERE id=%s",
            (job_id,),
        )
        conn.execute(
            "INSERT INTO job_outbox (routing_key, payload) VALUES ('transcript.completed', %s)",
            (json.dumps(payload),),
        )
        conn.commit()
        return True


def _try_finalize_transcript(conn, job_id: str) -> None:
    job = conn.execute("SELECT * FROM jobs WHERE id=%s FOR UPDATE", (job_id,)).fetchone()
    if not job or not job["total_segments"] or job["status"] in ("TRANSLATING", "DONE", "PARTLY_DONE", "FAILED"):
        return
    rows = conn.execute(
        "SELECT * FROM transcript_segments WHERE job_id=%s ORDER BY segment_number",
        (job_id,),
    ).fetchall()
    if len(rows) < job["total_segments"]:
        return
    parts: list[str] = []
    failed = 0
    for row in rows:
        if row["status"] == "DONE":
            parts.append(row["text"] or "")
        else:
            failed += 1
            parts.append(
                f"[TRANSCRIPTION FAILED {row['start_ms'] / 1000:.1f}s-{row['end_ms'] / 1000:.1f}s]"
            )
    transcript = merge_overlapping_text(parts)
    transcription_models = sorted(
        {row["model_version"] for row in rows if row.get("model_version")}
    )
    if failed == len(rows):
        conn.execute(
            "UPDATE jobs SET status='FAILED', failed_segments=%s, error='All segments failed', updated_at=NOW(), completed_at=NOW() WHERE id=%s",
            (failed, job_id),
        )
        return
    completed = len(rows) - failed
    payload = event(
        "transcript.completed",
        job_id=job_id,
        transcript=transcript,
        source_language=job["source_language"],
        target_language=job["target_language"],
    )
    conn.execute(
        """
        UPDATE jobs SET status='TRANSLATING', transcript=%s, completed_segments=%s,
        failed_segments=%s, model_versions=model_versions || %s::jsonb,
        updated_at=NOW() WHERE id=%s
        """,
        (
            transcript,
            completed,
            failed,
            json.dumps({"transcription_models": transcription_models}),
            job_id,
        ),
    )
    conn.execute(
        "INSERT INTO job_outbox (routing_key, payload) VALUES ('transcript.completed', %s)",
        (json.dumps(payload),),
    )


def handle_event(payload: dict[str, Any]) -> str | None:
    event_id = payload["event_id"]
    event_type = payload["event_type"]
    job_id = payload.get("job_id")
    terminal_status: str | None = None
    with connection() as conn:
        inserted = conn.execute(
            "INSERT INTO job_inbox (event_id) VALUES (%s) ON CONFLICT DO NOTHING RETURNING event_id",
            (event_id,),
        ).fetchone()
        if not inserted:
            conn.commit()
            return None

        if event_type == "segmentation.completed":
            conn.execute(
                "UPDATE jobs SET status='TRANSCRIBING', total_segments=%s, updated_at=NOW() WHERE id=%s",
                (payload["total_segments"], job_id),
            )
            _try_finalize_transcript(conn, job_id)
        elif event_type in ("segment.transcribed", "segment.failed"):
            status = "DONE" if event_type == "segment.transcribed" else "FAILED"
            conn.execute(
                """
                INSERT INTO transcript_segments
                    (job_id, segment_number, start_ms, end_ms, status, text, error, model_version, processing_seconds)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (job_id, segment_number) DO UPDATE SET
                    status=EXCLUDED.status, text=EXCLUDED.text, error=EXCLUDED.error,
                    model_version=EXCLUDED.model_version, processing_seconds=EXCLUDED.processing_seconds
                """,
                (
                    job_id,
                    payload["segment_number"],
                    payload["start_ms"],
                    payload["end_ms"],
                    status,
                    payload.get("text"),
                    payload.get("error"),
                    payload.get("model_version"),
                    payload.get("processing_seconds"),
                ),
            )
            _try_finalize_transcript(conn, job_id)
        elif event_type == "translation.completed":
            row = conn.execute("SELECT failed_segments, created_at FROM jobs WHERE id=%s", (job_id,)).fetchone()
            terminal_status = "PARTLY_DONE" if row and row["failed_segments"] else "DONE"
            conn.execute(
                """
                UPDATE jobs SET status=%s, translation=%s,
                model_versions=model_versions || %s::jsonb, updated_at=NOW(), completed_at=NOW()
                WHERE id=%s
                """,
                (
                    terminal_status,
                    payload["translation"],
                    json.dumps(
                        {
                            "translation_model": payload["model_version"],
                            "prompt_version": payload.get("prompt_version", "not-applicable"),
                        }
                    ),
                    job_id,
                ),
            )
        elif event_type == "translation.failed":
            terminal_status = "PARTLY_DONE"
            conn.execute(
                "UPDATE jobs SET status='PARTLY_DONE', error=%s, updated_at=NOW(), completed_at=NOW() WHERE id=%s",
                (f"Translation unavailable: {payload.get('error', 'unknown error')}", job_id),
            )
        elif event_type == "pipeline.failed":
            terminal_status = "FAILED"
            conn.execute(
                "UPDATE jobs SET status='FAILED', error=%s, updated_at=NOW(), completed_at=NOW() WHERE id=%s",
                (payload.get("error", "Pipeline failed"), job_id),
            )
        conn.commit()
    return terminal_status


async def outbox_loop(channel: aio_pika.abc.AbstractChannel) -> None:
    while True:
        try:
            with connection() as conn:
                rows = conn.execute(
                    "SELECT id, routing_key, payload FROM job_outbox WHERE published_at IS NULL ORDER BY id LIMIT 20"
                ).fetchall()
            for row in rows:
                payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
                await publish(channel, row["routing_key"], payload)
                with connection() as conn:
                    conn.execute("UPDATE job_outbox SET published_at=NOW() WHERE id=%s", (row["id"],))
                    conn.commit()
        except Exception:
            await asyncio.sleep(2)
        await asyncio.sleep(0.5)


async def consume_events(channel: aio_pika.abc.AbstractChannel) -> None:
    queue = await channel.get_queue("job.events")

    async def callback(message: aio_pika.IncomingMessage) -> None:
        async with message.process(requeue=True):
            payload = decode(message)
            status = await asyncio.to_thread(handle_event, payload)
            if status:
                JOBS_COMPLETED.labels(status=status).inc()
                ACTIVE_JOBS.set(await asyncio.to_thread(active_job_count))
                job = await asyncio.to_thread(get_job, payload["job_id"])
                if job and job.get("created_at"):
                    created = __import__("datetime").datetime.fromisoformat(job["created_at"])
                    JOB_DURATION.observe(max(0, time.time() - created.timestamp()))

    await queue.consume(callback)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(initialise_database)
    app.state.storage = await asyncio.to_thread(wait_for_storage)
    app.state.rabbit = await connect_with_retry()
    channel = await app.state.rabbit.channel()
    await channel.set_qos(prefetch_count=20)
    await declare_topology(channel)
    app.state.channel = channel
    app.state.outbox_task = asyncio.create_task(outbox_loop(channel))
    await consume_events(channel)
    yield
    app.state.outbox_task.cancel()
    await app.state.rabbit.close()


app = FastAPI(title="Job Service", version="1.0.0", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    route = request.url.path
    HTTP_REQUESTS.labels(request.method, route, response.status_code).inc()
    HTTP_LATENCY.labels(request.method, route).observe(time.perf_counter() - start)
    return response


@app.get("/health")
def health():
    return {"status": "ok", "service": "job-service"}


@app.post("/jobs", status_code=202)
async def create_job(
    request: Request,
    file: UploadFile = File(...),
    source_language: str = Form("en"),
    target_language: str = Form("zh"),
):
    if (source_language, target_language) != ("en", "zh"):
        raise HTTPException(400, "The assessed version supports English to Chinese only")
    filename = Path(file.filename or "audio").name
    extension = Path(filename).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, f"Supported formats: {sorted(ALLOWED_EXTENSIONS)}")
    active = await asyncio.to_thread(active_job_count)
    ACTIVE_JOBS.set(active)
    if active >= MAX_ACTIVE_JOBS:
        raise HTTPException(429, "The system is busy. Please try again later.")

    job_id = str(uuid4())
    object_name = f"source/{job_id}/{filename}"
    size = 0
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as temp:
            temp_path = temp.name
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(413, "Audio file exceeds the 200 MB limit")
                temp.write(chunk)
        await asyncio.to_thread(
            upload_file,
            request.app.state.storage,
            object_name,
            temp_path,
            file.content_type or "application/octet-stream",
        )
        await asyncio.to_thread(
            insert_job,
            {
                "id": job_id,
                "filename": filename,
                "source_language": source_language,
                "target_language": target_language,
                "object_name": object_name,
            },
        )
        JOBS_CREATED.inc()
        ACTIVE_JOBS.set(active + 1)
        return {"job_id": job_id, "status": "QUEUED", "message": "File accepted for processing"}
    finally:
        if temp_path:
            Path(temp_path).unlink(missing_ok=True)


@app.get("/jobs/{job_id}")
async def read_job(job_id: str):
    job = await asyncio.to_thread(get_job, job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@app.get("/jobs/{job_id}/segments")
async def read_segments(job_id: str):
    if not await asyncio.to_thread(get_job, job_id):
        raise HTTPException(404, "Job not found")
    return await asyncio.to_thread(get_segments, job_id)


@app.post("/jobs/{job_id}/retry-translation", status_code=202)
async def retry_translation(job_id: str):
    if not await asyncio.to_thread(queue_translation_retry, job_id):
        raise HTTPException(409, "No completed transcript is available")
    return {"job_id": job_id, "status": "TRANSLATING"}


@app.get("/", response_class=HTMLResponse)
def index():
    return """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Audio Transcription and Translation</title>
<style>
:root{color-scheme:light;font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#172033;background:#f4f7fb}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:linear-gradient(145deg,#eef5ff 0%,#f8f5ff 52%,#f7fafc 100%)}
.page{width:min(900px,calc(100% - 32px));margin:0 auto;padding:56px 0 72px}
h1{margin:0;font-size:clamp(30px,5vw,46px);line-height:1.1;letter-spacing:-.035em}
.card{margin-top:26px;background:rgba(255,255,255,.9);border:1px solid #e1e7f0;border-radius:18px;box-shadow:0 16px 45px rgba(36,50,83,.08);padding:24px}
.upload{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
input[type=file]{flex:1;min-width:250px;padding:14px;border:1px dashed #aeb9cc;border-radius:12px;background:#f8faff;color:#526072}
button{border:0;border-radius:11px;padding:14px 22px;background:#5266c9;color:white;font:inherit;font-weight:700;cursor:pointer;transition:.18s ease}
button:hover{background:#4356b2;transform:translateY(-1px)}
button:disabled{opacity:.55;cursor:wait;transform:none}
.hint{margin:14px 0 0;color:#788396;font-size:13px}
.progress{display:flex;align-items:center;gap:8px;margin:22px 0 0;padding-top:18px;border-top:1px solid #e8ecf3}
.step{flex:1;height:5px;border-radius:99px;background:#dfe4ed;transition:.25s ease}.step.active{background:#5266c9}
.status-row{display:flex;align-items:center;gap:10px;margin:18px 0 0;min-height:30px;color:#566175}
.status-badge{display:none;padding:6px 11px;border-radius:999px;background:#e9edff;color:#4052ae;font-size:13px;font-weight:750}
.status-badge.visible{display:inline-block}.status-badge.error{background:#ffebed;color:#ae3743}.status-badge.done{background:#e7f7ef;color:#18724a}
.results{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:22px}
.result-card{min-height:220px;background:rgba(255,255,255,.86);border:1px solid #e1e7f0;border-radius:16px;padding:22px;box-shadow:0 10px 32px rgba(36,50,83,.05)}
.result-card h2{margin:0 0 14px;font-size:17px}.language{float:right;color:#8791a2;font-size:12px;font-weight:600}
pre{margin:0;white-space:pre-wrap;word-break:break-word;font:15px/1.7 inherit;color:#354054}
.empty{color:#a1a9b6}
@media(max-width:680px){.page{padding-top:34px}.results{grid-template-columns:1fr}.result-card{min-height:170px}button{width:100%}}
</style>
</head>
<body><main class="page">
<h1>Audio Transcription & Translation</h1>

<section class="card">
  <form id="form" class="upload">
    <input id="file" name="file" type="file" accept="audio/wav,.wav" required>
    <input type="hidden" name="source_language" value="en">
    <input type="hidden" name="target_language" value="zh">
    <button id="submit" type="submit">Upload and Process</button>
  </form>
  <p class="hint">WAV only · Up to 30 minutes · Maximum 200 MB</p>
  <div class="progress" aria-label="Processing progress"><span class="step"></span><span class="step"></span><span class="step"></span></div>
  <div class="status-row"><span id="statusBadge" class="status-badge"></span><span id="statusText">Select an audio file</span></div>
</section>

<section class="results">
  <article class="result-card"><h2>English Transcript</h2><pre id="transcript" class="empty">Waiting</pre></article>
  <article class="result-card"><h2>Chinese Translation</h2><pre id="translation" class="empty">Waiting</pre></article>
</section>
</main>
<script>
const form=document.querySelector('#form'),button=document.querySelector('#submit');
const badge=document.querySelector('#statusBadge'),statusText=document.querySelector('#statusText');
const transcript=document.querySelector('#transcript'),translation=document.querySelector('#translation');
const steps=[...document.querySelectorAll('.step')];
const terminal=['DONE','PARTLY_DONE','FAILED'];
const labels={QUEUED:'Queued',TRANSCRIBING:'Transcribing',TRANSLATING:'Translating',DONE:'Done',PARTLY_DONE:'Partly done',FAILED:'Failed'};

function showStatus(state,error=''){
  const levels={QUEUED:1,TRANSCRIBING:1,TRANSLATING:2,DONE:3,PARTLY_DONE:3,FAILED:3};
  steps.forEach((step,index)=>step.classList.toggle('active',index<(levels[state]||0)));
  badge.textContent=labels[state]||state;badge.className='status-badge visible';
  if(state==='FAILED')badge.classList.add('error');
  if(state==='DONE')badge.classList.add('done');
  statusText.textContent=error||'Processing';
}
function showResult(element,text,placeholder){element.textContent=text||placeholder;element.classList.toggle('empty',!text)}

form.onsubmit=async(event)=>{
  event.preventDefault();button.disabled=true;button.textContent='Uploading…';
  statusText.textContent='Uploading';badge.className='status-badge';steps.forEach(s=>s.classList.remove('active'));
  try{
    const response=await fetch('/jobs',{method:'POST',body:new FormData(form)}),data=await response.json();
    if(!response.ok)throw new Error(data.detail||'Upload failed');
    showStatus(data.status);poll(data.job_id);
  }catch(error){badge.textContent='Error';badge.className='status-badge visible error';statusText.textContent=error.message;button.disabled=false;button.textContent='Upload and Process'}
};
async function poll(id){
  try{
    const response=await fetch('/jobs/'+id),data=await response.json();if(!response.ok)throw new Error('Unable to read status');
    showStatus(data.status,data.error||'');
    showResult(transcript,data.transcript,'Waiting for transcription');
    showResult(translation,data.translation,data.status==='TRANSLATING'?'Translating…':'Waiting for translation');
    if(!terminal.includes(data.status)){setTimeout(()=>poll(id),2000);return}
    button.disabled=false;button.textContent='Process Another File';
  }catch(error){statusText.textContent=error.message;setTimeout(()=>poll(id),3000)}
}
</script></body></html>
"""
