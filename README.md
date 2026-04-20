# Document intake (mini data platform)

FastAPI service that ingests messy JSONL document feeds, normalizes fields, enriches records (keywords, classification, score, summary), logs per-row pipeline events, and exposes a small query API backed by PostgreSQL. **Ingestion runs on a Celery worker** with **Redis** as the broker so `POST /ingestions` returns immediately while the file is processed asynchronously.

## How to run

```
cp .env.example .env
# copy files to input_docs/
uv sync
source .venv/bin/activate
docker compose up -d
uv run alembic upgrade head
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

```bash
curl -X POST "http://localhost:8000/ingestions?file_path=input_docs/documents_1.jsonl"

# stats
curl -sS "http://localhost:8000/stats" | jq

# ingestion details
curl -sS "http://localhost:8000/ingestions/1" | jq

# document details
curl -sS "http://localhost:8000//documents/1" | jq

# Pagination: skip (offset) and limit (1–200, default 20)
curl -sS "http://localhost:8000/documents?skip=0&limit=20" | jq

# published_at >= date_from (ISO YYYY-MM-DD)
curl -sS "http://localhost:8000/documents?date_from=2020-01-01" | jq

# published_at <= date_to
curl -sS "http://localhost:8000/documents?date_to=2024-12-31" | jq

# Date range on published_at
curl -sS "http://localhost:8000/documents?date_from=2020-01-01&date_to=2024-12-31" | jq

# Tag (exact tag name)
curl -sS "http://localhost:8000/documents?tag=biology" | jq

# Organization (exact organization name; encode spaces as %20)
curl -sS "http://localhost:8000/documents?organization=Example%20University" | jq

# Status (exact status string)
curl -sS "http://localhost:8000/documents?status=published" | jq

# Search title or body (case-insensitive)
curl -sS "http://localhost:8000/documents?search=climate%20change" | jq

# All query parameters together
curl -sS "http://localhost:8000/documents?skip=0&limit=50&date_from=2020-01-01&date_to=2025-12-31&tag=biology&organization=Example%20University&status=published&search=health" | jq
```

**Prerequisites:** Python 3.11 or newer, [uv](https://docs.astral.sh/uv/) for dependencies, and Docker with Compose (for PostgreSQL, Redis, Elasticsearch, and the bundled Celery worker).

```bash
cp .env.example .env
# copy files to input_docs/
uv sync
source .venv/bin/activate
docker compose up -d
uv run alembic upgrade head
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Ingest data

With **uvicorn** and the **Celery worker** (Docker or local) running, enqueue a JSONL ingest by `POST`ing to `/ingestions`. Use the `file_path` query parameter for a path relative to the project root (or an absolute path); it must be readable by the worker process (e.g. files under `input_docs/` when that directory is mounted).

```bash
curl -X POST "http://localhost:8000/ingestions?file_path=input_docs/documents_1.jsonl"
```

The response is JSON with `run_id` and `status: "queued"`. Poll `GET /ingestions/{run_id}` until `status` is `completed` or `failed`.

### `GET /documents` (curl examples)

The examples below assume the API is at `http://localhost:8000` (see [How to run](#how-to-run)). Each pipes the response through **`jq`** for indented JSON (see [Pretty-printing JSON responses](#pretty-printing-json-responses); you can use `python -m json.tool` instead). Response shape: `{ "items": [...], "total": <int>, "skip": <int>, "limit": <int> }`. Each document object includes `author` and `organization` as `{ "id": <int>, "name": <string> }` when set, or `null` when not linked (top-level `author_id` / `organization_id` are not returned).

Replace placeholder values (`biology`, `Example University`, dates, etc.) with values that exist in your database. `tag`, `organization`, and `status` are **exact** string matches. With **Elasticsearch** enabled (`ELASTICSEARCH_URL` and not disabled), `search` uses the ES index (title/body text); otherwise it uses SQL **`ILIKE`** on **title or body** (case-insensitive substring). `date_from` / `date_to` filter on **`published_at`** (inclusive range when both are set).

```bash
# Pagination: skip (offset) and limit (1–200, default 20)
curl -sS "http://localhost:8000/documents?skip=0&limit=20" | jq

# published_at >= date_from (ISO YYYY-MM-DD)
curl -sS "http://localhost:8000/documents?date_from=2020-01-01" | jq

# published_at <= date_to
curl -sS "http://localhost:8000/documents?date_to=2024-12-31" | jq

# Date range on published_at
curl -sS "http://localhost:8000/documents?date_from=2020-01-01&date_to=2024-12-31" | jq

# Tag (exact tag name)
curl -sS "http://localhost:8000/documents?tag=biology" | jq

# Organization (exact organization name; encode spaces as %20)
curl -sS "http://localhost:8000/documents?organization=Example%20University" | jq

# Status (exact status string)
curl -sS "http://localhost:8000/documents?status=published" | jq

# Search title or body (case-insensitive)
curl -sS "http://localhost:8000/documents?search=climate%20change" | jq

# All query parameters together
curl -sS "http://localhost:8000/documents?skip=0&limit=50&date_from=2020-01-01&date_to=2025-12-31&tag=biology&organization=Example%20University&status=published&search=health" | jq
```


- The first `docker compose up` **builds** the worker image (`worker` in `docker-compose.yml`) and can take several minutes. **Elasticsearch** may need extra time on a cold start before its health check passes; the worker starts only after `db`, `redis`, and `elasticsearch` are healthy.
- Create a local **`input_docs/`** directory and place your JSONL there (it is gitignored). Defaults match `.env.example` (`DEFAULT_JSONL_PATH=input_docs/documents_1.jsonl`). Alternatively pass an absolute or project-relative path via `POST /ingestions?file_path=...`.
- `docker compose` starts **PostgreSQL**, **Redis**, **Elasticsearch**, and a **Celery worker** (see `docker-compose.yml`). The worker uses the same image as the API, mounts `./input_docs` so `DEFAULT_JSONL_PATH` resolves inside the container, and sets `ELASTICSEARCH_URL=http://elasticsearch:9200` so successful ingests can be indexed for search.
- Configure `DATABASE_URL` (localhost for uvicorn on the host), `DEFAULT_JSONL_PATH`, `CELERY_BROKER_URL` / `CELERY_RESULT_BACKEND`, and optionally `ELASTICSEARCH_URL` (e.g. `http://localhost:9200` when the compose ES service is up) in `.env` (see `.env.example`). Uncomment `ELASTICSEARCH_URL` if you want the **host-run API** to use the same ES instance as compose (for index setup and search). The worker service overrides `DATABASE_URL` to reach Postgres at hostname `db`. Set `ELASTICSEARCH_ENABLED=0` to force SQL `ILIKE` for `search=` even when a URL is set.
- **Smoke check:** `curl -sS http://localhost:8000/health` should return `{"status":"ok"}`. Interactive API: [`http://localhost:8000/docs`](http://localhost:8000/docs).
- If Elasticsearch is enabled after data already exists in Postgres, you can backfill the index with `uv run python scripts/reindex_elasticsearch.py` (requires `ELASTICSEARCH_URL` and a running ES).
- Trigger ingestion: `POST /ingestions` (optional query param `file_path`). Response: `{ "run_id": ..., "status": "queued" }`. Poll `GET /ingestions/{run_id}` for progress (`status`: `queued` → `running` → `completed` or `failed`).

**Celery worker on your machine (not Docker)** — Start Redis (and Postgres if needed). Export `DATABASE_URL`, `CELERY_BROKER_URL`, and `CELERY_RESULT_BACKEND` from `.env` (or use a shell that loads `.env`). From the project root after `uv sync`:

```bash
# Uses the project virtualenv via uv (recommended)
uv run celery -A app.celery_app:celery_app worker --loglevel=info
```

To run the same binary from `.venv` explicitly:

```bash
source .venv/bin/activate
celery -A app.celery_app:celery_app worker --loglevel=info
```

Or without activating:

```bash
.venv/bin/celery -A app.celery_app:celery_app worker --loglevel=info
```

**Tests** set `CELERY_TASK_ALWAYS_EAGER=1` so tasks run in-process without Redis.

**Running tests** — from the project root after `uv sync`:

```bash
uv run pytest -q
```

`pytest-cov` is included so the `--no-cov` flag is valid (it disables coverage when something enables it, e.g. a wrapper script or `PYTEST_ADDOPTS`). For day-to-day runs, plain `pytest -q` is enough; use `--no-cov` when your environment injects `--cov` and you want a faster loop.

### Async ingestion (high level)

```mermaid
flowchart LR
    Client[Client] --> API[FastAPI]
    API --> DB1[(PostgreSQL)]
    API --> Redis[(Redis broker)]
    Redis --> W[Celery worker]
    W --> Pipe[Ingestion pipeline]
    Pipe --> DB2[(PostgreSQL)]
    Client --> Poll[GET /ingestions/id]
    Poll --> DB1
```

## Architecture overview

- **FastAPI** HTTP layer: ingestions, document search, stats.
- **PostgreSQL** for durable storage and `UNIQUE(external_id)` idempotency.
- **Redis + Celery** for async ingestion: `POST /ingestions` enqueues work; the **worker** opens the JSONL file and runs the pipeline (not the API process).
- **Elasticsearch** (optional): when `ELASTICSEARCH_URL` is set and not disabled (`app/config.py`), **FastAPI** (`app/main.py` lifespan) and the **Celery worker** (`worker_ready` in `app/celery_app.py`) both call `ensure_elasticsearch_index`. Each successful upsert calls `maybe_index_document` during ingestion (`app/ingestion/runner.py`). On `GET /documents`, when ES is enabled and a `search` term is present, matching and most filters run in ES (see `app/search/queries.py`); the API then loads full rows from PostgreSQL by ID. If ES is unavailable, disabled, or search fails, listing falls back to SQL (`ILIKE` on title and body when searching).
- **Ingestion pipeline** (see `app/ingestion/`): parse JSON line → **validate raw** (required `external_id`; `published_at` / `updated_at` must be valid `YYYY-MM-DD` when present) → **normalize** (coerce types, clean strings, tags, language, booleans; invalid DOI/URL dropped to `null`) → **validate normalized** (non-empty `external_id`) → upsert by `external_id` → semantic dedup via `content_fingerprint` (SHA-256 of normalized title + body) → enrichment in the same write path.
- **Processing layer** (see `app/processing/`): `apply_processing` (in `app/processing/__init__.py`) fills keywords, classification, score, and summary after each successful upsert. Semantic duplicate detection is enforced in `app/repositories/document_repo.py` using `content_fingerprint` from `app/utils/hash.py` (also re-exported from `app/processing/deduplication.py`).

**Application layout**

| Layer | Location | Role |
|--------|----------|------|
| HTTP routes | `app/api/routes/` | Thin handlers for `/ingestions`, `/documents`, `/stats`; `GET /health` lives in `app/main.py`. |
| Dependencies | `app/api/deps.py` | `get_db` — FastAPI `Depends` for SQLAlchemy `Session`. |
| Services | `app/services/` | `ingestion_service` — resolve JSONL path, create queued run; `document_service` — list/get with filters and ES vs SQL search; `stats_service` — aggregates for `/stats`. |
| Repositories | `app/repositories/` | `document_repo` — upsert, tag/author/org linking, fingerprint dedup, calls `apply_processing`; `ingestion_repo` — run rows and `ingestion_events`. |
| Schemas | `app/schemas/` | Pydantic models for API responses (e.g. documents, stats, ingestion payloads). |
| Database | `app/db.py` | SQLAlchemy engine and session factory (used by services, repositories, Celery tasks, and scripts). |

**Search** (`app/search/`)

| Module | Role |
|--------|------|
| `client.py` | Elasticsearch client and index name. |
| `index.py` | Index mapping, `ensure_elasticsearch_index` (API and worker startup), `maybe_index_document` after each successful upsert, bulk reindex helpers. |
| `queries.py` | `search_document_ids` — full-text `multi_match` plus tag, organization, status, and `published_at` range in one ES query; returns ordered IDs and total for pagination; `document_service` loads `Document` rows from PostgreSQL in that order. |

**Processing modules** (called from `document_repo` via `apply_processing`)

| Module | Role |
|--------|------|
| `keywords.py` | Stopword-stripped keyword frequencies from title/abstract/body text. |
| `classification.py` | Simple title/body classification from document type and text. |
| `scoring.py` | Composite score from document fields and extracted signals. |
| `summary.py` | Short (two-sentence) summary from body or abstract. |

**Ingestion package layout**

| Module | Role |
|--------|------|
| `app/ingestion/parser.py` | `json.loads` per line; `None` on invalid JSON |
| `app/ingestion/validator.py` | Raw record rules + post-normalize checks |
| `app/ingestion/normalizer.py` | Coercion and cleanup into `NormalizedRecord` |
| `app/ingestion/runner.py` | File loop, counters, `ingestion_events` logging |

`app/normalize.py` and `app/validate.py` re-export the canonical implementations for backward-compatible imports.

**Celery**

| Module | Role |
|--------|------|
| `app/celery_app.py` | Celery app, JSON serialization, optional `CELERY_TASK_ALWAYS_EAGER`; `worker_ready` ensures the ES index when configured |
| `app/tasks/ingestion_tasks.py` | `run_ingestion_task(run_id, file_path)` — opens DB session, runs `ingest_file`, commits or marks run `failed` |

```mermaid
flowchart TD
    Client[Client] --> API[FastAPI]
    API --> Redis[(Redis broker)]
    Redis --> W[Celery worker]
    J[JSONL file] --> W
    W --> C[Ingestion run + pipeline]
    C --> P[Parse line]
    P --> VR[Validate raw]
    VR --> N[Normalize]
    N --> VN[Validate normalized]
    VN --> U[Upsert + fingerprint + enrichment]
    U --> F[(PostgreSQL)]
    U -. optional index .-> ES[(Elasticsearch)]
    C --> M[ingestion_events]
    M --> F
    API --> Q[Query API]
    Q --> F
    Q -. search when enabled .-> ES
```

## Data model (ERD)

Tables match SQLAlchemy models under `app/models/`. The diagram lists main columns; `documents` has additional nullable metadata fields (e.g. `citation_count`, `word_count`, `open_access`) used by normalization and the API.

```mermaid
erDiagram
    documents ||--o| authors : "author_id"
    documents ||--o| organizations : "organization_id"
    documents }o--o{ tags : "document_tags"
    ingestion_runs ||--o{ ingestion_events : "ingestion_id"

    documents {
        int id PK
        string external_id UK
        text title
        text abstract
        text body
        date published_at
        date updated_at
        string status
        string document_type
        string language
        string region
        text url
        string doi
        text summary
        float score
        string classification
        json keywords
        string content_fingerprint "UNIQUE when non-null"
        int author_id FK
        int organization_id FK
    }

    authors {
        int id PK
        string name UK
    }

    organizations {
        int id PK
        string name UK
    }

    tags {
        int id PK
        string name UK
    }

    document_tags {
        int document_id FK
        int tag_id FK
    }

    ingestion_runs {
        int id PK
        datetime started_at "nullable while queued"
        datetime finished_at
        int total_records
        int success_count
        int error_count
        int skipped_count
        string status
    }

    ingestion_events {
        int id PK
        int ingestion_id FK
        string external_id "nullable"
        string stage
        string status
        string message
        json raw_payload
        datetime created_at
    }
```

## API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/ingestions` | Queue ingestion (`file_path` query optional). Returns `{ "run_id", "status": "queued" }`. |
| `GET` | `/ingestions/{run_id}` | Run summary and event log. |
| `GET` | `/documents` | Paginated list; each item includes `tags`, and optional nested `author` / `organization` (`{ "id", "name" }` or `null`). Filters: `date_from`, `date_to`, `tag`, `organization`, `status`, `search`, `skip`, `limit`. Response fields align with `DocumentOut` in `app/schemas/document.py` (full document columns plus `author`, `organization`, `tags`). |
| `GET` | `/documents/{id}` | Single document with the same shape (tags plus nested `author` / `organization` when linked). |
| `GET` | `/stats` | Counts, breakdowns, top tags, average score. |
| `GET` | `/health` | Liveness. |

### Pretty-printing JSON responses

API responses are compact JSON. To view them with indentation in the terminal, pipe through **`jq`** or Python’s **`json.tool`**:

```bash
curl -sS "http://localhost:8000/documents?skip=0&limit=5" | jq

curl -sS "http://localhost:8000/stats" | jq
```

Interactive browsing with formatted bodies: open **Swagger UI** at [`http://localhost:8000/docs`](http://localhost:8000/docs) (when the server is running).


## Assumptions

- `external_id` is the stable business key; re-ingesting the same id updates the row (idempotent upsert).
- Missing optional fields are allowed; normalization is best-effort (types, tags, language, status, booleans, etc.).
- **Soft cleanup**: malformed DOI strings and non-`http*` URLs are normalized to `null` (the line still ingests). Other bad values typically become `null` or safe defaults (for example empty status → `"unknown"`).
- **Hard validation** (line fails with a validation event): missing or blank `external_id`; non-empty `published_at` or `updated_at` that is not a valid `YYYY-MM-DD` string; empty JSON object `{}` after parse; unparseable JSON lines (logged under parsing).
- Blank lines in the file are skipped and do not increment the run’s line counter.
- Semantic duplicates share the same content fingerprint; the second **distinct** `external_id` is skipped and logged under `deduplication`.

## Stats produced (`GET /stats`)

Aggregates over all stored documents and ingestion-event telemetry. Shape:

| Field | Meaning |
|--------|--------|
| `total_documents` | Row count in `documents`. |
| `by_status` | Counts grouped by `Document.status` (missing status appears as key `"null"`). |
| `by_type` | Counts grouped by `document_type` (missing → `"null"`). |
| `top_tags` | Up to 10 tags by frequency across `document_tags`. |
| `avg_score` | Mean of `Document.score` where score is non-null; `null` if none. |
| `total_ingestion_events` | Rows in `ingestion_events` (lifetime, all runs). |
| `events_by_stage` | Counts of ingestion events by `stage` (`parsing`, `validation`, `deduplication`, `completed`, etc.). |
| `events_by_status` | Counts by event `status` (`success`, `error`, `skipped`). |

Example response (illustrative; numbers depend on your DB):

```json
{
  "total_documents": 1240,
  "by_status": {
    "published": 980,
    "draft": 120,
    "unknown": 140
  },
  "by_type": {
    "article": 800,
    "report": 200,
    "null": 240
  },
  "top_tags": [
    { "name": "biology", "count": 310 },
    { "name": "climate", "count": 205 }
  ],
  "avg_score": 42.7,
  "total_ingestion_events": 5600,
  "events_by_stage": {
    "completed": 1240,
    "parsing": 45,
    "validation": 30,
    "deduplication": 15
  },
  "events_by_status": {
    "success": 1240,
    "error": 75,
    "skipped": 15
  }
}
```

```bash
curl -sS "http://localhost:8000/stats" | jq
```

## Sample execution log

Ingestion emits **structured application logs** (worker / API process) and persists a **per-run summary plus per-line events** queryable via `GET /ingestions/{run_id}`.

### Worker log excerpt (Celery + pipeline)

After `POST /ingestions`, the worker logs task lifecycle and the runner logs start/finish with counters. Example lines (timestamps and IDs will differ):

```text
Celery ingestion task started run_id=3 path=/app/input_docs/documents_1.jsonl
Starting ingestion run_id=3 path=/app/input_docs/documents_1.jsonl
Finished ingestion run_id=3 total=4000 success=3950 errors=35 skipped=15
Celery ingestion task completed run_id=3 total=4000 success=3950 errors=35 skipped=15
```

- **`total`** — non-blank JSONL lines read (see [Assumptions](#assumptions) for blank lines).
- **`success`** — lines upserted successfully.
- **`errors`** — parse failures, empty `{}`, or validation failures (`stage` `parsing` or `validation` in events).
- **`skipped`** — semantic duplicates (`stage` `deduplication`).

### Run summary JSON (`GET /ingestions/{run_id}`)

Poll until `status` is `completed` or `failed`. Example body after a successful run (truncated `events` for readability; real responses include one event per processed line where logging applies):

```bash
curl -sS "http://localhost:8000/ingestions/3" | jq
```

```json
{
  "ingestion_id": 3,
  "started_at": "2026-04-17T12:01:02.123456+00:00",
  "finished_at": "2026-04-17T12:03:44.987654+00:00",
  "total_records": 4000,
  "success_count": 3950,
  "error_count": 35,
  "skipped_count": 15,
  "status": "completed",
  "events": [
    {
      "external_id": "doc-00001",
      "status": "success",
      "message": null,
      "stage": "completed"
    },
    {
      "external_id": null,
      "status": "error",
      "message": "invalid JSON or empty payload",
      "stage": "parsing"
    },
    {
      "external_id": "doc-00442",
      "status": "skipped",
      "message": "semantic duplicate of document doc-00102",
      "stage": "deduplication"
    }
  ]
}
```

## Future improvements

Roadmap ideas for data quality, scale, search, processing, observability, consistency, API/DB, and developer experience.

### 1. Data quality & ingestion

- Add data quality scoring for documents.
- Introduce a Dead Letter Queue (DLQ) for failed records.
- Implement per-record retry instead of skipping.
- Support schema versioning.

### 2. Scalability

- Batch (chunked) ingestion instead of per-record processing.
- Increase Celery concurrency for parallel processing.
- Support streaming ingestion (e.g., S3 / queues).

### 3. Search enhancements

- Improve relevance ranking (field boosting).
- Add highlighting and faceted search (filters, aggregations).
- Implement autocomplete.
- Add synonym support.

### 4. Processing improvements

- Upgrade keyword extraction (TF-IDF / TextRank).
- Use ML-based classification.
- Improve duplicate detection (text similarity).
- Add semantic search (embeddings).

### 5. Observability

- Add metrics (ingestion rate, errors, latency).
- Structured logging.
- Distributed tracing.

### 6. Data consistency

- Implement outbox pattern for DB → Elasticsearch sync.
- Add reindexing jobs.
- Ensure idempotent indexing.

### 7. API & DB

- Advanced filtering and cursor-based pagination.
- Add PostgreSQL full-text search fallback.
- Optimize indexes and partition large tables.

### 8. Developer experience

- Admin panel for ingestion monitoring.
- CLI tools (reindex, retry, validate).
- Improved documentation.

### Additional ideas

- **Authentication and rate limits** on `/ingestions` and bulk export endpoints.
- **Explicit enrichment events** — optional `ingestion_events` rows for keyword/classification/summary stages (today enrichment runs in-process; only dedup/validation/parsing surface as stages in the event list).
- **Load and contract tests** — performance budgets against sample multi-GB JSONL; OpenAPI response examples generated from fixtures.
