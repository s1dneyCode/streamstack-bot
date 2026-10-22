# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A headless data-ingestion bot for a movie/TV streaming-availability catalog. It has no app server and no UI — it's a collection of Python scripts that pull data from TMDB / OMDb, write to a Supabase (Postgres) database, and run on schedules via GitHub Actions. There is a separate Flutter app that reads from the same Supabase database, but it is not part of this repo.

## Commands

Tests: `python -m unittest discover -s tests -t .` (standard library, no pytest) and `python -m py_compile bot/*.py tests/*.py` — both run in Actions (the `tests` job of `nightly.yml`, on push and pull_request, without secrets). There is no linter or `pyproject.toml`. The gate for any change is those two commands with exit 0.

```bash
# Install dependencies
pip install -r requirements.txt

# The gates (run both before committing)
python -m py_compile bot/*.py tests/*.py
python -m unittest discover -s tests -t .

# Ingest scripts — HUMANS ONLY, ON PURPOSE. Every one of these writes to the
# production database. Claude Code must never run them; the Actions workflows do.
# (each script needs its own env vars — see below)
python -m bot.main
python -m bot.backfill_credits --role cast-order
python -m bot.bulk_import --batch-size 2000
```

**Known environment gotcha**: `requirements.txt` only pins `supabase==2.9.1` with no lock file for transitive deps. Depending on when you `pip install`, you can get a `realtime` version that needs `websockets>=13`, while pip resolves an older `websockets==12.0` — this fails with `ModuleNotFoundError: No module named 'websockets.asyncio'` on `import supabase`. Fix locally with `pip install -U websockets`. This isn't pinned in `requirements.txt` because it hasn't broken CI yet, but it's a real local-dev trap.

Every entry-point script requires env vars and **fails fast with a clear message** if one is missing (no silent partial runs):
```
[BOT] ERROR: Required environment variable 'TMDB_API_KEY' is not set.
```
Required vars by script vary — `TMDB_API_KEY` + `SUPABASE_URL` + `SUPABASE_KEY` are needed almost everywhere; `OMDB_API_KEY` is only needed by `bot/main.py` and `bot/backfill_rt_scores.py` (Rotten Tomatoes scores). There is no `.env.example` — check the script's own `load_env()` function (always near the top) for its exact requirements before running it. `WATCHMODE_API_KEY` is **not used anywhere** — streaming data moved entirely to TMDB's `/watch/providers` endpoint; if you see a reference to it, it's stale.

## Architecture

### The three clients, one persistence layer

- `bot/tmdb.py` (`TmdbClient`) — all TMDB v3 API reads. One method per endpoint (`get_popular_movies`, `get_credits`, `get_watch_providers`, etc.), each returning plain dicts/lists so callers never touch raw API shapes. Has a shared `_get()` with retry/backoff baked in.
- `bot/omdb.py` (`OmdbClient`) — only used for Rotten Tomatoes scores (`get_rt_score`), via a multi-strategy title-matching fallback chain (IMDb ID → title+year → title → simplified title).
- `bot/supabase_client.py` (`SupabaseClient`) — the only module that writes to the DB. Every other module treats it as an opaque persistence interface and never builds raw SQL. The expected schema is documented in its module docstring — **read that first** when touching any table shape.

### Pipeline stages (read in this order to understand the system)

1. **`bot/bulk_import.py`** — one-time/occasional catalog seeding. Hits ~40 different TMDB discover/genre/year endpoints, dedupes by `tmdb_id`, applies quality filters (language allowlist, tiered vote-count thresholds, runtime floor for movies), and inserts **bare** rows only — no credits/trailers/seasons/streaming. Enrichment is deliberately deferred to later stages. Run manually via the `Bulk Import` workflow.

2. **`bot/main.py`** — the nightly bot (`nightly.yml`, cron `0 6 * * *`). Numbered steps (1, 1b, 2, 2b, ... 17b) that:
   - Fetch ~12 TMDB list endpoints, dedupe (Step 7)
   - For genuinely new titles: fetch detail + watch providers + release dates, upsert (Step 9)
   - Re-verify streaming providers for existing titles due for a check (Step 10) — see staleness logic below
   - Refresh popularity scores and RT scores for the same due-for-recheck set (Steps 10b, 11, 11b)
   - Sync `is_in_theatres` from the now-playing list (Step 13)
   - Detect new seasons/episodes for `Returning Series` shows (Steps 14, 17, 17b)

   The step numbers have gaps (10, 14-16 were removed/replaced) — this is intentional history, not a bug; don't "fix" the numbering.

3. **`bot/enrich_new_titles.py`** — runs 3 hours after the nightly bot (`nightly_enrichment.yml`, cron `0 9 * * *`). Queries `media WHERE created_at >= NOW() - 6h` (i.e. whatever the nightly bot just inserted) and fills in the secondary fields Step 9 doesn't set: poster/carousel image migration to Supabase Storage, trailers, credits, seasons+episodes, title logos, certifications, genres, runtime, and `is_in_theatres`/`is_streamable_now` state. Each field is skipped if Step 9 already populated it.

4. **Backfills** (`bot/backfill_*.py`, `migrate_images.py`) — one-off or periodic correction scripts, each scoped to a single field/table and each independently runnable. Two ways to trigger them:
   - `backfills.yml` — manual `workflow_dispatch` with a `job` dropdown (19 options), runs exactly one backfill.
   - `full_backfill_new.yml` / `full_backfill_update.yml` — two curated multi-step sequences, both `workflow_dispatch`-only (not scheduled). **The split is deliberate**: `_new` chains backfills that only touch rows where a field `IS NULL` (images → trailers → credits → seasons → title-logos → certifications → runtime → genres → metadata → states); `_update` chains backfills that unconditionally refresh already-populated data (popularity → popularity-score → vote-count → status → tmdb-scores → season-ratings → rt-scores → imdb-ids → streaming → release-dates). If you add a new backfill script, decide which bucket it belongs in before wiring it into either workflow.

5. **`bot/cleanup_low_quality.py`** — monthly (`nightly.yml`'s second cron, 1st of month). Deletes titles with `vote_count < 10`, released >90 days ago, not in theatres, not streamable. All four conditions must hold.

### Conventions that span files

- **`load_env()` fail-fast pattern**: every script's `main()` starts by calling a local `load_env()` that checks `os.environ` for its required keys and `sys.exit(1)`s with a labeled error if any are missing. Copy this pattern exactly when adding a new script — don't introduce `python-dotenv` loading or default-fallback behavior.
- **Upsert conflict keys** are load-bearing and table-specific — get them wrong and you silently duplicate rows instead of updating:
  - `media`: `tmdb_id`
  - `media_seasons`: `(media_id, season_number)`
  - `media_episodes`: `(season_id, episode_number)`
  - `media_credits`: `(media_id, name, role)`
  - `media_trailers`: `(media_id, youtube_key)`
  - `streaming_availability`: `(media_id, provider_name, region, monetization_type)`
- **PostgREST `order` column gotcha**: `media_credits.order` can't be filtered via the normal `.eq()`/`.is_()` query-param style, because PostgREST reserves the `order` query parameter for sort directives — `?order=is.null` gets parsed as a (broken) sort clause, not a filter. See `fetch_cast_order_targets()` in `bot/backfill_credits.py` for the workaround (fetch role='cast' rows, filter for `order IS NULL` in Python).
- **`ALLOWED_PROVIDERS`** (`bot/supabase_client.py`) is the single source of truth for which streaming platforms get persisted — TMDB's `/watch/providers` returns far more (regional/niche) services than the catalog wants. `get_watch_providers()` in `tmdb.py` returns the raw `{flatrate, rent, buy}` dict; callers (`main.py`, `backfill_streaming.py`) filter each list through `ALLOWED_PROVIDERS` before upserting. `is_streamable_now` is derived from `flatrate` only — rent/buy availability doesn't count as "streamable."
- **`streaming_availability.first_seen_at`** is intentionally omitted from upsert payloads so its DB-level `DEFAULT now()` only fires on true INSERT; it's never touched on conflict/UPDATE, so it always reflects when a title first appeared on a platform.
- **`compute_popularity_score()`** (`bot/supabase_client.py`) is the one piece of non-trivial business logic in the codebase, shared by `upsert_media()`, `backfill_popularity_score.py`, and the nightly bot's Step 10b. Bayesian-weighted blend of normalized popularity (30%), TMDB score (65%, or 70% if `rt_score` is `None` — its 5% weight gets redistributed rather than treating missing as zero), and RT score (5%), shrunk toward a global mean (`_BAYES_C = 72.07`) based on vote count (`_BAYES_M = 500`), then scaled by a tiered freshness multiplier: 0 if no `release_date`, vote-scaled (full boost at 200+ votes) if ≤30 days old, exponential decay (vote-scaled, full boost at 100+ votes) beyond that. Read the function before changing any weight — it's been tuned iteratively and the tiers are not symmetric by accident.
- **`get_titles_to_reverify()`** (`bot/supabase_client.py`) drives Step 10/10b/11's "who gets rechecked today" — 8 overlapping, deduped buckets keyed on `streaming_last_checked` staleness (3/7/30/90 days depending on category: coming soon, in-theatres recent/old, streamable, old titles, and three TV-specific age bands). If you change re-check cadence, this is the one function to touch — don't add ad-hoc date math in `main.py`.
- **`_PRE_RELEASE_STATUSES`** (`In Production`, `Post Production`, `Planned`, `Rumored`) gates OMDb/RT lookups — there's no point querying Rotten Tomatoes for a movie that hasn't released. Checked in `upsert_media()` and the Step 11 movie filter in `main.py`.
