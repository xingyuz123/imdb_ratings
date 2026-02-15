# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Python data pipeline that aggregates IMDB ratings, reviews, and metadata into a Supabase (PostgreSQL) database. Uses Poetry for dependency management, Polars for data processing, and Pydantic for configuration.

## Commands

```bash
# Install dependencies
poetry install

# Run the full pipeline
python -m imdb_ratings.main

# Skip specific stages
python -m imdb_ratings.main --skip-titles --skip-reviews --skip-ratings --skip-export

# Run individual modules directly
python -m imdb_ratings.updater.update_titles
python -m imdb_ratings.updater.update_reviews
python -m imdb_ratings.updater.update_weighted_ratings
python -m imdb_ratings.export_excel
```

No test suite or linter is configured.

## Architecture

**Pipeline stages** (run in order from `imdb_ratings/main.py`):
1. **Update Titles** — Downloads IMDB public TSV datasets, filters by vote threshold (15k), upserts to Supabase
2. **First World Classification** — Uses OMDB API to classify titles by country/region
3. **Update Reviews** — Scrapes IMDB GraphQL API for user reviews
4. **Weighted Ratings** — Calls a Supabase stored procedure to recalculate ratings
5. **Excel Export** — Generates `titles.xlsx` via xlsxwriter

**Key patterns:**
- **Repository pattern** (`repository/`): `BaseRepository` abstract class with `TitleRepository`, `ReviewRepository`, `WeightedRatingsRepository` implementations. All DB access goes through repositories.
- **Singleton DB connection** (`core/database.py`): `DatabaseConnectionManager` with thread-safe locking, auto-reconnection, and exponential backoff. Access via `get_database_client()`.
- **Pydantic config** (`core/config.py`): Hierarchical settings — `IMDBDataConfig`, `SupabaseConfig`, `ScrapingConfig`, `LoggingConfig` — loaded from `.env` file. Access via `get_settings()` singleton.
- **Custom exceptions** (`core/exceptions.py`): 9-class hierarchy for specific error types (scraping, database, config, rate limit, etc.).

**Data sources** (`updater/sources/`):
- `imdb_dataset.py` — IMDB public dataset downloader/processor (TSV files)
- `scrape_reviews.py` — GraphQL-based review scraper with retry logic
- `omdb_client.py` — OMDB REST API client

## Environment Variables

Defined in `.env`, loaded by pydantic-settings:
- `PROJECT_URL` — Supabase project URL
- `SECRET_KEY` — Supabase JWT token
- `OMDB_API` — OMDB API key

## Key Constants

Defined in `core/constants.py`: IMDB GraphQL headers/operations, genre enum values (28 genres), retry policies, vote thresholds, HTTP status codes.
