# Day X: [Topic / Milestone] - YYYY-MM-DD

## What I Did Today
- Configured Docker Compose services for PostgreSQL and Kafka.
- Tested consumer/producer scripts.

## Challenges & Solutions
- **Issue:** Port collision with local postgres.
- **Fix:** Remapped the container port to `5433:5432` in `docker-compose.yaml`.

## Tomorrow's Goal
- Set up the initial Python ingestion script.
