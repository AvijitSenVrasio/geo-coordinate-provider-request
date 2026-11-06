# geo-coordinate-provider-request

## Setup

Each folder has its own dependencies and reads `.env` (e.g. `MONGODB_URI`, `MONGODB_DB`, `API_URL`).

```bash
cd import-geo-cord && npm install
cd request-city-hotels && npm install
```

## Commands

### import-geo-cord

```bash
cd import-geo-cord

# Import cities from an Excel file into MongoDB
node import-cities-to-mongodb.js <file.xlsx>
node import-cities-to-mongodb.js <file.xlsx> --clear      # clear the collection first

# Mark the top cities per country as target: 1
node mark-top-cities-target.js                            # uses vrasio_top2_cities_by_country.xlsx
node mark-top-cities-target.js <file.xlsx>
node mark-top-cities-target.js --dry-run                  # preview only, no writes
```

### request-city-hotels

```bash
cd request-city-hotels

# Single request per city (no pagination)
node city-hotel-fetch.js

# Paginated fetch, ascending _id order
node city-hotel-fetch-pagination.js

# Paginated fetch, descending _id order
node city-hotel-fetch-pagination-reverse.js

# Load test with autocannon
node city-hotel-autocannon.js
```

#### Flags (city-hotel-fetch*.js)

| Flag | Env equivalent | Effect |
| --- | --- | --- |
| `--target` | `TARGET_ONLY=true` | Only process `city_hotel_list` documents with `target: 1` instead of every city |
| `--retry-failed` | `RETRY_FAILED=true` | Pagination scripts only: re-run only the cities that failed in the current round (does not advance the round) |

Flags can be combined:

```bash
node city-hotel-fetch.js --target
node city-hotel-fetch-pagination.js --target
node city-hotel-fetch-pagination-reverse.js --target
node city-hotel-fetch-pagination.js --target --retry-failed
TARGET_ONLY=true node city-hotel-fetch-pagination.js
```

#### Optional environment variables

`BATCH_SIZE` (60), `REQUEST_TIMEOUT_MS` (60000), `API_URL`, `MONGODB_DB`, `RATE_LIMIT_FALLBACK_COOL_MS`, `RATE_LIMIT_BUFFER_MS`, `MAX_RATE_LIMIT_RETRIES`.
Pagination scripts also use `MIN_RECORDS_PER_PAGE`, `MAX_CONSECUTIVE_LOW_PAGES`, `MAX_PAGES_PER_CITY`, `PAGE_DELAY_MS`, `MAX_CONNECTION_RETRIES`, `CONNECTION_RETRY_BASE_MS`, `CONNECTION_RETRY_MAX_MS`.
