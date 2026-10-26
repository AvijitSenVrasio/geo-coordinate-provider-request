require("dotenv").config();

const { MongoClient } = require("mongodb");

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = process.env.MONGODB_DB || "vrasio_geo_cord";

// Local NestJS endpoint. Override with API_URL in .env if required.
const API_URL =
    process.env.API_URL || "http://localhost:3005/hotel-list/getHotelList";

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 60);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 60000);

const CITY_COLLECTION = "city_hotel_list";
const META_COLLECTION = "meta_data";
const ERROR_LOG_COLLECTION = "error_logs";

const CHECKPOINT_ID = "city_hotel_list_checkpoint";

const DEFAULT_PAYLOAD = {
    radius: 10,
    checkIn: "2026-12-15",
    checkOut: "2026-12-18",
    rooms: 1,
    adults: 2,
    children: 0,
    childAges: [],
    providers: ["DidaTravel", "TBO", "HotelBeds", "RateHawkDirect"],
    sort: "recommended",
    apply_fitlers: false,
    visitor_geo: {
        timezone: "America/New_York",
        country_code: "US",
    },
    user_id: "usr_948172635",
};

// Cooling period fallback (ms) when Retry-After header is absent.
// Matches THROTTLE_SEARCH ttl: 60 000 ms = 1 minute.
const RATE_LIMIT_FALLBACK_COOL_MS = Number(
    process.env.RATE_LIMIT_FALLBACK_COOL_MS || 61000,
);
// Extra buffer added on top of Retry-After to avoid an immediate 429 on resume.
const RATE_LIMIT_BUFFER_MS = Number(process.env.RATE_LIMIT_BUFFER_MS || 1000);
// Max retries per city after a 429.
const MAX_RATE_LIMIT_RETRIES = Number(process.env.MAX_RATE_LIMIT_RETRIES || 3);

/**
 * Sleeps for `ms` milliseconds. Logs a countdown so you know the
 * script is alive during the cooling period.
 */
function sleep(ms, label = "Cooling") {
    return new Promise((resolve) => {
        const seconds = Math.ceil(ms / 1000);
        console.log(`\n⏳ ${label}: sleeping for ${seconds}s (${ms} ms) …`);

        let remaining = seconds;
        const ticker = setInterval(() => {
            remaining -= 1;
            if (remaining > 0 && remaining % 5 === 0) {
                process.stdout.write(`   ${remaining}s remaining …\r`);
            }
        }, 1000);

        setTimeout(() => {
            clearInterval(ticker);
            process.stdout.write("\n");
            resolve();
        }, ms);
    });
}

function buildPayload(city) {
    const latitude = Number(city.lat);
    const longitude = Number(city.long);
    const locationName = String(city.city || "").trim();

    if (
        !locationName ||
        !Number.isFinite(latitude) ||
        !Number.isFinite(longitude)
    ) {
        throw new Error(`Invalid city data for MongoDB _id=${city._id}`);
    }

    return {
        ...DEFAULT_PAYLOAD,
        latitude,
        longitude,
        locationName,
    };
}

function parseResponseBody(text) {
    if (!text) return null;

    try {
        return JSON.parse(text);
    } catch {
        return text;
    }
}

async function hitApi(payload) {
    const startedAt = Date.now();
    const controller = new AbortController();

    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

    try {
        const response = await fetch(API_URL, {
            method: "POST",
            headers: {
                "content-type": "application/json",
                accept: "application/json",
            },
            body: JSON.stringify(payload),
            signal: controller.signal,
        });

        let responseBody;
        let responseReadError = null;

        try {
            const responseText = await response.text();
            responseBody = parseResponseBody(responseText);
        } catch (error) {
            responseBody = null;
            responseReadError = error.message;
        }

        const durationMs = Date.now() - startedAt;
        const success = response.ok && !responseReadError;

        // NestJS throttler sets Retry-After in seconds on a 429.
        const retryAfterSec =
            response.status === 429
                ? Number(response.headers.get("retry-after") || 0)
                : 0;
        const retryAfterMs =
            retryAfterSec > 0
                ? retryAfterSec * 1000
                : RATE_LIMIT_FALLBACK_COOL_MS;

        return {
            success,
            isRateLimited: response.status === 429,
            retryAfterMs,
            statusCode: response.status,
            response: responseBody,
            error: success
                ? null
                : responseReadError
                  ? `Failed to read response body: ${responseReadError}`
                  : `HTTP ${response.status}${response.statusText ? ` ${response.statusText}` : ""}`,
            durationMs,
            responseHeaders: {
                contentType: response.headers.get("content-type"),
                requestId:
                    response.headers.get("x-request-id") ||
                    response.headers.get("request-id"),
                retryAfter: response.headers.get("retry-after"), // ← add this
            },
        };
    } catch (error) {
        const durationMs = Date.now() - startedAt;
        const isTimeout =
            error.name === "AbortError" || controller.signal.aborted;

        return {
            success: false,
            statusCode: null,
            response: null,
            error: isTimeout
                ? `Request timed out after ${REQUEST_TIMEOUT_MS} ms`
                : error.message || "Unknown network error",
            errorName: error.name || null,
            errorCode: error.cause?.code || null,
            durationMs,
        };
    } finally {
        clearTimeout(timeout);
    }
}

async function main() {
    if (!MONGODB_URI) {
        throw new Error("MONGODB_URI is missing from .env");
    }

    if (!Number.isInteger(BATCH_SIZE) || BATCH_SIZE < 1) {
        throw new Error("BATCH_SIZE must be a positive integer");
    }

    if (!Number.isFinite(REQUEST_TIMEOUT_MS) || REQUEST_TIMEOUT_MS <= 0) {
        throw new Error("REQUEST_TIMEOUT_MS must be a positive number");
    }

    let parsedUrl;
    try {
        parsedUrl = new URL(API_URL);
    } catch {
        throw new Error(`API_URL is not a valid URL: ${API_URL}`);
    }

    if (!["http:", "https:"].includes(parsedUrl.protocol)) {
        throw new Error("API_URL must use HTTP or HTTPS");
    }

    const client = new MongoClient(MONGODB_URI);

    try {
        await client.connect();

        const db = client.db(DB_NAME);
        const cityCollection = db.collection(CITY_COLLECTION);
        const metaCollection = db.collection(META_COLLECTION);
        const failedLogCollection = db.collection(ERROR_LOG_COLLECTION);

        console.log(`Connected to MongoDB: ${DB_NAME}`);
        console.log(`Target API URL: ${API_URL}`);
        console.log(`Batch size: ${BATCH_SIZE}`);
        console.log("Concurrency: 1 (sequential requests)");
        console.log(`Request timeout: ${REQUEST_TIMEOUT_MS} ms`);

        // ─── Grand totals across all batches ───────────────────────────────
        let batchNumber = 0;
        let grandTotalFetched = 0;
        let grandTotalSuccessful = 0;
        let grandTotalFailed = 0;

        // ─── Outer loop: keep going until no unhitted records remain ───────
        while (true) {
            batchNumber += 1;

            // Read the current checkpoint (re-read each iteration so it
            // reflects what was written at the end of the previous batch).
            const checkpoint = await metaCollection.findOne({
                _id: CHECKPOINT_ID,
            });
            const lastId = checkpoint?.lastProcessedId;

            const filter = { hitted: { $ne: 1 } };
            if (lastId) {
                filter._id = { $gt: lastId };
            }

            // Fetch next batch in ascending _id order.
            const cities = await cityCollection
                .find(filter)
                .sort({ _id: 1 })
                .limit(BATCH_SIZE)
                .toArray();

            if (cities.length === 0) {
                console.log(
                    "\n✅ No more eligible records found. All cities processed!",
                );
                break;
            }

            console.log(
                `\n${"═".repeat(42)}\n` +
                `BATCH ${batchNumber} — ${cities.length} cities to process\n` +
                `${"═".repeat(42)}`,
            );

            const successfulIds = [];
            const failedLogs = [];

            // ─── Process cities one at a time ──────────────────────────────
            for (let i = 0; i < cities.length; i++) {
                const city = cities[i];
                let payload = null;
                let result;

                console.log(
                    `\n[Batch ${batchNumber}][${i + 1}/${cities.length}] Requesting: ${city.city}`,
                );

                let rateLimitRetries = 0;

                try {
                    payload = buildPayload(city);

                    // Retry loop: re-send after cooling if rate-limited.
                    while (true) {
                        result = await hitApi(payload);

                        if (!result.isRateLimited) break;

                        rateLimitRetries += 1;
                        if (rateLimitRetries > MAX_RATE_LIMIT_RETRIES) {
                            console.error(
                                `Rate-limit retries exhausted (${MAX_RATE_LIMIT_RETRIES}) for ${city.city}. Skipping.`,
                            );
                            break;
                        }

                        const coolMs = result.retryAfterMs + RATE_LIMIT_BUFFER_MS;
                        console.warn(
                            `⚠️  429 for ${city.city} ` +
                                `(retry ${rateLimitRetries}/${MAX_RATE_LIMIT_RETRIES}). ` +
                                `Retry-After: ${result.responseHeaders?.retryAfter ?? "N/A"}s`,
                        );
                        await sleep(coolMs, "Rate-limit cooling");
                    }
                } catch (error) {
                    result = {
                        success: false,
                        statusCode: null,
                        response: null,
                        error: error.message,
                        errorName: error.name,
                        errorCode: null,
                        durationMs: 0,
                    };
                }

                console.log("Request result:", {
                    city: city.city,
                    success: result.success,
                    statusCode: result.statusCode,
                    durationMs: result.durationMs,
                    error: result.error,
                    errorCode: result.errorCode,
                });

                if (result.success) {
                    successfulIds.push(city._id);
                    console.log(`SUCCESS | ${city.city} | HTTP ${result.statusCode}`);
                } else {
                    console.error(
                        `FAILED | ${city.city} | ` +
                            `HTTP ${result.statusCode ?? "N/A"} | ${result.error}`,
                    );
                    failedLogs.push({
                        recordId: city._id,
                        city: city.city,
                        request: payload,
                        response: result.response,
                        statusCode: result.statusCode,
                        error: result.error,
                        errorName: result.errorName || null,
                        errorCode: result.errorCode || null,
                        durationMs: result.durationMs ?? null,
                        responseHeaders: result.responseHeaders || null,
                        targetUrl: API_URL,
                        createdAt: new Date(),
                    });
                }
            }

            // ─── Persist batch results ──────────────────────────────────────

            if (failedLogs.length > 0) {
                await failedLogCollection.insertMany(failedLogs, {
                    ordered: true,
                });
                console.log(
                    `Saved ${failedLogs.length} failed request(s) to ${ERROR_LOG_COLLECTION}.`,
                );
            }

            if (successfulIds.length > 0) {
                await cityCollection.bulkWrite(
                    successfulIds.map((_id) => ({
                        updateOne: {
                            filter: { _id },
                            update: { $set: { hitted: 1, hittedAt: new Date() } },
                        },
                    })),
                    { ordered: false },
                );
            }

            const lastProcessedId = cities[cities.length - 1]._id;

            await metaCollection.updateOne(
                { _id: CHECKPOINT_ID },
                {
                    $set: {
                        lastProcessedId,
                        lastBatchAt: new Date(),
                        batchNumber,
                        batchSize: cities.length,
                        successfulCount: successfulIds.length,
                        failedCount: failedLogs.length,
                    },
                },
                { upsert: true },
            );

            grandTotalFetched += cities.length;
            grandTotalSuccessful += successfulIds.length;
            grandTotalFailed += failedLogs.length;

            console.log(`\n─── Batch ${batchNumber} Summary ───────────────────`);
            console.log(`  Fetched:    ${cities.length}`);
            console.log(`  Successful: ${successfulIds.length}`);
            console.log(`  Failed:     ${failedLogs.length}`);
            console.log(`  Last _id:   ${lastProcessedId}`);
            console.log(`────────────────────────────────────────────`);

            // If this batch returned fewer records than BATCH_SIZE, we are
            // almost certainly at the end — but we let the while-loop's
            // own query confirm that on the next iteration.
        }

        // ─── Grand total after all batches ──────────────────────────────────
        console.log("\n╔══════════════════════════════════════════╗");
        console.log("║           GRAND TOTAL SUMMARY            ║");
        console.log("╠══════════════════════════════════════════╣");
        console.log(`║  Batches completed : ${String(batchNumber).padEnd(20)}║`);
        console.log(`║  Total fetched     : ${String(grandTotalFetched).padEnd(20)}║`);
        console.log(`║  Total successful  : ${String(grandTotalSuccessful).padEnd(20)}║`);
        console.log(`║  Total failed      : ${String(grandTotalFailed).padEnd(20)}║`);
        console.log("╚══════════════════════════════════════════╝");
    } finally {
        await client.close();
    }
}

main().catch((error) => {
    console.error("Script failed:", error);
    process.exitCode = 1;
});
