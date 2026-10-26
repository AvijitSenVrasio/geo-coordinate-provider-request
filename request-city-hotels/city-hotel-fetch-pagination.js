require("dotenv").config();

const { MongoClient } = require("mongodb");

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = process.env.MONGODB_DB || "vrasio_geo_cord";

// Local NestJS endpoint. Override with API_URL in .env if required.
const API_URL =
    process.env.API_URL || "http://localhost:3005/hotel-list/getHotelList";

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 60);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 60000);

// ─── Pagination settings ────────────────────────────────────────────────────
// A page with fewer than MIN_RECORDS_PER_PAGE hotels counts as a "low" page.
const MIN_RECORDS_PER_PAGE = Number(process.env.MIN_RECORDS_PER_PAGE || 2);
// Move to the next city after this many CONSECUTIVE low pages.
const MAX_CONSECUTIVE_LOW_PAGES = Number(
    process.env.MAX_CONSECUTIVE_LOW_PAGES || 3,
);
// Safety net so a misbehaving API can never loop forever on one city.
const MAX_PAGES_PER_CITY = Number(process.env.MAX_PAGES_PER_CITY || 1000);
// Optional pause between pages of the same city (ms).
const PAGE_DELAY_MS = Number(process.env.PAGE_DELAY_MS || 0);

const CITY_COLLECTION = "city_hotel_list";
const META_COLLECTION = "meta_data";
const FAILED_LOG_COLLECTION = "meta_data_request_log";

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
// Max retries per request after a 429.
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
                retryAfter: response.headers.get("retry-after"),
            },
        };
    } catch (error) {
        const durationMs = Date.now() - startedAt;
        const isTimeout =
            error.name === "AbortError" || controller.signal.aborted;

        return {
            success: false,
            isRateLimited: false,
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

/**
 * Sends one request and, if the API answers 429, waits for the cooling
 * period and re-sends the SAME payload (same refreshToken) up to
 * MAX_RATE_LIMIT_RETRIES times.
 */
async function hitApiWithRateLimitRetry(payload, cityName) {
    let rateLimitRetries = 0;
    let result;

    while (true) {
        result = await hitApi(payload);

        if (!result.isRateLimited) break;

        rateLimitRetries += 1;
        if (rateLimitRetries > MAX_RATE_LIMIT_RETRIES) {
            console.error(
                `Rate-limit retries exhausted (${MAX_RATE_LIMIT_RETRIES}) for ${cityName}. Giving up on this request.`,
            );
            break;
        }

        const coolMs = result.retryAfterMs + RATE_LIMIT_BUFFER_MS;
        console.warn(
            `⚠️  429 for ${cityName} ` +
                `(retry ${rateLimitRetries}/${MAX_RATE_LIMIT_RETRIES}). ` +
                `Retry-After: ${result.responseHeaders?.retryAfter ?? "N/A"}s`,
        );
        await sleep(coolMs, "Rate-limit cooling");
    }

    return result;
}

function buildFailureLog({ city, payload, result, page }) {
    return {
        recordId: city._id,
        city: city.city,
        page,
        request: payload,
        response: result.response ?? null,
        statusCode: result.statusCode ?? null,
        error: result.error,
        errorName: result.errorName || null,
        errorCode: result.errorCode || null,
        durationMs: result.durationMs ?? null,
        responseHeaders: result.responseHeaders || null,
        targetUrl: API_URL,
        createdAt: new Date(),
    };
}

/**
 * Pages through ONE city until it is exhausted.
 *
 * Page 1 is sent without a refreshToken. Every following page re-sends the
 * same payload plus the `refreshToken` returned by the previous response.
 *
 * Stops (success) when:
 *   - a page returns zero hotels, or
 *   - MAX_CONSECUTIVE_LOW_PAGES pages in a row return fewer than
 *     MIN_RECORDS_PER_PAGE hotels, or
 *   - the response carries no (new) refreshToken, so there is no next page.
 *
 * Stops (failure) when a request fails (non-2xx, timeout, retries exhausted)
 * or MAX_PAGES_PER_CITY is reached.
 */
async function fetchAllPagesForCity(city, tag) {
    const summary = {
        success: false,
        pages: 0,
        totalHotels: 0,
        stopReason: null,
        failureLog: null,
    };

    let basePayload;
    try {
        basePayload = buildPayload(city);
    } catch (error) {
        summary.stopReason = "invalid city data";
        summary.failureLog = buildFailureLog({
            city,
            payload: null,
            page: 0,
            result: {
                response: null,
                statusCode: null,
                error: error.message,
                errorName: error.name,
                errorCode: null,
                durationMs: 0,
            },
        });
        return summary;
    }

    let refreshToken = null;
    let consecutiveLowPages = 0;

    for (let page = 1; page <= MAX_PAGES_PER_CITY; page++) {
        const payload = refreshToken
            ? { ...basePayload, refreshToken }
            : basePayload;

        console.log(
            `${tag} page ${page} → ${city.city}` +
                (refreshToken ? " (with refreshToken)" : " (first request)"),
        );

        const result = await hitApiWithRateLimitRetry(payload, city.city);

        console.log("Request result:", {
            city: city.city,
            page,
            success: result.success,
            statusCode: result.statusCode,
            durationMs: result.durationMs,
            error: result.error,
            errorCode: result.errorCode,
        });

        if (!result.success) {
            console.error(
                `FAILED | ${city.city} | page ${page} | ` +
                    `HTTP ${result.statusCode ?? "N/A"} | ${result.error}`,
            );
            summary.stopReason = `request failed on page ${page}`;
            summary.failureLog = buildFailureLog({
                city,
                payload,
                result,
                page,
            });
            return summary;
        }

        summary.pages = page;

        const data = result.response?.data;
        const hotelCount = Array.isArray(data?.hotels) ? data.hotels.length : 0;
        summary.totalHotels += hotelCount;

        console.log(
            `   ↳ ${hotelCount} hotel(s) on page ${page} ` +
                `(city total: ${summary.totalHotels}` +
                `${data?.totalRecords != null ? ` / ${data.totalRecords}` : ""})`,
        );

        // 1) Empty page → city is done.
        if (hotelCount === 0) {
            summary.success = true;
            summary.stopReason = "empty hotels list";
            return summary;
        }

        // 2) Track consecutive low-yield pages.
        if (hotelCount < MIN_RECORDS_PER_PAGE) {
            consecutiveLowPages += 1;
            console.log(
                `   ↳ low page (${consecutiveLowPages}/${MAX_CONSECUTIVE_LOW_PAGES})`,
            );
        } else {
            consecutiveLowPages = 0;
        }

        if (consecutiveLowPages >= MAX_CONSECUTIVE_LOW_PAGES) {
            summary.success = true;
            summary.stopReason = `${MAX_CONSECUTIVE_LOW_PAGES} consecutive pages with < ${MIN_RECORDS_PER_PAGE} hotels`;
            return summary;
        }

        // 3) Need a fresh token to continue.
        const nextToken = data?.refreshToken;
        if (!nextToken) {
            summary.success = true;
            summary.stopReason = "no refreshToken in response";
            return summary;
        }
        if (nextToken === refreshToken) {
            summary.success = true;
            summary.stopReason = "refreshToken did not change";
            return summary;
        }

        refreshToken = nextToken;

        if (PAGE_DELAY_MS > 0) {
            await new Promise((resolve) => setTimeout(resolve, PAGE_DELAY_MS));
        }
    }

    // Hit the safety cap: treat as incomplete so it is visible in the log.
    summary.stopReason = `MAX_PAGES_PER_CITY (${MAX_PAGES_PER_CITY}) reached`;
    summary.failureLog = buildFailureLog({
        city,
        payload: null,
        page: MAX_PAGES_PER_CITY,
        result: {
            response: null,
            statusCode: null,
            error: summary.stopReason,
            errorName: null,
            errorCode: null,
            durationMs: null,
        },
    });
    return summary;
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

    if (!Number.isInteger(MIN_RECORDS_PER_PAGE) || MIN_RECORDS_PER_PAGE < 1) {
        throw new Error("MIN_RECORDS_PER_PAGE must be a positive integer");
    }

    if (
        !Number.isInteger(MAX_CONSECUTIVE_LOW_PAGES) ||
        MAX_CONSECUTIVE_LOW_PAGES < 1
    ) {
        throw new Error("MAX_CONSECUTIVE_LOW_PAGES must be a positive integer");
    }

    if (!Number.isInteger(MAX_PAGES_PER_CITY) || MAX_PAGES_PER_CITY < 1) {
        throw new Error("MAX_PAGES_PER_CITY must be a positive integer");
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
        const failedLogCollection = db.collection(FAILED_LOG_COLLECTION);

        console.log(`Connected to MongoDB: ${DB_NAME}`);
        console.log(`Target API URL: ${API_URL}`);
        console.log(`Batch size: ${BATCH_SIZE} cities`);
        console.log("Concurrency: 1 (sequential requests)");
        console.log(`Request timeout: ${REQUEST_TIMEOUT_MS} ms`);
        console.log(
            `Pagination: stop on empty page or ${MAX_CONSECUTIVE_LOW_PAGES} consecutive pages with < ${MIN_RECORDS_PER_PAGE} hotels`,
        );

        // ─── Grand totals across all batches ───────────────────────────────
        let batchNumber = 0;
        let grandTotalFetched = 0;
        let grandTotalSuccessful = 0;
        let grandTotalFailed = 0;
        let grandTotalPages = 0;
        let grandTotalHotels = 0;

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

            const successfulCities = []; // { _id, pages, totalHotels }
            const failedLogs = [];
            let batchPages = 0;
            let batchHotels = 0;

            // ─── Process cities one at a time; each city is fully paginated
            //     before the next city starts ───────────────────────────────
            for (let i = 0; i < cities.length; i++) {
                const city = cities[i];
                const tag = `[Batch ${batchNumber}][${i + 1}/${cities.length}]`;

                console.log(`\n${tag} Starting city: ${city.city}`);

                const summary = await fetchAllPagesForCity(city, tag);

                batchPages += summary.pages;
                batchHotels += summary.totalHotels;

                if (summary.success) {
                    successfulCities.push({
                        _id: city._id,
                        pages: summary.pages,
                        totalHotels: summary.totalHotels,
                    });
                    console.log(
                        `SUCCESS | ${city.city} | ${summary.pages} page(s) | ` +
                            `${summary.totalHotels} hotel(s) | stop: ${summary.stopReason}`,
                    );
                } else {
                    console.error(
                        `CITY FAILED | ${city.city} | after ${summary.pages} successful page(s) | ${summary.stopReason}`,
                    );
                    if (summary.failureLog) {
                        failedLogs.push(summary.failureLog);
                    }
                }
            }

            // ─── Persist batch results ──────────────────────────────────────

            if (failedLogs.length > 0) {
                await failedLogCollection.insertMany(failedLogs, {
                    ordered: true,
                });
                console.log(
                    `Saved ${failedLogs.length} failed request(s) to ${FAILED_LOG_COLLECTION}.`,
                );
            }

            if (successfulCities.length > 0) {
                await cityCollection.bulkWrite(
                    successfulCities.map(({ _id, pages, totalHotels }) => ({
                        updateOne: {
                            filter: { _id },
                            update: {
                                $set: {
                                    hitted: 1,
                                    hittedAt: new Date(),
                                    pagesFetched: pages,
                                    hotelsFetched: totalHotels,
                                },
                            },
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
                        successfulCount: successfulCities.length,
                        failedCount: failedLogs.length,
                        pagesFetched: batchPages,
                        hotelsFetched: batchHotels,
                    },
                },
                { upsert: true },
            );

            grandTotalFetched += cities.length;
            grandTotalSuccessful += successfulCities.length;
            grandTotalFailed += failedLogs.length;
            grandTotalPages += batchPages;
            grandTotalHotels += batchHotels;

            console.log(`\n─── Batch ${batchNumber} Summary ───────────────────`);
            console.log(`  Cities fetched:    ${cities.length}`);
            console.log(`  Cities successful: ${successfulCities.length}`);
            console.log(`  Cities failed:     ${failedLogs.length}`);
            console.log(`  Pages requested:   ${batchPages}`);
            console.log(`  Hotels received:   ${batchHotels}`);
            console.log(`  Last _id:          ${lastProcessedId}`);
            console.log(`────────────────────────────────────────────`);
        }

        // ─── Grand total after all batches ──────────────────────────────────
        console.log("\n╔══════════════════════════════════════════╗");
        console.log("║           GRAND TOTAL SUMMARY            ║");
        console.log("╠══════════════════════════════════════════╣");
        console.log(`║  Batches completed : ${String(batchNumber).padEnd(20)}║`);
        console.log(`║  Cities processed  : ${String(grandTotalFetched).padEnd(20)}║`);
        console.log(`║  Cities successful : ${String(grandTotalSuccessful).padEnd(20)}║`);
        console.log(`║  Cities failed     : ${String(grandTotalFailed).padEnd(20)}║`);
        console.log(`║  Total pages       : ${String(grandTotalPages).padEnd(20)}║`);
        console.log(`║  Total hotels      : ${String(grandTotalHotels).padEnd(20)}║`);
        console.log("╚══════════════════════════════════════════╝");
    } finally {
        await client.close();
    }
}

main().catch((error) => {
    console.error("Script failed:", error);
    process.exitCode = 1;
});