
require('dotenv').config();

const { MongoClient } = require('mongodb');
const autocannon = require('autocannon');

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = process.env.MONGODB_DB || 'vrasio_geo_cord';

const API_URL =
  process.env.API_URL ||
  'https://vrasio2.com/vrasio-hotel/hotel-list/getHotelList';

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 60);
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 1000);
const REQUEST_TIMEOUT_SECONDS = Number(
  process.env.REQUEST_TIMEOUT_SECONDS || 60,
);

const CITY_COLLECTION = 'city_hotel_list';
const META_COLLECTION = 'meta_data';
const FAILED_LOG_COLLECTION = 'meta_data_request_log';

const CHECKPOINT_ID = 'city_hotel_list_checkpoint';

const DEFAULT_PAYLOAD = {
  radius: 10,
  checkIn: '2026-12-15',
  checkOut: '2026-12-18',
  rooms: 1,
  adults: 2,
  children: 0,
  childAges: [],
  providers: [
    'DidaTravel',
    'TBO',
    'HotelBeds',
    'RateHawkDirect',
  ],
  sort: 'recommended',
  apply_fitlers: false,
  visitor_geo: {
    timezone: 'America/New_York',
    country_code: 'US',
  },
  user_id: 'usr_948172635',
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildPayload(city) {
  const latitude = Number(city.lat);
  const longitude = Number(city.long);
  const locationName = String(city.city || '').trim();

  if (
    !locationName ||
    !Number.isFinite(latitude) ||
    !Number.isFinite(longitude)
  ) {
    throw new Error(
      `Invalid city data: ${city._id}`,
    );
  }

  return {
    ...DEFAULT_PAYLOAD,
    latitude,
    longitude,
    locationName,
  };
}

// Execute one request using Autocannon.
// No concurrent requests are created.
function hitApi(payload) {
  return new Promise((resolve) => {
    const endpoint = new URL(API_URL);

    const options = {
      url: endpoint.origin,
      method: 'POST',
      path: `${endpoint.pathname}${endpoint.search}`,
      connections: 1,
      pipelining: 1,
      amount: 1,
      timeout: REQUEST_TIMEOUT_SECONDS,
      headers: {
        'content-type': 'application/json',
        accept: 'application/json',
      },
      requests: [
        {
          method: 'POST',
          path: `${endpoint.pathname}${endpoint.search}`,
          headers: {
            'content-type': 'application/json',
            accept: 'application/json',
          },
          body: JSON.stringify(payload),
        },
      ],
    };

    let settled = false;

    function finish(result) {
      if (settled) return;
      settled = true;
      resolve(result);
    }

    try {
      const instance = autocannon(options, (error, result) => {
        if (error || !result) {
          return finish({
            success: false,
            statusCode: 3,
            response: null,
            error: error?.message || 'No Autocannon result',
          });
        }

        const statusStats = result.statusCodeStats || {};
        const statusCodes = Object.keys(statusStats).filter(
          (code) => Number(statusStats[code]?.count || 0) > 0,
        );

        const statusCode =
          statusCodes.length === 1
            ? Number(statusCodes[0])
            : null;

        const success =
          statusCodes.length === 1 &&
          statusCode >= 200 &&
          statusCode < 300 &&
          Number(result.errors || 0) === 0;

        finish({
          success,
          statusCode,
          response: {
            statusCodeStats: statusStats,
            latencyMs: result.latency?.average ?? null,
            responseBytes: result.throughput?.total ?? null,
            responseBodyAvailable: false,
          },
          error: success
            ? null
            : statusCode
              ? `HTTP ${statusCode}`
              : 'Request failed or status could not be determined',
        });
      });

      instance.on('error', (error) => {
        finish({
          success: false,
          statusCode: 1,
          response: null,
          error: error.message,
        });
      });
    } catch (error) {
      finish({
        success: false,
        statusCode: 2,
        response: null,
        error: error.message,
      });
    }
  });
}

async function main() {
  if (!MONGODB_URI) {
    throw new Error('MONGODB_URI is missing in .env');
  }

  if (
    !Number.isInteger(BATCH_SIZE) ||
    BATCH_SIZE < 1 ||
    // !Number.isFinite(REQUEST_DELAY_MS) ||
    // REQUEST_DELAY_MS < 0 ||
    !Number.isFinite(REQUEST_TIMEOUT_SECONDS) ||
    REQUEST_TIMEOUT_SECONDS <= 0
  ) {
    throw new Error('Invalid batch, delay, or timeout configuration');
  }

  const client = new MongoClient(MONGODB_URI);

  try {
    await client.connect();

    const db = client.db(DB_NAME);

    const cityCollection = db.collection(CITY_COLLECTION);
    const metaCollection = db.collection(META_COLLECTION);
    const failedLogCollection = db.collection(
      FAILED_LOG_COLLECTION,
    );

    console.log(`Connected to MongoDB: ${DB_NAME}`);

    // Read the last processed record ID.
    const checkpoint = await metaCollection.findOne({
      _id: CHECKPOINT_ID,
    });

    const lastId = checkpoint?.lastProcessedId;

    // Never fetch previously processed or already successful records.
    const filter = {
      hitted: { $ne: 1 },
    };

    if (lastId) {
      filter._id = { $gt: lastId };
    }

    // Fetch the next batch in deterministic _id order.
    const cities = await cityCollection
      .find(filter)
      .sort({ _id: 1 })
      .limit(BATCH_SIZE)
      .toArray();

    if (cities.length === 0) {
      console.log(
        'No more eligible records after the checkpoint. Nothing to process.',
      );
      return;
    }

    console.log(`Batch size: ${cities.length}`);
    console.log('Concurrency: 1');
    console.log(`Delay between requests: ${REQUEST_DELAY_MS} ms`);

    const successfulIds = [];
    const failedLogs = [];

    // Process each city sequentially.
    for (let i = 0; i < cities.length; i++) {
      const city = cities[i];

      let payload = null;
      let result;

      try {
        payload = buildPayload(city);

        console.log(
          `\n[${i + 1}/${cities.length}] Requesting: ${city.city}`,
        );

        result = await hitApi(payload);
        console.log('result', result);
        
      } catch (error) {
        console.error('request error', error);
        
        result = {
          success: false,
          statusCode: 4,
          response: null,
          error: error.message,
        };
      }

      if (result.success) {
        successfulIds.push(city._id);

        console.log(
          `SUCCESS | ${city.city} | HTTP ${result.statusCode}`,
        );
      } else {
        console.error(
          `FAILED | ${city.city} | ` +
            `${result.statusCode ?? 'No HTTP status'} | ${result.error}`,
        );

        // Save failure details for later investigation/reprocessing.
        failedLogs.push({
          recordId: city._id,
          request: payload,
          response: result.response,
          statusCode: result.statusCode,
          error: result.error,
          city: city.city,
          createdAt: new Date(),
        });
      }

      // Never retry this request in this execution.
      // Wait before starting the next request.
    //   if (i < cities.length - 1 && REQUEST_DELAY_MS > 0) {
    //     await sleep(REQUEST_DELAY_MS);
    //   }
    }

    // 1. Persist failed requests before advancing the checkpoint.
    if (failedLogs.length > 0) {
      await failedLogCollection.insertMany(failedLogs, {
        ordered: true,
      });

      console.log(
        `Logged ${failedLogs.length} failed requests in ${FAILED_LOG_COLLECTION}`,
      );
    }

    // 2. Bulk-update successful city records.
    if (successfulIds.length > 0) {
      await cityCollection.bulkWrite(
        successfulIds.map((_id) => ({
          updateOne: {
            filter: { _id },
            update: {
              $set: {
                hitted: 1,
                hittedAt: new Date(),
              },
            },
          },
        })),
        { ordered: false },
      );
    }

    // 3. Advance checkpoint only after logging failures and
    // updating successful records have completed.
    const lastProcessedId = cities[cities.length - 1]._id;

    await metaCollection.updateOne(
      { _id: CHECKPOINT_ID },
      {
        $set: {
          lastProcessedId,
          lastBatchAt: new Date(),
          batchSize: cities.length,
          successfulCount: successfulIds.length,
          failedCount: failedLogs.length,
        },
      },
      { upsert: true },
    );

    console.log('\n========== BATCH SUMMARY ==========');
    console.log(`Fetched: ${cities.length}`);
    console.log(`Successful: ${successfulIds.length}`);
    console.log(`Failed: ${failedLogs.length}`);
    console.log(`Checkpoint: ${lastProcessedId}`);
    console.log('Failed requests will NOT be retried in this run.');
    console.log('===================================');
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error('Script failed:', error);
  process.exitCode = 1;
});
