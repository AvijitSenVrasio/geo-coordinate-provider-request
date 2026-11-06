#!/usr/bin/env node
require('dotenv').config();

const path = require('path');
const { MongoClient } = require('mongodb');
const XLSX = require('xlsx');

const URI = process.env.MONGODB_URI;
const DB_NAME = process.env.MONGODB_DATABASE || process.env.MONGODB_DB;
const COLLECTION_NAME = process.env.MONGODB_COLLECTION || 'city_hotel_list';

const DEFAULT_FILE = 'vrasio_top2_cities_by_country.xlsx';

// Case-insensitive exact match on country / city.
const COLLATION = { locale: 'en', strength: 2 };

function toText(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text === '' ? null : text;
}

function readRows(filePath) {
  const workbook = XLSX.readFile(filePath);
  const sheetName = workbook.SheetNames[0];

  if (!sheetName) throw new Error('No worksheet found');

  const rows = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], {
    defval: null,
    raw: true
  });

  return rows
    .map((row, index) => ({
      excelRow: index + 2,
      country: toText(row.Country),
      city: toText(row.City)
    }))
    // Skips blank rows and footnote rows (country text without a city).
    .filter(row => row.country && row.city);
}

async function main() {
  if (!URI) throw new Error('MONGODB_URI is missing in .env');
  if (!DB_NAME) throw new Error('MONGODB_DATABASE is missing in .env');

  const inputFile = process.argv.slice(2).find(arg => !arg.startsWith('--'));
  const dryRun = process.argv.includes('--dry-run');
  const rows = readRows(path.resolve(inputFile || DEFAULT_FILE));

  if (!rows.length) throw new Error('Worksheet contains no data rows');

  const client = new MongoClient(URI);

  try {
    await client.connect();
    const collection = client.db(DB_NAME).collection(COLLECTION_NAME);

    let matched = 0;
    let modified = 0;
    const notFound = [];

    // Two rows (the two cities of a country) per bulk operation.
    for (let i = 0; i < rows.length; i += 2) {
      const pair = rows.slice(i, i + 2);

      if (dryRun) {
        pair.forEach(r => console.log(`[dry-run] ${r.country} / ${r.city}`));
        continue;
      }

      const result = await collection.bulkWrite(
        pair.map(({ country, city }) => ({
          updateMany: {
            filter: { country, city },
            update: { $set: { target: 1 } },
            collation: COLLATION
          }
        })),
        { ordered: false }
      );

      matched += result.matchedCount;
      modified += result.modifiedCount;

      // Per-row misses can't be read from bulk result, so recheck only if counts are short.
      if (result.matchedCount < pair.length) {
        for (const { country, city, excelRow } of pair) {
          const exists = await collection.countDocuments(
            { country, city },
            { collation: COLLATION, limit: 1 }
          );
          if (!exists) notFound.push(`row ${excelRow}: ${country} / ${city}`);
        }
      }

      console.log(
        `Processed ${Math.min(i + 2, rows.length)}/${rows.length} rows`
      );
    }

    console.log('\nDone');
    console.log(`Database: ${DB_NAME}`);
    console.log(`Collection: ${COLLECTION_NAME}`);
    console.log(`Rows read: ${rows.length}`);
    console.log(`Documents matched: ${matched}`);
    console.log(`Documents modified: ${modified}`);

    if (notFound.length) {
      console.log(`\nNo document found for ${notFound.length} row(s):`);
      notFound.forEach(line => console.log(`  ${line}`));
    }
  } finally {
    await client.close();
  }
}

main().catch(error => {
  console.error('Update failed:', error.message);
  process.exitCode = 1;
});
