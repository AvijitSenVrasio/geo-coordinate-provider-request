#!/usr/bin/env node
require('dotenv').config();

const path = require('path');
const { MongoClient } = require('mongodb');
const XLSX = require('xlsx');

const URI = process.env.MONGODB_URI;
const DB_NAME = process.env.MONGODB_DATABASE;
const COLLECTION_NAME =
  process.env.MONGODB_COLLECTION || 'city_hotel_counts';

function normalize(value) {
  return String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/[\s\-./()]+/g, '_')
    .replace(/[^a-z0-9_]/g, '')
    .replace(/^_+|_+$/g, '');
}

function getCell(row, headers, aliases) {
  // Prefer an exact original header match first.
  for (const alias of aliases) {
    const exact = headers.find(
      header => header.original.trim().toLowerCase() === alias.toLowerCase()
    );

    if (exact) return row[exact.original];
  }

  // Fall back to normalized matching.
  for (const alias of aliases) {
    const normalizedAlias = normalize(alias);
    const match = headers.find(
      header => header.normalized === normalizedAlias
    );

    if (match) return row[match.original];
  }

  return undefined;
}

function toText(value) {
  if (value === null || value === undefined || String(value).trim() === '') {
    return null;
  }
  return String(value).trim();
}

function toNumber(value) {
  if (value === null || value === undefined || String(value).trim() === '') {
    return null;
  }

  if (typeof value === 'number' && Number.isFinite(value)) return value;

  const number = Number(String(value).replace(/,/g, '').trim());
  return Number.isFinite(number) ? number : null;
}

function makeDocument(row, headers, excelRow) {
  const country = toText(
    getCell(row, headers, ['country', 'country_name'])
  );
  const city = toText(
    getCell(row, headers, ['city', 'city_name', 'destination'])
  );

  const lat = toNumber(
    getCell(row, headers, ['lat', 'latitude'])
  );
  const long = toNumber(
    getCell(row, headers, ['long', 'lng', 'lon', 'longitude'])
  );

  // Ignore completely blank rows.
  if (!country && !city && lat === null && long === null) {
    return null;
  }

  if (!country || !city) {
    throw new Error(
      `Excel row ${excelRow}: Country and City are required`
    );
  }

  const document = { country, city, lat, long };

  // Import these extra fields when they exist in the worksheet.
  const optionalColumns = [
    { field: 'Type', aliases: ['type'], text: true },
    { field: '3_Star', aliases: ['3_star', '3 star', '3-star', '3star'] },
    { field: '4_Star', aliases: ['4_star', '4 star', '4-star', '4star'] },
    { field: '5_Star', aliases: ['5_star', '5 star', '5-star', '5star'] },
    { field: 'TOTAL', aliases: ['TOTAL'] },
    { field: 'T_A', aliases: ['T/A'] }
  ];

  for (const column of optionalColumns) {
    const value = getCell(row, headers, column.aliases);

    if (value === undefined) continue;

    document[column.field] = column.text
      ? toText(value)
      : (toNumber(value) ?? toText(value));
  }

  return document;
}

async function main() {
  if (!URI) throw new Error('MONGODB_URI is missing in .env');
  if (!DB_NAME) throw new Error('MONGODB_DATABASE is missing in .env');

  const args = process.argv.slice(2);
  const clearFirst = args.includes('--clear');
  const inputFile = args.find(arg => !arg.startsWith('--'));

  if (!inputFile) {
    throw new Error(
      'Usage: node import-cities-to-mongodb.js <file.xlsx> [--clear]'
    );
  }

  const workbookPath = path.resolve(inputFile);
  const workbook = XLSX.readFile(workbookPath);
  const sheetName = workbook.SheetNames[0];

  if (!sheetName) throw new Error('No worksheet found');

  const rows = XLSX.utils.sheet_to_json(
    workbook.Sheets[sheetName],
    { defval: null, raw: true }
  );

  if (!rows.length) throw new Error('Worksheet contains no data rows');

  const headers = Object.keys(rows[0]).map(key => ({
    normalized: normalize(key),
    original: key
  }));

  const hasHeader = (aliases) =>
    aliases.some(alias =>
      headers.some(header => header.normalized === normalize(alias))
    );

  if (!hasHeader(['country', 'country_name'])) {
    throw new Error('Country column not found');
  }

  if (!hasHeader(['city', 'city_name', 'destination'])) {
    throw new Error('City column not found');
  }

  const documents = rows
    .map((row, index) => makeDocument(row, headers, index + 2))
    .filter(Boolean);

  if (!documents.length) throw new Error('No valid rows to import');

  const client = new MongoClient(URI);

  try {
    await client.connect();

    const collection = client
      .db(DB_NAME)
      .collection(COLLECTION_NAME);

    if (clearFirst) {
      const result = await collection.deleteMany({});
      console.log(`Deleted ${result.deletedCount} existing documents`);
    }

    let inserted = 0;
    const batchSize = 500;

    for (let i = 0; i < documents.length; i += batchSize) {
      const batch = documents.slice(i, i + batchSize);
      const result = await collection.insertMany(batch);

      inserted += result.insertedCount;
      console.log(`Inserted ${inserted}/${documents.length}`);
    }

    console.log('\nImport completed successfully');
    console.log(`Database: ${DB_NAME}`);
    console.log(`Collection: ${COLLECTION_NAME}`);
    console.log(`Worksheet: ${sheetName}`);
    console.log(`Documents inserted: ${inserted}`);
  } finally {
    await client.close();
  }
}

main().catch(error => {
  console.error('Import failed:', error.message);
  process.exitCode = 1;
});
