// -----------------------------
// 📁 server.js (Node.js Backend)
// -----------------------------

const express = require('express');
const bodyParser = require('body-parser');
const { Client } = require('pg');
const csvParser = require('csv-parse/sync');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 10000;

app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.text({ type: 'text/csv', limit: '50mb' }));

// ✅ PostgreSQL client
const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

client.connect(err => {
  if (err) {
    console.error('❌ PostgreSQL connection error:', err.stack);
  } else {
    console.log('✅ Connected to PostgreSQL!');
  }
});

// ✅ JSON endpoint (for older Apex logic)
app.post('/postgres/ingest', async (req, res) => {
  const { objectName, records } = req.body;

  if (!objectName || !Array.isArray(records) || records.length === 0) {
    return res.status(400).json({ error: 'Invalid payload' });
  }

  try {
    const columns = Object.keys(records[0]);

    const createTableSQL = `
      CREATE TABLE IF NOT EXISTS "${objectName}" (
        ${columns.map(col => `"${col}" TEXT`).join(', ')}
      );
    `;
    await client.query(createTableSQL);

    const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
    const insertSQL = `INSERT INTO "${objectName}" (${columns.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`;

    for (const record of records) {
      const values = columns.map(col => record[col] ?? null);
      await client.query(insertSQL, values);
    }

    res.status(200).json({ status: 'success', inserted: records.length });
  } catch (err) {
    console.error('❌ JSON Ingest Error:', err.message);
    res.status(500).json({ error: 'Insert failed', details: err.message });
  }
});

// ✅ CSV endpoint (for Bulk API Apex integration)
app.post('/postgres/ingestCsv', async (req, res) => {
  try {
    const { objectName, csv } = JSON.parse(req.body);

    if (!objectName || !csv) {
      return res.status(400).json({ error: 'Missing objectName or csv' });
    }

    const records = csvParser.parse(csv, {
      columns: true,
      skip_empty_lines: true
    });

    const columns = Object.keys(records[0]);

    const createTableSQL = `
      CREATE TABLE IF NOT EXISTS "${objectName}" (
        ${columns.map(col => `"${col}" TEXT`).join(', ')}
      );
    `;
    await client.query(createTableSQL);

    const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
    const insertSQL = `INSERT INTO "${objectName}" (${columns.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`;

    for (const record of records) {
      const values = columns.map(col => record[col] ?? null);
      await client.query(insertSQL, values);
    }

    res.status(200).json({ status: 'success', inserted: records.length });
  } catch (err) {
    console.error('❌ CSV Ingest Error:', err.message);
    res.status(500).json({ error: 'CSV insert failed', details: err.message });
  }
});

// ✅ Start server
app.listen(PORT, () => {
  console.log(`🚀 Server running at https://sf-backup-server.onrender.com (port ${PORT})`);
});
