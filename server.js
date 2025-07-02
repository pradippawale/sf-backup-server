const express = require('express');
const bodyParser = require('body-parser');
const { Client } = require('pg');
const { Readable } = require('stream');
const csv = require('csv-parser');

const app = express();
app.use(bodyParser.json({ limit: '10mb' }));

app.post('/postgres/ingest', async (req, res) => {
  console.log('🔵 [DEBUG] Ingest API hit');

  const { objectName, csvData } = req.body;

  if (!objectName || !csvData) {
    return res.status(400).json({ error: 'Missing objectName or csvData' });
  }

  const client = new Client({
    user: 'sfdatabaseuser',
    host: 'dpg-d1i3u8fdiees73cf0dug-a.oregon-postgres.render.com',
    database: 'sfdatabase_34oi',
    password: 'D898TUsAal4ksBUs5QoQffxMZ6MY5aAH',
    port: 5432,
    ssl: { require: true, rejectUnauthorized: false },
  });

  try {
    console.log(`🟡 [DEBUG] Received object: ${objectName}`);
    console.log(`🟡 [DEBUG] CSV size: ${csvData.length} characters`);

    await client.connect();
    console.log('🟢 [DEBUG] Connected to PostgreSQL ✅');

    const tableName = objectName.toLowerCase().replace(/[^a-z0-9_]/gi, '_');

    // Parse CSV and get rows and headers
    const csvStream = Readable.from([csvData]);
    const rows = [];
    const headersSet = new Set();

    await new Promise((resolve, reject) => {
      csvStream
        .pipe(csv())
        .on('headers', (headers) => headers.forEach((h) => headersSet.add(h.toLowerCase().replace(/[^a-z0-9_]/gi, '_'))))
        .on('data', (row) => rows.push(row))
        .on('end', resolve)
        .on('error', reject);
    });

    const headers = [...headersSet];
    console.log(`📄 [DEBUG] Parsed ${rows.length} rows with ${headers.length} columns`);

    // Dynamically build CREATE TABLE
    const columnsDDL = headers.map(h => `"${h}" TEXT`).join(', ');
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS "${tableName}" (
        id SERIAL PRIMARY KEY,
        ${columnsDDL}
      );
    `;
    await client.query(createTableQuery);
    console.log(`🛠️ [DEBUG] Created/ensured table "${tableName}" with fields: ${headers.join(', ')}`);

    // Insert each row
    for (const row of rows) {
      const columns = [];
      const values = [];
      const placeholders = [];

      let i = 1;
      for (const key of headers) {
        const value = row[key] || row[Object.keys(row).find(k => k.toLowerCase().replace(/[^a-z0-9_]/gi, '_') === key)];
        columns.push(`"${key}"`);
        values.push(value ?? null);
        placeholders.push(`$${i++}`);
      }

      const insertQuery = `
        INSERT INTO "${tableName}" (${columns.join(', ')})
        VALUES (${placeholders.join(', ')})
      `;
      await client.query(insertQuery, values);
    }

    console.log('✅ [DEBUG] All rows inserted');
    res.status(200).json({ status: 'success', message: `${rows.length} rows inserted into ${tableName}` });
  } catch (error) {
    console.error('🔴 [ERROR] Failed to insert data:', error);
    res.status(500).json({ error: 'Failed to insert parsed CSV data', details: error.message });
  } finally {
    await client.end();
    console.log('🔵 [DEBUG] PostgreSQL connection closed');
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT}`);
});
