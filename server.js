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
    console.error('🔴 [ERROR] Missing objectName or csvData in request body');
    return res.status(400).json({ error: 'Missing objectName or csvData' });
  }

  const tableName = objectName.toLowerCase().replace(/[^a-z0-9_]/gi, '_');
  console.log(`🟡 [DEBUG] Received object: ${objectName}`);
  console.log(`🟡 [DEBUG] CSV size: ${csvData.length} characters`);

  const client = new Client({
    user: 'sfdatabaseuser',
    host: 'dpg-d1i3u8fdiees73cf0dug-a.oregon-postgres.render.com',
    database: 'sfdatabase_34oi',
    password: 'D898TUsAal4ksBUs5QoQffxMZ6MY5aAH',
    port: 5432,
    ssl: { require: true, rejectUnauthorized: false },
  });

  try {
    await client.connect();
    console.log('🟢 [DEBUG] Connected to PostgreSQL ✅');

    const csvStream = Readable.from([csvData]);
    const rows = [];
    await new Promise((resolve, reject) => {
      csvStream
        .pipe(csv())
        .on('data', (row) => rows.push(row))
        .on('end', resolve)
        .on('error', reject);
    });

    console.log(`📄 [DEBUG] Parsed ${rows.length} rows from CSV`);

    if (rows.length === 0) throw new Error('CSV is empty');

    // Normalize headers and map them
    const firstRow = rows[0];
    const headerMap = {};
    const normalizedHeaders = Object.keys(firstRow).map((originalHeader) => {
      let cleanHeader = originalHeader.toLowerCase().replace(/[^a-z0-9_]/gi, '_');
      if (cleanHeader === 'id') cleanHeader = `${tableName}_id`;
      headerMap[originalHeader] = cleanHeader;
      return cleanHeader;
    });

    // Create table if not exists
    const columnDefinitions = normalizedHeaders.map(col => `"${col}" TEXT`);
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS "${tableName}" (
        ${columnDefinitions.join(', ')},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `;
    await client.query(createTableQuery);
    console.log(`🛠️ [DEBUG] Created/ensured table "${tableName}" with fields: ${normalizedHeaders.join(', ')}`);

    // Insert each row
    for (const row of rows) {
      const columns = [];
      const values = [];
      const placeholders = [];

      let i = 1;
      for (const originalKey of Object.keys(firstRow)) {
        const normalizedKey = headerMap[originalKey];
        columns.push(`"${normalizedKey}"`);
        values.push(row[originalKey]);
        placeholders.push(`$${i++}`);
      }

      const insertQuery = `
        INSERT INTO "${tableName}" (${columns.join(', ')})
        VALUES (${placeholders.join(', ')});
      `;
      await client.query(insertQuery, values);
    }

    console.log(`✅ [DEBUG] Inserted ${rows.length} rows into "${tableName}"`);
    res.status(200).json({ status: 'success', message: `${rows.length} rows saved to ${tableName}` });
  } catch (error) {
    console.error('🔴 [ERROR] Failed to insert data:', error);
    res.status(500).json({ error: 'Failed to insert parsed CSV data', details: error.message });
  } finally {
    try {
      await client.end();
      console.log('🔵 [DEBUG] PostgreSQL connection closed');
    } catch (err) {
      console.error('🔴 [ERROR] Error while closing connection:', err);
    }
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT}`);
});
