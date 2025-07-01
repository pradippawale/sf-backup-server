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

  const client = new Client({
    user: 'sfdatabaseuser',
    host: 'dpg-d1i3u8fdiees73cf0dug-a.oregon-postgres.render.com',
    database: 'sfdatabase_34oi',
    password: 'D898TUsAal4ksBUs5QoQffxMZ6MY5aAH',
    port: 5432,
    ssl: {
      require: true,
      rejectUnauthorized: false
    }
  });

  try {
    console.log('🟠 [DEBUG] Connecting to PostgreSQL...');
    await client.connect();
    console.log('🟢 [DEBUG] Connected to PostgreSQL ✅');

    const tableName = objectName.toLowerCase().replace(/[^a-z0-9_]/gi, '_');

    const csvStream = Readable.from([csvData]);
    let headers = [];
    const rows = [];

    await new Promise((resolve, reject) => {
      csvStream
        .pipe(csv())
        .on('headers', (hdrs) => {
          headers = hdrs.map(h => h.toLowerCase().replace(/[^a-z0-9_]/gi, '_'));
        })
        .on('data', (row) => rows.push(row))
        .on('end', resolve)
        .on('error', reject);
    });

    console.log(`📄 [DEBUG] Parsed ${rows.length} rows with headers: ${headers}`);

    const columnsSQL = headers.map(col => `"${col}" TEXT`).join(',\n');
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS "${tableName}" (
        id SERIAL PRIMARY KEY,
        ${columnsSQL},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `;
    await client.query(createTableQuery);
    console.log(`🛠️ [DEBUG] Created table "${tableName}" with columns: ${headers.join(', ')}`);

    for (const row of rows) {
      const values = headers.map(col => row[col] || '');
      const placeholders = headers.map((_, i) => `$${i + 1}`).join(', ');
      const insertQuery = `
        INSERT INTO "${tableName}" (${headers.map(h => `"${h}"`).join(', ')})
        VALUES (${placeholders})
      `;
      await client.query(insertQuery, values);
    }

    console.log('✅ [DEBUG] All rows inserted');
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
