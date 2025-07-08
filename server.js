const express = require('express');
const bodyParser = require('body-parser');
const { Client } = require('pg');
const { Readable } = require('stream');
const csv = require('csv-parser');

const app = express();
app.use(bodyParser.json({ limit: '20mb' }));

app.post('/postgres/ingest', async (req, res) => {
  console.log('🔵 [DEBUG] Ingest API hit');

  const { objectName, csvData, part, filename } = req.body;

  if (!objectName || !csvData) {
    console.error('🔴 [ERROR] Missing objectName or csvData');
    return res.status(400).json({ error: 'Missing objectName or csvData (base64-encoded)' });
  }

  const tableName = objectName.toLowerCase().replace(/[^a-z0-9_]/gi, '_');
  const objectIdColumnName = `${tableName}_id`;

  console.log(`🟡 [DEBUG] Received object: ${objectName}, file: ${filename}, part: ${part}`);

  let csvString;
  try {
    csvString = Buffer.from(csvData, 'base64').toString('utf-8');
  } catch (err) {
    return res.status(400).json({ error: 'Invalid base64 csvData' });
  }

  if (!csvString || csvString.trim().length === 0) {
    return res.status(400).json({ error: 'Decoded CSV is empty' });
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
    await client.connect();
    console.log('🟢 [DEBUG] Connected to PostgreSQL ✅');

    const rows = [];
    await new Promise((resolve, reject) => {
      Readable.from([csvString])
        .pipe(csv())
        .on('data', (row) => rows.push(row))
        .on('end', resolve)
        .on('error', reject);
    });

    if (rows.length === 0) throw new Error('Parsed CSV is empty');
    console.log(`📄 [DEBUG] Parsed ${rows.length} rows from CSV`);

    const originalHeaders = Object.keys(rows[0]);
    const headerMap = {};
    let tableColumns = [];

    const checkTableQuery = `SELECT column_name FROM information_schema.columns WHERE table_name = $1`;
    const result = await client.query(checkTableQuery, [tableName]);
    tableColumns = result.rows.map((row) => row.column_name);

    const normalizedHeaders = originalHeaders.map((h) => {
      let clean = h.toLowerCase().replace(/[^a-z0-9_]/gi, '_');
      if (clean === 'id') {
        clean = tableColumns.includes(objectIdColumnName) ? objectIdColumnName :
                tableColumns.includes('id') ? 'id' : objectIdColumnName;
      }
      headerMap[h] = clean;
      return clean;
    });

    const uniqueKey = tableColumns.includes(objectIdColumnName)
      ? objectIdColumnName
      : tableColumns.includes('id')
        ? 'id'
        : normalizedHeaders.find(h => h === objectIdColumnName || h === 'id');

    const columnDefinitions = normalizedHeaders.map((col) => `"${col}" TEXT`);
    const uniqueConstraint = uniqueKey ? `, UNIQUE("${uniqueKey}")` : '';

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS "${tableName}" (
        ${columnDefinitions.join(', ')},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ${uniqueConstraint}
      );
    `;
    await client.query(createTableQuery);
    console.log(`🛠️ [DEBUG] Table ensured: "${tableName}" with UNIQUE("${uniqueKey}")`);

    let insertedCount = 0;
    for (const row of rows) {
      const columns = [];
      const values = [];
      const placeholders = [];

      let i = 1;
      for (const originalKey of originalHeaders) {
        const normKey = headerMap[originalKey];
        columns.push(`"${normKey}"`);
        values.push(row[originalKey]);
        placeholders.push(`$${i++}`);
      }

      let insertQuery = `
        INSERT INTO "${tableName}" (${columns.join(', ')})
        VALUES (${placeholders.join(', ')})
      `;

      if (uniqueKey) insertQuery += ` ON CONFLICT ("${uniqueKey}") DO NOTHING`;

      const result = await client.query(insertQuery, values);
      if (result.rowCount > 0) insertedCount++;
    }

    console.log(`✅ [DEBUG] Inserted ${insertedCount} of ${rows.length} rows (part ${part})`);
    res.status(200).json({
      status: 'success',
      part,
      filename,
      inserted: insertedCount,
      total: rows.length
    });

  } catch (error) {
    console.error('🔴 [ERROR] Failed during processing:', error);
    res.status(500).json({ error: 'Failed to process CSV chunk', details: error.message });
  } finally {
    await client.end().catch(err => console.error('🔴 [ERROR] Closing DB connection:', err));
    console.log('🔵 [DEBUG] PostgreSQL connection closed');
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT}`);
});
