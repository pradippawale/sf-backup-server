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
  const objectIdColumnName = `${tableName}_id`;

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

    const originalHeaders = Object.keys(rows[0]);
    const headerMap = {};
    let tableColumns = [];

    // Step 1: Check if table already exists and get existing columns
    const checkTableQuery = `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = $1
    `;
    try {
      const result = await client.query(checkTableQuery, [tableName]);
      tableColumns = result.rows.map((row) => row.column_name);
    } catch (err) {
      console.warn(`⚠️ [WARNING] Could not check columns for "${tableName}"`, err);
    }

    // Step 2: Normalize headers and resolve "Id" properly
    const normalizedHeaders = originalHeaders.map((h) => {
      let clean = h.toLowerCase().replace(/[^a-z0-9_]/gi, '_');

      if (clean === 'id') {
        if (tableColumns.includes(objectIdColumnName)) {
          clean = objectIdColumnName;
        } else if (tableColumns.includes('id')) {
          clean = 'id';
        } else {
          clean = objectIdColumnName; // default if table doesn't exist
        }
      }

      headerMap[h] = clean;
      return clean;
    });

    // Step 3: Determine which field to use for uniqueness
    const uniqueKey = tableColumns.includes(objectIdColumnName)
      ? objectIdColumnName
      : tableColumns.includes('id')
        ? 'id'
        : normalizedHeaders.find(h => h === objectIdColumnName || h === 'id');

    // Step 4: Create table if not exists with proper schema and UNIQUE constraint
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
    console.log(`🛠️ [DEBUG] Created/ensured table "${tableName}" with UNIQUE on "${uniqueKey}"`);

    // Step 5: Insert rows with ON CONFLICT DO NOTHING
    let insertedCount = 0;

    for (const row of rows) {
      const columns = [];
      const values = [];
      const placeholders = [];

      let i = 1;
      for (const originalKey of originalHeaders) {
        const normalizedKey = headerMap[originalKey];
        columns.push(`"${normalizedKey}"`);
        values.push(row[originalKey]);
        placeholders.push(`$${i++}`);
      }

      let insertQuery = `
        INSERT INTO "${tableName}" (${columns.join(', ')})
        VALUES (${placeholders.join(', ')})
      `;

      if (uniqueKey) {
        insertQuery += ` ON CONFLICT ("${uniqueKey}") DO NOTHING`;
      }

      const result = await client.query(insertQuery, values);
      if (result.rowCount > 0) insertedCount++;
    }

    console.log(`✅ [DEBUG] Inserted ${insertedCount} of ${rows.length} rows into "${tableName}"`);
    res.status(200).json({ status: 'success', message: `${insertedCount} new rows saved to ${tableName}` });

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
