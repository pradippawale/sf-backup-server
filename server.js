const express = require('express');
const { Pool } = require('pg');
const csvParse = require('csv-parse/lib/sync');

const router = express.Router();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// Utility: Find first column that ends in "id"
function getObjectIdColumn(headers) {
  for (const h of headers) {
    if (h.toLowerCase().endsWith('id')) {
      return h;
    }
  }
  return null;
}

// Utility: Check if table exists
async function tableExists(tableName) {
  const query = `
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name = $1
    );
  `;
  const result = await pool.query(query, [tableName]);
  return result.rows[0].exists;
}

// Utility: Get existing column names in table
async function getTableColumns(tableName) {
  const query = `
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = $1;
  `;
  const result = await pool.query(query, [tableName]);
  return result.rows.map(row => row.column_name);
}

router.post('/postgres/ingest', async (req, res) => {
  try {
    const { objectName, csvData, part, filename } = req.body;

    if (!objectName || !csvData) {
      return res.status(400).json({ error: 'Missing objectName or csvData' });
    }

    const records = csvParse(csvData, {
      columns: true,
      skip_empty_lines: true,
    });

    if (!records.length) {
      return res.status(400).json({ error: 'No records found in CSV data' });
    }

    const headers = Object.keys(records[0]);
    const tableName = objectName.toLowerCase() + '_backup';
    let objectIdColumnName = getObjectIdColumn(headers);

    // STEP 1: CREATE TABLE IF NEEDED
    const exists = await tableExists(tableName);
    if (!exists) {
      const columnDefinitions = headers.map(col => {
        if (col === objectIdColumnName) {
          return `"${col}" TEXT PRIMARY KEY`;
        }
        return `"${col}" TEXT`;
      });

      const createQuery = `
        CREATE TABLE "${tableName}" (
          ${columnDefinitions.join(',\n')}
        );
      `;
      await pool.query(createQuery);
      console.log(`🆕 Created table "${tableName}"`);
    } else {
      // Table exists, check if it has a usable ID column
      const existingCols = await getTableColumns(tableName);
      objectIdColumnName = getObjectIdColumn(existingCols);
    }

    // STEP 2: INSERT DATA
    const columns = headers.map(col => `"${col}"`);
    const placeholders = headers.map((_, i) => `$${i + 1}`);

    let insertQuery = `
      INSERT INTO "${tableName}" (${columns.join(', ')})
      VALUES (${placeholders.join(', ')})
    `;

    if (objectIdColumnName) {
      insertQuery += ` ON CONFLICT ("${objectIdColumnName}") DO NOTHING`;
    }

    for (const row of records) {
      const values = headers.map(col => row[col] || null);
      await pool.query(insertQuery, values);
    }

    console.log(`✅ Ingested ${records.length} records for ${objectName} (part ${part}) from ${filename}`);
    res.status(200).json({ message: `Ingested ${records.length} records for ${objectName} (part ${part})` });

  } catch (err) {
    console.error('❌ Ingestion error:', err);
    res.status(500).json({ error: 'Internal Server Error', detail: err.message });
  }
});

module.exports = router;
