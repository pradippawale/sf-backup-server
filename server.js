// server.js
const express = require('express');
const { Pool } = require('pg');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());

// PostgreSQL connection pool
const pool = new Pool({
  user: process.env.PGUSER,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  port: 5432,
  ssl: true, // Required for Render
});

// Endpoint to receive Salesforce data
app.post('/postgres/ingest', async (req, res) => {
  const { objectName, records } = req.body;

  if (!objectName || !Array.isArray(records)) {
    return res.status(400).send({ error: 'Invalid payload' });
  }

  try {
    const client = await pool.connect();

    for (const record of records) {
      const fields = Object.keys(record);
      const values = Object.values(record);

      const placeholders = fields.map((_, i) => `$${i + 1}`).join(',');
      const query = `
        INSERT INTO ${objectName.toLowerCase()} (${fields.join(',')})
        VALUES (${placeholders})
        ON CONFLICT DO NOTHING;
      `;

      await client.query(query, values);
    }

    client.release();
    res.send({ status: 'Success', inserted: records.length });
  } catch (error) {
    console.error(error);
    res.status(500).send({ error: 'Failed to insert data' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
