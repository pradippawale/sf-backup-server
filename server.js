const express = require('express');
const bodyParser = require('body-parser');
const { Client } = require('pg');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 10000;

app.use(bodyParser.json());

// ✅ Create PostgreSQL client with proper SSL setup for Render
const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false // <-- this is what prevents the self-signed cert error
  }
});

// 🔄 Connect on startup
client.connect(err => {
  if (err) {
    console.error('❌ PostgreSQL connection error:', err.stack);
  } else {
    console.log('✅ Connected to PostgreSQL!');
  }
});

// 🔁 Endpoint to receive data from Salesforce
app.post('/postgres/ingest', async (req, res) => {
  const { objectName, records } = req.body;

  if (!objectName || !Array.isArray(records) || records.length === 0) {
    return res.status(400).json({ error: 'Invalid payload' });
  }

  try {
    const columns = Object.keys(records[0]);
    const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
    const insertQuery = `INSERT INTO "${objectName}" (${columns.join(', ')}) VALUES (${placeholders})`;

    for (const record of records) {
      const values = columns.map(col => record[col]);
      await client.query(insertQuery, values);
    }

    res.status(200).json({ status: 'success', inserted: records.length });
  } catch (error) {
    console.error('❌ Insert error:', error.message);
    res.status(500).json({ error: 'Failed to insert data' });
  }
});

// 🟢 Start server
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
