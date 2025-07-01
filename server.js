const express = require('express');
const bodyParser = require('body-parser');
const { Client } = require('pg');

const app = express();
app.use(bodyParser.json());

app.post('/postgres/ingest', async (req, res) => {
  console.log('🔵 [DEBUG] Ingest API hit');

  const { objectName, csvData } = req.body;

  if (!objectName || !csvData) {
    console.error('🔴 [ERROR] Missing objectName or csvData in request body');
    return res.status(400).json({ error: 'Missing objectName or csvData' });
  }

  console.log(`🟡 [DEBUG] Received object: ${objectName}`);
  console.log(`🟡 [DEBUG] CSV size: ${csvData.length} characters`);

  const client = new Client({
    user: 'sfdatabase_user',
    host: 'dpg-d1ek5bre5dus73bho0ag-a.oregon-postgres.render.com',
    database: 'sfdatabase',
    password: '8805739771Patil',
    port: 5432,
    ssl: {
      rejectUnauthorized: false, // Allow self-signed certs for dev
    },
  });

  try {
    console.log('🟠 [DEBUG] Connecting to PostgreSQL...');
    await client.connect();
    console.log('🟢 [DEBUG] Connected to PostgreSQL ✅');

    // Example: insert as raw log table (customize as needed)
    const insertQuery = 'INSERT INTO backup_logs (object_name, csv_data) VALUES ($1, $2)';
    await client.query(insertQuery, [objectName, csvData]);
    console.log('✅ [DEBUG] Data inserted successfully');

    res.status(200).json({ status: 'success', message: 'Data saved' });
  } catch (error) {
    console.error('🔴 [ERROR] PostgreSQL connection or insert failed:', error);
    res.status(500).json({ error: 'Failed to insert data into PostgreSQL', details: error.message });
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
