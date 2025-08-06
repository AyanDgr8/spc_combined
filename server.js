// server.js

import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import https from 'https';
import fs from 'fs';
// import { fetchAgentStatus } from './agentStatus.js';
import { fetchReport } from './reportFetcher.js';
import { getPortalToken, httpsAgent } from './tokenService.js';
import axios from 'axios';
import { parseBuffer } from 'music-metadata';
import cookieParser from 'cookie-parser';
import jwt from 'jsonwebtoken';
import bcrypt from 'bcrypt';
import mysql from 'mysql2/promise';

dotenv.config();

// Debug: Log environment variables to verify they're loaded correctly
console.log('🔧 Environment variables loaded:');
console.log(`   PORT: ${process.env.PORT}`);
console.log(`   HOST: ${process.env.HOST}`);
console.log(`   PUBLIC_URL: ${process.env.PUBLIC_URL}`);

const app = express();
app.use(express.json()); // parse JSON bodies
app.use(cookieParser()); // parse cookies
const PORT = process.env.PORT || 9595;
const HOST = process.env.HOST || '0.0.0.0'; // 0.0.0.0 ensures the server binds to all network interfaces
const PUBLIC_URL = process.env.PUBLIC_URL || `https://${HOST}:${PORT}`;

console.log(`🚀 Server will start on: ${PUBLIC_URL}`);

// Helper to resolve __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(path.join(__dirname, 'public')));

// --- Authentication setup ---
const JWT_SECRET = process.env.JWT_SECRET || 'dev_secret_key';

// const pool = mysql.createPool({
//   host: 'localhost',
//   user: 'root',
//   password: 'Ayan@1012',
//   database: 'shams',
//   port: 3306,
//   waitForConnections: true,
//   connectionLimit: 5,
// });

const pool = mysql.createPool({
  host:"0.0.0.0",
  user: 'root',
  password: 'WELcome@123',
  database: 'shams',
  port: 3306,
  waitForConnections: true,
  connectionLimit: 5,
});

// Login
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  try {
    const [rows] = await pool.query(
      'SELECT id, username, email, password FROM users WHERE username = ? OR email = ? LIMIT 1',
      [username, username]
    );
    if (!rows.length) return res.status(401).json({ error: 'Invalid credentials' });
    const user = rows[0];
    const ok = await bcrypt.compare(password, user.password);
    if (!ok) return res.status(401).json({ error: 'Invalid credentials' });

    await pool.query('UPDATE users SET last_login = NOW() WHERE id = ?', [user.id]);

    const token = jwt.sign({ id: user.id, username: user.username, email: user.email }, JWT_SECRET, { expiresIn: '2h' });
    res.cookie('token', token, { httpOnly: true, sameSite: 'lax', maxAge: 2 * 60 * 60 * 1000 });
    res.json({ success: true, token }); // Include token in response body for iframe scenarios
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Server error' });
  }
});

// Auth check
app.get('/api/auth/check', (req, res) => {
  // Check for token in cookie first
  const cookieToken = req.cookies?.token;
  
  // Then check for Authorization header (Bearer token)
  const authHeader = req.headers.authorization;
  const bearerToken = authHeader && authHeader.startsWith('Bearer ') ? authHeader.substring(7) : null;
  
  // Use either token source
  const token = cookieToken || bearerToken;
  
  if (!token) return res.json({ authenticated: false });
  
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    res.json({ authenticated: true, user: { id: decoded.id, username: decoded.username } });
  } catch {
    res.json({ authenticated: false });
  }
});

// Logout
app.post('/api/logout', (req, res) => {
  res.clearCookie('token');
  res.json({ success: true });
});

// GET /api/reports/:type?account=<tenant>&start=<ISO>&end=<ISO>
app.get('/api/reports/:type', async (req, res) => {
  const { type } = req.params;
  const { account, start, end, limit: limitStr, startKey } = req.query;

  if (!account) {
    return res.status(400).json({ error: 'Missing account query param' });
  }

  const params = {};
  if (start) {
    const startDate = Date.parse(start);
    if (Number.isNaN(startDate)) {
      return res.status(400).json({ error: 'Invalid start date' });
    }
    params.startDate = Math.floor(startDate / 1000);
  }
  if (end) {
    const endDate = Date.parse(end);
    if (Number.isNaN(endDate)) {
      return res.status(400).json({ error: 'Invalid end date' });
    }
    params.endDate = Math.floor(endDate / 1000);
  }

  const limit = Math.min(Number(limitStr) || 1000, 1000);

  // Debug: log exactly what we are about to request
  console.log('fetchReport payload', {
    type,
    account,
    startDate: params.startDate,
    endDate: params.endDate,
    startKey,
    limit
  });

  try {
    const result = await fetchReport(type, account, { ...params, ...(startKey && { start_key: startKey }), maxRows: limit });

    const rows = Array.isArray(result) ? result : result.rows;
    const nextToken = Array.isArray(result) ? null : result.next;

    const processedData = rows.map(row => {
      // Ensure agent_history is an array
      let history = row.agent_history;
      if (typeof history === 'string') {
        try { history = JSON.parse(history); } catch { history = []; }
      }

      let ts = row.answered_time;
      if (!ts && Array.isArray(history)) {
        const answerEvt = history.find(e => e.event === 'answer' || e.connected);
        if (answerEvt?.last_attempt) {
          const ms = answerEvt.last_attempt > 10_000_000_000 ? answerEvt.last_attempt : answerEvt.last_attempt * 1000;
          ts = new Date(ms).toISOString();
        }
      }
      // Preserve all original row properties including Extension column
      return { ...row, answered_time: ts ?? '--', agent_history: history };
    });
    res.json({ data: processedData, next: nextToken });
  } catch (err) {
    const upstreamErr = err.response?.data?.error;
    // Prefer specific message from upstream if present
    const msg = (typeof upstreamErr === 'string') ? upstreamErr : upstreamErr?.message || err.message;
    console.error(err.response?.data || err.stack || err.message);
    res.status(500).json({ error: msg });
  }
});

// Simple in-memory cache: recordingId ⇒ duration (seconds)
const durationCache = new Map();

// Lightweight endpoint to expose recording duration without downloading full file
app.get('/api/recordings/:id/meta', async (req, res) => {
  const { id } = req.params;
  const { account } = req.query;

  if (!account) {
    return res.status(400).json({ error: 'Missing account query param' });
  }

  // Return cached value if present
  if (durationCache.has(id)) {
    return res.json({ duration: durationCache.get(id) });
  }

  try {
    const token = await getPortalToken(account);
    const url = `${process.env.BASE_URL}/api/v2/reports/recordings/${id}`;

    // Fetch first 128 KB – enough for metadata / VBR TOC
    const upstreamRes = await axios.get(url, {
      responseType: 'arraybuffer',
      httpsAgent,
      headers: {
        Authorization: `Bearer ${token}`,
        'X-User-Agent': 'portal',
        'X-Account-ID': process.env.ACCOUNT_ID_HEADER ?? account,
        Range: 'bytes=0-131071',
        'Accept-Encoding': 'identity'
      },
      decompress: false
    });

    const { format } = await parseBuffer(Buffer.from(upstreamRes.data), 'audio/mpeg');
    if (!format.duration) throw new Error('Unable to determine duration');

    durationCache.set(id, format.duration);
    res.json({ duration: format.duration });
  } catch (err) {
    const status = err.response?.status || 500;
    if (status !== 404) {
      console.error(err.response?.data || err.stack || err.message);
    }
    res.status(status).json({ error: err.message });
  }
});

// Proxy: GET /api/recordings/:id?account=<tenant>
// Streams the MP3 recording from the upstream UC backend while adding the required auth token.
app.get('/api/recordings/:id', async (req, res) => {
  const { id } = req.params;
  const { account } = req.query;

  if (!account) {
    return res.status(400).json({ error: 'Missing account query param' });
  }

  try {
    // Obtain (cached) JWT for this tenant
    const token = await getPortalToken(account);

    const upstreamUrl = `${process.env.BASE_URL}/api/v2/reports/recordings/${id}`;
    // Ensure we get Content-Range/Length: if browser didn't request a range, request the full file starting from byte 0
    let rangeHdr = req.headers.range;
    if (!rangeHdr) {
      rangeHdr = 'bytes=0-';
    }

    const upstreamRes = await axios.get(upstreamUrl, {
      responseType: 'stream',
      httpsAgent,
      headers: {
        Authorization: `Bearer ${token}`,
        'X-User-Agent': 'portal',
        'X-Account-ID': process.env.ACCOUNT_ID_HEADER ?? account,
        Range: rangeHdr,
        'Accept-Encoding': 'identity'
      },
      // Ensure axios does not decompress so byte positions stay intact
      decompress: false
    });

    // Mirror upstream status (200 or 206 for range requests) and critical headers
    res.status(upstreamRes.status);

    // Pass through essential headers required for proper playback & seeking
    const forwardHeaders = [
      'content-type',
      'content-disposition',
      'content-length',
      'content-range',
      'accept-ranges'
    ];

    forwardHeaders.forEach(h => {
      if (upstreamRes.headers[h]) {
        res.setHeader(h, upstreamRes.headers[h]);
      }
    });

    // If we have cached duration, advertise it so browsers can show timeline immediately
    if (durationCache.has(id)) {
      const dur = durationCache.get(id);
      // Non-standard but understood by Chrome/Firefox
      res.setHeader('X-Content-Duration', dur.toFixed(3));
      // RFC 3803 (used by QuickTime / Safari)
      res.setHeader('Content-Duration', dur.toFixed(3));
    }

    // Stream data
    upstreamRes.data.pipe(res);
  } catch (err) {
    const status = err.response?.status || 500;
    if (status !== 404) {
      console.error(err.response?.data || err.stack || err.message);
    }
    res.status(status).json({ error: err.message });
  }
});

// SSL Certificate Management
const loadSSLCertificates = () => {
  try {
    const sslOptions = {
      key: fs.readFileSync('ssl/privkey.pem'),
      cert: fs.readFileSync('ssl/fullchain.pem')
    };
    
    console.log("🔒 SSL certificates loaded successfully");
    return sslOptions;
  } catch (error) {
    console.error("❌ Error loading SSL certificates:", error.message);
    
    // Check if SSL files exist
    const sslFiles = ['ssl/privkey.pem', 'ssl/fullchain.pem'];
    sslFiles.forEach(file => {
      if (!fs.existsSync(file)) {
        console.error(`❌ SSL file not found: ${file}`);
      }
    });
    
    console.log("⚠️  Falling back to HTTP server");
    return null;
  }
};

const sslOptions = loadSSLCertificates();

// Only use HTTPS if PUBLIC_URL starts with https://
const useHTTPS = PUBLIC_URL.startsWith('https://');

if (sslOptions && useHTTPS) {
  const server = https.createServer(sslOptions, app);
  server.listen(PORT, HOST, () => {
    console.log(`🔐 HTTPS server running at ${PUBLIC_URL}`);
    console.log(`🌐 Server accessible on all network interfaces (${HOST}:${PORT})`);
  });
  
  server.on('error', (err) => {
    console.error('❌ HTTPS Server error:', err);
    if (err.code === 'EADDRINUSE') {
      console.error(`❌ Port ${PORT} is already in use. Try a different port.`);
    } else if (err.code === 'EACCES') {
      console.error(`❌ Permission denied. Port ${PORT} might require sudo privileges.`);
    }
    process.exit(1);
  });
} else {
  const server = app.listen(PORT, HOST, () => {
    console.log(`🌐 HTTP server running at ${PUBLIC_URL}`);
    if (!useHTTPS) {
      console.log(`⚠️  Running in HTTP mode (PUBLIC_URL is set to HTTP)`);
    } else {
      console.log(`⚠️  Running in HTTP mode (no SSL certificates found)`);
    }
  });
  
  server.on('error', (err) => {
    console.error('❌ HTTP Server error:', err);
    if (err.code === 'EADDRINUSE') {
      console.error(`❌ Port ${PORT} is already in use. Try a different port.`);
    } else if (err.code === 'EACCES') {
      console.error(`❌ Permission denied. Port ${PORT} might require sudo privileges.`);
    }
    process.exit(1);
  });
}
