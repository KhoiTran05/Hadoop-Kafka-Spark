const { Kafka } = require('kafkajs');
const Redis = require('ioredis');
const { Pool } = require('pg');
const express = require('express');
const { Server } = require('socket.io');
const http = require('http');
const cors = require('cors');

// Initialize
const redis = new Redis();
const postgres = new Pool({
  host: "localhost",        
  port: 5432,
  database: "football",
  user: "airflow",
  password: "airflow",
  max: 10,                  
  idleTimeoutMillis: 30000, 
  connectionTimeoutMillis: 2000
});

const app = express();
app.use(cors());
app.use(express.static('.'));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// Kafka Consumer
const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'dashboard-service' });

// Deduplication cache
const processedMatches = new Map();

// Initialize Database
async function initializeDatabase() {
  try {
    await postgres.query(`
      CREATE TABLE IF NOT EXISTS matches (
        id BIGINT PRIMARY KEY,
        event_timestamp TIMESTAMP NOT NULL,
        status VARCHAR(20) NOT NULL,
        home_team_id BIGINT NOT NULL,
        home_team_name VARCHAR(255) NOT NULL,
        home_team_crest TEXT,
        away_team_id BIGINT NOT NULL,
        away_team_name VARCHAR(255) NOT NULL,
        away_team_crest TEXT,
        duration VARCHAR(20),
        home_score INTEGER,
        away_score INTEGER,
        half_time_home INTEGER,
        half_time_away INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await postgres.query(`
      CREATE INDEX IF NOT EXISTS idx_matches_status 
      ON matches(status)
    `);

    await postgres.query(`
      CREATE INDEX IF NOT EXISTS idx_matches_event_timestamp 
      ON matches(event_timestamp DESC)
    `);

    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Error initializing database:', error);
    process.exit(1);
  }
}

async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'processed-matches' });
  
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const match = JSON.parse(message.value.toString());
        
        // Deduplicate
        const cacheKey = `${match.id}-${match.event_timestamp}`;
        if (processedMatches.has(cacheKey)) {
          console.log('Duplicate match, skipping:', match.id);
          return;
        }
        processedMatches.set(cacheKey, true);
        
        // Route by status
        await handleMatch(match);
        
        // Broadcast to all connected dashboards
        io.emit('match:update', match);
      } catch (error) {
        console.error('Error processing message:', error);
      }
    }
  });
}

async function handleMatch(match) {
  try {
    switch(match.status) {
      case 'TIMED':
        await redis.set(`upcoming:${match.id}`, JSON.stringify(match));
        await redis.zadd('matches:upcoming', Date.parse(match.event_timestamp), match.id);
        console.log(`Added upcoming match: ${match.home_team_name} vs ${match.away_team_name}`);
        break;
        
      case 'LIVE':
        await redis.del(`upcoming:${match.id}`);
        await redis.set(`live:${match.id}`, JSON.stringify(match));
        await redis.zadd('matches:live', Date.now(), match.id);
        console.log(`Live match: ${match.home_team_name} vs ${match.away_team_name}`);
        break;
        
      case 'FINISHED':
        await redis.del(`live:${match.id}`);
        await redis.setex(`recent:${match.id}`, 7200, JSON.stringify(match));
        
        // Save to PostgreSQL
        await postgres.query(`
          INSERT INTO matches (
            id, event_timestamp, status, 
            home_team_id, home_team_name, home_team_crest,
            away_team_id, away_team_name, away_team_crest,
            duration, home_score, away_score,
            half_time_home, half_time_away
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
          ON CONFLICT (id) DO UPDATE 
          SET status = $3, 
              home_score = $11, 
              away_score = $12,
              updated_at = CURRENT_TIMESTAMP
        `, [
          match.id, 
          match.event_timestamp, 
          match.status,
          match.home_team_id, 
          match.home_team_name, 
          match.home_team_crest,
          match.away_team_id, 
          match.away_team_name, 
          match.away_team_crest,
          match.duration,
          match.fullTime?.home || null, 
          match.fullTime?.away || null,
          match.halfTime?.home || null,
          match.halfTime?.away || null
        ]);
        
        console.log(`Finished match saved: ${match.home_team_name} vs ${match.away_team_name}`);
        break;
    }
  } catch (error) {
    console.error('Error handling match:', error);
  }
}

// REST API endpoints
app.get('/api/matches/live', async (req, res) => {
  try {
    const ids = await redis.zrange('matches:live', 0, -1);
    const matches = await Promise.all(
      ids.map(id => redis.get(`live:${id}`))
    );
    res.json(matches.filter(Boolean).map(JSON.parse));
  } catch (error) {
    console.error('Error fetching live matches:', error);
    res.status(500).json({ error: 'Failed to fetch live matches' });
  }
});

app.get('/api/matches/upcoming', async (req, res) => {
  try {
    const ids = await redis.zrange('matches:upcoming', 0, -1);
    const matches = await Promise.all(
      ids.map(id => redis.get(`upcoming:${id}`))
    );
    res.json(matches.filter(Boolean).map(JSON.parse));
  } catch (error) {
    console.error('Error fetching upcoming matches:', error);
    res.status(500).json({ error: 'Failed to fetch upcoming matches' });
  }
});

app.get('/api/matches/recent', async (req, res) => {
  try {
    const result = await postgres.query(`
      SELECT * FROM matches 
      WHERE status = 'FINISHED' 
      AND event_timestamp >= NOW() - INTERVAL '14 days'
      ORDER BY event_timestamp DESC
      LIMIT 100
    `);
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching recent matches:', error);
    res.status(500).json({ error: 'Failed to fetch recent matches' });
  }
});

// WebSocket connection handler
io.on('connection', (socket) => {
  console.log('Dashboard connected:', socket.id);
  
  socket.on('disconnect', () => {
    console.log('Dashboard disconnected:', socket.id);
  });
});

// **ADD THIS - Start everything in order**
async function start() {
  try {
    console.log('Starting Football Dashboard Service...');
    
    // 1. Initialize database first
    await initializeDatabase();
    
    // 2. Start Kafka consumer
    await startConsumer();
    console.log('Kafka consumer started');
    
    // 3. Start HTTP server
    server.listen(3001, () => {
      console.log('Backend service running on http://localhost:3001');
      console.log('Dashboard: http://localhost:3001/dashboard.html');
    });
  } catch (error) {
    console.error('Failed to start service:', error);
    process.exit(1);
  }
}

// Start the application
start();