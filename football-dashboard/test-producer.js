const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'test-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

// Sample test matches
const testMatches = [
  {
    id: 1001,
    event_timestamp: new Date(Date.now() + 2 * 60 * 60 * 1000).toISOString(), // 2 hours from now
    status: "TIMED",
    home_team_id: 57,
    home_team_name: "Arsenal FC",
    home_team_crest: "https://crests.football-data.org/57.png",
    away_team_id: 64,
    away_team_name: "Liverpool FC",
    away_team_crest: "https://crests.football-data.org/64.png",
    duration: "REGULAR",
    fullTime: {},
    halfTime: {}
  },
  {
    id: 1002,
    event_timestamp: new Date(Date.now() + 4 * 60 * 60 * 1000).toISOString(), // 4 hours from now
    status: "TIMED",
    home_team_id: 65,
    home_team_name: "Manchester City FC",
    home_team_crest: "https://crests.football-data.org/65.png",
    away_team_id: 66,
    away_team_name: "Manchester United FC",
    away_team_crest: "https://crests.football-data.org/66.png",
    duration: "REGULAR",
    fullTime: {},
    halfTime: {}
  },
  {
    id: 1003,
    event_timestamp: new Date().toISOString(), // Now (LIVE)
    status: "LIVE",
    home_team_id: 61,
    home_team_name: "Chelsea FC",
    home_team_crest: "https://crests.football-data.org/61.png",
    away_team_id: 73,
    away_team_name: "Tottenham Hotspur FC",
    away_team_crest: "https://crests.football-data.org/73.png",
    duration: "REGULAR",
    fullTime: { home: 2, away: 1 },
    halfTime: { home: 1, away: 0 }
  },
  {
    id: 1004,
    event_timestamp: new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(), // 3 hours ago (FINISHED)
    status: "FINISHED",
    home_team_id: 354,
    home_team_name: "Crystal Palace FC",
    home_team_crest: "https://crests.football-data.org/354.png",
    away_team_id: 341,
    away_team_name: "Leeds United FC",
    away_team_crest: "https://crests.football-data.org/341.png",
    duration: "REGULAR",
    fullTime: { home: 3, away: 2 },
    halfTime: { home: 2, away: 1 }
  },
  {
    id: 1005,
    event_timestamp: new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString(), // Yesterday (FINISHED)
    status: "FINISHED",
    home_team_id: 338,
    home_team_name: "Leicester City FC",
    home_team_crest: "https://crests.football-data.org/338.png",
    away_team_id: 563,
    away_team_name: "West Ham United FC",
    away_team_crest: "https://crests.football-data.org/563.png",
    duration: "REGULAR",
    fullTime: { home: 1, away: 1 },
    halfTime: { home: 0, away: 1 }
  }
];

async function produceTestData() {
  await producer.connect();
  console.log('✅ Connected to Kafka');

  console.log('\n📤 Producing test matches to Kafka topic: processed-matches\n');

  for (const match of testMatches) {
    await producer.send({
      topic: 'processed-matches',
      messages: [
        {
          key: String(match.id),
          value: JSON.stringify(match)
        }
      ]
    });

    const statusEmoji = {
      'TIMED': '📅',
      'LIVE': '🔴',
      'FINISHED': '✅'
    };

    console.log(`${statusEmoji[match.status]} Sent: ${match.home_team_name} vs ${match.away_team_name} [${match.status}]`);
    
    // Wait a bit between messages
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  console.log('\n✅ All test matches sent!');
  console.log('👉 Check your dashboard at: http://localhost:3001/dashboard.html\n');

  await producer.disconnect();
}

produceTestData().catch(console.error);