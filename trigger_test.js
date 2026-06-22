const { initializeApp, cert } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');
const serviceAccount = require('./serviceAccountKey.json');

const app = initializeApp({
  credential: cert(serviceAccount),
  databaseURL: 'https://binwatch-iot-default-rtdb.firebaseio.com/'
});

const db = getDatabase(app);

async function trigger() {
  const dustbinId = 'dustbin-002'; // change if needed
  const ref = db.ref(`dustbin/${dustbinId}`);
  const now = new Date();
  const payload = {
    percentage: 85,
    lastUpdateDate: now.toISOString().split('T')[0],
    lastUpdateTime: now.toTimeString().slice(0,5),
    notificationSent: false
  };
  console.log('Updating', dustbinId, payload);
  await ref.update(payload);
  console.log('Update complete');
  process.exit(0);
}

trigger().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});