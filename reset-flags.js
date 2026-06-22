// Reset notification flags to trigger test notifications
const { initializeApp, cert } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');
const serviceAccount = require('./serviceAccountKey.json');

const app = initializeApp({
  credential: cert(serviceAccount),
  databaseURL: 'https://binwatch-iot-default-rtdb.firebaseio.com'
});

const db = getDatabase(app);

async function resetFlags() {
  try {
    console.log('Resetting notification flags...');
    
    await Promise.all([
      db.ref('dustbin/dustbin-001').update({ notificationSent: false }),
      db.ref('dustbin/dustbin-002').update({ notificationSent: false })
    ]);
    
    console.log('✅ Flags reset! Server will send notifications on next check.');
    process.exit(0);
  } catch (err) {
    console.error('❌ Error:', err);
    process.exit(1);
  }
}

resetFlags();
