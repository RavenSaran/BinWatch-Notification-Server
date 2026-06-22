/**
 * ============================================================================
 *  BinWatch Notification Server
 * ----------------------------------------------------------------------------
 *  1) Listens to Firebase Realtime Database for dustbins crossing the "full"
 *     threshold, resolves them against the dustbin_register, and pushes a
 *     data-only FCM notification to all cleaner/supervisor/admin devices.
 *  2) Listens to tasks/ for status changes: notifies the assigned cleaner
 *     when a task is pending, and the assigned supervisor when a task is
 *     marked completed.
 *
 *  Single-file by request. Organized into clearly delimited sections so it
 *  reads top-to-bottom like a pipeline: CONFIG -> SERVICES -> BIN
 *  NOTIFICATION -> BIN MONITOR -> TASK NOTIFICATION -> TASK MONITOR -> START.
 * ============================================================================
 */

const { initializeApp, cert } = require("firebase-admin/app");
const { getDatabase } = require("firebase-admin/database");
const { getMessaging } = require("firebase-admin/messaging");
const path = require("path");

/* ============================================================================
 * 1. CONFIG
 * ----------------------------------------------------------------------------
 * Service account path and database URL are read from environment variables
 * with a fallback to the original hardcoded values, so this still runs
 * out-of-the-box but is safe to deploy across environments.
 *
 * Set these before running in any environment other than your original one:
 *   GOOGLE_APPLICATION_CREDENTIALS_PATH=./serviceAccountKey.json
 *   FIREBASE_DATABASE_URL=https://your-project-default-rtdb.firebaseio.com/
 * ==========================================================================*/

const CONFIG = {
  serviceAccountPath:
    process.env.GOOGLE_APPLICATION_CREDENTIALS_PATH ||
    path.join(__dirname, "./serviceAccountKey.json"),
  databaseURL:
    process.env.FIREBASE_DATABASE_URL ||
    "https://binwatch-iot-default-rtdb.firebaseio.com/",
  fullThresholdPercent: 80,
  notificationTtlMs: 86400000, // 24 hours
  maxBinsInSummary: 3,
  userRoles: ["cleaners", "supervisors", "admins"],
};

if (!process.env.FIREBASE_DATABASE_URL) {
  console.warn(
    "[config] FIREBASE_DATABASE_URL not set in environment — using hardcoded " +
      "default. Set this explicitly before deploying to a different project."
  );
}

let serviceAccount;

if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  console.log("[firebase] Loading credentials from FIREBASE_SERVICE_ACCOUNT");

  serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

} else {
  console.log("[firebase] Loading credentials from local serviceAccountKey.json");

  serviceAccount = require(CONFIG.serviceAccountPath);
}

const app = initializeApp({
  credential: cert(serviceAccount),
  databaseURL: CONFIG.databaseURL,
});

const db = getDatabase(app);
const messaging = getMessaging(app);

console.log("[firebase] Connected:", CONFIG.databaseURL);

/* ============================================================================
 * 2. SERVICES — register lookup
 * ----------------------------------------------------------------------------
 * Builds a lookup map from dustbin_register so a dustbin/{dustbinId} entry
 * (e.g. "dustbin-001") can be resolved to its human-facing register info
 * (registerId like "B-001", hostel, block, floor, etc).
 *
 * NOTE: dustbin_register keys are NOT assumed to follow any fixed format.
 * The join is done strictly via the `deviceId` field inside each register
 * entry, which is expected to match a dustbin/ node key. (Unchanged from
 * your original join logic.)
 * ==========================================================================*/

async function getDustbinRegisterMap() {
  const snapshot = await db.ref("dustbin_register").once("value");
  const register = snapshot.val() || {};
  const map = {};

  for (const registerKey in register) {
    const entry = register[registerKey];
    if (entry && entry.deviceId) {
      map[entry.deviceId] = {
        registerId: registerKey, // e.g. "B-001"
        hostel: entry.hostel || null,
        block: entry.block || null,
        floor: entry.floor || null,
        number: entry.number || null,
        status: entry.status || null,
      };
    }
  }

  return map;
}

/* ============================================================================
 * 3. SERVICES — FCM token management
 * ----------------------------------------------------------------------------
 * Fetches FCM tokens from all user roles, and removes stale tokens once FCM
 * reports them as unregistered. Unchanged in behavior from your original.
 * ==========================================================================*/

async function getAllFCMTokens() {
  const tokens = [];

  for (const role of CONFIG.userRoles) {
    try {
      const snapshot = await db.ref(role).once("value");
      const users = snapshot.val();

      if (!users) continue;

      for (const uid in users) {
        const user = users[uid];
        if (!user.fcmTokens) continue;

        for (const key in user.fcmTokens) {
          if (user.fcmTokens[key].token) {
            tokens.push({ role, uid, key, token: user.fcmTokens[key].token });
          }
        }
      }
    } catch (err) {
      console.error(`[tokens] Failed to fetch tokens from ${role}:`, err);
    }
  }

  const uniqueTokens = dedupeTokenEntries(tokens);
  console.log(`[tokens] Fetched ${tokens.length} raw tokens from all roles, ${uniqueTokens.length} unique tokens`);
  return uniqueTokens;
}

function dedupeTokenEntries(entries) {
  const seen = new Map();
  const unique = [];

  for (const entry of entries) {
    if (!entry.token) continue;
    if (seen.has(entry.token)) {
      console.log(
        `[tokens] Duplicate FCM token removed for role=${entry.role}, uid=${entry.uid}, key=${entry.key}`
      );
      continue;
    }

    seen.set(entry.token, true);
    unique.push(entry);
  }

  return unique;
}

async function removeToken(role, uid, key) {
  try {
    await db.ref(`${role}/${uid}/fcmTokens/${key}`).remove();
    console.log(`[tokens] Removed stale token: ${role}/${uid}/fcmTokens/${key}`);
  } catch (err) {
    console.error(`[tokens] Failed to remove stale token from ${role}/${uid}:`, err);
  }
}

/**
 * Fetch FCM tokens for a single user under a given role node
 * (e.g. role="cleaners", uid=cleanerUid). Used for task notifications,
 * which target one specific person rather than broadcasting to a role.
 *
 * Same data shape as getAllFCMTokens() entries, so it works directly with
 * sendTaskNotification()'s stale-token cleanup logic.
 */
async function getUserTokens(role, uid) {
  if (!uid) {
    console.warn(`[tokens] getUserTokens called with empty uid for role=${role}`);
    return [];
  }

  try {
    const snapshot = await db.ref(`${role}/${uid}/fcmTokens`).once("value");
    const fcmTokens = snapshot.val();

    if (!fcmTokens) return [];

    const tokens = [];
    for (const key in fcmTokens) {
      if (fcmTokens[key].token) {
        tokens.push({ role, uid, key, token: fcmTokens[key].token });
      }
    }

    return tokens;
  } catch (err) {
    console.error(`[tokens] Failed to fetch tokens for ${role}/${uid}:`, err);
    return [];
  }
}

/* ============================================================================
 * 4. NOTIFICATION FORMATTING
 * ----------------------------------------------------------------------------
 * Formal, alert-system register: declarative field labels, no bullets, no
 * casual phrasing ("and N more" is replaced with an explicit "Additional
 * bins affected" count line).
 *
 * OUTPUT SHAPE: returns { title, bodyLines, body }.
 *   - bodyLines: array of strings, ONE per line. This is the field Flutter
 *     should use to render the notification — join it however the UI
 *     widget needs (Text widgets in a Column, '\n'.join() if the Flutter
 *     side does honor newlines after all, etc). Sending an array sidesteps
 *     any ambiguity about whether a literal "\n" survives JSON encoding /
 *     decoding / whatever notification-rendering API is used on the
 *     Android/iOS side.
 *   - body: same content pre-joined with "\n", kept ONLY for backward
 *     compatibility with any existing Flutter code that already reads
 *     data['body'] directly. Prefer bodyLines for new code.
 *
 * ASSUMPTION (flagging): the "Action Required: Immediate collection" line
 * is my own addition to make this read as a directive rather than a status
 * report. I have no spec requiring this exact line — tell me if you want
 * it removed or reworded.
 * ==========================================================================*/

function formatLocation(bin) {
  const parts = [
    bin.hostel ? `Hostel ${bin.hostel}` : null,
    bin.block ? `Block ${bin.block}` : null,
    bin.floor ? `Floor ${bin.floor}` : null,
  ].filter(Boolean);

  return parts.length > 0 ? parts.join(", ") : "Location unavailable";
}

function formatNotification(fullBins) {
  const title = "BinWatch Alert";
  const ACTION_LINE = "Action Required: Immediate collection";

  let bodyLines;

  if (fullBins.length === 1) {
    const bin = fullBins[0];

    const updatedLine =
      bin.lastUpdateDate || bin.lastUpdateTime
        ? `Last Updated: ${bin.lastUpdateDate || ""} ${bin.lastUpdateTime || ""}`.trim()
        : "Last Updated: Unavailable";

    bodyLines = [
      `ALERT: Bin ${bin.registerId} has reached capacity (${bin.percentage.toFixed(1)}%)`,
      `Location: ${formatLocation(bin)}`,
      updatedLine,
      ACTION_LINE,
    ];
  } else {
    const shown = fullBins.slice(0, CONFIG.maxBinsInSummary);
    const remaining = fullBins.length - shown.length;

    const lines = shown.map(
      (b) => `${b.registerId} — ${b.percentage.toFixed(1)}% — ${formatLocation(b)}`
    );

    if (remaining > 0) {
      lines.push(`Additional bins affected: ${remaining}`);
    }

    bodyLines = [`ALERT: ${fullBins.length} bins have reached capacity`, ...lines, ACTION_LINE];
  }

  return { title, bodyLines, body: bodyLines.join("\n") };
}

/* ============================================================================
 * 5. NOTIFICATION SENDING
 * ----------------------------------------------------------------------------
 * Sends a data-only notification matching the Flutter
 * NotificationService.showFullBinsNotification() payload shape.
 *
 * NOTE: `sendEachForMulticast` is preserved from your original code as-is.
 * I have not re-verified it against current firebase-admin docs for
 * whatever version you have installed — check your package.json /
 * the firebase-admin changelog before relying on this method name.
 * ==========================================================================*/

async function sendFullBinsNotification(tokenEntries, fullBins) {
  if (tokenEntries.length === 0) {
    console.log("[notify] No FCM tokens found — skipping send");
    return;
  }

  const tokens = tokenEntries.map((t) => t.token);
  const { title, bodyLines, body } = formatNotification(fullBins);

  const message = {
    data: {
      type: "full_bins",
      bins: JSON.stringify(fullBins),
      title,
      // FCM data payloads only support string values, so the lines array
      // must be JSON-stringified. On the Flutter side: jsonDecode this
      // back into a List<String> and render each entry as its own line
      // (e.g. one Text widget per line, or '\n'.join(list) if your
      // widget DOES honor newlines — your call on the Flutter side).
      bodyLines: JSON.stringify(bodyLines),
      // Kept for backward compatibility only. Prefer bodyLines above.
      body,
    },
    tokens,
    android: {
      priority: "high",
      ttl: CONFIG.notificationTtlMs,
    },
    apns: {
      headers: { "apns-priority": "10" },
      payload: {
        aps: {
          // data-only background notification — no alert block
          "content-available": 1,
          sound: "default",
          badge: 1,
          "mutable-content": 1,
        },
      },
    },
  };

  try {
    const response = await messaging.sendEachForMulticast(message);

    console.log(
      `[notify] Sent: ${response.successCount} succeeded, ${response.failureCount} failed`
    );

    response.responses.forEach((r, i) => {
      if (r.success) {
        console.log(`[notify] Token ${i} sent successfully`);
        return;
      }

      console.error(`[notify] Token ${i} failed: ${r.error?.code} — ${r.error?.message}`);

      if (r.error?.code === "messaging/registration-token-not-registered") {
        const { role, uid, key } = tokenEntries[i];
        console.log(`[notify] Removing stale token for role=${role}, uid=${uid}`);
        removeToken(role, uid, key);
      }
    });
  } catch (err) {
    console.error("[notify] Error sending notification:", err.message);
    console.error("[notify] Full error:", err);
  }
}

/* ============================================================================
 * 6. BIN MONITOR
 * ----------------------------------------------------------------------------
 * Listens to dustbin/, evaluates each bin against the threshold, and fires
 * one batched notification per update cycle covering everything that just
 * crossed the threshold. Threshold and reset logic unchanged from original.
 * ==========================================================================*/

async function evaluateBin(dustbinId, bin, registerMap) {
  const percentage = Number(bin.percentage) || 0;
  const notificationSent = bin.notificationSent || false;

  console.log(
    `[monitor] Checking ${dustbinId}: ${percentage}% (notificationSent: ${notificationSent})`
  );

  if (percentage >= CONFIG.fullThresholdPercent && !notificationSent) {
    console.log(`[monitor] Marking ${dustbinId} for alert`);

    const registerInfo = registerMap[dustbinId];

    if (!registerInfo) {
      console.warn(
        `[monitor] No dustbin_register entry found with deviceId="${dustbinId}". ` +
          `Notification will use raw bin fields as fallback.`
      );
    }

    const now = new Date();
    const lastNotificationDate = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
    const lastNotificationTime = `${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}:${String(now.getSeconds()).padStart(2, "0")}`;

    await db
      .ref(`dustbin/${dustbinId}`)
      .update({
        notificationSent: true,
        lastNotificationTimestamp: Date.now(),
        lastNotificationDate,
        lastNotificationTime,
      })
      .catch((err) => console.error(`[monitor] Failed to update ${dustbinId}:`, err));

    return {
      id: dustbinId,
      registerId: registerInfo ? registerInfo.registerId : dustbinId,
      hostel: registerInfo ? registerInfo.hostel : bin.hostel || null,
      block: registerInfo ? registerInfo.block : bin.block || null,
      floor: registerInfo ? registerInfo.floor : bin.floor || null,
      location: bin.location || bin.hostel || "Unknown location",
      percentage,
      lastUpdateDate: bin.lastUpdateDate || null,
      lastUpdateTime: bin.lastUpdateTime || null,
      lastNotificationDate,
      lastNotificationTime,
      lastNotificationTimestamp: Date.now(),
    };
  }

  if (percentage < CONFIG.fullThresholdPercent && notificationSent) {
    await db
      .ref(`dustbin/${dustbinId}`)
      .update({ notificationSent: false })
      .catch((err) => console.error(`[monitor] Failed to reset flag for ${dustbinId}:`, err));

    console.log(`[monitor] ${dustbinId} reset notification flag`);
  }

  return null;
}

function monitorDustbins() {
  const ref = db.ref("dustbin");

  ref.on(
    "value",
    async (snapshot) => {
      const data = snapshot.val();
      if (!data) return;

      let registerMap = {};
      try {
        registerMap = await getDustbinRegisterMap();
      } catch (err) {
        console.error("[monitor] Failed to load dustbin_register:", err);
      }

      const fullBins = [];

      for (const dustbinId in data) {
        const result = await evaluateBin(dustbinId, data[dustbinId], registerMap);
        if (result) fullBins.push(result);
      }

      if (fullBins.length > 0) {
        console.log(`[monitor] Found ${fullBins.length} bin(s) needing notification`);
        const tokens = await getAllFCMTokens();
        console.log(`[monitor] Fetched ${tokens.length} FCM tokens, sending notification...`);
        await sendFullBinsNotification(tokens, fullBins);
      }
    },
    (error) => {
      console.error("[monitor] Listener error:", error);
    }
  );
}

function formatInactivityNotification(bin) {
  const title = "Dustbin Inactivity Alert";

  const lastSeenLine =
    bin.lastUpdateDate || bin.lastUpdateTime
      ? `Last Seen: ${bin.lastUpdateDate || ""} ${bin.lastUpdateTime || ""}`.trim()
      : "Last Seen: Unavailable";

  const registerId = bin.registerId || bin.id || bin.binId || "Unknown Bin";
  const parsedMinutes = Number(bin.minutesSinceUpdate);
  const minutesSinceUpdate = Number.isFinite(parsedMinutes) ? parsedMinutes : null;

  const bodyLines = [
    `ALERT: Bin ${registerId} has not reported for ${
      minutesSinceUpdate !== null ? minutesSinceUpdate : "unknown"
    } minutes`,
    `Location: ${formatLocation(bin)}`,
    `Fill Status: ${bin.fillStatus || "Unknown"} (${Number(bin.percentage || 0).toFixed(1)}%)`,
    lastSeenLine,
    "Action Required: Please inspect the dustbin and restore connectivity",
  ];

  return { title, bodyLines, body: bodyLines.join("\n") };
}

async function sendInactivityNotification(tokenEntries, bin) {
  if (tokenEntries.length === 0) {
    console.log("[inactivity-notify] No FCM tokens found — skipping send");
    return;
  }

  const tokens = tokenEntries.map((t) => t.token);
  const { title, bodyLines, body } = formatInactivityNotification(bin);

  const message = {
    data: {
      type: "dustbin_inactivity",
      bin: JSON.stringify(bin),
      title,
      bodyLines: JSON.stringify(bodyLines),
      body,
    },
    tokens,
    android: {
      priority: "high",
      ttl: CONFIG.notificationTtlMs,
    },
    apns: {
      headers: { "apns-priority": "10" },
      payload: {
        aps: {
          "content-available": 1,
          sound: "default",
          badge: 1,
          "mutable-content": 1,
        },
      },
    },
  };

  try {
    const response = await messaging.sendEachForMulticast(message);

    console.log(
      `[inactivity-notify] Sent: ${response.successCount} succeeded, ${response.failureCount} failed`
    );

    response.responses.forEach((r, i) => {
      if (r.success) {
        return;
      }

      console.error(
        `[inactivity-notify] Token ${i} failed: ${r.error?.code} — ${r.error?.message}`
      );

      if (r.error?.code === "messaging/registration-token-not-registered") {
        const { role, uid, key } = tokenEntries[i];
        console.log(`[inactivity-notify] Removing stale token for role=${role}, uid=${uid}`);
        removeToken(role, uid, key);
      }
    });
  } catch (err) {
    console.error("[inactivity-notify] Error sending notification:", err.message);
    console.error("[inactivity-notify] Full error:", err);
  }
}

const inactivityNotificationCache = new Map();

async function loadDustbinInactivityCache() {
  try {
    const snapshot = await db.ref("dustbin").once("value");
    const data = snapshot.val() || {};
    for (const binId in data) {
      const entry = data[binId];
      if (entry?.lastNotificationType !== "DUSTBIN_INACTIVITY") {
        continue;
      }
      const timestamp = Number(entry?.lastNotificationTimestamp || 0);
      if (!timestamp) {
        continue;
      }
      inactivityNotificationCache.set(binId, timestamp);
    }
  } catch (err) {
    console.error("[inactivity-monitor] Failed to initialize inactivity cache:", err);
  }
}

async function evaluateDustbinInactivity(dustbinId, bin, registerMap) {
  if (!bin) {
    return;
  }

  const notificationTimestamp = Number(bin.lastNotificationTimestamp || 0);
  if (!notificationTimestamp) {
    return;
  }

  const notificationMinutes =
    bin.lastNotificationMinutes != null
      ? Number(bin.lastNotificationMinutes)
      : null;
  if (!Number.isFinite(notificationMinutes)) {
    return;
  }

  const notificationType = bin.lastNotificationType || null;
  if (notificationType && notificationType !== "DUSTBIN_INACTIVITY") {
    return;
  }

  const previousTimestamp = inactivityNotificationCache.get(dustbinId) || 0;
  if (notificationTimestamp === previousTimestamp) {
    return;
  }

  inactivityNotificationCache.set(dustbinId, notificationTimestamp);

  const registerInfo = registerMap[dustbinId];

  const notificationBin = {
    ...bin,
    id: dustbinId,
    registerId: (registerInfo && registerInfo.registerId) || dustbinId,
    hostel: registerInfo ? registerInfo.hostel : bin.hostel || null,
    block: registerInfo ? registerInfo.block : bin.block || null,
    floor: registerInfo ? registerInfo.floor : bin.floor || null,
    location: bin.location || null,
    percentage: Number(bin.percentage || 0),
    fillStatus: bin.fillStatus || "Unknown",
    lastUpdateDate: bin.lastUpdateDate || null,
    lastUpdateTime: bin.lastUpdateTime || null,
    minutesSinceUpdate: Number.isFinite(notificationMinutes) ? notificationMinutes : null,
    lastNotificationDate: bin.lastNotificationDate || null,
    lastNotificationTime: bin.lastNotificationTime || null,
    lastNotificationType: notificationType,
    lastNotificationTimestamp: notificationTimestamp,
    lastNotificationMinutes: Number.isFinite(notificationMinutes) ? notificationMinutes : null,
  };

  const tokens = await getAllFCMTokens();
  console.log(
    `[inactivity-monitor] Sending inactivity notification for ${dustbinId} to ${tokens.length} device(s)`
  );
  await sendInactivityNotification(tokens, notificationBin);
}

function monitorDustbinInactivity() {
  const ref = db.ref("dustbin");

  loadDustbinInactivityCache().then(() => {
    ref.on(
      "child_added",
      async (snapshot) => {
        const bin = snapshot.val();
        const registerMap = await getDustbinRegisterMap();
        await evaluateDustbinInactivity(snapshot.key, bin, registerMap).catch((err) =>
          console.error(
            `[inactivity-monitor] Error evaluating added dustbin ${snapshot.key}:`,
            err
          )
        );
      },
      (error) => console.error("[inactivity-monitor] child_added listener error:", error)
    );

    ref.on(
      "child_changed",
      async (snapshot) => {
        const bin = snapshot.val();
        const registerMap = await getDustbinRegisterMap();
        await evaluateDustbinInactivity(snapshot.key, bin, registerMap).catch((err) =>
          console.error(
            `[inactivity-monitor] Error evaluating changed dustbin ${snapshot.key}:`,
            err
          )
        );
      },
      (error) => console.error("[inactivity-monitor] child_changed listener error:", error)
    );
  });
}

/* ============================================================================
 * 7. TASK NOTIFICATION FORMATTING
 * ----------------------------------------------------------------------------
 * Same formal alert-system register as bin notifications, applied to
 * task assignment (-> cleaner) and task completion (-> supervisor).
 *
 * Same bodyLines/body output shape as section 4 — see that comment block
 * for why bodyLines (array) is the preferred field for Flutter to render,
 * and body (joined string) is kept only for backward compatibility.
 *
 * ASSUMPTION (flagging): field labels and wording below are my own design,
 * not based on a spec you provided — only the task object's shape
 * (assignedAt, binLocation, cleanerName, completedAt, description,
 * deviceId, status) is something I actually saw in your data.
 * ==========================================================================*/

function formatTaskAssignedNotification(task) {
  const title = "BinWatch Task Alert";

  const bodyLines = [
    `ALERT: New cleaning task assigned`,
    `Location: ${task.binLocation || "Location unavailable"}`,
    `Device: ${task.deviceId || "Unknown"}`,
    task.description ? `Instructions: ${task.description}` : null,
    "Action Required: Please proceed to the location and complete the task",
  ].filter(Boolean);

  return { title, bodyLines, body: bodyLines.join("\n") };
}

function formatTaskCompletedNotification(task) {
  const title = "BinWatch Task Alert";

  const completedLine = task.completedAt
    ? `Completed At: ${new Date(task.completedAt).toLocaleString()}`
    : "Completed At: Unavailable";

  const bodyLines = [
    `ALERT: Cleaning task completed`,
    `Location: ${task.binLocation || "Location unavailable"}`,
    `Device: ${task.deviceId || "Unknown"}`,
    `Completed By: ${task.cleanerName || "Unknown cleaner"}`,
    completedLine,
    "Action Required: Please review and verify the task",
  ];

  return { title, bodyLines, body: bodyLines.join("\n") };
}

/* ============================================================================
 * 8. TASK NOTIFICATION SENDING
 * ----------------------------------------------------------------------------
 * Sends a data-only notification to a single user (cleaner on assignment,
 * supervisor on completion). Reuses the same stale-token cleanup pattern
 * as sendFullBinsNotification().
 * ==========================================================================*/

async function sendTaskNotification(tokenEntries, type, task, taskId) {
  if (tokenEntries.length === 0) {
    console.log(`[task-notify] No FCM tokens found for this recipient — skipping send`);
    return;
  }

  const { title, bodyLines, body } =
    type === "assigned"
      ? formatTaskAssignedNotification(task)
      : formatTaskCompletedNotification(task);

  const tokens = tokenEntries.map((t) => t.token);

  const message = {
    data: {
      type: `task_${type}`,
      taskId,
      task: JSON.stringify(task),
      title,
      // See section 4 comment: array of lines, JSON-stringified because
      // FCM data values must be strings. Decode with jsonDecode() in
      // Flutter and render one line per entry.
      bodyLines: JSON.stringify(bodyLines),
      // Kept for backward compatibility only. Prefer bodyLines above.
      body,
    },
    tokens,
    android: {
      priority: "high",
      ttl: CONFIG.notificationTtlMs,
    },
    apns: {
      headers: { "apns-priority": "10" },
      payload: {
        aps: {
          "content-available": 1,
          sound: "default",
          badge: 1,
          "mutable-content": 1,
        },
      },
    },
  };

  try {
    const response = await messaging.sendEachForMulticast(message);

    console.log(
      `[task-notify] Sent (${type}): ${response.successCount} succeeded, ${response.failureCount} failed`
    );

    response.responses.forEach((r, i) => {
      if (r.success) {
        console.log(`[task-notify] Token ${i} sent successfully`);
        return;
      }

      console.error(`[task-notify] Token ${i} failed: ${r.error?.code} — ${r.error?.message}`);

      if (r.error?.code === "messaging/registration-token-not-registered") {
        const { role, uid, key } = tokenEntries[i];
        console.log(`[task-notify] Removing stale token for role=${role}, uid=${uid}`);
        removeToken(role, uid, key);
      }
    });
  } catch (err) {
    console.error("[task-notify] Error sending notification:", err.message);
    console.error("[task-notify] Full error:", err);
  }
}

/* ============================================================================
 * 9. TASK MONITOR
 * ----------------------------------------------------------------------------
 * Listens to tasks/, and notifies on STATUS CHANGE only (per your
 * confirmation — not on every field update):
 *   - status becomes "pending" for the first time this process has seen
 *     this task (covers brand-new task creation) -> notify the cleaner
 *   - status transitions to "completed" -> notify the supervisor
 *
 * CAVEAT (flagging, not glossing over): previous status is tracked in an
 * in-memory Map, not persisted in Firebase. If this process restarts, it
 * loses that history — on the next event for a pre-existing task, it will
 * look like "first time seen" again. Per your answer, a first-time-seen
 * task with status "pending" triggers a cleaner notification, so a
 * restart could cause an already-pending task to re-notify the cleaner
 * once. It will NOT cause incorrect supervisor notifications, since
 * "completed" only fires on an explicit transition, never on first-seen.
 * If this restart behavior is a problem in practice, a Firebase-persisted
 * "lastNotifiedStatus" field (mirroring your dustbin notificationSent
 * pattern) would remove the gap — tell me if you want that instead, I
 * have not built it since you did not ask for it yet.
 * ==========================================================================*/

const taskStatusCache = new Map(); // taskId -> last known status

async function evaluateTask(taskId, task) {
  const previousStatus = taskStatusCache.get(taskId);
  const currentStatus = task.status || null;

  taskStatusCache.set(taskId, currentStatus);

  if (previousStatus === currentStatus) {
    // No change — covers both "already seen, unchanged" and repeated
    // no-op writes. Avoids duplicate notifications per your requirement.
    return;
  }

  console.log(
    `[task-monitor] Task ${taskId} status: ${previousStatus ?? "(new)"} -> ${currentStatus}`
  );

  if (currentStatus === "pending") {
    if (!task.cleanerUid) {
      console.warn(`[task-monitor] Task ${taskId} is pending but has no cleanerUid — skipping notify`);
      return;
    }

    const tokens = await getUserTokens("cleaners", task.cleanerUid);
    await sendTaskNotification(tokens, "assigned", task, taskId);
    return;
  }

  if (currentStatus === "completed") {
    if (!task.supervisorUid) {
      console.warn(`[task-monitor] Task ${taskId} is completed but has no supervisorUid — skipping notify`);
      return;
    }

    const tokens = await getUserTokens("supervisors", task.supervisorUid);
    await sendTaskNotification(tokens, "completed", task, taskId);
    return;
  }

  // Any other status value: no notification defined for it. Not treated
  // as an error, since you haven't specified other states.
}

function monitorTasks() {
  const ref = db.ref("tasks");

  ref.on(
    "child_added",
    async (snapshot) => {
      const task = snapshot.val();
      if (!task) return;
      await evaluateTask(snapshot.key, task).catch((err) =>
        console.error(`[task-monitor] Error evaluating new task ${snapshot.key}:`, err)
      );
    },
    (error) => console.error("[task-monitor] child_added listener error:", error)
  );

  ref.on(
    "child_changed",
    async (snapshot) => {
      const task = snapshot.val();
      if (!task) return;
      await evaluateTask(snapshot.key, task).catch((err) =>
        console.error(`[task-monitor] Error evaluating changed task ${snapshot.key}:`, err)
      );
    },
    (error) => console.error("[task-monitor] child_changed listener error:", error)
  );
}

/* ============================================================================
 * 10. START
 * ==========================================================================*/

console.log("BinWatch Notification Server starting...");
monitorDustbins();
monitorDustbinInactivity();
monitorTasks();
console.log("BinWatch Notification Server running.");
