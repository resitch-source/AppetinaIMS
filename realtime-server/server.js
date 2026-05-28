/**
 * ═══════════════════════════════════════════════════════════════
 * APPETINA IMS — Real-time WebSocket Server
 * Stack: Node.js + Socket.IO + JWT
 * Deploy: Railway / Render / Fly.io
 * ═══════════════════════════════════════════════════════════════
 */

'use strict';

const express   = require('express');
const http      = require('http');
const { Server } = require('socket.io');
const jwt       = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const helmet    = require('helmet');
const cors      = require('cors');

// ── Config ────────────────────────────────────────────────────
const CONFIG = {
  PORT:        process.env.PORT       || 3001,
  JWT_SECRET:  process.env.JWT_SECRET || 'appetina-ws-secret-change-in-prod',
  CORS_ORIGIN: process.env.CORS_ORIGIN || [
    'https://resitch-source.github.io',
    'http://localhost:5173',
    'http://localhost:3000',
  ],
  PING_INTERVAL: 25000,
  PING_TIMEOUT:  60000,
};

// ── App Setup ─────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

// Security middleware
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors({ origin: CONFIG.CORS_ORIGIN, credentials: true }));
app.use(express.json({ limit: '1mb' }));

// Rate limiting for HTTP endpoints
app.use('/api/', rateLimit({
  windowMs: 60 * 1000,
  max:      60,
  message:  { error: 'Too many requests' },
}));

// ── Socket.IO ─────────────────────────────────────────────────
const io = new Server(server, {
  cors: {
    origin:      CONFIG.CORS_ORIGIN,
    methods:     ['GET', 'POST'],
    credentials: true,
  },
  pingInterval: CONFIG.PING_INTERVAL,
  pingTimeout:  CONFIG.PING_TIMEOUT,
});

// ── In-memory State ───────────────────────────────────────────
const state = {
  connectedUsers:  new Map(),   // socketId → { userId, name, role, branch }
  rooms:           new Set(),   // active room names
  offlineQueues:   new Map(),   // userId → [pending events]
  rfidSessions:    new Map(),   // tagId → { location, lastSeen, socketId }
  scanSessions:    new Map(),   // socketId → { active, lastScan }
  metrics: {
    totalConnections: 0,
    totalMessages:    0,
    totalScans:       0,
    startTime:        Date.now(),
  },
};

// ── Authentication Middleware ──────────────────────────────────
io.use((socket, next) => {
  const token = socket.handshake.auth.token ||
                socket.handshake.headers['authorization']?.replace('Bearer ', '');

  if (!token) return next(new Error('Authentication required'));

  try {
    const payload = jwt.verify(token, CONFIG.JWT_SECRET);
    socket.user   = payload.user || payload;
    next();
  } catch (err) {
    // Allow unauthenticated connections in dev mode
    if (process.env.NODE_ENV === 'development') {
      socket.user = { id: 'dev-' + socket.id, name: 'DevUser', role: 'admin', branch: 'main' };
      return next();
    }
    next(new Error('Invalid token: ' + err.message));
  }
});

// ── Connection Handler ────────────────────────────────────────
io.on('connection', (socket) => {
  const user = socket.user;
  state.metrics.totalConnections++;

  console.log(`[CONNECT] ${user.name} (${user.role}) | Socket: ${socket.id}`);

  // Register user
  state.connectedUsers.set(socket.id, {
    socketId: socket.id,
    userId:   user.id,
    name:     user.name,
    role:     user.role,
    branch:   user.branch || 'main',
    joinedAt: new Date().toISOString(),
  });

  // Auto-join branch room
  const branchRoom = `branch-${(user.branch || 'main').toLowerCase().replace(/\s+/g, '-')}`;
  socket.join(branchRoom);
  socket.join('global');
  if (user.role === 'admin') socket.join('admins');

  // Deliver queued offline events
  const queue = state.offlineQueues.get(user.id);
  if (queue?.length) {
    queue.forEach(ev => socket.emit(ev.event, ev.data));
    state.offlineQueues.delete(user.id);
    console.log(`[OFFLINE_DELIVERY] ${queue.length} events delivered to ${user.name}`);
  }

  // Emit current connection info
  socket.emit('connected', {
    socketId:    socket.id,
    user,
    room:        branchRoom,
    serverTime:  new Date().toISOString(),
    onlineUsers: state.connectedUsers.size,
  });

  // Broadcast new user to branch
  socket.to(branchRoom).emit('user_joined', {
    userId: user.id, name: user.name, role: user.role,
  });

  // Update online count to all
  io.to('global').emit('online_count', { count: state.connectedUsers.size });

  // ── Event Handlers ──────────────────────────────────────────

  // INVENTORY EVENTS
  socket.on('inventory_update', (data, ack) => {
    state.metrics.totalMessages++;
    validateEvent(data, ['inventory_id', 'action']);

    const event = {
      ...data,
      operator:   user.name,
      operator_id: user.id,
      timestamp:  new Date().toISOString(),
    };

    // Broadcast to branch
    socket.to(branchRoom).emit('inventory_updated', event);

    // Broadcast to global for cross-branch visibility
    socket.to('global').emit('inventory_activity', {
      type:    data.action,
      item:    data.item_name,
      qty:     data.quantity,
      branch:  user.branch,
      operator: user.name,
    });

    console.log(`[INVENTORY] ${user.name}: ${data.action} on ${data.item_name}`);
    if (typeof ack === 'function') ack({ success: true, timestamp: event.timestamp });
  });

  // BARCODE SCAN EVENTS
  socket.on('barcode_scan', (data, ack) => {
    state.metrics.totalScans++;
    validateEvent(data, ['barcode']);

    const scan = {
      ...data,
      scanned_by: user.name,
      socket_id:  socket.id,
      timestamp:  new Date().toISOString(),
    };

    // Update scan session
    state.scanSessions.set(socket.id, { active: true, lastScan: scan });

    // Emit to same branch (multi-device teams)
    io.to(branchRoom).emit('scan_received', scan);

    // Emit to admins for audit
    io.to('admins').emit('scan_audit', {
      ...scan, branch: user.branch,
    });

    console.log(`[SCAN] ${user.name}: ${data.barcode} @ ${data.location || 'unknown'}`);
    if (typeof ack === 'function') ack({ success: true, scanId: Date.now() });
  });

  // RFID EVENTS
  socket.on('rfid_event', (data, ack) => {
    validateEvent(data, ['tag_id', 'event_type']);

    const rfidEvent = {
      ...data,
      detected_by: user.name,
      socket_id:   socket.id,
      timestamp:   new Date().toISOString(),
    };

    // Update RFID state
    state.rfidSessions.set(data.tag_id, {
      location:  data.location_to || 'Unknown',
      lastSeen:  rfidEvent.timestamp,
      socketId:  socket.id,
      status:    data.event_type === 'missing' ? 'missing' : 'active',
    });

    // Broadcast live update
    io.to('global').emit('rfid_update', rfidEvent);

    // Missing tag alert to admins
    if (data.event_type === 'missing') {
      io.to('admins').emit('rfid_missing_alert', {
        tag_id:   data.tag_id,
        tag_name: data.tag_name,
        last_location: data.location_from,
        reported_by:   user.name,
        timestamp:     rfidEvent.timestamp,
      });
    }

    console.log(`[RFID] Tag: ${data.tag_id} | Event: ${data.event_type} | By: ${user.name}`);
    if (typeof ack === 'function') ack({ success: true });
  });

  // LOW STOCK ALERT BROADCAST
  socket.on('low_stock_alert', (data) => {
    if (user.role !== 'admin' && user.role !== 'manager') return;

    io.to('global').emit('alert', {
      type:    'low_stock',
      title:   `Low Stock: ${data.item_name}`,
      message: `${data.quantity} ${data.unit} remaining (min: ${data.reorder_level})`,
      item:    data,
      timestamp: new Date().toISOString(),
    });
  });

  // AI STREAMING
  socket.on('ai_stream_start', (data) => {
    socket.emit('ai_stream_chunk', { chunk: '', done: false, sessionId: data.sessionId });
  });

  socket.on('ai_stream_chunk', (data) => {
    // Relay AI chunks to requester
    io.to(socket.id).emit('ai_stream_chunk', data);
  });

  // ROOM MANAGEMENT
  socket.on('join_room', (data, ack) => {
    const room = sanitizeRoom(data.room);
    socket.join(room);
    state.rooms.add(room);
    console.log(`[ROOM] ${user.name} joined: ${room}`);
    if (typeof ack === 'function') ack({ success: true, room });
  });

  socket.on('leave_room', (data) => {
    socket.leave(sanitizeRoom(data.room));
  });

  // PING / HEARTBEAT
  socket.on('ping', (data, ack) => {
    if (typeof ack === 'function') {
      ack({ pong: true, serverTime: new Date().toISOString(), latency: Date.now() - (data.sent || Date.now()) });
    }
  });

  // EVENT SYNC (offline queue flush)
  socket.on('sync_offline_queue', (data, ack) => {
    const events = data.events || [];
    console.log(`[SYNC] ${user.name} syncing ${events.length} offline events`);

    // Process each queued event
    const results = events.map(ev => {
      try {
        socket.emit(ev.event, ev.data);
        return { id: ev.id, success: true };
      } catch (e) {
        return { id: ev.id, success: false, error: e.message };
      }
    });

    if (typeof ack === 'function') ack({ success: true, results });
  });

  // NOTIFICATION
  socket.on('send_notification', (data) => {
    if (user.role !== 'admin') return;

    const notification = {
      ...data,
      id:        Date.now(),
      sent_by:   user.name,
      timestamp: new Date().toISOString(),
    };

    const target = data.target || 'global';
    io.to(target).emit('notification', notification);
  });

  // DISCONNECT
  socket.on('disconnect', (reason) => {
    state.connectedUsers.delete(socket.id);
    state.scanSessions.delete(socket.id);

    console.log(`[DISCONNECT] ${user.name} | Reason: ${reason}`);

    socket.to(branchRoom).emit('user_left', {
      userId: user.id, name: user.name,
    });

    io.to('global').emit('online_count', { count: state.connectedUsers.size });
  });

  socket.on('error', (err) => {
    console.error(`[SOCKET_ERROR] ${user.name}:`, err.message);
  });
});

// ── HTTP API Endpoints ────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({
    status:    'ok',
    uptime_s:  Math.floor((Date.now() - state.metrics.startTime) / 1000),
    connected: state.connectedUsers.size,
    rooms:     state.rooms.size,
    metrics:   state.metrics,
    timestamp: new Date().toISOString(),
  });
});

app.get('/api/status', authenticateHTTP, (req, res) => {
  res.json({
    online_users: Array.from(state.connectedUsers.values()).map(u => ({
      name:     u.name,
      role:     u.role,
      branch:   u.branch,
      joinedAt: u.joinedAt,
    })),
    rfid_tags: Array.from(state.rfidSessions.entries()).map(([id, s]) => ({
      tag_id:   id,
      location: s.location,
      lastSeen: s.lastSeen,
      status:   s.status,
    })),
    metrics: state.metrics,
  });
});

// Broadcast endpoint (for GAS backend to push events)
app.post('/api/broadcast', authenticateHTTP, (req, res) => {
  const { event, data, room = 'global' } = req.body;

  if (!event || !data) {
    return res.status(400).json({ error: 'event and data required' });
  }

  io.to(room).emit(event, { ...data, broadcasted_at: new Date().toISOString() });
  console.log(`[BROADCAST] Event: ${event} → Room: ${room}`);
  res.json({ success: true, room, recipients: io.sockets.adapter.rooms.get(room)?.size || 0 });
});

// Queue event for offline user
app.post('/api/queue', authenticateHTTP, (req, res) => {
  const { userId, event, data } = req.body;
  if (!state.offlineQueues.has(userId)) state.offlineQueues.set(userId, []);
  state.offlineQueues.get(userId).push({ event, data, queuedAt: Date.now() });
  res.json({ success: true, queued: true });
});

// ── HTTP Auth Middleware ──────────────────────────────────────
function authenticateHTTP(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '') ||
                req.query.token;

  // Internal broadcast key (for GAS → WS server communication)
  const internalKey = req.headers['x-internal-key'];
  if (internalKey === process.env.INTERNAL_API_KEY) return next();

  if (!token) return res.status(401).json({ error: 'Unauthorized' });

  try {
    req.user = jwt.verify(token, CONFIG.JWT_SECRET);
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
    next();
  } catch (e) {
    res.status(401).json({ error: 'Invalid token' });
  }
}

// ── Helpers ───────────────────────────────────────────────────
function validateEvent(data, required) {
  for (const field of required) {
    if (!data[field]) throw new Error(`Missing required field: ${field}`);
  }
}

function sanitizeRoom(room) {
  return (room || 'global').replace(/[^a-z0-9\-_]/gi, '').toLowerCase().slice(0, 64);
}

// ── Scheduled Tasks ───────────────────────────────────────────
// Cleanup stale offline queues every 30 minutes
setInterval(() => {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000; // 24 hours
  for (const [userId, queue] of state.offlineQueues.entries()) {
    const fresh = queue.filter(ev => ev.queuedAt > cutoff);
    if (fresh.length === 0) {
      state.offlineQueues.delete(userId);
    } else {
      state.offlineQueues.set(userId, fresh);
    }
  }
  console.log(`[CLEANUP] Offline queues cleaned. Active users: ${state.connectedUsers.size}`);
}, 30 * 60 * 1000);

// Broadcast server heartbeat every 60 seconds
setInterval(() => {
  io.to('global').emit('server_heartbeat', {
    timestamp:   new Date().toISOString(),
    online:      state.connectedUsers.size,
    uptime_s:    Math.floor((Date.now() - state.metrics.startTime) / 1000),
  });
}, 60 * 1000);

// ── Start Server ──────────────────────────────────────────────
server.listen(CONFIG.PORT, () => {
  console.log(`
╔════════════════════════════════════════════╗
║   APPETINA IMS — WebSocket Server          ║
║   Port: ${CONFIG.PORT}                            ║
║   Environment: ${process.env.NODE_ENV || 'development'}             ║
╚════════════════════════════════════════════╝
  `);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[SHUTDOWN] Graceful shutdown initiated...');
  io.to('global').emit('server_shutdown', { message: 'Server restarting, please reconnect in a moment.' });
  server.close(() => {
    console.log('[SHUTDOWN] Server closed.');
    process.exit(0);
  });
});

module.exports = { app, server, io };
