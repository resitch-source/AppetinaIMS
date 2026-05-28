/**
 * ═══════════════════════════════════════════════════════════════
 * APPETINA IMS — Service Worker
 * Offline-first with IndexedDB sync queue
 * ═══════════════════════════════════════════════════════════════
 */

'use strict';

const SW_VERSION  = 'appetina-v2.0.0';
const CACHE_STATIC = SW_VERSION + '-static';
const CACHE_API    = SW_VERSION + '-api';

// Assets to precache
const PRECACHE_URLS = [
  './',
  './index.html',
  'https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@300;400;500&family=Instrument+Sans:wght@400;500;600&display=swap',
];

// API patterns that should be cached with network-first strategy
const API_CACHE_PATTERNS = [
  /\/exec\?action=(getInventory|getEvents|getRecipes|getVendors|config)/,
];

// Offline fallback data
const OFFLINE_INVENTORY_FALLBACK = {
  success: true,
  data: [],
  _offline: true,
  _message: 'Showing cached data. Connect to sync latest inventory.',
};

// ── Install ───────────────────────────────────────────────────
self.addEventListener('install', (event) => {
  console.log('[SW] Installing', SW_VERSION);
  event.waitUntil(
    caches.open(CACHE_STATIC)
      .then(cache => cache.addAll(PRECACHE_URLS))
      .then(() => self.skipWaiting())
  );
});

// ── Activate ──────────────────────────────────────────────────
self.addEventListener('activate', (event) => {
  console.log('[SW] Activating', SW_VERSION);
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_STATIC && key !== CACHE_API)
          .map(key => {
            console.log('[SW] Deleting old cache:', key);
            return caches.delete(key);
          })
      )
    ).then(() => self.clients.claim())
  );
});

// ── Fetch Strategy ────────────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Skip non-GET and non-http requests
  if (request.method !== 'GET' || !url.protocol.startsWith('http')) return;

  // Google Apps Script API → Network first, cache fallback
  if (isAPIRequest(request.url)) {
    event.respondWith(networkFirstWithCache(request));
    return;
  }

  // Static assets → Cache first
  event.respondWith(cacheFirstWithNetwork(request));
});

async function networkFirstWithCache(request) {
  const cache = await caches.open(CACHE_API);
  try {
    const response = await fetch(request.clone(), { signal: AbortSignal.timeout(8000) });
    if (response.ok) {
      cache.put(request, response.clone());
    }
    return response;
  } catch (err) {
    console.log('[SW] Network failed, serving cache for:', request.url);
    const cached = await cache.match(request);
    if (cached) return cached;

    // Return offline fallback for inventory requests
    return new Response(JSON.stringify(OFFLINE_INVENTORY_FALLBACK), {
      headers: { 'Content-Type': 'application/json', 'X-SW-Offline': 'true' },
    });
  }
}

async function cacheFirstWithNetwork(request) {
  const cached = await caches.match(request);
  if (cached) return cached;

  try {
    const response = await fetch(request);
    if (response.ok && response.type !== 'opaque') {
      const cache = await caches.open(CACHE_STATIC);
      cache.put(request, response.clone());
    }
    return response;
  } catch (err) {
    // Return offline page
    const offlinePage = await caches.match('./index.html');
    return offlinePage || new Response('Offline — please reconnect', {
      status: 503,
      headers: { 'Content-Type': 'text/plain' },
    });
  }
}

function isAPIRequest(url) {
  return API_CACHE_PATTERNS.some(pattern => pattern.test(url));
}

// ── Background Sync ───────────────────────────────────────────
self.addEventListener('sync', (event) => {
  console.log('[SW] Background sync:', event.tag);

  if (event.tag === 'sync-inventory-queue') {
    event.waitUntil(syncInventoryQueue());
  }
  if (event.tag === 'sync-scan-history') {
    event.waitUntil(syncScanHistory());
  }
});

async function syncInventoryQueue() {
  const db    = await openDB();
  const queue = await getAllFromStore(db, 'syncQueue');

  console.log(`[SW] Syncing ${queue.length} queued transactions...`);

  for (const item of queue) {
    try {
      const response = await fetch(item.url, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(item.payload),
      });

      if (response.ok) {
        await deleteFromStore(db, 'syncQueue', item.id);
        console.log(`[SW] Synced: ${item.id}`);
      }
    } catch (err) {
      console.error(`[SW] Sync failed for ${item.id}:`, err.message);
    }
  }

  // Notify clients
  const clients = await self.clients.matchAll();
  clients.forEach(client => client.postMessage({
    type: 'SYNC_COMPLETE',
    synced: queue.length,
  }));
}

async function syncScanHistory() {
  const db    = await openDB();
  const scans = await getAllFromStore(db, 'pendingScans');
  // Process similar to syncInventoryQueue
  console.log(`[SW] Syncing ${scans.length} pending scans`);
}

// ── Push Notifications ────────────────────────────────────────
self.addEventListener('push', (event) => {
  if (!event.data) return;

  let data;
  try {
    data = event.data.json();
  } catch (_) {
    data = { title: 'Appetina IMS', body: event.data.text() };
  }

  const options = {
    body:    data.body || data.message,
    icon:    './icons/icon-192.png',
    badge:   './icons/icon-72.png',
    tag:     data.tag || 'appetina-alert',
    renotify: true,
    vibrate: [200, 100, 200],
    data:    data,
    actions: data.actions || [
      { action: 'view', title: 'View Details' },
      { action: 'dismiss', title: 'Dismiss' },
    ],
  };

  event.waitUntil(
    self.registration.showNotification(data.title || 'Appetina IMS Alert', options)
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  if (event.action === 'dismiss') return;

  const url = event.notification.data?.url || './';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(cs => {
      const existing = cs.find(c => c.url.includes('Appetina') && 'focus' in c);
      if (existing) return existing.focus();
      return clients.openWindow(url);
    })
  );
});

// ── IndexedDB Helper ──────────────────────────────────────────
function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open('appetina-ims', 3);

    req.onupgradeneeded = (e) => {
      const db = e.target.result;

      if (!db.objectStoreNames.contains('syncQueue')) {
        const store = db.createObjectStore('syncQueue', { keyPath: 'id', autoIncrement: true });
        store.createIndex('createdAt', 'createdAt');
        store.createIndex('type',      'type');
      }
      if (!db.objectStoreNames.contains('pendingScans')) {
        db.createObjectStore('pendingScans', { keyPath: 'id', autoIncrement: true });
      }
      if (!db.objectStoreNames.contains('cachedInventory')) {
        db.createObjectStore('cachedInventory', { keyPath: 'id' });
      }
      if (!db.objectStoreNames.contains('cachedEvents')) {
        db.createObjectStore('cachedEvents', { keyPath: 'id' });
      }
    };

    req.onsuccess = () => resolve(req.result);
    req.onerror   = () => reject(req.error);
  });
}

function getAllFromStore(db, storeName) {
  return new Promise((resolve, reject) => {
    const tx   = db.transaction(storeName, 'readonly');
    const req  = tx.objectStore(storeName).getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror   = () => reject(req.error);
  });
}

function deleteFromStore(db, storeName, id) {
  return new Promise((resolve, reject) => {
    const tx  = db.transaction(storeName, 'readwrite');
    const req = tx.objectStore(storeName).delete(id);
    req.onsuccess = () => resolve();
    req.onerror   = () => reject(req.error);
  });
}

// ── Message Handler (from main thread) ───────────────────────
self.addEventListener('message', (event) => {
  const { type, data } = event.data || {};

  if (type === 'SKIP_WAITING') {
    self.skipWaiting();
  }

  if (type === 'CACHE_INVENTORY') {
    caches.open(CACHE_API).then(cache => {
      cache.put(
        new Request('/api/inventory'),
        new Response(JSON.stringify(data), { headers: { 'Content-Type': 'application/json' } })
      );
    });
  }

  if (type === 'QUEUE_TRANSACTION') {
    openDB().then(db => {
      const tx    = db.transaction('syncQueue', 'readwrite');
      const store = tx.objectStore('syncQueue');
      store.add({ ...data, createdAt: Date.now() });
    });
  }
});
