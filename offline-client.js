/**
 * ═══════════════════════════════════════════════════════════════
 * APPETINA IMS — Offline Client Library
 * IndexedDB sync queue + Socket.IO client wrapper
 * Include in index.html: <script src="offline-client.js"></script>
 * ═══════════════════════════════════════════════════════════════
 */

'use strict';

// ── IndexedDB Manager ─────────────────────────────────────────
const AppDB = {
  DB_NAME:    'appetina-ims',
  DB_VERSION: 3,
  _db:        null,

  async open() {
    if (this._db) return this._db;
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(this.DB_NAME, this.DB_VERSION);

      req.onupgradeneeded = (e) => {
        const db = e.target.result;

        // Sync queue for offline transactions
        if (!db.objectStoreNames.contains('syncQueue')) {
          const s = db.createObjectStore('syncQueue', { keyPath: 'id', autoIncrement: true });
          s.createIndex('status',    'status');
          s.createIndex('createdAt', 'createdAt');
          s.createIndex('type',      'type');
        }

        // Cached inventory (for offline viewing)
        if (!db.objectStoreNames.contains('inventory')) {
          const s = db.createObjectStore('inventory', { keyPath: 'id' });
          s.createIndex('category', 'category');
          s.createIndex('status',   'status');
          s.createIndex('sku',      'sku', { unique: false });
        }

        // Cached events
        if (!db.objectStoreNames.contains('events')) {
          db.createObjectStore('events', { keyPath: 'id' });
        }

        // Scan history (local log)
        if (!db.objectStoreNames.contains('scanHistory')) {
          const s = db.createObjectStore('scanHistory', { keyPath: 'id', autoIncrement: true });
          s.createIndex('barcode',   'barcode');
          s.createIndex('scannedAt', 'scannedAt');
        }

        // User preferences / settings
        if (!db.objectStoreNames.contains('settings')) {
          db.createObjectStore('settings', { keyPath: 'key' });
        }
      };

      req.onsuccess  = () => { this._db = req.result; resolve(req.result); };
      req.onerror    = () => reject(req.error);
    });
  },

  async get(store, key) {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const req = db.transaction(store, 'readonly').objectStore(store).get(key);
      req.onsuccess = () => resolve(req.result);
      req.onerror   = () => reject(req.error);
    });
  },

  async getAll(store, indexName, value) {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const tx    = db.transaction(store, 'readonly');
      const s     = tx.objectStore(store);
      const req   = indexName ? s.index(indexName).getAll(value) : s.getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror   = () => reject(req.error);
    });
  },

  async put(store, record) {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const req = db.transaction(store, 'readwrite').objectStore(store).put(record);
      req.onsuccess = () => resolve(req.result);
      req.onerror   = () => reject(req.error);
    });
  },

  async putAll(store, records) {
    const db = await this.open();
    const tx = db.transaction(store, 'readwrite');
    const s  = tx.objectStore(store);
    return new Promise((resolve, reject) => {
      records.forEach(r => s.put(r));
      tx.oncomplete = () => resolve(records.length);
      tx.onerror    = () => reject(tx.error);
    });
  },

  async delete(store, key) {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const req = db.transaction(store, 'readwrite').objectStore(store).delete(key);
      req.onsuccess = () => resolve();
      req.onerror   = () => reject(req.error);
    });
  },

  async clear(store) {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const req = db.transaction(store, 'readwrite').objectStore(store).clear();
      req.onsuccess = () => resolve();
      req.onerror   = () => reject(req.error);
    });
  },

  async count(store) {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const req = db.transaction(store, 'readonly').objectStore(store).count();
      req.onsuccess = () => resolve(req.result);
      req.onerror   = () => reject(req.error);
    });
  },
};

// ── Offline Sync Manager ──────────────────────────────────────
const OfflineSync = {
  GAS_URL:    null,
  isOnline:   navigator.onLine,
  syncActive: false,

  init(gasUrl) {
    this.GAS_URL = gasUrl;

    window.addEventListener('online',  () => this._onOnline());
    window.addEventListener('offline', () => this._onOffline());

    // Register background sync
    if ('serviceWorker' in navigator && 'SyncManager' in window) {
      navigator.serviceWorker.ready.then(sw => {
        this._swRegistration = sw;
      });
    }

    // Listen for sync complete from SW
    navigator.serviceWorker?.addEventListener('message', (e) => {
      if (e.data?.type === 'SYNC_COMPLETE') {
        console.log(`[OFFLINE_SYNC] SW synced ${e.data.synced} transactions`);
        this._notifyUI('sync_complete', { count: e.data.synced });
      }
    });

    console.log(`[OFFLINE_SYNC] Initialized. Online: ${this.isOnline}`);
  },

  async queue(type, url, payload) {
    const id = await AppDB.put('syncQueue', {
      type,
      url,
      payload,
      status:    'pending',
      attempts:  0,
      createdAt: Date.now(),
    });

    // Update UI badge
    const count = await AppDB.count('syncQueue');
    this._notifyUI('queue_updated', { count });

    // Register background sync
    if (this._swRegistration) {
      await this._swRegistration.sync.register('sync-inventory-queue').catch(() => {});
    }

    return id;
  },

  async flush() {
    if (this.syncActive || !this.isOnline) return;
    this.syncActive = true;

    const queue   = await AppDB.getAll('syncQueue', 'status', 'pending');
    const token   = sessionStorage.getItem('appetina_token');
    let   synced  = 0;
    let   failed  = 0;

    console.log(`[OFFLINE_SYNC] Flushing ${queue.length} queued operations...`);

    for (const item of queue) {
      try {
        const response = await fetch(item.url, {
          method:  'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`,
          },
          body: JSON.stringify({ ...item.payload, token }),
          signal: AbortSignal.timeout(15000),
        });

        if (response.ok) {
          await AppDB.delete('syncQueue', item.id);
          synced++;
        } else {
          await AppDB.put('syncQueue', {
            ...item, status: 'error', attempts: (item.attempts || 0) + 1,
          });
          failed++;
        }
      } catch (err) {
        const attempts = (item.attempts || 0) + 1;
        if (attempts >= 5) {
          // Give up after 5 attempts
          await AppDB.put('syncQueue', { ...item, status: 'failed', attempts });
        } else {
          await AppDB.put('syncQueue', { ...item, attempts });
        }
        failed++;
      }
    }

    this.syncActive = false;
    this._notifyUI('sync_complete', { synced, failed, total: queue.length });

    const remaining = await AppDB.count('syncQueue');
    this._notifyUI('queue_updated', { count: remaining });

    console.log(`[OFFLINE_SYNC] Done: ${synced} synced, ${failed} failed`);
    return { synced, failed };
  },

  _onOnline() {
    this.isOnline = true;
    console.log('[OFFLINE_SYNC] 🟢 Back online — starting sync...');
    this._notifyUI('online');
    setTimeout(() => this.flush(), 1000); // small delay to let connection stabilize
  },

  _onOffline() {
    this.isOnline = false;
    console.log('[OFFLINE_SYNC] 🔴 Offline mode activated');
    this._notifyUI('offline');
  },

  _notifyUI(event, data = {}) {
    window.dispatchEvent(new CustomEvent('appetina:' + event, { detail: data }));
  },

  async getQueueCount() {
    return AppDB.count('syncQueue');
  },
};

// ── Inventory Cache ───────────────────────────────────────────
const InventoryCache = {
  async save(items) {
    await AppDB.putAll('inventory', items);
    await AppDB.put('settings', {
      key:   'inventory_cached_at',
      value: Date.now(),
    });
    console.log(`[CACHE] Saved ${items.length} inventory items to IndexedDB`);
  },

  async load() {
    return AppDB.getAll('inventory');
  },

  async search(query) {
    const all = await AppDB.getAll('inventory');
    const q   = query.toLowerCase();
    return all.filter(i =>
      i.name?.toLowerCase().includes(q) ||
      i.sku?.toLowerCase().includes(q)  ||
      i.category?.toLowerCase().includes(q)
    );
  },

  async getByBarcode(barcode) {
    const all = await AppDB.getAll('inventory');
    return all.find(i => i.sku === barcode || i.rfid_tag === barcode) || null;
  },

  async getCachedAt() {
    const s = await AppDB.get('settings', 'inventory_cached_at');
    return s?.value || null;
  },
};

// ── Scan History (local) ──────────────────────────────────────
const ScanHistory = {
  async add(scan) {
    return AppDB.put('scanHistory', {
      ...scan,
      scannedAt: Date.now(),
    });
  },

  async getAll(limit = 50) {
    const all = await AppDB.getAll('scanHistory');
    return all
      .sort((a, b) => b.scannedAt - a.scannedAt)
      .slice(0, limit);
  },

  async clear() {
    return AppDB.clear('scanHistory');
  },
};

// ── Service Worker Registration ───────────────────────────────
async function registerServiceWorker() {
  if (!('serviceWorker' in navigator)) {
    console.log('[SW] Service Worker not supported');
    return;
  }

  try {
    const reg = await navigator.serviceWorker.register('./sw.js', {
      scope: './',
      updateViaCache: 'none',
    });

    console.log('[SW] Registered:', reg.scope);

    // Handle updates
    reg.addEventListener('updatefound', () => {
      const newSW = reg.installing;
      newSW.addEventListener('statechange', () => {
        if (newSW.state === 'installed' && navigator.serviceWorker.controller) {
          // New version available — notify user
          window.dispatchEvent(new CustomEvent('appetina:sw_update'));
          console.log('[SW] New version available — reload to update');
        }
      });
    });

    return reg;
  } catch (err) {
    console.error('[SW] Registration failed:', err);
  }
}

// ── PWA Install Prompt ────────────────────────────────────────
let deferredInstallPrompt = null;

window.addEventListener('beforeinstallprompt', (e) => {
  e.preventDefault();
  deferredInstallPrompt = e;
  window.dispatchEvent(new CustomEvent('appetina:install_available'));
});

async function showInstallPrompt() {
  if (!deferredInstallPrompt) return false;
  deferredInstallPrompt.prompt();
  const { outcome } = await deferredInstallPrompt.userChoice;
  deferredInstallPrompt = null;
  return outcome === 'accepted';
}

// ── Init ──────────────────────────────────────────────────────
(async function init() {
  await registerServiceWorker();
  // OfflineSync.init() is called from main app with GAS_URL
})();

// Export for use in main app
window.AppDB         = AppDB;
window.OfflineSync   = OfflineSync;
window.InventoryCache = InventoryCache;
window.ScanHistory   = ScanHistory;
window.showInstallPrompt = showInstallPrompt;
