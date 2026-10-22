/* 我的衣橱 · Service Worker
 * - 缓存 App 外壳（HTML/图标），让 App 离线也能打开
 * - 数据接口 /api 永远走网络，保证多设备实时同步
 */
const CACHE = 'wardrobe-shell-v1';
const SHELL = ['/', '/icon.svg', '/manifest.webmanifest'];

self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).catch(() => {}));
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  // 接口永远走网络，不缓存
  if (url.pathname.startsWith('/api')) return;
  // 页面导航：网络优先，失败回退到缓存的外壳
  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request).then((res) => {
        const copy = res.clone();
        caches.open(CACHE).then((c) => c.put('/', copy));
        return res;
      }).catch(() => caches.match('/'))
    );
    return;
  }
  // 其他静态资源：缓存优先
  event.respondWith(
    caches.match(event.request).then((hit) => hit || fetch(event.request))
  );
});
