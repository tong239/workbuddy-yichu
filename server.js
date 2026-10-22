/**
 * 我的衣橱 · 共享后端
 * - 单实例服务，多人多设备通过「房间(room)」隔离数据
 * - 数据持久化到 .data/store.json
 * - REST 增删改 + SSE 实时广播（别人改了，你的界面自动刷新）
 */
const express = require('express');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, '.data');
const DATA_FILE = path.join(DATA_DIR, 'store.json');

const app = express();
app.use(express.json({ limit: '12mb' })); // 照片以 base64 上传，放宽限制

/* ---------------- 跨域（CORS） ----------------
 * 让浏览器/APK 在「联网共享版」下能跨域访问本服务（含 SSE 实时推送）。
 * 回显请求来源，兼容 file://（来源为 null）与同源两种场景。            */
app.use((req, res, next) => {
  res.set('Access-Control-Allow-Origin', req.get('Origin') || '*');
  res.set('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
  res.set('Access-Control-Max-Age', '86400');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

/* ---------------- 数据存储 ---------------- */
let store = { rooms: {} };
function load() {
  try {
    store = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    if (!store.rooms) store.rooms = {};
  } catch (e) { store = { rooms: {} }; }
}
function save() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify(store));
  } catch (e) { console.error('保存失败', e); }
}
load();

const rand = () => Math.floor(Math.random() * 1e4);
function demoItems() {
  return [
    { id: 'd1', name: '燕麦色大衣', cat: '外套', season: '秋冬', color: '米白', price: 500, status: '干净', fav: false, img: '' },
    { id: 'd2', name: '直筒牛仔裤', cat: '裤子', season: '秋冬', color: '牛仔蓝', price: 299, status: '干净', fav: false, img: '' },
    { id: 'd3', name: '白色基础T恤', cat: '上衣', season: '春夏', color: '白', price: 79, status: '待清洗', fav: true, img: '' },
    { id: 'd4', name: '黑色针织衫', cat: '上衣', season: '秋冬', color: '黑', price: 219, status: '干净', fav: false, img: '' },
    { id: 'd5', name: '碎花连衣裙', cat: '连衣裙', season: '春夏', color: '米白', price: 359, status: '干净', fav: true, img: '' },
    { id: 'd6', name: '小白鞋', cat: '鞋子', season: '春夏', color: '白', price: 399, status: '待清洗', fav: false, img: '' },
  ];
}
function room(roomId) {
  if (!store.rooms[roomId]) {
    store.rooms[roomId] = {
      items: demoItems(),
      outfits: [{ id: 'o1', name: '秋天通勤穿搭', itemIds: ['d1', 'd2', 'd4'] }],
      wishes: [{ id: 'w1', name: '白色马丁靴', price: 600, note: '8孔 · 经典款' }],
      wears: [],
      rev: 1,
    };
    save();
  }
  return store.rooms[roomId];
}
function stateOf(r) {
  return { items: r.items, outfits: r.outfits, wishes: r.wishes, wears: r.wears || [], rev: r.rev };
}
function commit(roomId) {
  const r = room(roomId);
  r.rev = (r.rev || 0) + 1;
  save();
  broadcast(roomId);
  return stateOf(r);
}

/* ---------------- SSE 实时推送 ---------------- */
const clients = {}; // roomId -> Set(res)
function clientsOf(roomId) {
  if (!clients[roomId]) clients[roomId] = new Set();
  return clients[roomId];
}
function broadcast(roomId) {
  const set = clients[roomId];
  if (!set) return;
  set.forEach((res) => { try { res.write('data: update\n\n'); } catch (e) {} });
}

app.get('/api/stream', (req, res) => {
  const roomId = req.query.room || 'default';
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.write('retry: 3000\n\n');
  clientsOf(roomId).add(res);
  const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 25000);
  req.on('close', () => { clearInterval(ping); clientsOf(roomId).delete(res); });
});

/* ---------------- 状态读取 ---------------- */
app.get('/api/state', (req, res) => {
  const roomId = req.query.room || 'default';
  res.json(stateOf(room(roomId)));
});

/* ---------------- 衣物 ---------------- */
app.post('/api/item', (req, res) => {
  const roomId = req.query.room || 'default';
  const r = room(roomId);
  const b = req.body || {};
  const it = {
    id: 'i' + Date.now() + rand(),
    name: b.name || '未命名',
    cat: b.cat || '上衣',
    season: b.season || '春',
    color: b.color || '黑',
    price: Number(b.price) || 0,
    status: b.status || '干净',
    fav: !!b.fav,
    img: b.img || '',
  };
  r.items.unshift(it);
  res.json(commit(roomId));
});
app.put('/api/item/:id', (req, res) => {
  const roomId = req.query.room || 'default';
  const r = room(roomId);
  const it = r.items.find((i) => i.id === req.params.id);
  if (!it) return res.status(404).json({ error: 'not found' });
  Object.assign(it, req.body || {});
  res.json(commit(roomId));
});
app.delete('/api/item/:id', (req, res) => {
  const roomId = req.query.room || 'default';
  const r = room(roomId);
  r.items = r.items.filter((i) => i.id !== req.params.id);
  r.outfits.forEach((o) => { o.itemIds = (o.itemIds || []).filter((id) => id !== req.params.id); });
  res.json(commit(roomId));
});

/* ---------------- 搭配 ---------------- */
app.post('/api/outfit', (req, res) => {
  const roomId = req.query.room || 'default';
  const r = room(roomId);
  const b = req.body || {};
  r.outfits.unshift({
    id: 'o' + Date.now() + rand(),
    name: b.name || '未命名搭配',
    itemIds: Array.isArray(b.itemIds) ? b.itemIds : [],
  });
  res.json(commit(roomId));
});
app.delete('/api/outfit/:id', (req, res) => {
  const roomId = req.query.room || 'default';
  const r = room(roomId);
  r.outfits = r.outfits.filter((o) => o.id !== req.params.id);
  res.json(commit(roomId));
});

/* ---------------- 想买 ---------------- */
app.post('/api/wish', (req, res) => {
  const roomId = req.query.room || 'default';
  const r = room(roomId);
  const b = req.body || {};
  r.wishes.unshift({
    id: 'w' + Date.now() + rand(),
    name: b.name || '未命名',
    price: Number(b.price) || 0,
    note: b.note || '',
  });
  res.json(commit(roomId));
});
app.delete('/api/wish/:id', (req, res) => {
  const roomId = req.query.room || 'default';
  const r = room(roomId);
  r.wishes = r.wishes.filter((w) => w.id !== req.params.id);
  res.json(commit(roomId));
});
// 已入手：转入衣橱
app.post('/api/wish/:id/buy', (req, res) => {
  const roomId = req.query.room || 'default';
  const r = room(roomId);
  const w = r.wishes.find((x) => x.id === req.params.id);
  if (!w) return res.status(404).json({ error: 'not found' });
  r.items.unshift({
    id: 'i' + Date.now() + rand(),
    name: w.name, cat: '上衣', season: '春', color: '黑',
    price: w.price || 0, status: '干净', fav: false, img: '',
  });
  r.wishes = r.wishes.filter((x) => x.id !== req.params.id);
  res.json(commit(roomId));
});

/* ---------------- 穿搭记录（用于避免重复推荐） ---------------- */
app.post('/api/wear', (req, res) => {
  const roomId = req.query.room || 'default';
  const r = room(roomId);
  const b = req.body || {};
  r.wears = r.wears || [];
  r.wears.push({
    id: 'wr' + Date.now() + rand(),
    date: b.date || new Date().toDateString(),
    temp: b.temp != null ? Number(b.temp) : null,
    itemIds: Array.isArray(b.itemIds) ? b.itemIds : [],
  });
  res.json(commit(roomId));
});

/* ---------------- 前端单页 ---------------- */
function fileVersion(name) {
  try {
    const p = path.join(__dirname, name);
    const st = fs.statSync(p);
    const hash = crypto.createHash('md5').update(fs.readFileSync(p)).digest('hex').slice(0, 8);
    const d = new Date(st.mtimeMs);
    const vs = d.toISOString().slice(0, 10) + '-' + hash;
    return { version: vs, size: st.size, updatedAt: st.mtimeMs };
  } catch (e) { return { version: 'unknown', size: 0, updatedAt: 0 }; }
}
// App 内「检查更新」用：返回两个版本的最新信息
app.get('/version.json', (req, res) => {
  res.type('application/json').send({
    offline: fileVersion('我的衣橱.html'),   // 单机离线版（APK 更新默认渠道）
    online: fileVersion('index.html'),      // 联网共享版
    ts: Date.now(),
  });
});
// 离线单机版下载源
app.get('/mobile.html', (req, res) => {
  res.set('Cache-Control', 'no-cache');
  res.sendFile(path.join(__dirname, '我的衣橱.html'));
});
app.get('/download', (req, res) => {
  res.download(path.join(__dirname, '我的衣橱.apk'), '我的衣橱.apk');
});

app.get('/', (req, res) => {
  res.set('Cache-Control', 'no-cache');
  res.sendFile(path.join(__dirname, 'index.html'));
});
app.get('/manifest.webmanifest', (req, res) => {
  res.type('application/manifest+json');
  res.sendFile(path.join(__dirname, 'manifest.webmanifest'));
});
app.get('/sw.js', (req, res) => {
  res.type('application/javascript');
  res.sendFile(path.join(__dirname, 'sw.js'));
});
app.get('/icon.svg', (req, res) => {
  res.type('image/svg+xml');
  res.sendFile(path.join(__dirname, 'icon.svg'));
});

app.listen(PORT, () => console.log('衣橱服务已启动: http://localhost:' + PORT));
