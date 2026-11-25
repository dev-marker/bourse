// The live page: one symbol's book, trades and candles, from marketdata's feed. It keeps its copy
// of the book exactly the way marketdata.Mirror does, and draws it once a frame at most.

const $ = (id) => document.getElementById(id);
const scheme = location.protocol === "https:" ? "wss" : "ws";
const stream = `${scheme}://${location.host}/v1/stream`;
const params = new URLSearchParams(location.search);
// With ?drop=N, the page loses one update in N on purpose, to show a gap being caught.
const drop = Number(params.get("drop")) || 0;

let symbol = params.get("symbol") || "GLBX";
let mirror = null; // the book, the trades and the candles, up to mirror.seq
let ws = null;
let retries = 0;
let updates = 0;
let frames = 0;
let gaps = 0;

class Gap extends Error {}

// apply applies the next message from the feed, the way marketdata.Mirror does.
function apply(msg) {
  if (msg.type === "snapshot") {
    mirror = {
      seq: msg.seq,
      decimals: msg.decimals,
      bids: new Map(),
      asks: new Map(),
      trades: msg.trades ?? [],
      candles: msg.candles ?? [],
    };
  } else if (!mirror) {
    throw new Error("an update came before the snapshot");
  } else if (msg.seq !== mirror.seq + 1) {
    throw new Gap(`seq ${msg.seq} came after ${mirror.seq}`);
  }
  mirror.seq = msg.seq;
  set(mirror.bids, msg.bids);
  set(mirror.asks, msg.asks);
  if (msg.trade) {
    mirror.trades.push(msg.trade);
    if (mirror.trades.length > 50) mirror.trades.shift();
  }
  if (msg.candle) {
    const n = mirror.candles.length;
    if (n > 0 && mirror.candles[n - 1].time === msg.candle.time) {
      mirror.candles[n - 1] = msg.candle;
    } else {
      mirror.candles.push(msg.candle);
    }
  }
}

// set gives each level its new quantity. Zero means nothing rests there any more.
function set(side, levels = []) {
  for (const { price, qty } of levels) {
    if (qty === 0) side.delete(price);
    else side.set(price, qty);
  }
}

// connect follows the symbol: a snapshot, then every update, until something goes wrong.
function connect() {
  mirror = null;
  ws = new WebSocket(`${stream}?symbol=${symbol}`);
  let why = "reconnecting";
  let n = 0;
  ws.onmessage = (e) => {
    try {
      for (const msg of JSON.parse(e.data)) {
        if (drop > 0 && ++n % drop === 0) continue; // lost on purpose
        apply(msg);
        updates++;
      }
    } catch (err) {
      if (err instanceof Gap) gaps++;
      why = err instanceof Gap ? "gap, resyncing" : "reconnecting";
      ws.onmessage = null;
      ws.close();
      return;
    }
    retries = 0;
    badge("live", "live");
    changed();
  };
  ws.onclose = (e) => {
    if (e.code === 1013) why = "too slow, reconnecting";
    badge(why.startsWith("gap") ? "gap" : "down", why);
    reconnect();
  };
}

// reconnect tries again in a while: a quarter of a second at first, twice as long after every
// failure, ten seconds at most. Each wait is random, from half of that to all of it, so pages
// that lost the feed together don't all come back at the same moment.
function reconnect() {
  const wait = Math.min(10_000, 250 * 2 ** retries++);
  setTimeout(connect, wait / 2 + Math.random() * (wait / 2));
}

// changed asks for a frame. However many updates arrive before it, the page is drawn once.
let pending = false;
function changed() {
  if (pending) return;
  pending = true;
  requestAnimationFrame(() => {
    pending = false;
    frames++;
    draw();
  });
}

function draw() {
  if (!mirror) return;
  const last = mirror.trades.at(-1);
  $("last").textContent = last ? price(last.price) : "";
  $("last").className = `last ${last?.side ?? ""}`;
  const bid = Math.max(...mirror.bids.keys());
  const ask = Math.min(...mirror.asks.keys());
  const spread = isFinite(bid + ask) ? price(ask - bid) : "–";
  $("spread").textContent = `spread ${spread}`;
  drawBook();
  drawTape();
  drawChart();
}

const depth = 8; // the prices shown on each side of the book

// drawBook draws the ladder: asks above, bids below, the best prices in the middle. Each row's
// bar is its quantity, next to the biggest one shown.
function drawBook() {
  const asks = levels(mirror.asks, (a, b) => a - b);
  const bids = levels(mirror.bids, (a, b) => b - a);
  const most = Math.max(1, ...[...asks, ...bids].map(([, qty]) => qty));
  const row = (side) => (l) =>
    l
      ? `<div class="row ${side}" style="--bar:${(100 * l[1]) / most}%">` +
        `<span>${price(l[0])}</span><span>${l[1]}</span></div>`
      : `<div class="row"></div>`;
  $("book").innerHTML =
    pad(asks).reverse().map(row("sell")).join("") +
    `<div class="mid"></div>` +
    pad(bids).map(row("buy")).join("");
}

// levels are a side's best prices, best first, as [price, qty].
function levels(side, best) {
  return [...side].sort((a, b) => best(a[0], b[0])).slice(0, depth);
}

const pad = (ls) => [...ls, ...Array(depth - ls.length).fill(null)];

// drawTape lists the latest trades, newest on top, in the colour of the side that crossed.
function drawTape() {
  $("tape").innerHTML = mirror.trades
    .slice(-20)
    .reverse()
    .map(
      (t) =>
        `<div class="row ${t.side}"><span>${t.time.slice(11, 19)}</span>` +
        `<span>${price(t.price)}</span><span>${t.qty}</span></div>`,
    )
    .join("");
}

const up = "#34d399";
const down = "#f87171";

// drawChart draws the candles. Each one is a thin line from its low to its high, and a body
// from its open to its close: green when the minute went up, red when it went down.
function drawChart() {
  const canvas = $("chart");
  const { width, height } = canvas.getBoundingClientRect();
  canvas.width = width * devicePixelRatio;
  canvas.height = height * devicePixelRatio;
  const ctx = canvas.getContext("2d");
  ctx.scale(devicePixelRatio, devicePixelRatio);
  const candles = mirror.candles.slice(-40);
  if (candles.length === 0) return;
  const high = Math.max(...candles.map((c) => c.high));
  const low = Math.min(...candles.map((c) => c.low));
  const y = (p) => 12 + ((high - p) / Math.max(1, high - low)) * (height - 24);
  const w = (width - 80) / Math.max(20, candles.length);
  candles.forEach((c, i) => {
    const x = i * w + w / 2;
    ctx.fillStyle = c.close >= c.open ? up : down;
    ctx.fillRect(x - 1, y(c.high), 2, y(c.low) - y(c.high) + 2);
    const top = y(Math.max(c.open, c.close));
    const bottom = y(Math.min(c.open, c.close));
    ctx.fillRect(x - w / 3, top, (2 * w) / 3, Math.max(2, bottom - top));
  });
  ctx.fillStyle = "#8b8f98";
  ctx.font = "13px ui-monospace, Menlo, monospace";
  ctx.fillText(price(high), width - 64, y(high) + 4);
  ctx.fillText(price(low), width - 64, y(low) + 4);
}

// price shows a price in cents the way FormatPrice does: digits and a point, never a float.
function price(p) {
  const d = mirror.decimals;
  const s = String(p).padStart(d + 1, "0");
  return `${s.slice(0, -d)}.${s.slice(-d)}`;
}

function badge(state, text) {
  $("badge").className = `badge ${state}`;
  $("badge").textContent = text;
}

// Once a second: how many updates came in, and how many frames were drawn for them.
setInterval(() => {
  const seq = mirror ? mirror.seq : "–";
  $("stats").textContent =
    `${symbol} · seq ${seq} · ${updates} updates/s · ` +
    `${frames} frames/s · ${gaps} gaps`;
  updates = frames = 0;
}, 1000);

for (const b of document.querySelectorAll("#symbols button")) {
  b.classList.toggle("on", b.textContent === symbol);
  b.onclick = () => {
    symbol = b.textContent;
    for (const o of b.parentNode.children) o.classList.toggle("on", o === b);
    ws.onclose = null;
    ws.close();
    retries = 0;
    connect();
  };
}

connect();
