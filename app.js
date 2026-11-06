/*
 * XDC Network Stats · XDC Outpost
 * https://github.com/s4njk4n/XDC_Stats
 *
 * Plain browser JavaScript, no dependencies. Three sources, each polled on its own:
 *   1. XinFin stats server  live websocket {stream}/primus (mainnet, the same feed xinfin.network uses)
 *                           or GET {stats}/v2/snapshot (Apothem's newer backend)
 *   2. XDC JSON-RPC         new blocks, masternodes, gas price (with endpoint failover)
 *   3. CoinGecko            price + 24h sparkline (mainnet only)
 *
 * Overrides for testing: ?net=apothem  ?stats=https://host  ?rpc=https://host  ?stream=wss://host/primus
 */
(function () {
  'use strict';

  /* ================================================================
     Config
     ================================================================ */
  var NETS = {
    mainnet: {
      // Mainnet still runs the classic Node.js ethstats server: live data over a Primus websocket
      stream: ['wss://stats1.xinfin.network/primus', 'wss://xinfin.network/primus', 'wss://www.xinfin.network/primus'],
      // REST snapshot (newer Go backend), used only if the websocket can't be reached
      stats: ['https://stats1.xinfin.network', 'https://stats.xinfin.network'],
      rpc: ['https://rpc.xdc.org', 'https://erpc.xinfin.network', 'https://rpc.ankr.com/xdc', 'https://rpc.xinfin.network'],
      explorer: 'https://xdcscan.com',
      price: true
    },
    apothem: {
      stats: ['https://api.apothem.network', 'https://stats.apothem.network'],
      rpc: ['https://rpc.apothem.network', 'https://erpc.apothem.network'],
      explorer: 'https://testnet.xdcscan.com',
      price: false
    }
  };
  var EPOCH = 900;             // blocks per XDPoS epoch
  var LAG_BLOCKS = 30;         // a node this far behind the best block counts as "behind"
  var FEED_LEN = 8;            // blocks kept in the live feed
  var ROW_LIMIT = 120;         // rows drawn before "Show all"
  var COINGECKO_ID = 'xdce-crowd-sale';
  var VALIDATOR = '0x0000000000000000000000000000000000000088';

  /* ================================================================
     Small helpers
     ================================================================ */
  var $ = function (id) { return document.getElementById(id); };
  var qs = new URLSearchParams(location.search);
  var nf0 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
  var nf1 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1, minimumFractionDigits: 1 });
  var nf2 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2, minimumFractionDigits: 2 });
  var compact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 });
  var fmt = function (n) { return nf0.format(n); };
  var esc = function (s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  };
  var num = function (v) { var n = typeof v === 'string' && v.slice(0, 2) === '0x' ? parseInt(v, 16) : Number(v); return isFinite(n) ? n : 0; };
  var mean = function (a) { return a.length ? a.reduce(function (s, v) { return s + v; }, 0) / a.length : 0; };
  var median = function (a) { if (!a.length) return 0; var b = a.slice().sort(function (x, y) { return x - y; }); var m = b.length >> 1; return b.length % 2 ? b[m] : (b[m - 1] + b[m]) / 2; };
  var store = {
    get: function (k) { try { var v = localStorage.getItem(k); return v ? JSON.parse(v) : null; } catch (e) { return null; } },
    set: function (k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) { /* full or blocked */ } }
  };
  function ago(ms) {
    var s = Math.max(0, ms / 1000);
    if (s < 60) return Math.round(s) + ' s';
    if (s < 3600) return Math.round(s / 60) + ' min';
    if (s < 86400) return Math.round(s / 3600) + ' h';
    return Math.round(s / 86400) + ' d';
  }
  function dur(sec) {
    if (!isFinite(sec) || sec <= 0) return '–';
    if (sec < 90) return Math.round(sec) + ' s';
    var m = Math.round(sec / 60);
    return m < 60 ? m + ' min' : Math.floor(m / 60) + ' h ' + (m % 60) + ' min';
  }
  function setText(id, t) { var el = $(id); if (el && el.textContent !== t) el.textContent = t; }
  function setHTML(id, h) { var el = $(id); if (el) el.innerHTML = h; }
  function short(addr) { return addr ? addr.slice(0, 6) + '…' + addr.slice(-4) : ''; }
  function xdcAddr(a) { return a && /^0x/i.test(a) ? 'xdc' + a.slice(2) : a; }

  function fetchJSON(url, opts, timeout) {
    var ctl = new AbortController();
    var t = setTimeout(function () { ctl.abort(); }, timeout || 9000);
    opts = opts || {};
    opts.signal = ctl.signal;
    return fetch(url, opts).then(function (r) {
      clearTimeout(t);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    }, function (e) { clearTimeout(t); throw e; });
  }

  /* ================================================================
     Network selection + state
     ================================================================ */
  var netName = qs.get('net') === 'apothem' ? 'apothem' : (qs.get('net') === 'mainnet' ? 'mainnet' : (store.get('xdcstats-net') || 'mainnet'));
  if (!NETS[netName]) netName = 'mainnet';
  var NET, S, gen = 0;

  function cfg(name) {
    var c = NETS[name], o = {};
    for (var k in c) o[k] = c[k];
    if (qs.get('stats')) o.stats = [qs.get('stats')].concat(c.stats);
    if (qs.get('rpc')) o.rpc = [qs.get('rpc')].concat(c.rpc);
    if (qs.get('stream') && c.stream) o.stream = [qs.get('stream')].concat(c.stream || []);
    return o;
  }

  function freshState() {
    return {
      nodes: [], rows: [], charts: null, snapAt: 0, fromCache: false, statsFail: 0, statsIdx: 0,
      streamUp: false, streamGaveUp: !NET.stream || !NET.stream.length,
      statsBest: 0, rpcHead: 0, rpcIdx: 0, rpcFail: 0, headSeenAt: 0, headTs: 0,
      blocks: [], mn: null, gasPrice: null, price: null, priceSeries: null,
      pins: new Set(store.get('xdcstats-pins-' + netName) || [])
    };
  }

  /* ================================================================
     Source 1: stats server snapshot
     ================================================================ */
  function pollStats() {
    if (!S.streamGaveUp) return Promise.resolve();     // the websocket is (or may be) delivering
    var g = gen, list = NET.stats, i = S.statsIdx;
    function attempt(k) {
      var base = list[(i + k) % list.length];
      return fetchJSON(base + '/v2/snapshot', { cache: 'no-store' }, 12000).then(function (d) {
        if (!d || !Array.isArray(d.nodes)) throw new Error('bad snapshot');
        S.statsIdx = (i + k) % list.length;
        setText('src-stats', base.replace(/^https?:\/\//, ''));
        return d;
      }, function (e) {
        if (k + 1 < list.length) return attempt(k + 1);
        throw e;
      });
    }
    return attempt(0).then(function (d) {
      if (g !== gen) return;
      S.statsFail = 0;
      applySnapshot(d.nodes, d.charts, Date.now(), false);
      store.set('xdcstats-cache-' + netName, { at: S.snapAt, nodes: d.nodes, charts: d.charts });
    }, function (e) {
      if (g !== gen) return;
      S.statsFail++;
      renderStatus(); renderTiles();
      if (!S.rows.length) $('tbody').innerHTML = '<tr><td colspan="11" class="empty" style="padding:1.5rem;text-align:center">The stats server isn’t responding. Retrying…</td></tr>';
      throw e;
    });
  }

  function parseClient(s) {
    // e.g. "XDC/v2.6.0-stable-a1b2c3d4/linux-amd64/go1.21.3"
    var parts = String(s || '').split('/');
    var ver = (parts[1] || '').replace(/-[0-9a-f]{7,}$/i, '') || 'Unknown';
    var go = (String(s).match(/go\d+(\.\d+)?/) || [''])[0];
    return { ver: ver, go: go || 'Unknown', plat: parts[2] || '' };
  }

  function applySnapshot(nodes, charts, at, fromCache) {
    S.snapAt = at; S.fromCache = fromCache; S.charts = charts || S.charts;
    var best = 0, i, n;
    for (i = 0; i < nodes.length; i++) {
      n = nodes[i];
      var b = n.stats && n.stats.block ? num(n.stats.block.number) : 0;
      if (n.stats && n.stats.active && b > best) best = b;
    }
    S.statsBest = best;
    S.nodes = nodes;
    S.rows = nodes.map(function (n) {
      var st = n.stats || {}, info = n.info || {}, blk = st.block || {}, geo = n.geo || null;
      var c = parseClient(info.node);
      var bn = num(blk.number);
      var status = !st.active ? 'off' : (best - bn > LAG_BLOCKS ? 'lag' : 'ok');
      var os = info.os ? (info.os + (info.os_v ? ' ' + info.os_v : '')) : (c.plat || 'Unknown');
      var loc = geo && (geo.city || geo.country) ? [geo.city, geo.country].filter(Boolean).join(', ') : '';
      return {
        id: n.id, name: info.name || n.id || 'Unnamed', ver: c.ver, go: c.go, os: os.trim() || 'Unknown',
        block: bn, behind: best && bn ? best - bn : 0, prop: num(blk.propagation),
        lat: num(st.latency), peers: num(st.peers), pend: num(st.pending), up: num(st.uptime),
        hist: Array.isArray(n.history) ? n.history : [], loc: loc, country: geo && geo.country || '',
        ll: geo && Array.isArray(geo.ll) && (geo.ll[0] || geo.ll[1]) ? geo.ll : null,
        coinbase: info.coinbase || '', status: status,
        search: (info.name + ' ' + c.ver + ' ' + loc + ' ' + (info.coinbase || '') + ' ' + xdcAddr(info.coinbase || '') + ' ' + os).toLowerCase()
      };
    });
    renderAll();
  }

  /* ================================================================
     Source 1b: live stream from the classic ethstats server (Primus websocket)
     Wire format (Primus 6, JSON parser): every frame is JSON.
       client -> {"emit":["ready"]}, then "primus::ping::<ms>" every 25 s
       server -> {"emit":["init",{"nodes":[...]}]} then {"action":..., "data":...}
     ================================================================ */
  var WS = { sock: null, idx: 0, map: null, charts: null, ping: 0, flushT: 0, retryT: 0, hideT: 0, tries: 0, savedAt: 0, host: '' };

  function streamStop() {
    clearInterval(WS.ping); clearTimeout(WS.flushT); clearTimeout(WS.retryT); clearTimeout(WS.hideT);
    WS.flushT = 0;
    (WS.others || []).forEach(function (o) { o.onclose = o.onerror = o.onmessage = null; try { o.close(); } catch (e) {} });
    WS.others = []; WS.sock = null;
    if (S) S.streamUp = false;
  }

  function streamStart() {
    streamStop();
    WS.map = null; WS.charts = null; WS.idx = 0; WS.tries = 0;
    if (NET.stream && NET.stream.length) streamConnect();
  }

  function streamConnect() {
    if (document.hidden || WS.sock) return;          // reconnect happens on visibilitychange
    var g = gen, list = NET.stream, pending = list.length, winner = null;
    WS.sock = { racing: true };                       // placeholder so we don't start a second race

    function lost() {
      // Called when a candidate fails before it delivered anything
      if (--pending > 0 || winner) return;
      if (g !== gen) return;
      WS.sock = null; WS.tries++;
      if (!S.streamGaveUp) {
        // No websocket host answered: let the REST snapshot have a go meanwhile
        S.streamGaveUp = true; S.statsFail++;
        loops[0] && loops[0].run();
        renderStatus(); renderTiles();
      }
      WS.retryT = setTimeout(streamConnect, Math.min(60000, 2000 * Math.pow(2, Math.min(5, WS.tries))));
    }
    function dropped() {
      // The live connection ended after working: reconnect quickly
      if (g !== gen) return;
      clearInterval(WS.ping);
      WS.sock = null; S.streamUp = false;
      WS.retryT = setTimeout(streamConnect, 1500);
    }

    list.forEach(function (url) {
      var ws, done = false, got = false;
      function send(obj) { try { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); } catch (e) {} }
      function end() {
        if (done) return; done = true; clearTimeout(guard);
        if (got) dropped(); else lost();
      }
      try { ws = new WebSocket(url + (url.indexOf('?') < 0 ? '?' : '&') + '_primuscb=' + Date.now().toString(36)); }
      catch (e) { done = true; lost(); return; }
      var guard = setTimeout(function () { if (!got) { try { ws.close(); } catch (e) {} end(); } }, 8000);

      ws.onopen = function () { send({ emit: ['ready'] }); };
      ws.onerror = end;
      ws.onclose = end;
      ws.onmessage = function (e) {
        if (g !== gen || (winner && winner !== ws)) return;
        var m;
        try { m = JSON.parse(e.data); } catch (x) { return; }
        if (typeof m === 'string') {
          if (m.indexOf('primus::ping::') === 0) send('primus::pong::' + m.slice(14));
          else if (m === 'primus::server::close') { try { ws.close(); } catch (x) {} }
          return;
        }
        if (!m || typeof m !== 'object') return;
        var initNodes = null;
        if (Array.isArray(m.emit)) { if (m.emit[0] === 'init') initNodes = m.emit[1] && m.emit[1].nodes; else return; }
        else if (m.action === 'init') initNodes = Array.isArray(m.data) ? m.data : m.data && m.data.nodes;
        if (initNodes) {
          if (!Array.isArray(initNodes)) return;
          if (!winner) {
            // First host to deliver wins; close the rest
            winner = ws; got = true; clearTimeout(guard);
            WS.others && WS.others.forEach(function (o) { if (o !== ws) { o.onclose = o.onerror = o.onmessage = null; try { o.close(); } catch (x) {} } });
            WS.sock = ws; WS.tries = 0;
            WS.host = url.replace(/^wss?:\/\//, '').replace(/\/primus.*$/, '');
            setText('src-stats', WS.host);
            clearInterval(WS.ping);
            WS.ping = setInterval(function () { send('primus::ping::' + Date.now()); }, 25000);
          }
          return streamAction('init', initNodes);
        }
        if (m.action === 'client-ping' && m.data) { send({ emit: ['client-pong', { serverTime: m.data.serverTime, clientTime: Date.now() }] }); return; }
        if (WS.map) streamAction(m.action, m.data);
      };
      (WS.others = WS.others || []).push(ws);
    });
    WS.others = WS.others ? WS.others.slice(-list.length) : [];

    function streamAction(action, d) {
      if (action === 'init') {
        WS.map = new Map();
        d.forEach(function (n) { if (n && n.id != null) WS.map.set(n.id, n); });
        S.streamUp = true; S.statsFail = 0;
        if (S.streamGaveUp) S.streamGaveUp = false;   // stream is (back) up: stop REST polling
        return flush(true);
      }
      if (action === 'charts') { if (d) { WS.charts = d; dirty(); } return; }
      if (action === 'blockPropagationChart') { if (d) { WS.charts = WS.charts || {}; WS.charts.propagation = d; dirty(); } return; }
      if (!d || d.id == null) return;
      var n = WS.map.get(d.id);
      if (action === 'add') {
        WS.map.set(d.id, n ? Object.assign(n, { info: d.info || n.info, geo: d.geo || n.geo, stats: d.stats || n.stats, history: d.history || n.history }) : d);
        return dirty();
      }
      if (!n) return;
      n.stats = n.stats || {};
      switch (action) {
        case 'update': if (d.stats) n.stats = Object.assign(n.stats, d.stats); if (d.history) n.history = d.history; break;
        case 'block': if (d.block) n.stats.block = d.block; if (d.history) n.history = d.history; break;
        case 'pending': if (d.pending != null) n.stats.pending = d.pending; break;
        case 'stats':
          if (d.stats) ['active', 'peers', 'gasPrice', 'uptime', 'latency', 'mining', 'syncing'].forEach(function (k) { if (d.stats[k] !== undefined) n.stats[k] = d.stats[k]; });
          break;
        case 'info': if (d.info) n.info = d.info; break;
        case 'inactive': if (d.stats) n.stats = Object.assign(n.stats, d.stats); else n.stats.active = false; break;
        case 'latency': if (d.latency != null) n.stats.latency = d.latency; break;
        default: return;
      }
      dirty();
    }
  }

  // Coalesce the stream (dozens of messages a second) into at most one render a second
  function dirty() { if (!WS.flushT) WS.flushT = setTimeout(function () { flush(false); }, 1000); }
  function flush(now) {
    clearTimeout(WS.flushT); WS.flushT = 0;
    if (!WS.map) return;
    var nodes = Array.from(WS.map.values());
    applySnapshot(nodes, WS.charts, Date.now(), false);
    // Save for an instant first paint next visit (not on every message: it's ~100+ KB)
    if (now || Date.now() - WS.savedAt > 30000) {
      WS.savedAt = Date.now();
      store.set('xdcstats-cache-' + netName, { at: Date.now(), nodes: nodes, charts: WS.charts });
    }
  }

  /* ================================================================
     Source 2: JSON-RPC (failover across endpoints)
     ================================================================ */
  var rpcId = 1;
  function rpcCall(body) {
    var list = NET.rpc, start = S.rpcIdx;
    function attempt(k) {
      var url = list[(start + k) % list.length];
      return fetchJSON(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }, 8000)
        .then(function (r) {
          // A batch answer is an array; a single error is an object with .error
          if (!Array.isArray(body) && r && r.error) {
            var err = new Error(r.error.message || 'rpc error'); err.rpc = true; throw err;
          }
          S.rpcIdx = (start + k) % list.length; S.rpcFail = 0;
          setText('src-rpc', url.replace(/^https?:\/\//, ''));
          return r;
        })
        .catch(function (e) {
          if (e.rpc) throw e;                         // method-level error: another endpoint won't help much
          if (k + 1 < list.length) return attempt(k + 1);
          S.rpcFail++;
          if (!S.blocks.length) $('blocks').innerHTML = '<li class="empty" style="display:block">Can’t reach XDC RPC right now. Retrying…</li>';
          if (!S.mn) { tileVal('t-mn', '–'); setText('t-mn-s', 'RPC not reachable'); }
          throw e;
        });
    }
    return attempt(0);
  }
  function rpc(method, params) {
    return rpcCall({ jsonrpc: '2.0', id: rpcId++, method: method, params: params || [] }).then(function (r) { return r.result; });
  }
  function rpcBatch(calls) {
    var body = calls.map(function (c) { return { jsonrpc: '2.0', id: rpcId++, method: c[0], params: c[1] || [] }; });
    return rpcCall(body).then(function (res) {
      if (!Array.isArray(res)) throw new Error('no batch');
      var byId = {};
      res.forEach(function (r) { byId[r.id] = r.result; });
      return body.map(function (b) { return byId[b.id]; });
    }).catch(function () {
      // Some endpoints refuse batches: fall back to parallel single calls
      return Promise.all(calls.map(function (c) { return rpc(c[0], c[1]).catch(function () { return null; }); }));
    });
  }

  function pollHead() {
    var g = gen;
    return rpc('eth_blockNumber').then(function (h) {
      if (g !== gen) return;
      h = num(h);
      if (!h || h <= S.rpcHead) { renderStatus(); return; }
      var from = Math.max(h - FEED_LEN + 1, S.rpcHead + 1);
      var calls = [];
      for (var b = h; b >= from; b--) calls.push(['eth_getBlockByNumber', ['0x' + b.toString(16), false]]);
      var first = !S.rpcHead;
      S.rpcHead = h; S.headSeenAt = Date.now();
      return rpcBatch(calls).then(function (blocks) {
        if (g !== gen) return;
        var fresh = {};
        blocks.forEach(function (bk) {
          if (!bk || !bk.number) return;
          var n = num(bk.number);
          fresh[n] = !first;
          S.blocks.push({
            n: n, ts: num(bk.timestamp), tx: (bk.transactions || []).length,
            gas: num(bk.gasUsed), lim: num(bk.gasLimit), hash: bk.hash, miner: bk.miner, size: num(bk.size)
          });
        });
        var seen = {};
        S.blocks = S.blocks.filter(function (x) { if (seen[x.n]) return false; seen[x.n] = 1; return true; })
          .sort(function (a, b) { return b.n - a.n; }).slice(0, FEED_LEN);
        if (S.blocks[0]) {
          S.headTs = S.blocks[0].ts;
          // First load: trust the block timestamp for "age"; after that, the moment we saw it
          if (first) S.headSeenAt = Math.min(Date.now(), S.blocks[0].ts * 1000);
        }
        renderFeed(fresh);
        renderHead();
      });
    });
  }

  function pollChain() {
    var g = gen;
    return Promise.all([
      rpc('XDPoS_getMasternodesByNumber', ['latest']).catch(function () { return null; }),
      rpc('eth_gasPrice').catch(function () { return null; })
    ]).then(function (r) {
      if (g !== gen) return;
      if (r[1]) S.gasPrice = num(r[1]);
      var m = r[0];
      if (m && (m.Masternodes || m.MasternodesLen != null)) {
        var len = function (arr, n) { return Array.isArray(arr) ? arr.length : num(n); };
        S.mn = {
          src: 'xdpos',
          masternodes: len(m.Masternodes, m.MasternodesLen),
          standby: len(m.Standbynodes, m.StandbynodesLen),
          penalty: len(m.Penalty, m.PenaltyLen)
        };
        renderTiles(); renderHead();
        return;
      }
      // Fallback: count candidates in the validator contract, getCandidates()
      return rpc('eth_call', [{ to: VALIDATOR, data: '0x06a49fce' }, 'latest']).then(function (hex) {
        if (g !== gen || !hex || hex.length < 130) return;
        var count = parseInt(hex.slice(66, 130), 16), live = 0;
        for (var i = 0; i < count; i++) {
          var word = hex.slice(130 + i * 64, 194 + i * 64);
          if (/[1-9a-f]/i.test(word)) live++;
        }
        S.mn = { src: 'candidates', candidates: live };
        renderTiles(); renderHead();
      }).catch(function () {});
    });
  }

  /* ================================================================
     Source 3: price (CoinGecko)
     ================================================================ */
  function pollPrice() {
    if (!NET.price) return Promise.resolve();
    var g = gen;
    return fetchJSON('https://api.coingecko.com/api/v3/simple/price?ids=' + COINGECKO_ID +
      '&vs_currencies=usd&include_24hr_change=true&include_market_cap=true&include_24hr_vol=true')
      .then(function (d) {
        if (g !== gen || !d[COINGECKO_ID]) return;
        var p = d[COINGECKO_ID];
        S.price = { usd: p.usd, ch: p.usd_24h_change, mc: p.usd_market_cap, vol: p.usd_24h_vol, at: Date.now() };
        store.set('xdcstats-price', S.price);
        renderPrice();
      });
  }
  function pollSpark() {
    if (!NET.price) return Promise.resolve();
    var g = gen;
    return fetchJSON('https://api.coingecko.com/api/v3/coins/' + COINGECKO_ID + '/market_chart?vs_currency=usd&days=1')
      .then(function (d) {
        if (g !== gen || !d || !Array.isArray(d.prices)) return;
        // Downsample to ~60 points; plenty for a sparkline
        var p = d.prices, step = Math.max(1, Math.floor(p.length / 60)), out = [];
        for (var i = 0; i < p.length; i += step) out.push(p[i][1]);
        out.push(p[p.length - 1][1]);
        S.priceSeries = out;
        store.set('xdcstats-spark', out);
        renderPrice();
      });
  }

  /* ================================================================
     Scheduler: pause when hidden, back off on failure
     ================================================================ */
  var loops = [];
  function every(fn, ms) {
    var L = { fn: fn, ms: ms, timer: 0, fails: 0, busy: false };
    L.run = function () {
      clearTimeout(L.timer);
      if (document.hidden || L.busy) return;
      var g = gen;
      L.busy = true;
      Promise.resolve().then(fn).then(function () { L.fails = 0; }, function () { L.fails++; })
        .then(function () {
          L.busy = false;
          if (g !== gen) return;
          L.timer = setTimeout(L.run, ms * Math.min(6, Math.pow(2, L.fails)));
        });
    };
    loops.push(L);
    return L;
  }
  every(pollStats, 5000);
  every(pollHead, 2000);
  every(pollChain, 60000);
  every(pollPrice, 60000);
  every(pollSpark, 600000);

  document.addEventListener('visibilitychange', function () {
    if (!document.hidden) {
      loops.forEach(function (L) { L.run(); });
      clearTimeout(WS.hideT);
      if (NET && NET.stream && NET.stream.length && !WS.sock) { clearTimeout(WS.retryT); streamConnect(); }
    } else if (WS.sock) {
      // Don't hold a busy stream open for a background tab
      WS.hideT = setTimeout(function () { streamStop(); }, 60000);
    }
  });

  /* ================================================================
     Rendering: head + console
     ================================================================ */
  var lastBest = 0;
  function bestBlock() { return Math.max(S.statsBest, S.rpcHead); }

  function avgBlockTime() {
    var bt = S.charts && S.charts.blocktime;
    if (bt && bt.length > 3) return mean(bt.map(num).filter(function (v) { return v > 0 && v < 60; }));
    if (S.blocks.length > 2) { var a = S.blocks[0], z = S.blocks[S.blocks.length - 1]; return (a.ts - z.ts) / (a.n - z.n); }
    return 0;
  }
  function tps() {
    if (S.charts && S.charts.avgTransactionRate) return num(S.charts.avgTransactionRate);
    if (S.charts && S.charts.transactions && S.charts.blocktime && S.charts.blocktime.length > 3) {
      var tsum = S.charts.transactions.reduce(function (a, v) { return a + num(v); }, 0);
      var bsum = S.charts.blocktime.reduce(function (a, v) { return a + num(v); }, 0);
      if (bsum > 0) return tsum / bsum;
    }
    if (S.blocks.length > 2) {
      var a = S.blocks[0], z = S.blocks[S.blocks.length - 1], tx = 0;
      for (var i = 0; i < S.blocks.length - 1; i++) tx += S.blocks[i].tx;
      return a.ts > z.ts ? tx / (a.ts - z.ts) : 0;
    }
    return null;
  }

  function renderHead() {
    var best = bestBlock();
    if (best) {
      var el = $('best-n');
      el.classList.remove('skel');
      el.textContent = fmt(best);
      if (lastBest && best !== lastBest) {
        var wrap = $('best'); wrap.classList.remove('tick'); void wrap.offsetWidth; wrap.classList.add('tick');
      }
      lastBest = best;

      var into = best % EPOCH, left = EPOCH - into, next = best + left;
      var bt = avgBlockTime() || 2;
      setText('epoch-label', 'Epoch ' + nf0.format(Math.floor(best / EPOCH)) + ' · block ' + into + ' of ' + EPOCH);
      setText('epoch-eta', 'Next #' + fmt(next) + ' in ~' + dur(left * bt));
      $('epoch-fill').style.width = (into / EPOCH * 100).toFixed(2) + '%';
      $('epoch-meter').setAttribute('aria-valuenow', into);
    }
    var bt2 = avgBlockTime();
    setText('c-bt', bt2 ? nf2.format(bt2) + ' s' : '–');
    var t = tps();
    setText('c-tps', t == null ? '–' : (t < 10 ? nf2.format(t) : nf1.format(t)));
    var gp = S.gasPrice;
    if (gp == null) {
      var bn = S.nodes.find && S.nodes.find(function (n) { return n.stats && n.stats.active && n.stats.gasPrice; });
      if (bn) gp = num(bn.stats.gasPrice);
    }
    setText('c-gas', gp ? (gp / 1e9 < 1 ? (gp / 1e9).toPrecision(2) : nf2.format(gp / 1e9)) + ' gwei' : '–');

    // Heartbeat: one bar per recent block interval
    var series = (S.charts && S.charts.blocktime || []).map(num).slice(-28);
    if (series.length < 4 && S.blocks.length > 2) {
      series = [];
      for (var i = S.blocks.length - 1; i > 0; i--) series.push(S.blocks[i - 1].ts - S.blocks[i].ts);
    }
    var beat = $('beat');
    if (series.length) {
      var mx = Math.max(4, Math.max.apply(null, series));
      if (beat.children.length !== series.length) beat.innerHTML = series.map(function () { return '<i></i>'; }).join('');
      for (var j = 0; j < series.length; j++) {
        var bar = beat.children[j];
        bar.style.height = Math.max(10, series[j] / mx * 100) + '%';
        bar.className = series[j] > bt2 * 1.6 && series[j] > 3 ? 'slow' : '';
        bar.title = nf2.format(series[j]) + ' s';
      }
    }
  }

  function renderAge() {
    var el = $('age');
    if (!S.headSeenAt && S.charts && S.nodes.length) {
      // No RPC: fall back to the stats server's best block arrival time
      var arr = 0;
      S.nodes.forEach(function (n) { if (n.stats && n.stats.block && num(n.stats.block.number) === S.statsBest) arr = Math.max(arr, num(n.stats.block.arrived)); });
      if (arr) { var d0 = Date.now() - arr; el.textContent = (d0 / 1000).toFixed(1) + ' s ago'; return; }
    }
    if (!S.headSeenAt) { el.textContent = '–'; return; }
    var d = Math.max(0, Date.now() - S.headSeenAt);
    el.textContent = d < 60000 ? (d / 1000).toFixed(1) + ' s ago' : ago(d) + ' ago';
    if (d > 10000) el.setAttribute('data-slow', ''); else el.removeAttribute('data-slow');
  }

  function renderStatus() {
    var el = $('status'), state, text;
    var age = S.snapAt ? Date.now() - S.snapAt : Infinity;
    if (S.streamUp && S.snapAt && !S.fromCache && age < 30000) { state = 'live'; text = 'Live · streaming' + (age > 5000 ? ' · last update ' + ago(age) + ' ago' : ''); }
    else if (S.snapAt && !S.fromCache && age < 20000) { state = 'live'; text = 'Live · updated ' + ago(age) + ' ago'; }
    else if (S.statsFail >= 1 && S.rpcHead && Date.now() - S.headSeenAt < 30000) { state = 'stale'; text = 'Stats server not responding · blocks still live from RPC'; }
    else if (S.statsFail >= 1 && S.rpcFail >= 1) { state = 'down'; text = 'Can’t reach the data sources, retrying'; }
    else if (S.fromCache) { state = 'stale'; text = 'Showing saved data from ' + ago(age) + ' ago, refreshing…'; }
    else if (S.snapAt) { state = 'stale'; text = 'Last update ' + ago(age) + ' ago'; }
    else if (S.statsFail >= 1) { state = 'down'; text = 'Stats server not responding, retrying'; }
    else { state = 'idle'; text = 'Connecting…'; }
    if (el.getAttribute('data-state') !== state) el.setAttribute('data-state', state);
    setText('status-text', text);
  }

  /* ================================================================
     Rendering: tiles
     ================================================================ */
  function tileVal(id, html) { var el = $(id); el.innerHTML = html; }

  function renderTiles() {
    var rows = S.rows;
    if (rows.length) {
      var ok = 0, lag = 0, off = 0, lats = [], countries = {};
      rows.forEach(function (r) {
        if (r.status === 'ok') ok++; else if (r.status === 'lag') lag++; else off++;
        if (r.status !== 'off' && r.lat > 0) lats.push(r.lat);
        if (r.country) countries[r.country] = (countries[r.country] || 0) + 1;
      });
      var total = rows.length;
      tileVal('t-nodes', fmt(ok) + '<small>/ ' + fmt(total) + '</small>');
      $('t-nodes-bar').innerHTML =
        '<i style="width:' + (ok / total * 100) + '%;background:var(--ok)"></i>' +
        '<i style="width:' + (lag / total * 100) + '%;background:var(--amber)"></i>' +
        '<i style="width:' + (off / total * 100) + '%;background:#3A4066"></i>';
      setText('t-nodes-s', fmt(lag) + ' behind · ' + fmt(off) + ' offline');

      var avgLat = mean(lats);
      tileVal('t-lat', lats.length ? fmt(avgLat) + '<small>ms</small>' : '–');
      setText('t-lat-s', lats.length ? 'Median ' + fmt(median(lats)) + ' ms to the stats server' : 'No latency reported');

      var cs = Object.keys(countries).sort(function (a, b) { return countries[b] - countries[a]; });
      tileVal('t-geo', fmt(cs.length));
      setText('t-geo-s', cs.length ? 'Most in ' + cs[0] + ' (' + countries[cs[0]] + ')' : 'No locations reported');
    }
    else if (S.statsFail) {
      ['t-nodes', 't-lat', 't-geo', 't-prop'].forEach(function (id) { tileVal(id, '–'); });
      ['t-nodes-s', 't-lat-s', 't-geo-s', 't-prop-s'].forEach(function (id) { setText(id, 'Stats server not responding'); });
    }
    if (S.charts && S.charts.propagation) {
      var avg = num(S.charts.propagation.avg);
      var hist = S.charts.propagation.histogram || [];
      var under = 0, totalF = 0;
      hist.forEach(function (b) { totalF += num(b.frequency); if (num(b.x) + num(b.dx) <= 1000) under += num(b.frequency); });
      tileVal('t-prop', avg < 1000 ? fmt(avg) + '<small>ms</small>' : nf2.format(avg / 1000) + '<small>s</small>');
      setText('t-prop-s', totalF ? Math.round(under / totalF * 100) + '% of nodes get blocks within 1 s' : 'Average across reporting nodes');
    }
    if (S.mn) {
      if (S.mn.src === 'xdpos') {
        tileVal('t-mn', fmt(S.mn.masternodes) + '<small>active</small>');
        setText('t-mn-s', fmt(S.mn.standby) + ' standby · ' + fmt(S.mn.penalty) + ' penalised');
      } else {
        tileVal('t-mn', fmt(S.mn.candidates) + '<small>candidates</small>');
        setText('t-mn-s', 'Masternode and standby candidates');
      }
    } else if (S.rpcFail >= 2) {
      tileVal('t-mn', '–'); setText('t-mn-s', 'RPC not reachable');
    }
  }

  function renderPrice() {
    var tile = $('tile-price');
    tile.hidden = !NET.price;
    if (!NET.price) return;
    var p = S.price;
    if (p && p.usd) {
      var d = p.usd < 1 ? 4 : 2;
      tileVal('t-price', '$' + p.usd.toFixed(d));
      var ch = num(p.ch);
      $('t-price-s').innerHTML = '<span class="' + (ch >= 0 ? 'up' : 'down') + '">' + (ch >= 0 ? '+' : '') + nf2.format(ch) + '%</span> 24h · MC $' + compact.format(p.mc || 0);
    }
    if (S.priceSeries) {
      var up = S.priceSeries[S.priceSeries.length - 1] >= S.priceSeries[0];
      drawSpark($('price-spark'), S.priceSeries, up ? '126,235,192' : '255,143,180');
    }
  }

  /* ================================================================
     Canvas helpers
     ================================================================ */
  function fitCanvas(cv) {
    var dpr = Math.min(window.devicePixelRatio || 1, 2);
    var r = cv.getBoundingClientRect();
    var w = Math.max(1, Math.round(r.width)), h = Math.max(1, Math.round(r.height));
    if (cv.width !== w * dpr || cv.height !== h * dpr) { cv.width = w * dpr; cv.height = h * dpr; }
    var ctx = cv.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { ctx: ctx, w: w, h: h };
  }

  function drawSpark(cv, data, rgb) {
    var f = fitCanvas(cv), ctx = f.ctx, w = f.w, h = f.h;
    ctx.clearRect(0, 0, w, h);
    if (data.length < 2) return;
    var mn = Math.min.apply(null, data), mx = Math.max.apply(null, data), span = mx - mn || 1;
    var X = function (i) { return i / (data.length - 1) * w; };
    var Y = function (v) { return h - 4 - (v - mn) / span * (h - 10); };
    var g = ctx.createLinearGradient(0, 0, 0, h);
    g.addColorStop(0, 'rgba(' + rgb + ',0.22)'); g.addColorStop(1, 'rgba(' + rgb + ',0)');
    ctx.beginPath(); ctx.moveTo(0, h);
    data.forEach(function (v, i) { ctx.lineTo(X(i), Y(v)); });
    ctx.lineTo(w, h); ctx.closePath(); ctx.fillStyle = g; ctx.fill();
    ctx.beginPath();
    data.forEach(function (v, i) { i ? ctx.lineTo(X(i), Y(v)) : ctx.moveTo(X(i), Y(v)); });
    ctx.strokeStyle = 'rgba(' + rgb + ',0.9)'; ctx.lineWidth = 1.5; ctx.stroke();
  }

  // Bar chart with hover. opts: { color, hot(v) -> bool, label(i,v) -> string }
  var charts = {};
  function barChart(id, data, opts) {
    var cv = $(id);
    charts[id] = { data: data, opts: opts };
    var f = fitCanvas(cv), ctx = f.ctx, w = f.w, h = f.h;
    ctx.clearRect(0, 0, w, h);
    if (!data.length) return;
    var mx = Math.max.apply(null, data.concat([opts.min || 0])) || 1;
    var gap = data.length > 30 ? 2 : 3, bw = (w - gap * (data.length - 1)) / data.length;
    var hover = opts.hover;
    // faint guide at the mean
    if (opts.guide != null) {
      var gy = h - opts.guide / mx * (h - 4);
      ctx.strokeStyle = 'rgba(158,164,191,0.25)'; ctx.setLineDash([3, 4]); ctx.beginPath(); ctx.moveTo(0, gy); ctx.lineTo(w, gy); ctx.stroke(); ctx.setLineDash([]);
    }
    for (var i = 0; i < data.length; i++) {
      var v = data[i], bh = Math.max(2, v / mx * (h - 4));
      var x = i * (bw + gap), y = h - bh;
      var hot = opts.hot && opts.hot(v);
      ctx.fillStyle = i === hover ? '#EEF0FA' : hot ? 'rgba(255,211,138,0.85)' : (i === data.length - 1 && opts.lastGlow ? opts.color : opts.color.replace('1)', '0.55)'));
      roundRect(ctx, x, y, Math.max(1, bw), bh, Math.min(3, bw / 2));
    }
  }
  function roundRect(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x, y + h); ctx.lineTo(x, y + r); ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.lineTo(x + w - r, y); ctx.quadraticCurveTo(x + w, y, x + w, y + r); ctx.lineTo(x + w, y + h);
    ctx.closePath(); ctx.fill();
  }
  // Shared tooltip for all bar charts
  var tip = $('tip');
  ['ch-bt', 'ch-tx', 'ch-gas', 'ch-prop'].forEach(function (id) {
    var cv = $(id);
    function move(e) {
      var c = charts[id]; if (!c || !c.data.length) return;
      var r = cv.getBoundingClientRect();
      var i = Math.min(c.data.length - 1, Math.max(0, Math.floor((e.clientX - r.left) / r.width * c.data.length)));
      if (c.opts.hover !== i) { c.opts.hover = i; barChart(id, c.data, c.opts); }
      tip.hidden = false;
      tip.textContent = c.opts.label(i, c.data[i]);
      tip.style.left = e.clientX + 'px'; tip.style.top = (r.top + 4) + 'px';
    }
    function leave() { var c = charts[id]; tip.hidden = true; if (c) { c.opts.hover = null; barChart(id, c.data, c.opts); } }
    cv.addEventListener('pointermove', move);
    cv.addEventListener('pointerdown', move);
    cv.addEventListener('pointerleave', leave);
  });

  function renderCharts() {
    var c = S.charts; if (!c) return;
    var bt = (c.blocktime || []).map(num), tx = (c.transactions || []).map(num), gas = (c.gasSpending || []).map(num);
    var n = bt.length;
    var agoLabel = function (i, len) { var k = len - 1 - i; return k === 0 ? 'Latest block' : k + ' block' + (k > 1 ? 's' : '') + ' ago'; };
    var avgBt = mean(bt.filter(function (v) { return v > 0; }));
    barChart('ch-bt', bt, {
      color: 'rgba(111,223,255,1)', lastGlow: true, min: 3, guide: avgBt,
      hot: function (v) { return v > Math.max(3, avgBt * 1.6); },
      label: function (i, v) { return agoLabel(i, n) + ': ' + nf2.format(v) + ' s'; }, hover: charts['ch-bt'] && charts['ch-bt'].opts.hover
    });
    setText('r-bt', 'avg ' + nf2.format(avgBt) + ' s');
    barChart('ch-tx', tx, {
      color: 'rgba(205,191,255,1)', lastGlow: true, min: 1, guide: mean(tx),
      label: function (i, v) { return agoLabel(i, tx.length) + ': ' + fmt(v) + ' tx'; }, hover: charts['ch-tx'] && charts['ch-tx'].opts.hover
    });
    setText('r-tx', 'avg ' + nf1.format(mean(tx)) + ' · max ' + fmt(Math.max.apply(null, tx.concat([0]))));
    barChart('ch-gas', gas, {
      color: 'rgba(255,143,180,1)', lastGlow: true, min: 1, guide: mean(gas),
      label: function (i, v) { return agoLabel(i, gas.length) + ': ' + fmt(v) + ' gas'; }, hover: charts['ch-gas'] && charts['ch-gas'].opts.hover
    });
    setText('r-gas', 'avg ' + compact.format(mean(gas)));

    var hist = (c.propagation && c.propagation.histogram) || [];
    var last = 0;
    hist.forEach(function (b, i) { if (num(b.frequency) > 0) last = i; });
    var bins = hist.slice(0, Math.max(8, last + 1));
    var freq = bins.map(function (b) { return num(b.frequency); });
    var total = freq.reduce(function (s, v) { return s + v; }, 0);
    barChart('ch-prop', freq, {
      color: 'rgba(126,235,192,1)', min: 1,
      label: function (i) {
        var b = bins[i];
        return (num(b.x) / 1000).toFixed(2) + '–' + ((num(b.x) + num(b.dx)) / 1000).toFixed(2) + ' s: ' + fmt(freq[i]) + ' reports (' + (total ? Math.round(freq[i] / total * 100) : 0) + '%)';
      }, hover: charts['ch-prop'] && charts['ch-prop'].opts.hover
    });
    setText('r-prop', 'avg ' + fmt(num(c.propagation && c.propagation.avg)) + ' ms');
    if (bins.length) {
      var lastBin = bins[bins.length - 1];
      setText('ax-prop-1', ((num(lastBin.x) + num(lastBin.dx)) / 1000).toFixed(1) + ' s');
    }
  }

  /* ================================================================
     Rendering: block feed
     ================================================================ */
  function renderFeed(fresh) {
    var ul = $('blocks');
    if (!S.blocks.length) return;
    var now = Date.now() / 1000;
    ul.innerHTML = S.blocks.map(function (b) {
      var pct = b.lim ? Math.round(b.gas / b.lim * 100) : 0;
      var age = Math.max(0, now - b.ts);
      return '<li' + (fresh && fresh[b.n] ? ' class="new"' : '') + '>' +
        '<span class="cube"><svg aria-hidden="true"><use href="#i-cube"/></svg></span>' +
        '<a href="' + NET.explorer + '/block/' + b.n + '" target="_blank" rel="noopener">#' + fmt(b.n) + '</a>' +
        '<span class="txs"><b>' + fmt(b.tx) + '</b>tx</span>' +
        '<span class="meta" data-ts="' + b.ts + '">' + (age < 90 ? Math.round(age) + ' s ago' : ago(age * 1000) + ' ago') +
        ' · gas ' + pct + '%' + (b.miner && !/^0x0+$/.test(b.miner) ? ' · ' + esc(short(xdcAddr(b.miner))) : '') + '</span>' +
        '</li>';
    }).join('');
  }
  function tickFeedAges() {
    var now = Date.now() / 1000;
    var metas = document.querySelectorAll('#blocks .meta[data-ts]');
    for (var i = 0; i < metas.length; i++) {
      var m = metas[i], age = Math.max(0, now - num(m.getAttribute('data-ts')));
      var t = m.textContent, cut = t.indexOf(' ago');
      var next = (age < 90 ? Math.round(age) + ' s' : ago(age * 1000)) + t.slice(cut);
      if (next !== t) m.textContent = next;
    }
  }

  /* ================================================================
     Rendering: world map (dot grid baked at build time)
     ================================================================ */
  var MAP = { cols: 240, rows: 93, step: 1.5, top: 82.5, bits: 'AAAAAAAAAB//+f///8YAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP9/n/////gAAASAAZYAAAeAAAAAAAAAAAAAAAAAGj/8f////8AAAPwAAAAAAAAcAAAAAAAAAAAAAADgAAOAP/////AAADAAAAAAABAMAAAAAAAAAAAAAAA9zhjAAH///+AAAAAAAAHgAAP/8AAvAAAAAAAAAAdAAAAAAD///+AAAAAAAAYAAD//gAAAAAAAAAAAAAf8xq3AAA///YAAAAAAABgDA///7/wCAAAAwAEAAAI/wMb/gAf//sAAAAAAAAgHb/////z/+AABAB//wPI/+sEn4Bv//wAAAA/8AADHf////////+C8wH/////gQ3mF4AP/8AAAAB//xjf/v///////////7A////////+B/gf/gAAAAH/94//9f///////////GH////////0P5Af4AH4AAHx+H///////////////AAf//////9NA+AP4ADAAA/3////////////////+AD///////8COMAHwAAAQD/P////////////////4AB/7/////4APwABwAAAAH/P//////////////wfAAA+AH////4APzAAAAAAAH/D////////////+RhgAAAFAB////8AH/gAAAAAIAOj////////////4AHgAAAQAAP////gX/gAAAAAMBmP////////////wAPgAACAAAX////+f/4AAAAAyAwf////////////AAPAAAAAAAD////+f/+AAAAA3D//////////////8AOAAAAAAAB/////f/8AAAABHn//////////////9AIAAAAAAAB//////8QAAAAAMf//////////////9AQAAAAAAAAf/////2HAAAAAB///////////////4AAAAAAAAAAf/////+AgAAAAB///////////////4AAAAAAAAAAf//////AAAAAAB///yfx/////////wAAAAAAAAAAf/////yAAAAAAB/z/gPj/////////jAAAAAAAAAAf/////gAAAAAA/wY/gDx////////8HAAAAAAAAAAf////+AAAAAAA/gGfnn4////////wAAAAAAAAAAAf////+AAAAAAA/AAZ//4///////1gGAAAAAAAAAAP////8AAAAAAA/ACM//4///////hwEAAAAAAAAAAH////4AAAAAAAILgA3/9///////4wcAAAAAAAAAAH////4AAAAAAAN/gDC/////////wz8AAAAAAAAAAB////gAAAAAAAf/gAA/////////wLgAAAAAAAAAAA////AAAAAAAA//8YB/////////4EAAAAAAAAAAAAX///AAAAAAAB///f//////////4AAAAAAAAAAAAAf/5hAAAAAAAB//////z///////4AAAAAAAAAAAAAb/gBAAAAAAAH////8/5///////wAAAAAAAAAAAAAF/gBgAAAAAAP////+/4P//////wAAAAAAAAAAAAAE/gAAAAAAAAP////+f8wH/////gAAAAAAAAAAAAAAfgAAAAAAAAf/////P/4D/////IAAAAAAAAAAAAAAPgAQAAAAAAf/////v/8D//f/4AAAAAAAAAAAAAAAPgwOAAAAAAf/////n/4Af8P+AAAAAAAAAAAAAAAAHxwAwAAAAAf/////n/wAfwH8wAAAAAAAAAAAAAAAB/gAAAAAAAf/////z/gAfgH+AMAAAAAAAAAAAAAAAT8AAAAAAAf/////z+AAfAB/AIAAAAAAAAAAAAAAAB+AAAAAAAf/////94AAOAB/gIAAAAAAAAAAAAAAAAMAAAAAAAf//////AAAOAA/gAAAAAAAAAAAAAAAAAEBQAAAAAf/////+MAAGAAHgHAAAAAAAAAAAAAAAACDfgAAAAH//////8AAHAACAAAAAAAAAAAAAAAAAABP/wAAAAH//////4AAFAAgADAAAAAAAAAAAAAAAAAP/4AAAAD//////4AABgAQABAAAAAAAAAAAAAAAAAP//gAAAA+D////wAAAAGYBwAAAAAAAAAAAAAAAAAH//wAAAAAA////wAAAADYDAAAAAAAAAAAAAAAAAAP//wAAAAAAf///AAAAABoPgQAAAAAAAAAAAAAAAAf//wAAAAAA///+AAAAAAwfuQAAAAAAAAAAAAAAAA///+AAAAAA///8AAAAAAYfASgAAAAAAAAAAAAAAA////AAAAAA///4AAAAAA8fYCYAAAAAAAAAAAAAAA////8AAAAAf//4AAAAAAODEh/AAAAAAAAAAAAAAA/////AAAAAP//4AAAAAAGAAAPgAAAAAAAAAAAAAAf////gAAAAP//wAAAAAADAABHwAAAAAAAAAAAAAAf////gAAAAH//wAAAAAAAOCAPYAAAAAAAAAAAAAAP////AAAAAH//wAAAAAAAACAAMAAAAAAAAAAAAAAP///+AAAAAH//4AAAAAAAAAEAAAAAAAAAAAAAAAAH///+AAAAAH//4AAAAAAAAAHhAAAAAAAAAAAAAAAH///8AAAAAP//4IAAAAAAAAvjAAAAAAAAAAAAAAAD///8AAAAAP//4cAAAAAAAB/jgAAAAAAAAAAAAAAA///8AAAAAP//h4AAAAAAAH/7gAAAAAAAAAAAAAAAf//8AAAAAP//B4AAAAAAAH//wAAAAAAAAAAAAAAAf//4AAAAAH/+AwIAAAAAAf//4AAAAAAAAAAAAAAAf//4AAAAAH//BwAAAAAAB///8AIAAAAAAAAAAAAAf//AAAAAAD//BwAAAAAAD///+AAAAAAAAAAAAAAAf/8AAAAAAD/+BgAAAAAAD////AAAAAAAAAAAAAAAf/8AAAAAAD/8AAAAAAAAH////AAAAAAAAAAAAAAAf/8AAAAAAD/8AAAAAAAAD////AAAAAAAAAAAAAAA//4AAAAAAB/4AAAAAAAAB////AAAAAAAAAAAAAAA//wAAAAAAA/wAAAAAAAAB////AAAAAAAAAAAAAAA//gAAAAAAA/gAAAAAAAAB/h//AAAAAAAAAAAAAAA//AAAAAAAA+AAAAAAAAAB8Av+AAAAAAAAAAAAAAA/8AAAAAAAAAAAAAAAAAAAAAP8AAQAAAAAAAAAAAB/8AAAAAAAAAAAAAAAAAAAAAH8AAIAAAAAAAAAAAB/4AAAAAAAAAAAAAAAAAAAAADQAAOAAAAAAAAAAAB/gAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAB+AAAAAAAAAAAAAAAAAAAAAAA4AAIAAAAAAAAAAAB/AAAAAAAAAAAAAAAAAAAAAAAYAAwAAAAAAAAAAAB8AAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAB4AAAAAAAAAAAAAAAAAAAAAAAAAHAAAAAAAAAAAAD8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABwIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' };
  var land = null, mapCv = $('worldmap'), mapLayer = null, mapPts = [], mapAnim = 0, mapVisible = true;
  function decodeMap() {
    var bin = atob(MAP.bits), out = [];
    for (var i = 0; i < MAP.cols * MAP.rows; i++) {
      if (bin.charCodeAt(i >> 3) & (0x80 >> (i & 7))) out.push([i % MAP.cols, (i / MAP.cols) | 0]);
    }
    return out;
  }
  function project(lat, lon, w, h) {
    return [(lon + 180) / 360 * w, (MAP.top - lat) / (MAP.rows * MAP.step) * h];
  }
  function buildMapLayer() {
    if (!land) land = decodeMap();
    var f = fitCanvas(mapCv), w = f.w, h = f.h;
    var dpr = Math.min(window.devicePixelRatio || 1, 2);
    mapLayer = document.createElement('canvas');
    mapLayer.width = w * dpr; mapLayer.height = h * dpr;
    var c = mapLayer.getContext('2d');
    c.setTransform(dpr, 0, 0, dpr, 0, 0);
    var cw = w / MAP.cols, ch = h / MAP.rows, r = Math.max(0.6, Math.min(cw, ch) * 0.3);
    c.fillStyle = 'rgba(150,165,230,0.26)';
    c.beginPath();
    for (var i = 0; i < land.length; i++) {
      var x = (land[i][0] + 0.5) * cw, y = (land[i][1] + 0.5) * ch;
      c.moveTo(x + r, y); c.arc(x, y, r, 0, 6.2832);
    }
    c.fill();
  }
  function clusterNodes() {
    var w = mapCv.getBoundingClientRect().width, h = mapCv.getBoundingClientRect().height, by = {};
    S.rows.forEach(function (r) {
      if (!r.ll) return;
      var p = project(r.ll[0], r.ll[1], w, h);
      var key = Math.round(p[0] / 6) + ':' + Math.round(p[1] / 6);
      var c = by[key] || (by[key] = { x: p[0], y: p[1], n: 0, ok: 0, lag: 0, off: 0, places: {} });
      c.n++; c[r.status]++;
      if (r.loc) c.places[r.loc] = (c.places[r.loc] || 0) + 1;
    });
    mapPts = Object.keys(by).map(function (k) { return by[k]; }).sort(function (a, b) { return a.n - b.n; });
  }
  function drawMap(t) {
    var f = fitCanvas(mapCv), ctx = f.ctx, w = f.w, h = f.h;
    ctx.clearRect(0, 0, w, h);
    if (mapLayer) ctx.drawImage(mapLayer, 0, 0, w, h);
    var pulse = (Math.sin((t || 0) / 700) + 1) / 2;
    for (var i = 0; i < mapPts.length; i++) {
      var p = mapPts[i];
      var col = p.ok ? '126,235,192' : p.lag ? '255,211,138' : '90,96,128';
      var r = Math.min(9, 2.2 + Math.sqrt(p.n) * 1.3);
      if (p.ok || p.lag) {
        var halo = r * (2.2 + pulse * 1.2);
        var g = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, halo);
        g.addColorStop(0, 'rgba(' + col + ',0.35)'); g.addColorStop(1, 'rgba(' + col + ',0)');
        ctx.fillStyle = g; ctx.beginPath(); ctx.arc(p.x, p.y, halo, 0, 6.2832); ctx.fill();
      }
      ctx.fillStyle = 'rgba(' + col + ',0.95)';
      ctx.beginPath(); ctx.arc(p.x, p.y, r, 0, 6.2832); ctx.fill();
    }
  }
  var reduceMotion = window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches;
  function mapLoop(t) {
    drawMap(t);
    mapAnim = (!reduceMotion && mapVisible && !document.hidden) ? requestAnimationFrame(mapLoop) : 0;
  }
  function renderMap() {
    if (!mapLayer) buildMapLayer();
    clusterNodes();
    if (!mapAnim) mapLoop(performance.now());
  }
  if (window.IntersectionObserver) {
    new IntersectionObserver(function (es) {
      mapVisible = es[0].isIntersecting;
      if (mapVisible && !mapAnim) mapLoop(performance.now());
    }).observe(mapCv);
  }
  document.addEventListener('visibilitychange', function () { if (!document.hidden && !mapAnim) mapLoop(performance.now()); });
  var mapTip = $('maptip');
  mapCv.addEventListener('pointermove', function (e) {
    var r = mapCv.getBoundingClientRect(), x = e.clientX - r.left, y = e.clientY - r.top, hit = null, bd = 144;
    mapPts.forEach(function (p) { var d = (p.x - x) * (p.x - x) + (p.y - y) * (p.y - y); if (d < bd) { bd = d; hit = p; } });
    if (!hit) { mapTip.hidden = true; return; }
    var places = Object.keys(hit.places).sort(function (a, b) { return hit.places[b] - hit.places[a]; });
    mapTip.hidden = false;
    mapTip.textContent = (places[0] || 'Unknown location') + (places.length > 1 ? ' +' + (places.length - 1) + ' nearby' : '') +
      ' · ' + hit.n + ' node' + (hit.n > 1 ? 's' : '') + (hit.off ? ' (' + hit.off + ' offline)' : '');
    mapTip.style.left = (hit.x + mapCv.offsetLeft) + 'px';
    mapTip.style.top = (hit.y + mapCv.offsetTop) + 'px';
  });
  mapCv.addEventListener('pointerleave', function () { mapTip.hidden = true; });

  /* ================================================================
     Rendering: distributions
     ================================================================ */
  function hbars(id, counts, color, max) {
    var keys = Object.keys(counts).sort(function (a, b) { return counts[b] - counts[a] || a.localeCompare(b); });
    var total = keys.reduce(function (s, k) { return s + counts[k]; }, 0);
    var shown = keys.slice(0, max || 6), rest = keys.slice(max || 6);
    var other = rest.reduce(function (s, k) { return s + counts[k]; }, 0);
    var top = counts[shown[0]] || 1;
    var list = shown.map(function (k) { return [k, counts[k]]; });
    if (other) list.push(['Other (' + rest.length + ')', other]);
    setHTML(id, list.map(function (kv) {
      return '<li><span class="k" title="' + esc(kv[0]) + '">' + esc(kv[0]) + '</span><span class="n">' + fmt(kv[1]) +
        ' · ' + Math.round(kv[1] / total * 100) + '%</span><span class="track"><i style="width:' + (kv[1] / top * 100).toFixed(1) + '%;--c:' + color + '"></i></span></li>';
    }).join('') || '<li class="empty">Nothing reported yet</li>');
    setText(id + '-n', keys.length + ' total');
  }
  function renderDists() {
    var ver = {}, go = {}, os = {}, geo = {};
    S.rows.forEach(function (r) {
      if (r.status === 'off') return;           // composition of the live network
      ver[r.ver] = (ver[r.ver] || 0) + 1;
      go[r.go] = (go[r.go] || 0) + 1;
      os[r.os] = (os[r.os] || 0) + 1;
      if (r.country) geo[r.country] = (geo[r.country] || 0) + 1;
    });
    hbars('d-ver', ver, 'var(--signal)', 6);
    hbars('d-go', go, '#CDBFFF', 6);
    hbars('d-os', os, 'var(--flare)', 5);
    hbars('d-geo', geo, 'var(--ok)', 7);
  }

  /* ================================================================
     Rendering: node table
     ================================================================ */
  var view = { f: 'all', q: '', sort: null, dir: -1, all: false };
  var RANK = { ok: 0, lag: 1, off: 2 };
  var sorters = {
    name: function (r) { return r.name.toLowerCase(); },
    ver: function (r) { return r.ver; },
    block: function (r) { return r.block; },
    prop: function (r) { return r.status === 'off' ? Infinity : r.prop; },
    lat: function (r) { return r.status === 'off' || !r.lat ? Infinity : r.lat; },
    peers: function (r) { return r.peers; },
    pend: function (r) { return r.pend; },
    up: function (r) { return r.up; },
    loc: function (r) { return r.loc || '~'; }
  };
  function cls(v, a, b) { return v <= a ? 't-ok' : v <= b ? 't-lag' : 't-bad'; }
  function histBars(h) {
    var a = h.slice(-16), out = '';
    for (var i = 0; i < a.length; i++) {
      var v = num(a[i]);
      if (v < 0) { out += '<i style="height:15%;--c:#3A4066"></i>'; continue; }
      var c = v <= 500 ? 'var(--ok)' : v <= 1500 ? 'var(--amber)' : 'var(--flare)';
      out += '<i style="height:' + Math.max(15, Math.min(100, v / 2000 * 100)).toFixed(0) + '%;--c:' + c + '"></i>';
    }
    return '<span class="hist" aria-hidden="true">' + out + '</span>';
  }
  function renderTable() {
    var rows = S.rows, counts = { all: rows.length, ok: 0, lag: 0, off: 0, pin: 0 };
    rows.forEach(function (r) { counts[r.status]++; if (S.pins.has(r.id)) counts.pin++; });
    for (var k in counts) setText('f-' + k, counts[k] ? fmt(counts[k]) : '');

    var q = view.q;
    var list = rows.filter(function (r) {
      if (view.f === 'pin' ? !S.pins.has(r.id) : (view.f !== 'all' && r.status !== view.f)) return false;
      return !q || r.search.indexOf(q) !== -1;
    });
    var key = view.sort && sorters[view.sort], dir = view.dir;
    list.sort(function (a, b) {
      var pa = S.pins.has(a.id), pb = S.pins.has(b.id);
      if (pa !== pb) return pa ? -1 : 1;
      if (key) {
        var x = key(a), y = key(b);
        if (x < y) return -dir; if (x > y) return dir;
      } else {
        if (RANK[a.status] !== RANK[b.status]) return RANK[a.status] - RANK[b.status];
        if (a.block !== b.block) return b.block - a.block;
        if (a.prop !== b.prop) return a.prop - b.prop;
      }
      return a.name.localeCompare(b.name);
    });
    var shown = view.all ? list : list.slice(0, ROW_LIMIT);
    $('more').hidden = list.length <= ROW_LIMIT || view.all;
    $('more').textContent = 'Show all ' + fmt(list.length) + ' nodes';
    setText('tcount', list.length === rows.length ? fmt(rows.length) + ' nodes' : fmt(list.length) + ' of ' + fmt(rows.length));

    var html = '';
    for (var i = 0; i < shown.length; i++) {
      var r = shown[i], off = r.status === 'off', pinned = S.pins.has(r.id);
      var sc = r.status === 'ok' ? 'c-ok' : r.status === 'lag' ? 'c-lag' : 'c-off';
      var label = r.status === 'ok' ? 'In sync' : r.status === 'lag' ? fmt(r.behind) + ' blocks behind' : 'Offline';
      html += '<tr' + (pinned ? ' class="pinned"' : '') + ' data-id="' + esc(r.id) + '">' +
        '<td class="pin"><button type="button" class="pinbtn" aria-pressed="' + pinned + '" aria-label="' + (pinned ? 'Unpin ' : 'Pin ') + esc(r.name) + '"><svg aria-hidden="true"><use href="#i-star"/></svg></button></td>' +
        '<td><div class="nname"><span class="sdot ' + sc + '" title="' + label + '"></span><b title="' + esc(r.name) + (r.coinbase ? '\n' + esc(xdcAddr(r.coinbase)) : '') + '">' + esc(r.name) + '</b></div></td>' +
        '<td>' + esc(r.ver) + '</td>' +
        '<td class="r' + (off ? ' t-dim' : '') + '">' + (r.block ? fmt(r.block) : '–') + (r.status === 'lag' ? ' <span class="t-lag">−' + fmt(r.behind) + '</span>' : '') + '</td>' +
        '<td class="r ' + (off ? 't-dim' : cls(r.prop, 500, 1500)) + '">' + (off ? '–' : (r.prop / 1000).toFixed(2) + ' s') + '</td>' +
        '<td>' + (off ? '' : histBars(r.hist)) + '</td>' +
        '<td class="r ' + (off || !r.lat ? 't-dim' : cls(r.lat, 150, 600)) + '">' + (off || !r.lat ? '–' : fmt(r.lat) + ' ms') + '</td>' +
        '<td class="r ' + (off ? 't-dim' : r.peers === 0 ? 't-bad' : r.peers < 3 ? 't-lag' : '') + '">' + (off ? '–' : fmt(r.peers)) + '</td>' +
        '<td class="r' + (off ? ' t-dim' : '') + '">' + (off ? '–' : fmt(r.pend)) + '</td>' +
        '<td class="r ' + cls(100 - r.up, 5, 25) + '">' + Math.round(r.up) + '%</td>' +
        '<td class="t-dim">' + esc(r.loc || '–') + '</td>' +
        '</tr>';
    }
    if (!shown.length) html = '<tr><td colspan="11" class="empty" style="padding:1.5rem;text-align:center">' + (rows.length ? 'No nodes match.' : 'Loading nodes…') + '</td></tr>';
    $('tbody').innerHTML = html;
  }

  $('tbody').addEventListener('click', function (e) {
    var btn = e.target.closest('.pinbtn'); if (!btn) return;
    var id = btn.closest('tr').getAttribute('data-id');
    if (S.pins.has(id)) S.pins.delete(id); else S.pins.add(id);
    store.set('xdcstats-pins-' + netName, Array.from(S.pins));
    if (window.xdcTrack) window.xdcTrack('stats/pin');
    renderTable();
  });
  var qTimer;
  $('q').addEventListener('input', function (e) {
    clearTimeout(qTimer);
    qTimer = setTimeout(function () { view.q = e.target.value.trim().toLowerCase(); renderTable(); }, 120);
  });
  document.querySelectorAll('.filters button').forEach(function (b) {
    b.addEventListener('click', function () {
      view.f = b.getAttribute('data-f');
      document.querySelectorAll('.filters button').forEach(function (x) { x.setAttribute('aria-pressed', x === b); });
      renderTable();
    });
  });
  document.querySelectorAll('thead th[data-k] button:not([disabled])').forEach(function (b) {
    b.addEventListener('click', function () {
      var th = b.parentNode, k = th.getAttribute('data-k');
      if (view.sort === k) {
        if (view.dir === (k === 'name' || k === 'loc' || k === 'ver' ? 1 : -1)) view.dir = -view.dir;
        else { view.sort = null; }                       // third click: back to the default order
      } else {
        view.sort = k; view.dir = (k === 'name' || k === 'loc' || k === 'prop' || k === 'lat') ? 1 : -1;
      }
      document.querySelectorAll('thead th[data-k]').forEach(function (x) { x.removeAttribute('aria-sort'); });
      if (view.sort) th.setAttribute('aria-sort', view.dir === 1 ? 'ascending' : 'descending');
      renderTable();
    });
  });
  $('more').addEventListener('click', function () { view.all = true; renderTable(); });

  /* ================================================================
     Orchestration
     ================================================================ */
  var renderQueued = false;
  function renderAll() {
    if (renderQueued) return;
    renderQueued = true;
    requestAnimationFrame(function () {
      renderQueued = false;
      renderHead(); renderTiles(); renderCharts(); renderMap(); renderDists(); renderTable(); renderStatus();
    });
  }

  function start(name) {
    gen++;
    streamStop();
    netName = name; NET = cfg(name); S = freshState(); lastBest = 0;
    store.set('xdcstats-net', name);
    document.querySelectorAll('.netswitch button').forEach(function (b) { b.setAttribute('aria-pressed', b.getAttribute('data-net') === name); });
    document.title = (name === 'apothem' ? 'Apothem testnet' : 'XDC Network') + ' Stats, live | XDC Outpost';
    setText('src-stats', NET.stats[0].replace(/^https?:\/\//, ''));
    setText('src-rpc', NET.rpc[0].replace(/^https?:\/\//, ''));

    // Reset visible state
    $('best-n').textContent = '00,000,000'; $('best-n').classList.add('skel');
    $('blocks').innerHTML = '<li class="empty" style="display:block">Waiting for the first block…</li>';
    $('beat').innerHTML = '';
    ['t-nodes', 't-mn', 't-prop', 't-lat', 't-geo'].forEach(function (id) { $(id).innerHTML = '<span class="skel">000</span>'; });
    ['t-nodes-s', 't-mn-s', 't-prop-s', 't-lat-s', 't-geo-s'].forEach(function (id) { $(id).innerHTML = '&nbsp;'; });
    $('t-nodes-bar').innerHTML = '';
    view.all = false;

    // Paint whatever we saved last time, straight away
    if (NET.price) {
      S.price = store.get('xdcstats-price'); S.priceSeries = store.get('xdcstats-spark');
    }
    renderPrice();
    var cached = store.get('xdcstats-cache-' + name);
    if (cached && Array.isArray(cached.nodes) && Date.now() - cached.at < 6 * 3600e3) applySnapshot(cached.nodes, cached.charts, cached.at, true);
    else renderAll();

    loops.forEach(function (L) { L.fails = 0; L.busy = false; L.run(); });
    streamStart();
  }

  document.querySelectorAll('.netswitch button').forEach(function (b) {
    b.addEventListener('click', function () {
      var n = b.getAttribute('data-net');
      if (n === netName) return;
      var u = new URL(location.href);
      if (n === 'mainnet') u.searchParams.delete('net'); else u.searchParams.set('net', n);
      history.replaceState(null, '', u);
      if (window.xdcTrack) window.xdcTrack('stats/net-' + n);
      start(n);
    });
  });

  // 4 Hz clock for the "x s ago" counters and the status line
  setInterval(function () {
    if (document.hidden) return;
    renderAge(); renderStatus();
  }, 250);
  setInterval(function () { if (!document.hidden) tickFeedAges(); }, 1000);

  var rz;
  addEventListener('resize', function () {
    clearTimeout(rz);
    rz = setTimeout(function () { mapLayer = null; renderPrice(); renderCharts(); renderMap(); }, 150);
  });

  $('year').textContent = new Date().getFullYear();
  start(netName);
})();
