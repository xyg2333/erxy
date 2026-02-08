/*
// 基于二协议双传输_wasm版本修改
// 集成功能：D1流量统计、测速屏蔽、Web后台/前台UI、自动订阅生成
// 仅支持 VLESS 和 Trojan 入站
 */
import {connect} from 'cloudflare:sockets';
import wasmModule from './protocol.wasm';

// ===========================================
// [1. 配置区域]
// ===========================================
// 默认 UUID 和 Trojan 密码 (如果环境变量未设置，将使用这些值)
const defaultUuid = '51f93bca-c05c-4075-bdb7-d6a576511b6f'; 
const defaultPassword = '114514'; 

// 网页相关配置
let WebToken = '2333';          // 后台访问密码
let AdminPath = 'hhll';        // 后台流量统计页路径
let LinkPath = 'ff343rab7n3xuc3';       // 前台订阅页路径
let BackgroundImg = 'https://api.yppp.net/api.php'; // 背景图片

// 优选IP API (每行一个 IP:Port)
const ADD = 'https://api.hh1.indevs.in/xy4ajzvb'; 



// ===========================================
// [2. D1 数据库 & 流量统计逻辑]
// ===========================================
const FLUSH_INTERVAL_MS = 60_000; 
let pendingBytes = 0;
let lastFlushAt = Date.now();
let flushInFlight = null;
let lastCleanupDay = null;

function getBeijingDay(ts = Date.now()) { return new Date(ts + 8 * 3600 * 1000).toISOString().slice(0, 10); }
function getBeijingCutoffDay(ts = Date.now()) { return getBeijingDay(ts - 7 * 24 * 3600 * 1000); } 

async function d1UpdateTotalAndDaily(env, addBytes, day) {
  if (!env.DB) return;
  try {
      const stTotal = env.DB.prepare("UPDATE traffic_counter SET bytes = bytes + ? WHERE id = 'global'").bind(addBytes);
      const stDaily = env.DB.prepare("INSERT INTO traffic_daily(day, bytes) VALUES (?, ?) ON CONFLICT(day) DO UPDATE SET bytes = bytes + excluded.bytes").bind(day, addBytes);
      await env.DB.batch([stTotal, stDaily]);
  } catch (e) { console.error('D1 Update Error:', e); }
}

async function d1CleanupOldDaily(env, todayDay) {
  if (!env.DB) return;
  const cutoff = getBeijingCutoffDay(Date.now());
  await env.DB.prepare("DELETE FROM traffic_daily WHERE day < ?").bind(cutoff).run();
  lastCleanupDay = todayDay;
}

async function flushTraffic(env) {
  if (!env.DB || pendingBytes <= 0) return;
  if (flushInFlight) return flushInFlight;
  const bytesToWrite = pendingBytes;
  pendingBytes = 0;
  const todayDay = getBeijingDay(Date.now());
  flushInFlight = (async () => {
    try {
      await d1UpdateTotalAndDaily(env, bytesToWrite, todayDay);
      if (lastCleanupDay !== todayDay) await d1CleanupOldDaily(env, todayDay);
    } catch (e) { pendingBytes += bytesToWrite; } finally { flushInFlight = null; }
  })();
  return flushInFlight;
}

async function addTrafficAndMaybeFlush(env, addBytes) {
  if (!addBytes || addBytes <= 0) return;
  pendingBytes += addBytes;
  const now = Date.now();
  if (now - lastFlushAt >= FLUSH_INTERVAL_MS) { lastFlushAt = now; await flushTraffic(env); }
}

async function maybeFlush(env) {
    const now = Date.now();
    if (now - lastFlushAt >= FLUSH_INTERVAL_MS) { lastFlushAt = now; await flushTraffic(env); }
}

async function getStats(env) {
    if (!env.DB) return { total: 0, today: 0, recent: [] };
    try {
        const totalRow = await env.DB.prepare("SELECT bytes FROM traffic_counter WHERE id='global' LIMIT 1").first();
        const total = totalRow ? totalRow.bytes : 0;
        const day = getBeijingDay(Date.now());
        const todayRow = await env.DB.prepare("SELECT bytes FROM traffic_daily WHERE day = ? LIMIT 1").bind(day).first();
        const today = todayRow ? todayRow.bytes : 0;
        const recent = await env.DB.prepare("SELECT day, bytes FROM traffic_daily ORDER BY day DESC LIMIT 7").all();
        return { total, today, recent: recent.results || [] };
    } catch (e) { return { total: 0, today: 0, recent: [] }; }
}

// ===========================================
// [3. UI 页面生成]
// ===========================================
function getCSS() {
    return `
    :root { --main: #f8a5c2; --text: #ffffff; --text-dim: #dfe6e9; --card-bg: rgba(30, 30, 40, 0.6); --card-border: rgba(255, 255, 255, 0.15); }
    * { box-sizing: border-box; margin:0; padding:0; outline:none; }
    body { font-family: 'Fredoka', sans-serif; background: url('${BackgroundImg}') no-repeat center center fixed; background-size: cover; color: var(--text); min-height: 100vh; padding: 20px; }
    body::before { content: ''; position: absolute; top:0; left:0; right:0; bottom:0; background: rgba(0,0,0,0.3); z-index: -1; }
    .container { max-width: 900px; margin: 0 auto; display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; }
    .container-mini { max-width: 420px; margin: 15vh auto; display: flex; flex-direction: column; }
    .full-width { grid-column: span 2; }
    .card { background: var(--card-bg); border-radius: 16px; padding: 20px; backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px); border: 1px solid var(--card-border); box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2); }
    .card-title { font-size: 1rem; color: var(--main); margin-bottom: 15px; font-weight: bold; border-bottom: 1px dashed rgba(255,255,255,0.1); padding-bottom:10px; }
    .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
    .stat-item { background: rgba(0,0,0,0.2); padding: 15px; border-radius: 10px; text-align: center; }
    .stat-val { font-size: 1.4rem; color: #fff; font-weight: bold; }
    .stat-label { font-size: 0.85rem; color: var(--text-dim); }
    .trend-item { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; font-size: 0.9rem; }
    .t-date { width: 50px; color: var(--text-dim); }
    .t-bar-box { flex: 1; height: 6px; background: rgba(255,255,255,0.1); border-radius: 3px; overflow: hidden; }
    .t-bar { height: 100%; background: linear-gradient(90deg, #f78fb3, #e66767); width: 0; transition: width 0.8s; }
    details { width: 100%; }
    summary { list-style: none; cursor: pointer; text-align: center; color: var(--main); font-weight: bold; padding: 10px; background: rgba(255,255,255,0.1); border-radius: 20px; transition: 0.2s; font-size: 0.9rem; margin: 0 auto; display: inline-block; padding-left: 25px; padding-right: 25px; }
    summary:hover { background: rgba(255,255,255,0.2); transform: scale(1.02); }
    summary::after { content: ' 👇 点击展开使用'; }
    details[open] summary { margin-bottom: 20px; }
    details[open] summary::after { content: ' 👆 收起'; }
    .link-group { animation: fadeIn 0.4s; }
    @keyframes fadeIn { from { opacity:0; transform:translateY(-10px); } to { opacity:1; transform:translateY(0); } }
    .link-box { background: rgba(0,0,0,0.3); border-radius: 8px; padding: 12px; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
    .link-url { font-family: monospace; font-size: 0.8rem; color: var(--text-dim); overflow: hidden; white-space: nowrap; text-overflow: ellipsis; margin-right: 10px; }
    .btn { background: var(--main); color: #fff; border: none; padding: 6px 12px; border-radius: 6px; cursor: pointer; font-weight: bold; font-size: 0.8rem; white-space: nowrap; }
    .btn:hover { filter: brightness(1.1); }
    .btn-outline { background: transparent; border: 1px solid var(--main); color: var(--main); width: 100%; padding: 10px; margin-top: 15px; border-radius: 10px; cursor: pointer; transition: 0.2s; }
    .btn-outline:hover { background: rgba(248, 165, 194, 0.1); }
    .modal { display: none; position: fixed; z-index: 999; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0,0,0,0.4); backdrop-filter: blur(5px); }
    .modal-content { background-color: #2d3436; margin: 15% auto; padding: 25px; border: 1px solid #888; width: 85%; max-width: 600px; border-radius: 15px; color: #dfe6e9; position: relative; box-shadow: 0 10px 40px rgba(0,0,0,0.5); animation: slideDown 0.3s; }
    @keyframes slideDown { from {top: -50px; opacity: 0;} to {top: 0; opacity: 1;} }
    .close-btn { color: #f8a5c2; position: absolute; top: 15px; right: 20px; font-size: 28px; font-weight: bold; cursor: pointer; transition: 0.2s; }
    .close-btn:hover { color: #fff; transform: rotate(90deg); }
    .modal-text { font-family: monospace; font-size: 0.85rem; line-height: 1.6; white-space: pre-wrap; word-break: break-all; max-height: 60vh; overflow-y: auto; background: rgba(0,0,0,0.2); padding: 15px; border-radius: 8px; margin-top: 20px; }
    @media (max-width: 768px) { .container { grid-template-columns: 1fr; } .full-width { grid-column: span 1; } }`;
}

function getAdminStatsPage(stats) {
    const fmt = (bytes) => {
        if (!bytes || bytes <= 0) return '0 B';
        const k = 1024, sizes = ['B','KB','MB','GB','TB'], i = Math.floor(Math.log(bytes) / Math.log(k));
        return (bytes / Math.pow(k, i)).toFixed(2) + ' ' + sizes[i];
    };
    let list = (stats.recent || []).slice().sort((a,b) => new Date(b.day) - new Date(a.day));
    while(list.length < 7) list.push({ day: '-', bytes: 0 });
    list = list.slice(0, 7);
    const maxBytes = Math.max(...list.map(x => Number(x.bytes || 0)));
    const totalRecent = list.reduce((a, b) => a + Number(b.bytes||0), 0);
    const avgBytes = totalRecent / (list.filter(r => r.bytes > 0).length || 1);
    const trendHtml = list.map(r => {
        const pct = maxBytes > 0 ? Math.min(100, (Number(r.bytes||0) / maxBytes) * 100) : 0;
        return `<div class="trend-item"><div class="t-date">${r.day.slice(5)}</div><div class="t-bar-box"><div class="t-bar" style="width: ${pct}%"></div></div><div class="t-val">${fmt(r.bytes)}</div></div>`;
    }).join('');
    return `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>流量监控</title><link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@500&display=swap" rel="stylesheet"><style>${getCSS()}</style></head><body><div class="container"><div class="card full-width"><div class="card-title">📊 流量统计概览</div><div class="stat-grid"><div class="stat-item"><div class="stat-label">🔥 今日用量</div><div class="stat-val">${fmt(stats.today)}</div></div><div class="stat-item"><div class="stat-label">💾 累计总耗</div><div class="stat-val">${fmt(stats.total)}</div></div><div class="stat-item"><div class="stat-label">⚡ 日均使用</div><div class="stat-val">${fmt(avgBytes)}</div></div><div class="stat-item"><div class="stat-label">🏔️ 历史峰值</div><div class="stat-val">${fmt(maxBytes)}</div></div></div></div><div class="card full-width"><div class="card-title">📈 近七日趋势</div>${trendHtml}</div></div><script>window.onload=()=>{document.querySelectorAll('.t-bar').forEach(e=>{const w=e.style.width;e.style.width='0';setTimeout(()=>e.style.width=w,100)})}</script></body></html>`;
}

function getLinksPage(vUrl, claLink) {
    const infoText = `此节点基于 WASM 技术构建，支持 VLESS 和 Trojan 协议。\n请确保客户端已更新至最新版本以获得最佳体验。\n\n注意事项：\n1. 请勿用于非法用途。\n2. 每日流量统计更新可能有一分钟延迟。`;
    return `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>订阅中心</title><link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@500&display=swap" rel="stylesheet"><style>${getCSS()}</style></head>
    <body><div class="container-mini"><div class="card" style="text-align:center; padding: 25px;"><h1 style="color: #f8a5c2; font-size: 1.2rem; margin-bottom: 15px;">🌸 双协议订阅中心</h1><details><summary>配置列表</summary><div class="link-group" style="text-align:left;"><h3 style="color:#dfe6e9; font-size:0.8rem; margin-bottom:5px; margin-top:5px;">🚀 V2ray / Nekobox / Base64订阅</h3><div class="link-box"><div class="link-url">${vUrl}</div><button class="btn" onclick="copy('${vUrl}')">复制</button></div><h3 style="color:#dfe6e9; font-size:0.8rem; margin-bottom:5px; margin-top:15px;">🐱 Clash 订阅链接</h3><div class="link-box"><div class="link-url">${claLink}</div><button class="btn" onclick="copy('${claLink}')">复制</button></div><button class="btn-outline" onclick="openModal()">📄 查看更多使用说明</button></div></details></div></div><div id="infoModal" class="modal"><div class="modal-content"><h2 style="color:#f8a5c2; font-size:1.1rem; margin-bottom:10px;">📌 使用说明</h2><span class="close-btn" onclick="closeModal()">&times;</span><div class="modal-text">${infoText}</div></div></div><div id="msg" style="position:fixed; top:20px; left:50%; transform:translateX(-50%); background:#f8a5c2; color:white; padding:8px 20px; border-radius:20px; display:none; box-shadow:0 5px 15px rgba(0,0,0,0.3); font-size:0.9rem;">已复制 ✨</div><script>function copy(text) { navigator.clipboard.writeText(text).then(()=>{ const m = document.getElementById('msg'); m.style.display = 'block'; setTimeout(()=>m.style.display='none', 2000); }); } function openModal() { document.getElementById("infoModal").style.display = "block"; } function closeModal() { document.getElementById("infoModal").style.display = "none"; } window.onclick = function(event) { if (event.target == document.getElementById("infoModal")) { closeModal(); } } </script></body></html>`;
}

function getPasswordPage() {
    return `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>Login</title><style>body{margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#2d3436;font-family:sans-serif;}.box{text-align:center;padding:30px;background:#fff;border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,0.2);}input{padding:10px;border:1px solid #ddd;border-radius:5px;outline:none;margin-right:5px;}button{padding:10px 20px;background:#f8a5c2;color:#fff;border:none;border-radius:5px;cursor:pointer;}</style></head><body><div class="box"><h3>🔒 请输入密码</h3><br><form><input type="password" name="token" placeholder="Password"><button>进入</button></form></div></body></html>`;
}

// ===========================================
// [4. WASM & 网络底层逻辑]
// ===========================================
// 理论最低带宽计算公式 (Theoretical Max Bandwidth Calculation):
//    - 速度上限 (Mbps) = (bufferSize (字节) / flushTime (毫秒)) * 0.008
//    - 示例: (512 * 1024 字节 / 10 毫秒) * 0.008 ≈ 419 Mbps
const bufferSize = 512 * 1024;         // 512KB
const startThreshold = 50 * 1024 * 1024; //50MB
const maxChunkLen = 64 * 1024;        // 64KB
const flushTime = 20;                 // 20ms
let concurrency = 4;
const proxyStrategyOrder = ['socks', 'http', 'nat64'];
const dohEndpoints = ['https://cloudflare-dns.com/dns-query', 'https://dns.google/dns-query'];
const dohNatEndpoints = ['https://cloudflare-dns.com/dns-query', 'https://dns.google/resolve'];
const proxyIpAddrs = {EU: 'ProxyIP.DE.CMLiussss.net', AS: 'ProxyIP.SG.CMLiussss.net', JP: 'ProxyIP.JP.CMLiussss.net', US: 'ProxyIP.US.CMLiussss.net'};
const finallyProxyHost = 'ProxyIP.CMLiussss.net';
const coloRegions = {
    JP: new Set(['FUK', 'ICN', 'KIX', 'NRT', 'OKA']),
    EU: new Set(['ACC', 'ADB', 'ALA', 'ALG', 'AMM', 'AMS', 'ARN', 'ATH', 'BAH', 'BCN', 'BEG', 'BGW', 'BOD', 'BRU', 'BTS', 'BUD', 'CAI','CDG', 'CPH', 'CPT', 'DAR', 'DKR', 'DMM', 'DOH', 'DUB', 'DUR', 'DUS', 'DXB', 'EBB', 'EDI', 'EVN', 'FCO', 'FRA', 'GOT','GVA', 'HAM', 'HEL', 'HRE', 'IST', 'JED', 'JIB', 'JNB', 'KBP', 'KEF', 'KWI', 'LAD', 'LED', 'LHR', 'LIS', 'LOS', 'LUX','LYS', 'MAD', 'MAN', 'MCT', 'MPM', 'MRS', 'MUC', 'MXP', 'NBO', 'OSL', 'OTP', 'PMO', 'PRG', 'RIX', 'RUH', 'RUN', 'SKG','SOF', 'STR', 'TBS', 'TLL', 'TLV', 'TUN', 'VIE', 'VNO', 'WAW', 'ZAG', 'ZRH']),
    AS: new Set(['ADL', 'AKL', 'AMD', 'BKK', 'BLR', 'BNE', 'BOM', 'CBR', 'CCU', 'CEB', 'CGK', 'CMB', 'COK', 'DAC', 'DEL', 'HAN', 'HKG','HYD', 'ISB', 'JHB', 'JOG', 'KCH', 'KHH', 'KHI', 'KTM', 'KUL', 'LHE', 'MAA', 'MEL', 'MFM', 'MLE', 'MNL', 'NAG', 'NOU','PAT', 'PBH', 'PER', 'PNH', 'SGN', 'SIN', 'SYD', 'TPE', 'ULN', 'VTE'])
};
const coloToProxyMap = new Map();
for (const [region, colos] of Object.entries(coloRegions)) {for (const colo of colos) coloToProxyMap.set(colo, proxyIpAddrs[region])}
const textEncoder = new TextEncoder(), textDecoder = new TextDecoder();

// WASM 初始化
const instance = new WebAssembly.Instance(wasmModule, {env: {abort: () => {}}});
const {memory, getDataPtr, getUuidPtr, getResultPtr, getUrlPtr, parseProtocolWasm, parseUrlWasm, initCredentialsWasm} = instance.exports;
const wasmMem = new Uint8Array(memory.buffer);
const wasmRes = new Int32Array(memory.buffer, getResultPtr(), 32);
const dataPtr = getDataPtr(), urlPtr = getUrlPtr(), uuidPtr = getUuidPtr();
let isInitialized = false;

const initializeWasm = (env) => {
    if (isInitialized) return;
    const cleanUuid = (env.UUID || defaultUuid).trim().replace(/-/g, "");
    if (cleanUuid.length === 32) {
        wasmRes[18] = 1;
        const uuidBytes = new Uint8Array(16);
        for (let i = 0, c; i < 16; i++) {uuidBytes[i] = (((c = cleanUuid.charCodeAt(i * 2)) > 64 ? c + 9 : c) & 0xF) << 4 | (((c = cleanUuid.charCodeAt(i * 2 + 1)) > 64 ? c + 9 : c) & 0xF);}
        wasmMem.set(uuidBytes, uuidPtr);
    } else {wasmRes[18] = 0}
    const password = (env.PASSWORD || defaultPassword).trim();
    if (password.length > 0) {
        wasmRes[19] = 1;
        const passBytes = textEncoder.encode(password);
        wasmMem.set(passBytes, urlPtr);
        initCredentialsWasm(passBytes.length);
    } else {wasmRes[19] = 0}
    isInitialized = true;
};

// 辅助函数
const binaryAddrToString = (addrType, addrBytes) => {
    if (addrType === 3) return textDecoder.decode(addrBytes);
    if (addrType === 1) return `${addrBytes[0]}.${addrBytes[1]}.${addrBytes[2]}.${addrBytes[3]}`;
    let ipv6 = ((addrBytes[0] << 8) | addrBytes[1]).toString(16);
    for (let i = 1; i < 8; i++) ipv6 += ':' + ((addrBytes[i * 2] << 8) | addrBytes[i * 2 + 1]).toString(16);
    return `[${ipv6}]`;
};
const parseHostPort = (addr, defaultPort) => {
    let host = addr, port = defaultPort, idx;
    if (addr.charCodeAt(0) === 91) {
        if ((idx = addr.indexOf(']:')) !== -1) {
            host = addr.substring(0, idx + 1);
            port = addr.substring(idx + 2);
        }
    } else if ((idx = addr.indexOf('.tp')) !== -1 && addr.lastIndexOf(':') === -1) {
        port = addr.substring(idx + 3, addr.indexOf('.', idx + 3));
    } else if ((idx = addr.lastIndexOf(':')) !== -1) {
        host = addr.substring(0, idx);
        port = addr.substring(idx + 1);
    }
    return [host, (port = parseInt(port), isNaN(port) ? defaultPort : port)];
};
const parseAuthString = (authParam) => {
    let username, password, hostStr;
    const atIndex = authParam.lastIndexOf('@');
    if (atIndex === -1) {hostStr = authParam} else {
        const cred = authParam.substring(0, atIndex);
        hostStr = authParam.substring(atIndex + 1);
        const colonIndex = cred.indexOf(':');
        if (colonIndex === -1) {username = cred} else {
            username = cred.substring(0, colonIndex);
            password = cred.substring(colonIndex + 1);
        }
    }
    const [hostname, port] = parseHostPort(hostStr, 1080);
    return {username, password, hostname, port};
};
const createConnect = (hostname, port, socket = connect({hostname, port})) => socket.opened.then(() => socket);
const concurrentConnect = (hostname, port, limit = concurrency) => {
    if (limit === 1) return createConnect(hostname, port);
    return Promise.any(Array(limit).fill(null).map(() => createConnect(hostname, port)));
};
const connectViaSocksProxy = async (targetAddrType, targetPortNum, socksAuth, addrBytes, limit) => {
    const socksSocket = await concurrentConnect(socksAuth.hostname, socksAuth.port, limit);
    const writer = socksSocket.writable.getWriter();
    const reader = socksSocket.readable.getReader();
    await writer.write(new Uint8Array([5, 2, 0, 2]));
    const {value: authResponse} = await reader.read();
    if (!authResponse || authResponse[0] !== 5 || authResponse[1] === 0xFF) return null;
    if (authResponse[1] === 2) {
        if (!socksAuth.username) return null;
        const userBytes = textEncoder.encode(socksAuth.username);
        const passBytes = textEncoder.encode(socksAuth.password || '');
        const uLen = userBytes.length, pLen = passBytes.length, authReq = new Uint8Array(3 + uLen + pLen)
        authReq[0] = 1, authReq[1] = uLen, authReq.set(userBytes, 2), authReq[2 + uLen] = pLen, authReq.set(passBytes, 3 + uLen);
        await writer.write(authReq);
        const {value: authResult} = await reader.read();
        if (!authResult || authResult[0] !== 1 || authResult[1] !== 0) return null;
    } else if (authResponse[1] !== 0) {return null}
    const isDomain = targetAddrType === 3, socksReq = new Uint8Array(6 + addrBytes.length + (isDomain ? 1 : 0));
    socksReq[0] = 5, socksReq[1] = 1, socksReq[2] = 0, socksReq[3] = targetAddrType;
    isDomain ? (socksReq[4] = addrBytes.length, socksReq.set(addrBytes, 5)) : socksReq.set(addrBytes, 4);
    socksReq[socksReq.length - 2] = targetPortNum >> 8, socksReq[socksReq.length - 1] = targetPortNum & 0xff;
    await writer.write(socksReq);
    const {value: finalResponse} = await reader.read();
    if (!finalResponse || finalResponse[1] !== 0) return null;
    writer.releaseLock(), reader.releaseLock();
    return socksSocket;
};
const connectViaHttpProxy = async (targetAddrType, targetPortNum, httpAuth, addrBytes, limit) => {
    const {username, password, hostname, port} = httpAuth;
    const proxySocket = await concurrentConnect(hostname, port, limit);
    const writer = proxySocket.writable.getWriter();
    const httpHost = binaryAddrToString(targetAddrType, addrBytes);
    let dynamicHeaders = `CONNECT ${httpHost}:${targetPortNum} HTTP/1.1\r\nHost: ${httpHost}:${targetPortNum}\r\n`;
    if (username) dynamicHeaders += `Proxy-Authorization: Basic ${btoa(`${username}:${password || ''}`)}\r\n`;
    const fullHeaders = new Uint8Array(dynamicHeaders.length * 3 + 128); // rough buffer size
    const {written} = textEncoder.encodeInto(dynamicHeaders + `User-Agent: Mozilla/5.0\r\n\r\n`, fullHeaders);
    await writer.write(fullHeaders.subarray(0, written));
    writer.releaseLock();
    const reader = proxySocket.readable.getReader();
    const buffer = new Uint8Array(256);
    let bytesRead = 0, statusChecked = false;
    while (bytesRead < buffer.length) {
        const {value, done} = await reader.read();
        if (done || bytesRead + value.length > buffer.length) return null;
        buffer.set(value, bytesRead);
        bytesRead += value.length;
        if (!statusChecked && bytesRead >= 12) {
            if (buffer[9] !== 50) return null; // Check for '200'
            statusChecked = true;
        }
        let i = Math.max(0, bytesRead - 100);
        while ((i = buffer.indexOf(13, i)) !== -1 && i <= bytesRead - 4) {
            if (buffer[i + 1] === 10 && buffer[i + 2] === 13 && buffer[i + 3] === 10) {
                reader.releaseLock();
                if (bytesRead > i + 4) {
                    const {readable, writable} = new TransformStream();
                    const writer = writable.getWriter();
                    writer.write(buffer.subarray(i + 4, bytesRead));
                    writer.releaseLock();
                    proxySocket.readable.pipeTo(writable).catch(() => {});
                    // @ts-ignore
                    proxySocket.readable = readable;
                }
                return proxySocket;
            }
            i++;
        }
    }
    return null;
};
const ipv4ToNat64Ipv6 = (ipv4Address, nat64Prefixes) => {
    const parts = ipv4Address.split('.');
    let hexStr = "";
    for (let i = 0; i < 4; i++) {
        let h = (parts[i] | 0).toString(16);
        hexStr += (h.length === 1 ? "0" + h : h);
        if (i === 1) hexStr += ":";
    }
    return `[${nat64Prefixes}${hexStr}]`;
};
const concurrentDnsResolve = async (hostname, recordType) => {
    const dnsResult = await Promise.any(dohNatEndpoints.map(endpoint =>
        fetch(`${endpoint}?name=${hostname}&type=${recordType}`, {headers: {'Accept': 'application/dns-json'}}).then(response => {
            if (!response.ok) throw new Error();
            return response.json();
        })
    ));
    const answer = dnsResult.Answer || dnsResult.answer;
    if (!answer || answer.length === 0) return null;
    return answer;
};
const dohDnsHandler = async (payload) => {
    if (payload.byteLength < 2) return null;
    const dnsQueryData = payload.subarray(2);
    const resp = await Promise.any(dohEndpoints.map(endpoint =>
        fetch(endpoint, {method: 'POST', headers: {'content-type': 'application/dns-message'}, body: dnsQueryData}).then(response => {
            if (!response.ok) throw new Error();
            return response;
        })
    ));
    const dnsQueryResult = await resp.arrayBuffer();
    const udpSize = dnsQueryResult.byteLength;
    const packet = new Uint8Array(2 + udpSize);
    packet[0] = (udpSize >> 8) & 0xff, packet[1] = udpSize & 0xff;
    packet.set(new Uint8Array(dnsQueryResult), 2);
    return packet;
};
const connectNat64 = async (addrType, port, nat64Auth, addrBytes, proxyAll, limit) => {
    const nat64Prefixes = nat64Auth.charCodeAt(0) === 91 ? nat64Auth.slice(1, -1) : nat64Auth;
    if (!proxyAll) return concurrentConnect(`[${nat64Prefixes}6815:3598]`, port, limit);
    const hostname = binaryAddrToString(addrType, addrBytes);
    if (addrType === 3) {
        const answer = await concurrentDnsResolve(hostname, 'A');
        const aRecord = answer?.find(record => record.type === 1);
        return aRecord ? concurrentConnect(ipv4ToNat64Ipv6(aRecord.data, nat64Prefixes), port, limit) : null;
    }
    if (addrType === 1) return concurrentConnect(ipv4ToNat64Ipv6(hostname, nat64Prefixes), port, limit);
    return concurrentConnect(hostname, port, limit);
};
const williamResult = async (william) => {
    const answer = await concurrentDnsResolve(william, 'TXT');
    if (!answer) return null;
    let txtData, i = 0, len = answer.length;
    for (; i < len; i++) if (answer[i].type === 16) {
        txtData = answer[i].data;
        break;
    }
    if (!txtData) return null;
    if (txtData.charCodeAt(0) === 34 && txtData.charCodeAt(txtData.length - 1) === 34) txtData = txtData.slice(1, -1);
    const raw = txtData.split(/,|\\010|\n/), prefixes = [];
    for (i = 0, len = raw.length; i < len; i++) {
        const s = raw[i].trim();
        if (s) prefixes.push(s);
    }
    return prefixes.length ? prefixes : null;
};
const proxyIpRegex = /william|fxpip/;
const connectProxyIp = async (param, limit) => {
    if (proxyIpRegex.test(param)) {
        const resolvedIps = await williamResult(param);
        if (!resolvedIps || resolvedIps.length === 0) return null;
        const connectionPromises = resolvedIps.map(ip => {
            const [host, port] = parseHostPort(ip, 443);
            return createConnect(host, port);
        });
        return await Promise.any(connectionPromises);
    }
    const [host, port] = parseHostPort(param, 443);
    return concurrentConnect(host, port, limit);
};
const strategyExecutorMap = new Map([
    [0, async ({addrType, port, addrBytes}) => {
        const hostname = binaryAddrToString(addrType, addrBytes);
        return concurrentConnect(hostname, port);
    }],
    [1, async ({addrType, port, addrBytes}, param, limit) => {
        const socksAuth = parseAuthString(param);
        return connectViaSocksProxy(addrType, port, socksAuth, addrBytes, limit);
    }],
    [2, async ({addrType, port, addrBytes}, param, limit) => {
        const httpAuth = parseAuthString(param);
        return connectViaHttpProxy(addrType, port, httpAuth, addrBytes, limit);
    }],
    [3, async (_parsedRequest, param, limit) => {
        return connectProxyIp(param, limit);
    }],
    [4, async ({addrType, port, addrBytes}, param, limit) => {
        const {nat64Auth, proxyAll} = param;
        return connectNat64(addrType, port, nat64Auth, addrBytes, proxyAll, limit);
    }]
]);
const getUrlParam = (offset, len) => {
    if (len <= 0) return null;
    return textDecoder.decode(wasmMem.subarray(urlPtr + offset, urlPtr + offset + len));
};
const establishTcpConnection = async (parsedRequest, request) => {
    const u = request.url, clean = u.slice(u.indexOf('/', 10) + 1);
    let list = [];
    if (clean.length < 6) {
        list.push({type: 0}, {type: 3, param: coloToProxyMap.get(request.cf?.colo) ?? proxyIpAddrs.US}, {type: 3, param: finallyProxyHost});
    } else {
        const urlBytes = textEncoder.encode(clean);
        wasmMem.set(urlBytes, urlPtr);
        parseUrlWasm(urlBytes.length);
        const r = wasmRes, s5Val = getUrlParam(r[9], r[10]), httpVal = getUrlParam(r[11], r[12]), nat64Val = getUrlParam(r[13], r[14]), ipVal = getUrlParam(r[15], r[16]), proxyAll = r[17] === 1;
        !proxyAll && list.push({type: 0});
        const add = (v, t) => {
            const parts = v && decodeURIComponent(v).split(',').filter(Boolean);
            parts?.length && list.push({type: t, param: parts.map(p => t === 4 ? {nat64Auth: p, proxyAll} : p), concurrent: true});
        };
        for (const k of proxyStrategyOrder) k === 'socks' ? add(s5Val, 1) : k === 'http' ? add(httpVal, 2) : add(nat64Val, 4);
        if (proxyAll) {
            !list.length && list.push({type: 0});
        } else {
            add(ipVal, 3), list.push({type: 3, param: coloToProxyMap.get(request.cf?.colo) ?? proxyIpAddrs.US}, {type: 3, param: finallyProxyHost});
        }
    }
    for (let i = 0; i < list.length; i++) {
        try {
            const exec = strategyExecutorMap.get(list[i].type);
            const sub = (list[i].concurrent && Array.isArray(list[i].param)) ? Math.max(1, Math.floor(concurrency / list[i].param.length)) : undefined;
            const socket = await (list[i].concurrent && Array.isArray(list[i].param) ? Promise.any(list[i].param.map(ip => exec(parsedRequest, ip, sub))) : exec(parsedRequest, list[i].param));
            if (socket) return socket;
        } catch {}
    }
    return null;
};

// 【修改】manualPipe 增加流量统计
const manualPipe = async (readable, writable, env) => {
    const _bufferSize = bufferSize, _maxChunkLen = maxChunkLen, _startThreshold = startThreshold, _flushTime = flushTime, _safeBufferSize = _bufferSize - _maxChunkLen;
    let mainBuf = new ArrayBuffer(_bufferSize), offset = 0, time = 2, timerId = null, resume = null, isReading = false, needsFlush = false, totalBytes = 0;
    const flush = () => {
        if (isReading) return needsFlush = true;
        offset > 0 && (writable.send(mainBuf.slice(0, offset)), offset = 0);
        needsFlush = false, timerId && (clearTimeout(timerId), timerId = null), resume?.(), resume = null;
    };
    const reader = readable.getReader({mode: 'byob'});
    try {
        while (true) {
            isReading = true;
            const {done, value} = await reader.read(new Uint8Array(mainBuf, offset, _maxChunkLen));
            if (isReading = false, done) break;
            
            // 流量统计
            if (value && value.byteLength > 0 && env && env.DB) {
                addTrafficAndMaybeFlush(env, value.byteLength).catch(()=>{});
            }

            mainBuf = value.buffer;
            const chunkLen = value.byteLength;
            if (chunkLen < _maxChunkLen) {
                time = 2, chunkLen < 4096 && (totalBytes = 0);
                offset > 0 ? (offset += chunkLen, flush()) : writable.send(value.slice());
            } else {
                totalBytes += chunkLen;
                offset += chunkLen, timerId ||= setTimeout(flush, time), needsFlush && flush();
                offset > _safeBufferSize && (totalBytes > _startThreshold && (time = _flushTime), await new Promise(r => resume = r));
            }
        }
    } finally {isReading = false, flush(), reader.releaseLock()}
};

const handleSession = async (chunk, state, request, writable, close, env) => {
    wasmMem.set(chunk, dataPtr);
    if (!parseProtocolWasm(chunk.length)) return close();
    const r = wasmRes;
    const parsedRequest = {addrType: r[0], port: r[1], dataOffset: r[2], isDns: r[3] === 1, addrBytes: chunk.subarray(r[4], r[4] + r[5])}
    
    // 【新增】测速网站屏蔽逻辑
    try {
        const hostname = binaryAddrToString(parsedRequest.addrType, parsedRequest.addrBytes);
        for (const word of BLOCK_WORDS) {
            if (hostname.includes(word)) {
                return close();
            }
        }
    } catch(e) {}

    if (r[6] === 0) {writable.send(new Uint8Array([chunk[0], 0]))}
    const payload = chunk.subarray(parsedRequest.dataOffset);
    if (parsedRequest.isDns) {
        const dnsPack = await dohDnsHandler(payload);
        if (dnsPack?.byteLength) writable.send(dnsPack);
        return close();
    } else {
        state.tcpSocket = await establishTcpConnection(parsedRequest, request);
        if (!state.tcpSocket) return close();
        const tcpWriter = state.tcpSocket.writable.getWriter();
        if (payload.byteLength) await tcpWriter.write(payload);
        state.tcpWriter = (c) => tcpWriter.write(c);
        manualPipe(state.tcpSocket.readable, writable, env).finally(() => close());
    }
};

const handleWebSocketConn = async (webSocket, request, env) => {
    const protocolHeader = request.headers.get('sec-websocket-protocol');
    // @ts-ignore
    const earlyData = protocolHeader ? Uint8Array.fromBase64(protocolHeader, {alphabet: 'base64url'}) : null;
    const state = {tcpWriter: null, tcpSocket: null};
    const close = () => {state.tcpSocket?.close(), !earlyData && webSocket.close()};
    let processingChain = Promise.resolve();
    const process = async (chunk) => {
        if (state.tcpWriter) return state.tcpWriter(chunk);
        await handleSession(earlyData ? chunk : new Uint8Array(chunk), state, request, webSocket, close, env);
    };
    if (earlyData) processingChain = processingChain.then(() => process(earlyData).catch(close));
    webSocket.addEventListener("message", event => {processingChain = processingChain.then(() => process(event.data).catch(close))});
};

const xhttpResponseHeaders = {'Content-Type': 'application/octet-stream', 'X-Accel-Buffering': 'no', 'Cache-Control': 'no-store'};
const handleXhttp = async (request, env) => {
    const _maxChunkLen = maxChunkLen;
    const reader = request.body.getReader({mode: 'byob'});
    const state = {tcpWriter: null, tcpSocket: null};
    let sessionBuffer = new ArrayBuffer(_maxChunkLen), used = 0;
    return new Response(new ReadableStream({
        async start(controller) {
            const writable = {send: (chunk) => controller.enqueue(chunk)}, close = () => {reader.releaseLock(), state.tcpSocket?.close(), controller.close()};
            try {
                while (true) {
                    let offset = 0, readLen = _maxChunkLen;
                    !state.tcpWriter && (offset = used, readLen = 8192);
                    const {done, value} = await reader.read(new Uint8Array(sessionBuffer, offset, readLen));
                    if (done) break;
                    sessionBuffer = value.buffer;
                    if (state.tcpWriter) {
                        state.tcpWriter(value.slice());
                        continue;
                    }
                    used += value.byteLength;
                    if (used < 48) continue;
                    await handleSession(new Uint8Array(sessionBuffer, 0, used).slice(), state, request, writable, close, env);
                    used = 0;
                }
            } catch {close()} finally {close()}
        },
        cancel() {state.tcpSocket?.close(), reader.releaseLock()}
    }), {headers: xhttpResponseHeaders});
};

export default {
    async fetch(request, env, ctx) {
        if (ctx && ctx.waitUntil) ctx.waitUntil(maybeFlush(env));
        initializeWasm(env);
        
        try {
            // 环境变量覆盖
            if (env.WEBTOKEN) WebToken = env.WEBTOKEN;
            if (env.ADMINPATH) AdminPath = env.ADMINPATH;
            if (env.LINKPATH) LinkPath = env.LINKPATH;

            const url = new URL(request.url);
            
            // --- 1. 后台管理页面 (查看流量) ---
            if (url.pathname.toLowerCase() === `/${AdminPath.toLowerCase()}`) {
                if (WebToken && url.searchParams.get('token') !== WebToken) return new Response(getPasswordPage(), { headers: { 'Content-Type': 'text/html;charset=utf-8' } });
                const stats = await getStats(env);
                return new Response(getAdminStatsPage(stats), { headers: { 'Content-Type': 'text/html;charset=utf-8' } });
            }

            // --- 2. 前台订阅页面 (展示链接) ---
            if (url.pathname.toLowerCase() === `/${LinkPath.toLowerCase()}`) {
                const hostname = url.hostname;
                // 生成订阅链接用于展示
                const vUrl = `https://${hostname}/sub?token=${WebToken}`; 
                const claLink = `https://sub.ssss.xx.kg/clash?config=${encodeURIComponent(vUrl)}`;
                return new Response(getLinksPage(vUrl, claLink), { headers: { 'Content-Type': 'text/html;charset=utf-8' } });
            }

            // --- 3. 订阅接口 (API) ---
            if (url.pathname === '/sub' || url.searchParams.has('sub')) {
                const hostname = url.hostname;
                const cleanHost = 'www.shopify.com'; 
                const cleanPort = '443';
                let configList = [];
                const finalUuid = env.UUID || defaultUuid;
                const finalPass = env.PASSWORD || defaultPassword;
                
                // 默认节点
                configList.push(`vless://${finalUuid}@${cleanHost}:${cleanPort}?encryption=none&security=tls&sni=${hostname}&fp=chrome&type=ws&host=${hostname}&path=%2F%3Fed%3D2048#哄哄优选域_VL_WS`);
                configList.push(`trojan://${finalPass}@${cleanHost}:${cleanPort}?security=tls&sni=${hostname}&fp=chrome&type=ws&host=${hostname}&path=%2F%3Fed%3D2048#哄哄优选域_TR_WS`);

                // 优选IP节点
                if (ADD) {
                    try {
                        const response = await fetch(ADD);
                        if (response.status === 200) {
                            const text = await response.text();
                            text.split('\n').forEach((line) => {
                                const trimLine = line.trim();
                                if (!trimLine) return;
                                let [addr, remark] = trimLine.split('#');
                                if (!remark) remark = '';
                                let [ip, port] = addr.split(':');
                                if (!port) port = '443';
                                const nodeName = remark ? `${remark}` : `${ip}:${port}`;
                                configList.push(`vless://${finalUuid}@${ip}:${port}?encryption=none&security=tls&sni=${hostname}&fp=chrome&type=ws&host=${hostname}&path=%2F%3Fed%3D2048#${nodeName}_VL`);
                                configList.push(`trojan://${finalPass}@${ip}:${port}?security=tls&sni=${hostname}&fp=chrome&type=ws&host=${hostname}&path=%2F%3Fed%3D2048#${nodeName}_TR`);
                            });
                        }
                    } catch (e) {}
                }

                const finalText = configList.join('\n');
                const encoder = new TextEncoder();
                const data = encoder.encode(finalText);
                let binary = '';
                for (let i = 0; i < data.byteLength; i++) { binary += String.fromCharCode(data[i]); }
                const base64Content = btoa(binary);

                return new Response(base64Content, {
                    status: 200,
                    headers: {
                        'Content-Type': 'text/plain; charset=utf-8',
                        'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
                        'Subscription-Userinfo': `upload=0; download=0; total=${10*1024*1024*1024*1024}; expire=0`
                    }
                });
            }

            // --- 4. 代理流量处理 ---
            if (request.method === 'POST') return handleXhttp(request, env);
            if (request.headers.get('Upgrade') === 'websocket') {
                const {0: clientSocket, 1: webSocket} = new WebSocketPair();
                webSocket.accept();
                handleWebSocketConn(webSocket, request, env);
                return new Response(null, {status: 101, webSocket: clientSocket});
            }
            return new Response('Service Running', {status: 200, headers: {'Content-Type': 'text/html; charset=UTF-8'}});
        
        } catch (err) {
            return new Response("Worker Error: " + err.toString(), { status: 500 });
        }
    }
};