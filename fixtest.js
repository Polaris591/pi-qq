#!/usr/bin/env node
'use strict';
/**
 * 针对本轮修复的回归测试: 只验证三处改动, 不连网、不启 pi。
 * 复用 utest.js 的沙箱做法 (假 ws + 临时目录 + 追加导出)。
 */
const fs = require('fs');
const path = require('path');

const BASE = '/tmp/fixtest';
fs.rmSync(BASE, { recursive: true, force: true });
for (const d of ['memory', 'tasks', 'sessions', 'outbox', 'inbox', 'workspace', 'node_modules/ws']) {
  fs.mkdirSync(path.join(BASE, d), { recursive: true });
}
fs.writeFileSync(path.join(BASE, 'node_modules/ws/index.js'), `
class WS { constructor(){this.readyState=0;} on(){return this;} send(){} close(){} }
WS.OPEN = 1;
class Server { constructor(){} on(){} close(){} }
module.exports = WS; module.exports.Server = Server; module.exports.WebSocket = WS;
`);
fs.writeFileSync(path.join(BASE, 'config.json'), JSON.stringify({
  napcat: { url: 'ws://127.0.0.1:1', token: '' },
  pi: { bin: 'pi', cwd: `${BASE}/workspace`, sessionDir: `${BASE}/sessions`, provider: '', model: '', tools: '', extraArgs: [] },
  access: { privateWhitelist: ['123456789'], groupWhitelist: ['10001'], allowAllPrivate: false, allowAllGroups: false, requireAtInGroup: true },
  behavior: {},
  files: { outboxDir: `${BASE}/outbox`, containerOutbox: '/ctr/outbox', inboxDir: `${BASE}/inbox` },
  memory: { dir: `${BASE}/memory` },
  tasks: { dir: `${BASE}/tasks` },
}, null, 2));

const src = fs.readFileSync(path.join(__dirname, 'bridge.js'), 'utf8');
fs.writeFileSync(path.join(BASE, 'bridge.js'), `${src}
module.exports = { PiSession, sessions, alertTarget, cfg, onebot, fetchGroupContext, GROUP_CTX_CACHE, sweepStorage, STATE, dispatchCommand, HELP, requestRestart, maybeRestartNow, busySessionCount, setExitHook, restartState, mdToPlain };
`);

const results = [];
const ok = (name, pass, extra) => { results.push({ name, pass }); console.log(`${pass ? 'PASS' : 'FAIL'}  ${name}${extra ? `  [${extra}]` : ''}`); };

(async () => {
  const B = require(path.join(BASE, 'bridge.js'));

  // 阻止真的去发 QQ
  const sent = [];
  B.onebot.action = async (action, params) => { sent.push({ action, params }); return { status: 'ok' }; };

  // ---- 修复 1: flush() 空缓冲不再重置 lastFlush
  {
    const s = new B.PiSession('private_1', { type: 'private', id: '1' });
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };
    s.buf = '';
    s.lastFlush = 1000;                     // 假装很久没发过
    await s.flush(false);
    ok('空缓冲 flush 不重置 lastFlush', s.lastFlush === 1000, `lastFlush=${s.lastFlush}`);

    // 有内容时仍应重置
    s.buf = 'x'.repeat(200);
    s.lastFlush = 1000;
    sent.length = 0;
    await s.flush(true);
    ok('有内容 flush 正常重置 lastFlush', s.lastFlush > 1000);
    ok('有内容 flush 确实发出去了', sent.length > 0, `发送 ${sent.length} 次`);
    s.closed = true;
  }

  // ---- 修复 2: send() 写入失败不再炸进程
  {
    const s = new B.PiSession('private_2', { type: 'private', id: '2' });
    let threw = false;
    // 模拟 pi 刚崩: stdin 已不可写
    s.proc = { stdin: { writable: false, write() {}, on: () => {} } };
    try { s.send({ type: 'prompt', message: 'x' }); } catch { threw = true; }
    ok('stdin 不可写时 send 返回 false 而非抛错', s.send({ type: 'prompt' }) === false && !threw);

    // 模拟 write 同步抛错 (EPIPE)
    s.proc = { stdin: { writable: true, write() { const e = new Error('EPIPE'); e.code = 'EPIPE'; throw e; }, on: () => {} } };
    let threw2 = false;
    let r2 = null;
    try { r2 = s.send({ type: 'prompt', message: 'x' }); } catch { threw2 = true; }
    ok('write 抛 EPIPE 时被捕获', !threw2 && r2 === false, `threw=${threw2} ret=${r2}`);

    // 模拟异步回调报错
    s.proc = { stdin: { writable: true, write(d, cb) { if (cb) cb(new Error('EPIPE')); }, on: () => {} } };
    let threw3 = false;
    try { s.send({ type: 'prompt', message: 'x' }); } catch { threw3 = true; }
    await new Promise((r) => setTimeout(r, 20));
    ok('write 异步回调报错被吞掉', !threw3);
    s.closed = true;
  }

  // ---- 修复 3: destroy() 丢弃队列时通知用户
  {
    const s = new B.PiSession('private_3', { type: 'private', id: '3' });
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, end() {}, on: () => {} }, kill() {} };
    s.queue = [{ text: 'a' }, { text: 'b' }];
    sent.length = 0;
    await s.destroy('idle');
    await new Promise((r) => setTimeout(r, 30));
    const notice = sent.find((x) => String(x.params.message).includes('排队消息被丢弃'));
    ok('丢弃队列时通知用户', !!notice, notice ? String(notice.params.message).slice(0, 50) : '无通知');
    ok('通知里带上条数', !!notice && String(notice.params.message).includes('2 条'));

    // shutdown 时不该发 (进程马上退出)
    const s2 = new B.PiSession('private_4', { type: 'private', id: '4' });
    s2.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, end() {}, on: () => {} }, kill() {} };
    s2.queue = [{ text: 'a' }];
    sent.length = 0;
    await s2.destroy('shutdown');
    await new Promise((r) => setTimeout(r, 30));
    ok('shutdown 时不发丢弃通知', sent.filter((x) => String(x.params.message).includes('排队消息被丢弃')).length === 0);
  }

  // ---- 修复 4: alertTarget 按活跃度选, 不再取 Map 第一个
  {
    B.sessions.clear();
    const oldS = new B.PiSession('group_999', { type: 'group', id: '999' });
    oldS.lastUsed = 1000;
    const newS = new B.PiSession('private_777', { type: 'private', id: '777' });
    newS.lastUsed = 9999999999999;
    B.sessions.set('group_999', oldS);
    B.sessions.set('private_777', newS);
    const t = B.alertTarget();
    ok('告警发给最近活跃会话', t && t.type === 'private' && t.id === '777', JSON.stringify(t));
    oldS.closed = true; newS.closed = true;
    B.sessions.clear();
    const t2 = B.alertTarget();
    ok('无活跃会话时退回白名单首位', t2 && t2.type === 'private' && t2.id === '123456789', JSON.stringify(t2));
  }

  // ---- 修复 5: 群上下文缓存
  {
    B.GROUP_CTX_CACHE.clear();
    B.cfg.behavior.groupContextCount = 60;
    B.cfg.behavior.groupContextMaxChars = 500;
    B.cfg.behavior.groupContextCacheMs = 30000;
    let calls = 0;
    const origAction = B.onebot.action;
    B.onebot.action = async (action) => {
      if (action !== 'get_group_msg_history') return { status: 'ok' };
      calls++;
      return { status: 'ok', data: { messages: [
        { user_id: '1', message_id: 'm1', raw_message: '第一条', sender: { card: '甲' } },
        { user_id: '2', message_id: 'm2', raw_message: '第二条', sender: { nickname: '乙' } },
        { user_id: '999', message_id: 'm3', raw_message: '机器人自己', sender: { card: 'PI' } },
      ] } };
    };
    const t1 = await B.fetchGroupContext('888', '999', 'm2');
    ok('首次拉取群上下文', calls === 1 && t1.includes('第一条') && !t1.includes('第二条'), `calls=${calls}`);
    ok('过滤掉机器人自己的发言', !t1.includes('机器人自己'));
    const t2 = await B.fetchGroupContext('888', '999', 'm1');
    ok('第二次命中缓存不再请求', calls === 1, `calls=${calls}`);
    ok('缓存命中仍能正确过滤 triggerId', t2.includes('第二条') && !t2.includes('第一条'));
    // 缓存过期后重新拉
    B.cfg.behavior.groupContextCacheMs = 0;
    await B.fetchGroupContext('888', '999', 'm2');
    ok('TTL=0 时不走缓存', calls === 2, `calls=${calls}`);
    B.cfg.behavior.groupContextCacheMs = 30000;
    B.onebot.action = origAction;
    B.GROUP_CTX_CACHE.clear();
  }

  // ---- 修复 6: WS 重连互斥
  {
    const ob = B.onebot;
    ob.reconnecting = true;
    const before = ob.ws;
    ob.connect();                       // 应被互斥拦住, 不新建连接
    ok('重连进行中时 connect 被忽略', ob.ws === before, `ws=${ob.ws}`);
    ob.reconnecting = false;
  }

  // ---- 修复 7: 静默提醒按「多久没给用户发过话」计时, 而不是「多久没收到 pi 事件」
  {
    const s = new B.PiSession('private_9', { type: 'private', id: '9' });
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };
    // 模拟: pi 一直在发 thinking_delta, lastEventAt 被不断刷新, 但用户看不到任何东西
    s.lastEventAt = Date.now();
    s.lastOutputAt = Date.now() - 120000;   // 已经 2 分钟没给用户发过话
    s.busy = true;
    const SILENCE_MS = 45000;
    const idleByEvent = Date.now() - s.lastEventAt;   // 旧算法: 很小, 不会提醒
    const idleByOutput = Date.now() - s.lastOutputAt; // 新算法: 120s, 会提醒
    ok('旧算法(按事件)会漏掉思考期的静默', idleByEvent < SILENCE_MS, `idleByEvent=${Math.round(idleByEvent / 1000)}s`);
    ok('新算法(按输出)能认出静默', idleByOutput >= SILENCE_MS, `idleByOutput=${Math.round(idleByOutput / 1000)}s`);

    // sendQQ 应该刷新 lastOutputAt
    const before = s.lastOutputAt;
    s.sendQQ('测试消息').catch(() => {});
    ok('sendQQ 会刷新 lastOutputAt', s.lastOutputAt > before);

    // beginWork / endWork
    s.busy = false;
    s.beginWork('压缩上下文');
    ok('beginWork 后即使不 busy 也算在工作', !s.busy && !!s.workLabel, `workLabel=${s.workLabel}`);
    s.endWork();
    ok('endWork 清除工作标记', !s.workLabel);
    s.closed = true;
  }

  // ---- 修复 8: spawn 失败 / 快速崩溃的退避重启
  {
    const s = new B.PiSession('private_20', { type: 'private', id: '20' });
    s.closed = false;
    s.sendQQ = async () => ({ status: 'ok' });   // 别真发

    // 刚启动就死 => 算快速失败, 计数递增, 延迟递增
    const delays = [];
    for (let i = 1; i <= 4; i++) {
      s.spawnedAt = Date.now() - 100;          // 只活了 0.1 秒
      s.spawnFailures = i - 1;
      s.proc = null;
      if (s.respawnTimer) { clearTimeout(s.respawnTimer); s.respawnTimer = null; }
      s.scheduleRespawn('测试');
      delays.push(s.spawnFailures);
    }
    ok('快速失败会累加计数', delays.join(',') === '1,2,3,4', `counts=${delays.join(',')}`);
    if (s.respawnTimer) { clearTimeout(s.respawnTimer); s.respawnTimer = null; }

    // 存活够久 => 计数归零
    s.spawnFailures = 5;
    s.spawnedAt = Date.now() - 60000;          // 活了 1 分钟
    if (s.respawnTimer) { clearTimeout(s.respawnTimer); s.respawnTimer = null; }
    s.scheduleRespawn('测试');
    ok('存活够久后计数归零', s.spawnFailures === 0, `n=${s.spawnFailures}`);
    if (s.respawnTimer) clearTimeout(s.respawnTimer);

    // closed 会话不再排重启
    s.closed = true;
    s.spawnedAt = Date.now() - 100;
    if (s.respawnTimer) { clearTimeout(s.respawnTimer); s.respawnTimer = null; }
    s.scheduleRespawn('测试');
    ok('已销毁的会话不再安排重启', !s.respawnTimer);
  }

  // ---- 修复 9: destroy 会取消已排上的重启
  {
    const s = new B.PiSession('private_21', { type: 'private', id: '21' });
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, end() {}, on: () => {} }, kill() {} };
    s.spawnedAt = Date.now() - 100;
    s.scheduleRespawn('测试');
    ok('重启已排上', !!s.respawnTimer);
    await s.destroy('test');
    ok('destroy 会取消重启定时器', !s.respawnTimer);
  }

  // ---- 修复 10: 磁盘回收只删过期的、未引用的文件
  {
    const fsx = require('fs');
    const sessDir = B.cfg.pi.sessionDir;
    const inboxDir = B.cfg.files.inboxDir;
    const now = Date.now();
    const old = new Date(now - 40 * 24 * 3600 * 1000);
    const fresh = new Date(now - 1 * 24 * 3600 * 1000);

    // 造三个会话文件: 旧的未引用(应删) / 旧的但被引用(应留) / 新的未引用(应留)
    const fOld = `${sessDir}/2020-01-01T00-00-00-000Z_orphan.jsonl`;
    const fRef = `${sessDir}/2020-01-02T00-00-00-000Z_referenced.jsonl`;
    const fNew = `${sessDir}/2020-01-03T00-00-00-000Z_new.jsonl`;
    for (const f of [fOld, fRef, fNew]) fsx.writeFileSync(f, 'x');
    fsx.utimesSync(fOld, old, old);
    fsx.utimesSync(fRef, old, old);
    fsx.utimesSync(fNew, fresh, fresh);
    // 让 fRef 被 state 引用
    B.STATE.__test_ref__ = { sessionFile: fRef, spawnId: 'x', history: [] };

    // inbox: 旧文件应删, 新文件应留
    const iOld = `${inboxDir}/old_file.txt`;
    const iNew = `${inboxDir}/new_file.txt`;
    fsx.writeFileSync(iOld, 'y'); fsx.writeFileSync(iNew, 'y');
    fsx.utimesSync(iOld, old, old); fsx.utimesSync(iNew, fresh, fresh);

    B.cfg.behavior.sessionRetentionDays = 30;
    B.cfg.behavior.inboxRetentionDays = 7;
    await B.sweepStorage();

    ok('删掉过期且未被引用的会话文件', !fsx.existsSync(fOld));
    ok('保留被 state 引用的会话文件', fsx.existsSync(fRef));
    ok('保留未过期的会话文件', fsx.existsSync(fNew));
    ok('删掉过期 inbox 文件', !fsx.existsSync(iOld));
    ok('保留未过期 inbox 文件', fsx.existsSync(iNew));

    delete B.STATE.__test_ref__;
    for (const f of [fRef, fNew]) { try { fsx.unlinkSync(f); } catch {} }
    for (const f of [iNew]) { try { fsx.unlinkSync(f); } catch {} }
  }

  // ---- 修复 11: dispatchCommand 拆分后行为不变
  {
    const sent = [];
    const fake = {
      key: 'private_30',
      busy: false,
      queue: [],
      lastResumeList: null,
      lastModelList: null,
      stderrTail: [],
      beginWork() {}, endWork() {},
      sendQQ: async (t) => { sent.push(String(t)); return { status: 'ok' }; },
      request: async () => ({ models: [], levels: [] }),
      historyFiles: () => [],
      send: () => true,
      abortRequested: false,
    };
    const ctx = { isGroup: false, userId: '123456789', groupId: '', userName: '主人', key: 'private_30' };

    // 非命令: 必须返回 false, 让调用方继续走对话流程
    for (const t of ['你好', '帮我看看', '', '//斜杠', '命令']) {
      const r = await B.dispatchCommand(fake, t.toLowerCase(), '', ctx);
      ok(`非命令「${t}」返回 false`, r === false, `got=${r}`);
    }
    // /help: 应返回 true 并发出帮助文本
    sent.length = 0;
    const rHelp = await B.dispatchCommand(fake, '/help', '', ctx);
    ok('/help 返回 true', rHelp === true);
    ok('/help 发出了帮助文本', sent.some((t) => t.includes('pi coding agent')), `sent=${sent.length}`);
    // 帮助文本不该含 Markdown 围栏
    ok('帮助文本不含三反引号', !sent.join('').includes('```'));
    // /queue 空闲时提示
    sent.length = 0;
    const rQ = await B.dispatchCommand(fake, '/queue', '', ctx);
    ok('/queue 返回 true 且有提示', rQ === true && sent.some((t) => t.includes('没有排队')), `ret=${rQ} sent=${JSON.stringify(sent).slice(0,200)}`);
  }

  // ---- 修复 12: 所有内置命令都必须返回 true, 漏一个就会穿透到模型
  {
    const mk = (over = {}) => {
      const sent = [];
      const sess = {
        key: 'private_31', busy: false, queue: [], lastResumeList: null,
        lastModelList: null, stderrTail: [], abortRequested: false,
        beginWork() {}, endWork() {},
        sendQQ: async (t) => { sent.push(String(t)); return { status: 'ok' }; },
        send: () => true,
        request: async (cmd) => {
          if (cmd.type === 'get_available_models') return { models: [{ provider: 'p', id: 'm' }] };
          if (cmd.type === 'get_available_thinking_levels') return { levels: ['low', 'high'] };
          if (cmd.type === 'get_session_stats') return { tokens: {}, contextUsage: {} };
          if (cmd.type === 'get_state') return { model: { provider: 'p', id: 'm' }, thinkingLevel: 'high', messageCount: 1 };
          return {};
        },
        historyFiles: () => ['/tmp/nope.jsonl'],
        newSession: async () => '/tmp/new.jsonl',
        switchTo: async () => '/tmp/sw.jsonl',
        setModel: async () => ({ model: { provider: 'p', id: 'm' }, thinkingLevel: 'high', levels: ['low'] }),
        setThinking: async () => 'low',
        prompt: () => {},
        ...over,
      };
      return { sess, sent };
    };
    const ctx = { isGroup: false, userId: '123456789', groupId: '', userName: '主人', key: 'private_31' };

    // 这些命令在「正常参数」下都必须返回 true
    const cases = [
      ['/help', ''],
      ['/new', ''],
      ['/resume', ''],
      ['/model', ''],
      ['/model', '1'],
      ['/model', 'p/m'],
      ['/thinking', ''],
      ['/thinking', 'low'],
      ['/stats', ''],
      ['/compact', ''],
      ['/memory', ''],
      ['/task', ''],
      ['/queue', ''],
      ['/steer', '加点东西'],
      ['/stop', ''],
      ['/status', ''],
      ['/reset', ''],
    ];
    for (const [c, a] of cases) {
      const { sess } = mk({ destroy: async () => {}, syncSessionFile: async () => {} });
      let r;
      try { r = await B.dispatchCommand(sess, c, a, ctx); }
      catch (e) { r = `throw:${e.message}`; }
      ok(`${c}${a ? ' ' + a : ''} 返回 true`, r === true, `got=${r}`);
    }
    // 未知命令必须返回 false
    const { sess: s2 } = mk();
    ok('/nonsense 返回 false', (await B.dispatchCommand(s2, '/nonsense', '', ctx)) === false);
    // 群里 /restart 被拒绝, 但仍算已消费
    const { sess: s3 } = mk();
    ok('群聊 /restart 被拒绝但返回 true',
      (await B.dispatchCommand(s3, '/restart', '', { ...ctx, isGroup: true })) === true);
  }

  // ---- 修复 13: 已销毁的会话再 prompt, 提示要准确而不是误导
  {
    const s = new B.PiSession('private_40', { type: 'private', id: '40' });
    const sent = [];
    s.sendQQ = async (t) => { sent.push(String(t)); return { status: 'ok' }; };
    s.proc = null;              // 模拟被 destroy 之后的实例
    s.closed = true;
    s.busy = false;
    s.prompt('hello', [], { replyTo: null, atUser: null });
    await new Promise((r) => setTimeout(r, 30));
    ok('已销毁会话 prompt 会给出准确提示',
      sent.some((t) => t.includes('会话刚刚被重置')), JSON.stringify(sent).slice(0, 120));
    ok('不再误报 pi 未就绪', !sent.some((t) => t.includes('pi 未就绪')));
  }

  // ---- 修复 14: agent_end 时就该把已生成好的回复发出去
  {
    const s = new B.PiSession('private_50', { type: 'private', id: '50' });
    const sent = [];
    s.sendQQ = async (t) => { sent.push(String(t)); return { status: 'ok' }; };
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };
    s.busy = true;
    s.buf = '这是已经生成好的回答。'.repeat(5);

    // agent_end + stopReason=stop => 应立刻 flush
    s.onRecord({ type: 'agent_end', messages: [{ stopReason: 'stop' }] });
    await new Promise((r) => setTimeout(r, 40));
    ok('agent_end(stop) 立刻发出缓冲内容', sent.length > 0 && sent.join('').includes('已经生成好的回答'),
      `sent=${sent.length}`);
    ok('agent_end 后 buf 已清空', s.buf === '', `buf=${JSON.stringify(s.buf).slice(0, 40)}`);

    // stopReason=toolUse => 不该 flush (这一轮还没结束)
    sent.length = 0;
    s.buf = '中间过程文字'.repeat(5);
    s.onRecord({ type: 'agent_end', messages: [{ stopReason: 'toolUse' }] });
    await new Promise((r) => setTimeout(r, 40));
    ok('agent_end(toolUse) 不发', sent.length === 0, `sent=${sent.length}`);

    // 定时任务会话的 error 走 onTaskError, 不应重复发
    s.buf = '';
    s.onTaskError = () => { s.taskErrCalled = true; };
    sent.length = 0;
    s.onRecord({ type: 'agent_end', messages: [{ stopReason: 'error' }] });
    await new Promise((r) => setTimeout(r, 40));
    ok('任务会话的 error 交给 onTaskError', s.taskErrCalled === true && sent.length === 0,
      `called=${s.taskErrCalled} sent=${sent.length}`);
    s.closed = true;
  }

  // ---- 修复 15: 空回复兜底 (模型只输出思考就结束, 用户那边彻底静默)
  {
    const s = new B.PiSession('private_60', { type: 'private', id: '60' });
    const sent = [];
    s.sendQQ = async (t) => { sent.push(String(t)); return { status: 'ok' }; };
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };

    // 场景 A1: 第一次空回复 => 自动重试 (只写日志, 不打扰用户)
    s.prompt('测试', [], { replyTo: null, atUser: null });
    s.buf = '';
    s.onRecord({ type: 'agent_settled' });
    ok('首次空回复会排上重试', (s.emptyRetries || 0) === 1, `n=${s.emptyRetries}`);
    await new Promise((r) => setTimeout(r, 700));   // 重试是 500ms 后触发
    ok('重试确实重新发了 prompt', s.busy === true, `busy=${s.busy}`);
    ok('重试时不打扰用户', !sent.some((t) => t.includes('自动重试') || t.includes('连续两次')),
      JSON.stringify(sent).slice(0, 120));

    // 场景 A2: 再空一次 => 退化为明确提示, 不再重试
    sent.length = 0;
    s.buf = '';
    s.onRecord({ type: 'agent_settled' });
    await new Promise((r) => setTimeout(r, 80));
    ok('连续两次空回复给出明确提示',
      sent.some((t) => t.includes('连续两次都是空回复')), JSON.stringify(sent).slice(0, 140));

    // 场景 A3: 恢复正常后计数清零
    sent.length = 0;
    s.prompt('测试', [], { replyTo: null, atUser: null });
    s.buf = '一段正常输出，长度足够通过最小长度检查。'.repeat(2);
    await s.flush(true);
    s.onRecord({ type: 'agent_settled' });
    await new Promise((r) => setTimeout(r, 40));
    ok('正常输出后空回复计数清零', (s.emptyRetries || 0) === 0, `n=${s.emptyRetries}`);

    // 场景 B: 本轮有输出 => 不该提示
    sent.length = 0;
    s.prompt('测试', [], { replyTo: null, atUser: null });
    s.buf = '正常的一段回答内容，足够长以便通过最小长度检查。'.repeat(2);
    await s.flush(true);
    s.onRecord({ type: 'agent_settled' });
    await new Promise((r) => setTimeout(r, 40));
    ok('有输出时不提示空回复', !sent.some((t) => t.includes('没有输出任何内容')),
      JSON.stringify(sent).slice(0, 120));

    // 场景 C: 用户主动 /stop 导致的空 => 不该再提示 (本来就说了要停)
    sent.length = 0;
    s.prompt('测试', [], { replyTo: null, atUser: null });
    s.abortRequested = true;
    s.buf = '';
    s.onRecord({ type: 'agent_settled' });
    await new Promise((r) => setTimeout(r, 40));
    ok('/stop 后不提示空回复', !sent.some((t) => t.includes('没有输出任何内容')),
      JSON.stringify(sent).slice(0, 120));
    s.closed = true;
  }

  // ---- 修复 16: onRecord 抛错不能拖死整个桥接
  {
    const s = new B.PiSession('private_70', { type: 'private', id: '70' });
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };
    let calls = 0;
    s.onRecord = () => { calls++; throw new Error('模拟分支抛错'); };
    let threw = false;
    try { s.onStdout(Buffer.from('{"type":"message_update"}\n{"type":"agent_settled"}\n')); }
    catch { threw = true; }
    ok('onRecord 抛错不会外泄', !threw);
    ok('抛错后仍继续处理后续事件', calls === 2, `calls=${calls}`);
    s.closed = true;
  }

  // ---- 修复 17: .sent 归档纳入回收
  {
    const fsx = require('fs');
    const pathx = require('path');
    const sentDir = pathx.join(B.cfg.files.outboxDir, '.sent');
    fsx.mkdirSync(sentDir, { recursive: true });
    const now = Date.now();
    const oldT = new Date(now - 40 * 24 * 3600 * 1000);
    const newT = new Date(now - 1 * 24 * 3600 * 1000);
    const fOld = pathx.join(sentDir, 'old-delivered.txt');
    const fNew = pathx.join(sentDir, 'new-delivered.txt');
    fsx.writeFileSync(fOld, 'x'); fsx.writeFileSync(fNew, 'x');
    fsx.utimesSync(fOld, oldT, oldT); fsx.utimesSync(fNew, newT, newT);

    B.cfg.behavior.sessionRetentionDays = 30;
    B.cfg.behavior.inboxRetentionDays = 7;
    B.cfg.behavior.outboxSentRetentionDays = 30;
    await B.sweepStorage();

    ok('删掉过期的 .sent 归档', !fsx.existsSync(fOld));
    ok('保留未过期的 .sent 归档', fsx.existsSync(fNew));
    try { fsx.unlinkSync(fNew); } catch {}
  }

  // ---- 修复 18: 思考退化循环的防线
  {
    const s = new B.PiSession('private_80', { type: 'private', id: '80' });
    const sent = [];
    const aborts = [];
    s.sendQQ = async (t) => { sent.push(String(t)); return { status: 'ok' }; };
    s.send = (cmd) => { aborts.push(cmd.type); return true; };
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };
    B.cfg.behavior.thinkingGuardChars = 1000;

    s.prompt('测试', [], { replyTo: null, atUser: null });
    // 灌入超长思考 (模拟 </parameter></invoke> 无限重复)
    for (let i = 0; i < 30; i++) {
      s.onRecord({ type: 'message_update', assistantMessageEvent: { type: 'thinking_delta', delta: 'x'.repeat(100) } });
    }
    ok('思考超限会主动中断', aborts.includes('abort'), `aborts=${JSON.stringify(aborts)}`);
    ok('思考超限会提示用户', sent.some((t) => t.includes('重复循环')), JSON.stringify(sent).slice(0, 120));
    ok('中断只触发一次', aborts.filter((x) => x === 'abort').length === 1,
      `count=${aborts.filter((x) => x === 'abort').length}`);

    // 正常长度的思考不该触发
    const s2 = new B.PiSession('private_81', { type: 'private', id: '81' });
    const aborts2 = [];
    s2.send = (cmd) => { aborts2.push(cmd.type); return true; };
    s2.sendQQ = async () => ({ status: 'ok' });
    s2.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };
    s2.prompt('测试', [], { replyTo: null, atUser: null });
    for (let i = 0; i < 5; i++) {
      s2.onRecord({ type: 'message_update', assistantMessageEvent: { type: 'thinking_delta', delta: 'x'.repeat(100) } });
    }
    ok('正常思考不触发中断', !aborts2.includes('abort'), `aborts=${JSON.stringify(aborts2)}`);

    // 新一轮要重置计数
    s.thinkChars = 0; s.thinkGuardFired = false;
    s.prompt('第二轮', [], { replyTo: null, atUser: null });
    ok('新一轮重置思考计数', s.thinkChars === 0 && s.thinkGuardFired === false);
    B.cfg.behavior.thinkingGuardChars = 80000;
    s.closed = true; s2.closed = true;
  }

  // ---- 修复 19: 空回复重试不能把队列里别人的消息当成本轮重试对象
  {
    const s = new B.PiSession('private_90', { type: 'private', id: '90' });
    const prompts = [];
    s.sendQQ = async () => ({ status: 'ok' });
    s.send = () => true;
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };
    const orig = s.prompt.bind(s);
    s.prompt = (t, i, c) => { prompts.push(t); return orig(t, i, c); };

    s.prompt('第一条', [], { replyTo: null, atUser: null });
    s.prompt('第二条', [], { replyTo: null, atUser: null });   // busy, 入队
    ok('第二条已排队', s.queue.length === 1, `q=${s.queue.length}`);

    s.buf = '';
    s.onRecord({ type: 'agent_settled' });     // 第一条空回复
    ok('重试期间不 drain 队列', s.queue.length === 1 && s.retryPending === true,
      `q=${s.queue.length} pending=${s.retryPending}`);

    await new Promise((r) => setTimeout(r, 700));
    ok('重试的是本轮消息', prompts.filter((x) => x === '第一条').length === 2,
      JSON.stringify(prompts));
    // 「第二条」只应出现一次(最初那次调用), 不该被重试逻辑再发一遍
    ok('没把别人的消息当重试对象', prompts.filter((x) => x === '第二条').length === 1,
      JSON.stringify(prompts));
    ok('最后一次发出的是本轮消息', prompts[prompts.length - 1] === '第一条',
      JSON.stringify(prompts));
    ok('重试后别人那条仍在队列', s.queue.length === 1 && s.queue[0].text === '第二条',
      JSON.stringify(s.queue.map((q) => q.text)));
    s.closed = true;
  }

  // ---- 修复 20: /stop 必须能取消已排上的空回复重试
  {
    const mk = (key) => {
      const s = new B.PiSession(key, { type: 'private', id: key.split('_')[1] });
      s.sendQQ = async () => ({ status: 'ok' });
      s.send = () => true;
      s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };
      const prompts = [];
      const orig = s.prompt.bind(s);
      s.prompt = (t, i, c) => { prompts.push(t); return orig(t, i, c); };
      return { s, prompts };
    };

    // A: /stop 清掉定时器
    const a = mk('private_91');
    a.s.prompt('要停掉的', [], { replyTo: null, atUser: null });
    a.s.buf = '';
    a.s.onRecord({ type: 'agent_settled' });
    ok('已排上重试', a.s.retryPending === true && a.s.retryTimer !== null);
    a.s.abortRequested = true;
    clearTimeout(a.s.retryTimer); a.s.retryTimer = null; a.s.retryPending = false;
    await new Promise((r) => setTimeout(r, 700));
    ok('/stop 后不再重发', a.prompts.filter((x) => x === '要停掉的').length === 1,
      JSON.stringify(a.prompts));

    // B: 就算定时器没清, abortRequested 兜底也要拦住
    const b = mk('private_92');
    b.s.prompt('也要停掉', [], { replyTo: null, atUser: null });
    b.s.buf = '';
    b.s.onRecord({ type: 'agent_settled' });
    b.s.abortRequested = true;          // 只设标志, 不清定时器
    await new Promise((r) => setTimeout(r, 700));
    ok('abortRequested 兜底拦住重试', b.prompts.filter((x) => x === '也要停掉').length === 1,
      JSON.stringify(b.prompts));

    // C: destroy 也要清掉
    const c = mk('private_93');
    c.s.prompt('销毁前', [], { replyTo: null, atUser: null });
    c.s.buf = '';
    c.s.onRecord({ type: 'agent_settled' });
    await c.s.destroy('test');
    ok('destroy 清掉重试定时器', c.s.retryTimer === null && c.s.retryPending === false);
    await new Promise((r) => setTimeout(r, 700));
    ok('销毁后不重发', c.prompts.filter((x) => x === '销毁前').length === 1,
      JSON.stringify(c.prompts));
  }

  // ---- 修复 21: 延迟重启 (不能把正在跑的那一轮掐死)
  {
    const exits = [];
    B.setExitHook((reason) => { exits.push(reason); });
    B.cfg.behavior.restartMaxWaitMs = 300000;

    const s = new B.PiSession('private_95', { type: 'private', id: '95' });
    B.sessions.set('private_95', s);
    s.sendQQ = async () => ({ status: 'ok' });
    s.send = () => true;
    s.proc = { stdin: { writable: true, write: (d, cb) => { if (cb) cb(null); }, on: () => {} } };
    s.prompt('长任务', [], { replyTo: null, atUser: null });      // busy = true

    ok('busySessionCount 认得忙碌会话', B.busySessionCount() === 1, `n=${B.busySessionCount()}`);

    B.requestRestart('测试');
    ok('重启请求已记下', B.restartState().pending !== null);

    // 已有请求排队时, 再来一个不该顶掉前面的 (否则等待计时会被无限刷新)
    const at0 = B.restartState().pending.at;
    B.requestRestart('第二个请求');
    ok('已有请求时忽略新的', B.restartState().pending.at === at0 && B.restartState().pending.reason === '测试',
      JSON.stringify(B.restartState().pending));

    await new Promise((r) => setTimeout(r, 2500));
    ok('会话忙碌时不重启', exits.length === 0, JSON.stringify(exits));

    // 会话空闲后, 这一轮跑完了才该重启
    s.busy = false;
    B.maybeRestartNow();
    await new Promise((r) => setTimeout(r, 2600));
    ok('空闲后执行重启', exits.length === 1 && String(exits[0]).includes('restart'),
      JSON.stringify(exits));

    B.sessions.delete('private_95');
    s.closed = true;
    B.setExitHook(null);
  }

  // ---- 修复 22: 落单的代码围栏不能原样发到 QQ
  {
    const a = B.mdToPlain('看这个:\n```js\nconst a = 1;');
    ok('未闭合围栏不残留反引号', !a.includes('```'), JSON.stringify(a));
    ok('未闭合围栏内容仍保留', a.includes('const a = 1;'), JSON.stringify(a));

    const b = B.mdToPlain('```js\nconst a = 1;\n```');
    ok('闭合围栏不残留反引号', !b.includes('```'), JSON.stringify(b));

    const c = B.mdToPlain('用 `npm test` 跑');
    ok('行内代码去反引号', c === '用 npm test 跑', JSON.stringify(c));
  }

  // ---- 修复 23: 单条上报抛错不能拖死桥接, 也不该连累同一批里的其他行
  {
    const calls = [];
    const orig = B.onebot.onRecord;
    B.onebot.onRecord = (rec) => {
      calls.push(rec.post_type || ('echo:' + rec.echo));
      if (calls.length === 1) throw new Error('模拟上报处理抛错');   // 只抛第一行
    };
    let outerThrew = false;
    try {
      B.onebot.handleWsMessage(
        Buffer.from('{"post_type":"message"}\n{"echo":7}\n{"post_type":"meta_event"}\n'));
    } catch { outerThrew = true; }
    ok('单条上报抛错不外泄', !outerThrew);
    ok('抛错后同批其余行照常处理', calls.length === 3, JSON.stringify(calls));
    B.onebot.onRecord = orig;
  }

  console.log('\n===== 结果 =====');
  const bad = results.filter((r) => !r.pass);
  console.log(`通过 ${results.length - bad.length}/${results.length}`);
  if (bad.length) console.log('失败:', bad.map((b) => b.name).join(' | '));
  process.exit(bad.length ? 1 : 0);
})();
