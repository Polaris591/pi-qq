#!/usr/bin/env node
'use strict';
/**
 * 集成测试: 起一个假 NapCat + 真 bridge 子进程, 端到端验证。
 *
 * 与 utest.js 的分工:
 *   utest.js  —— 纯函数级, 不联网、不启进程, 快 (200+ 条)
 *   itest.js  —— 集成级, 走完整链路, 慢但真实
 *
 * 默认不启真 pi (太慢), 只测 bridge 自身的协议/路由/记忆/文件逻辑。
 * 需要真 pi 的用例用 --with-pi 打开。
 *
 * 用法:
 *   node itest.js              只跑不需要 pi 的用例
 *   node itest.js --with-pi    连真 pi 一起跑 (慢, 需要模型可用)
 *   node itest.js --keep       保留临时目录, 方便排查
 */

const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const WITH_PI = process.argv.includes('--with-pi');
const KEEP = process.argv.includes('--keep');

const ROOT = __dirname;
const WORK = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-qq-itest-'));

// ---------------------------------------------------------------- 测试框架

const results = [];
function ok(name, pass, extra = '') {
  results.push({ name, pass });
  const tag = pass ? '\x1b[32mPASS\x1b[0m' : '\x1b[31mFAIL\x1b[0m';
  console.log(`${tag}  ${name}${extra ? `  \x1b[90m${extra}\x1b[0m` : ''}`);
}
function section(title) {
  console.log(`\n\x1b[36m── ${title} ──\x1b[0m`);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- 假 NapCat

/**
 * 假 NapCat: 一个 WS 服务端, 扮演 OneBot 实现。
 * 可以通过 setHandler 覆盖某个 action 的返回, 用来构造各种场景。
 */
class FakeNapCat {
  constructor(port) {
    this.port = port;
    this.sent = [];          // 记录 bridge 发出来的所有 action
    this.handlers = new Map();
    this.connections = 0;
    this.log = [];
    this.wss = new WebSocketServer({ port });
    this.wss.on('connection', (ws) => this.onConn(ws));
  }

  onConn(ws) {
    this.connections++;
    // 连上就发 lifecycle, 让 bridge 认为已就绪
    ws.send(JSON.stringify({
      post_type: 'meta_event', meta_event_type: 'lifecycle', sub_type: 'connect',
    }));
    ws.on('message', (d) => {
      for (const line of d.toString().split('\n')) {
        if (!line.trim()) continue;
        let r; try { r = JSON.parse(line); } catch { continue; }
        this.onAction(ws, r);
      }
    });
  }

  async onAction(ws, r) {
    const reply = (data) => {
      try { ws.send(JSON.stringify({ status: 'ok', retcode: 0, echo: r.echo, data })); } catch {}
    };
    // 自定义 handler 优先。约定: handler 直接返回「data 内容」(不用自己包 data 字段)。
    const h = this.handlers.get(r.action);
    if (h) {
      try {
        const out = await h(r.params, r);
        if (out !== undefined) reply(out);
      } catch (e) {
        try { ws.send(JSON.stringify({ status: 'failed', retcode: 1400, echo: r.echo, message: e.message })); } catch {}
      }
      return;
    }
    // 默认行为
    switch (r.action) {
      case 'get_login_info': return reply({ user_id: 10000, nickname: 'PI' });
      case 'get_msg': return reply(null);
      case 'get_group_msg_history': return reply({ messages: [] });
      case 'get_group_member_info': return reply({ data: { card: '', nickname: `用户${r.params.user_id}` } });
      case 'get_forward_msg': return reply({ messages: [] });
      case 'send_private_msg':
      case 'send_group_msg':
        this.sent.push({ action: r.action, params: r.params, at: Date.now() });
        return reply({ message_id: this.sent.length });
      default:
        this.sent.push({ action: r.action, params: r.params, at: Date.now() });
        return reply({});
    }
  }

  setHandler(action, fn) { this.handlers.set(action, fn); }

  /** 模拟 QQ 侧上报一条消息 */
  emit(rec) {
    for (const c of this.wss.clients) {
      if (c.readyState === 1) c.send(JSON.stringify(rec));
    }
  }

  /** 上报一条私聊消息 */
  sayPrivate(text, opts = {}) {
    this.emit({
      post_type: 'message', message_type: 'private', self_id: 10000,
      user_id: opts.userId ?? 123456789,
      message_id: opts.messageId ?? Math.floor(Math.random() * 1e9),
      sender: { user_id: opts.userId ?? 123456789, nickname: opts.nickname || '主人' },
      raw_message: text,
      message: opts.message || [{ type: 'text', data: { text } }],
    });
  }

  /** 上报一条群聊消息 (默认 @ 了机器人) */
  sayGroup(text, opts = {}) {
    const segs = opts.message || [
      { type: 'at', data: { qq: '10000' } },
      { type: 'text', data: { text: ` ${text}` } },
    ];
    this.emit({
      post_type: 'message', message_type: 'group', self_id: 10000,
      group_id: opts.groupId ?? 10001,
      user_id: opts.userId ?? 111,
      message_id: opts.messageId ?? Math.floor(Math.random() * 1e9),
      sender: { user_id: opts.userId ?? 111, card: opts.card || '小明', nickname: opts.card || '小明' },
      raw_message: segs.map((s) => (s.type === 'text' ? s.data.text : `[CQ:${s.type}]`)).join(''),
      message: segs,
    });
  }

  /** 取出 bridge 发到某个会话的文本 */
  texts(filter = {}) {
    return this.sent
      .filter((s) => (filter.action ? s.action === filter.action : true))
      .filter((s) => (filter.groupId ? String(s.params.group_id) === String(filter.groupId) : true))
      .filter((s) => (filter.userId ? String(s.params.user_id) === String(filter.userId) : true))
      .map((s) => this.textOf(s.params));
  }

  textOf(params) {
    const m = params.message;
    if (typeof m === 'string') return m;
    return (m || []).filter((s) => s.type === 'text').map((s) => s.data.text).join('');
  }

  close() {
    try { this.wss.close(); } catch {}
  }
}

// ---------------------------------------------------------------- 启动 bridge

const FAKE_PI = path.join(__dirname, 'fixtures', 'fake-pi.js');
const FAKE_PI_LOG = path.join(os.tmpdir(), 'pi-qq-fakepi-prompts.jsonl');

function makeConfig(port, extra = {}) {
  const cfg = {
    napcat: { url: `ws://127.0.0.1:${port}`, token: '' },
    pi: {
      bin: FAKE_PI,
      cwd: path.join(WORK, 'workspace'),
      sessionDir: path.join(WORK, 'sessions'),
      provider: '', model: '', tools: '', extraArgs: [],
    },
    access: {
      privateWhitelist: ['123456789'],
      groupWhitelist: ['10001'],
      allowAllPrivate: false, allowAllGroups: false, requireAtInGroup: true,
    },
    behavior: {
      startupNotice: false, heartbeatTimeoutMs: 0, groupContextCount: 0,
      emojiReaction: false, progressOnToolCall: false, turnTimeoutMs: 0,
      markdownToPlain: true, maxChars: 2500, idleTimeoutMs: 60 * 60 * 1000,
    },
    files: {
      outboxDir: path.join(WORK, 'outbox'),
      containerOutbox: path.join(WORK, 'outbox'),
      inboxDir: path.join(WORK, 'inbox'),
      stateDir: WORK,
    },
    memory: { dir: path.join(WORK, 'memory') },
    tasks: { dir: path.join(WORK, 'tasks') },
    persona: { text: '', groupBoundary: '' },
  };
  return deepMerge(cfg, extra);
}

function deepMerge(a, b) {
  const out = { ...a };
  for (const [k, v] of Object.entries(b || {})) {
    out[k] = (v && typeof v === 'object' && !Array.isArray(v) && a[k] && typeof a[k] === 'object')
      ? deepMerge(a[k], v) : v;
  }
  return out;
}

function startBridge(cfg) {
  try { fs.writeFileSync(FAKE_PI_LOG, ''); } catch {}
  const cfgPath = path.join(WORK, 'config.json');
  fs.writeFileSync(cfgPath, JSON.stringify(cfg, null, 2));
  const child = spawn(process.execPath, [path.join(ROOT, 'bridge.js')], {
    env: { ...process.env, PI_QQ_CONFIG: cfgPath, FAKE_PI_LOG },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let out = '';
  child.stdout.on('data', (c) => { out += c; });
  child.stderr.on('data', (c) => { out += c; });
  return { child, getLog: () => out, cfgPath };
}

// ---------------------------------------------------------------- 用例

(async () => {
  const PORT = 39900 + Math.floor(Math.random() * 90);
  const napcat = new FakeNapCat(PORT);
  await sleep(200);

  const cfg = makeConfig(PORT);
  const { child, getLog } = startBridge(cfg);
  await sleep(2500);   // 等 WS 连上

  const alive = () => child.exitCode === null && !child.killed;
  /** 读 pi 实际收到的 prompt (不清空, 只读) */
  const piPrompts = () => {
    try {
      return fs.readFileSync(FAKE_PI_LOG, 'utf8').split('\n').filter(Boolean)
        .map((l) => { try { return JSON.parse(l); } catch { return null; } })
        .filter(Boolean);
    } catch { return []; }
  };
  /** 清空 pi 日志 (用于隔离每次断言) */
  const clearPiLog = () => { try { fs.writeFileSync(FAKE_PI_LOG, ''); } catch {} };
  /**
   * 等 pi 收到至少一条 prompt。
   * 注意: 不在这里清空日志 —— 调用方随后要读它。
   * 隔离靠调用前先 clearPiLog()。
   */
  const waitForPi = async (ms = 8000) => {
    const t0 = Date.now();
    while (Date.now() - t0 < ms) {
      if (piPrompts().length) return true;
      await sleep(200);
    }
    return false;
  };

  // ============================================================ 1. 启动与连接
  section('启动与连接');
  ok('bridge 已连上 NapCat', napcat.connections >= 1, `connections=${napcat.connections}`);
  ok('bridge 进程存活', alive());
  ok('日志有启动标记', /桥接启动/.test(getLog()));

  // ============================================================ 2. 访问控制
  section('访问控制');
  napcat.sent.length = 0;
  // 白名单外的私聊: 应该被忽略
  // 断言必须看「这一段新增的日志」, 不能扫全量: bridge 打印的是
  // \`spawn: \${cfg.pi.bin}\`, itest 里 bin 是绝对路径, /spawn: pi/ 永远不匹配,
  // 之前这条断言恒真, 等于访问控制根本没测。
  const aclLogBefore = getLog().length;
  napcat.sayPrivate('你好', { userId: 999 });
  await sleep(1200);
  const aclAdded = getLog().slice(aclLogBefore);
  ok('白名单外私聊被忽略', !aclAdded.includes('spawn:'), aclAdded.trim().slice(0, 200));

  // 群里没 @ 机器人: 应该被忽略
  const before = getLog().length;
  napcat.sayGroup('随便说句话', { message: [{ type: 'text', data: { text: '随便说句话' } }] });
  await sleep(1200);
  ok('群里未 @ 被忽略', getLog().length === before, '');

  // ============================================================ 3. 消息去重
  section('消息去重');
  napcat.setHandler('get_group_msg_history', () => ({ messages: [] }));
  const dupId = 888001;
  napcat.sayGroup('重复测试', { messageId: dupId });
  await sleep(600);
  napcat.sayGroup('重复测试', { messageId: dupId });
  await sleep(600);
  napcat.sayGroup('重复测试', { messageId: dupId });
  await sleep(1500);
  const dupLogs = (getLog().match(/\[dup\] 忽略重复消息/g) || []).length;
  ok('重复消息被识别并忽略', dupLogs >= 2, `忽略 ${dupLogs} 次`);

  // ============================================================ 4. 卡片解析
  section('卡片 / 合并转发解析');
  // 合并转发
  napcat.setHandler('get_forward_msg', () => ({
    messages: [
      { user_id: 222, sender: { card: '小红', nickname: 'hong' }, raw_message: '第一条转发',
        message: [{ type: 'text', data: { text: '第一条转发' } }] },
      { user_id: 333, sender: { card: '老王', nickname: 'wang' }, raw_message: '第二条转发',
        message: [{ type: 'text', data: { text: '第二条转发' } }] },
    ],
  }));
  clearPiLog();
  const forwardJson = JSON.stringify({
    app: 'com.tencent.multimsg',
    desc: '[聊天记录]',
    meta: { detail: { resid: 'RESID-ABC', summary: '查看2条转发消息', source: '群聊的聊天记录' } },
  });
  napcat.sayGroup('看看这个', {
    message: [
      { type: 'at', data: { qq: '10000' } },
      { type: 'json', data: { data: forwardJson } },
    ],
  });
  await waitForPi();
  const fwdPrompt = piPrompts().map((p) => p.message).join('\n');
  ok('合并转发已解析', /合并转发/.test(fwdPrompt), '');
  ok('转发内容被取出', /第一条转发/.test(fwdPrompt) || /第二条转发/.test(fwdPrompt), '');
  ok('转发标注了发送者', /小红|老王/.test(fwdPrompt), '');

  // 普通 JSON 卡片
  clearPiLog();
  const newsJson = JSON.stringify({
    app: 'com.tencent.structmsg',
    meta: { detail: { title: '某篇文章', desc: '这是描述', qqdocurl: 'https://example.com/a' } },
  });
  napcat.sayGroup('看这个链接', {
    message: [
      { type: 'at', data: { qq: '10000' } },
      { type: 'json', data: { data: newsJson } },
    ],
  });
  await waitForPi();
  ok('普通卡片已解析', piPrompts().map((p) => p.message).join('\n').includes('某篇文章'), '');

  // XML 卡片
  clearPiLog();
  const xml = '<msg><item><title>XML标题</title><des>XML描述</des></item></msg>';
  napcat.sayGroup('xml 卡片', {
    message: [
      { type: 'at', data: { qq: '10000' } },
      { type: 'xml', data: { data: xml } },
    ],
  });
  await waitForPi();
  ok('XML 卡片已解析', piPrompts().map((p) => p.message).join('\n').includes('XML标题'), '');

  // ============================================================ 5. 引用消息
  section('引用消息');
  clearPiLog();
  napcat.setHandler('get_msg', () => ({
    user_id: 444, sender: { card: '老张' },
    raw_message: '明天三点开会', message: [{ type: 'text', data: { text: '明天三点开会' } }],
  }));
  napcat.sayGroup('他说的几点？', {
    message: [
      { type: 'at', data: { qq: '10000' } },
      { type: 'reply', data: { id: '555' } },
      { type: 'text', data: { text: ' 他说的几点？' } },
    ],
  });
  await waitForPi();
  const qPrompt = piPrompts().map((p) => p.message).join('\n');
  ok('引用消息已解析', /明天三点开会/.test(qPrompt), '');
  ok('引用消息标注发送者', /老张/.test(qPrompt), '');

  // ============================================================ 6. 队列与 /queue
  section('队列行为');
  // 先占住 busy: 发一条需要 pi 的消息 (会 spawn pi)
  napcat.sent.length = 0;
  napcat.sayPrivate('/queue');
  await sleep(1200);
  const qtexts = napcat.texts({ action: 'send_private_msg' });
  ok('/queue 空闲时正常回应', qtexts.some((t) => /没有排队/.test(t)), JSON.stringify(qtexts.slice(-2)));

  // ============================================================ 7. 内置命令
  section('内置命令');
  napcat.sent.length = 0;
  napcat.sayPrivate('/help');
  await sleep(1200);
  const helpTexts = napcat.texts({ action: 'send_private_msg' });
  ok('/help 有输出', helpTexts.some((t) => /pi coding agent/.test(t)));
  ok('/help 列出 steer', helpTexts.some((t) => /steer/.test(t)));
  ok('/help 列出 queue', helpTexts.some((t) => /queue/.test(t)));

  napcat.sent.length = 0;
  napcat.sayPrivate('/status');
  await sleep(1500);
  ok('/status 有输出', napcat.texts({ action: 'send_private_msg' }).some((t) => /会话/.test(t)));

  napcat.sent.length = 0;
  napcat.sayPrivate('/memory');
  await sleep(1200);
  ok('/memory 有输出', napcat.texts({ action: 'send_private_msg' }).some((t) => /记忆/.test(t)));

  napcat.sent.length = 0;
  napcat.sayPrivate('/task');
  await sleep(1200);
  ok('/task 无任务时提示', napcat.texts({ action: 'send_private_msg' }).some((t) => /定时任务/.test(t)));

  // /steer 空闲时应转成普通消息
  napcat.sent.length = 0;
  napcat.sayPrivate('/steer 补充一句');
  await sleep(1500);
  ok('/steer 空闲时提示当普通消息', napcat.texts({ action: 'send_private_msg' }).some((t) => /普通消息/.test(t)));

  // /restart 群里应被拒绝
  napcat.sent.length = 0;
  napcat.sayGroup('/restart');
  await sleep(1500);
  ok('群里 /restart 被拒绝', napcat.texts({ action: 'send_group_msg' }).some((t) => /仅限主人/.test(t)));

  // ============================================================ 8. Markdown 降级
  section('Markdown 降级');
  fs.mkdirSync(path.join(WORK, 'memory'), { recursive: true });
  fs.writeFileSync(path.join(WORK, 'memory', 'private_20001.md'), '## 标题测试\n- **加粗内容**\n');
  napcat.sent.length = 0;
  napcat.sayPrivate('/memory');
  await sleep(1200);
  const mdTexts = napcat.texts({ action: 'send_private_msg' });
  ok('输出不含 ##', mdTexts.length > 0 && !mdTexts.some((t) => /(^|\n)#{1,6}\s/.test(t)));
  ok('输出不含 **', mdTexts.length > 0 && !mdTexts.some((t) => t.includes('**')));

  // ============================================================ 9. 定时任务
  section('定时任务');
  const taskFile = path.join(WORK, 'tasks', 'itest-task.json');
  fs.mkdirSync(path.dirname(taskFile), { recursive: true });
  fs.writeFileSync(taskFile, JSON.stringify({
    id: 'itest-once',
    target: 'private_123456789',
    schedule: { type: 'once', at: Date.now() - 60000 },   // 已过期 -> 应被跳过
    prompt: '这条不该执行',
    enabled: true,
  }));
  await sleep(22000);   // 等任务巡检 (默认 20s)
  const tl = getLog();
  // once 任务时间已过 => recalcTasks 会直接把它停用 (nextRun=0), 不一定走"跳过"分支
  ok('过期任务未执行', !/执行定时任务: itest-once/.test(tl), '');
  // 原来还有一条 \`跳过过期任务 || 新增定时任务: itest-once\` 的断言, 后半只要
  // 任务文件被读进来就成立, 与「有没有跳过过期任务」无关, 等于恒真, 已删除。
  // once 过期走的是「直接停用」分支 (nextRun=0, enabled=false), 那条路径不打日志,
  // 从子进程外部观测不到, 所以这里只保留「未执行」这一条有判别力的断言。

  // ============================================================ 10. 稳定性
  section('稳定性');
  ok('进程始终存活', alive());
  ok('无 fatal', !/\[fatal\]/.test(getLog()), '');
  ok('无 unhandledRejection', !/unhandledRejection/.test(getLog()), '');
  ok('无未捕获异常', !/uncaughtException/.test(getLog()), '');

  // ============================================================ 11. 真 pi (可选)
  if (WITH_PI) {
    section('真 pi 端到端');
    napcat.sent.length = 0;
    napcat.sayPrivate('不要用工具，只回四个字：收到消息');
    await sleep(60000);
    const replies = napcat.texts({ action: 'send_private_msg' });
    ok('pi 有回复', replies.length > 0, `${replies.length} 段`);
    ok('回复内容合理', replies.join('').includes('收到消息'), replies.join('').slice(0, 80));
  } else {
    section('真 pi 端到端');
    console.log('  \x1b[90m(跳过, 加 --with-pi 打开)\x1b[0m');
  }

  // ---------------------------------------------------------------- 收尾
  child.kill('SIGTERM');
  await sleep(800);
  napcat.close();
  if (!KEEP) fs.rmSync(WORK, { recursive: true, force: true });

  const passed = results.filter((r) => r.pass).length;
  const failed = results.length - passed;
  console.log(`\n\x1b[36m===== 结果 =====\x1b[0m`);
  console.log(`通过 ${passed}/${results.length}`);
  if (failed) {
    console.log(`\x1b[31m失败: ${results.filter((r) => !r.pass).map((r) => r.name).join(' | ')}\x1b[0m`);
  }
  if (KEEP) console.log(`临时目录保留在: ${WORK}`);
  process.exit(failed ? 1 : 0);
})().catch((e) => {
  console.error('集成测试自身出错:', e);
  if (!KEEP) fs.rmSync(WORK, { recursive: true, force: true });
  process.exit(1);
});
