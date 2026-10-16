#!/usr/bin/env node
'use strict';
/**
 * pi-goal-x（/goal 目标模式）在 QQ 桥接上的端到端测试。
 *
 * 与 itest.js 的区别: 这里必须用真 pi, 因为要验的是 pi 的扩展命令 + 扩展 UI 协议
 * (extension_ui_request / extension_ui_response)。假 pi 不会注册 /goal, 测不了。
 *
 * 只覆盖不需要模型额度的路径（命令转发 + 对话框往返）; /goal-direct 真跑一轮
 * 会花额度且耗时, 用 --with-agent 单独打开。
 *
 * 用法:
 *   node goaltest.js                 只跑命令转发与对话框
 *   node goaltest.js --with-agent    额外真跑一轮 /goal-direct（要模型额度）
 *   node goaltest.js --keep          保留临时目录
 *
 * 前置: pi 里装了 pi-goal-x（pi install npm:pi-goal-x）。
 */

const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const WITH_AGENT = process.argv.includes('--with-agent');
const KEEP = process.argv.includes('--keep');
const ROOT = __dirname;
const WORK = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-qq-goaltest-'));
const WS_DIR = path.join(WORK, 'workspace');
fs.mkdirSync(WS_DIR, { recursive: true });

const results = [];
function ok(name, pass, extra = '') {
  results.push({ name, pass });
  const tag = pass ? '\x1b[32mPASS\x1b[0m' : '\x1b[31mFAIL\x1b[0m';
  console.log(`${tag}  ${name}${extra ? `  \x1b[90m${extra}\x1b[0m` : ''}`);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const section = (t) => console.log(`\n\x1b[36m── ${t} ──\x1b[0m`);

// ---------------------------------------------------------------- 假 NapCat

const PORT = 39700 + Math.floor(Math.random() * 200);
const sent = [];
const wss = new WebSocketServer({ port: PORT });
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ post_type: 'meta_event', meta_event_type: 'lifecycle', sub_type: 'connect' }));
  ws.on('message', (d) => {
    for (const line of d.toString().split('\n')) {
      if (!line.trim()) continue;
      let r; try { r = JSON.parse(line); } catch { continue; }
      const reply = (data) => { try { ws.send(JSON.stringify({ status: 'ok', retcode: 0, echo: r.echo, data })); } catch {} };
      if (r.action === 'send_private_msg' || r.action === 'send_group_msg') {
        const m = r.params.message;
        const text = typeof m === 'string' ? m
          : (Array.isArray(m) ? m.filter((s) => s.type === 'text').map((s) => s.data.text).join('') : String(m));
        sent.push(text);
        console.log(`  \x1b[90m<< ${JSON.stringify(text).slice(0, 150)}\x1b[0m`);
        reply({ message_id: 1000 + sent.length });
      } else reply({});
    }
  });
});

const texts = () => sent.join('\n');
const clearSent = () => { sent.length = 0; };
const waitFor = async (pred, ms) => {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) {
    if (pred()) return true;
    await sleep(300);
  }
  return pred();
};

let mid = 0;
function say(text) {
  const rec = {
    post_type: 'message', message_type: 'private', user_id: '123456789', self_id: '9999',
    message_id: String(++mid), raw_message: text, message: [{ type: 'text', data: { text } }],
  };
  for (const ws of wss.clients) ws.send(JSON.stringify(rec));
  console.log(`  \x1b[90m>> ${text}\x1b[0m`);
}

// ---------------------------------------------------------------- 启动 bridge

const cfg = {
  napcat: { url: `ws://127.0.0.1:${PORT}`, token: '' },
  pi: {
    bin: process.env.PI_QQ_TEST_BIN || 'pi', cwd: WS_DIR,
    sessionDir: path.join(WORK, 'sessions'),
    provider: process.env.PI_QQ_TEST_PROVIDER || 'cobblemon',
    model: process.env.PI_QQ_TEST_MODEL || 'gemini-3.8-flash-high',
    tools: '', extraArgs: [],
  },
  access: { privateWhitelist: ['123456789'], groupWhitelist: [], allowAllPrivate: false, allowAllGroups: false, requireAtInGroup: true },
  behavior: {
    startupNotice: false, heartbeatTimeoutMs: 0, groupContextCount: 0, emojiReaction: false,
    progressOnToolCall: false, turnTimeoutMs: 0, silenceNoticeMs: 0, markdownToPlain: true, maxChars: 2500,
    idleTimeoutMs: 60 * 60 * 1000, uiDialogTimeoutMs: 120000,
  },
  files: {
    outboxDir: path.join(WORK, 'outbox'), containerOutbox: path.join(WORK, 'outbox'),
    inboxDir: path.join(WORK, 'inbox'), stateDir: WORK,
  },
  memory: { dir: path.join(WORK, 'memory') },
  tasks: { dir: path.join(WORK, 'tasks') },
  persona: { text: '', groupBoundary: '' },
};
const cfgPath = path.join(WORK, 'config.json');
fs.writeFileSync(cfgPath, JSON.stringify(cfg, null, 2));

const child = spawn(process.execPath, [path.join(ROOT, 'bridge.js')], {
  env: { ...process.env, PI_QQ_CONFIG: cfgPath }, stdio: ['ignore', 'pipe', 'pipe'],
});
let log = '';
child.stdout.on('data', (c) => { log += c; });
child.stderr.on('data', (c) => { log += c; });

// ---------------------------------------------------------------- 用例

(async () => {
  await sleep(3000);

  section('命令转发（/goal 系列的 notify 出口）');
  clearSent();
  say('/goal-list');
  const gotList = await waitFor(() => /open goal|goal/i.test(texts()), 25000);
  ok('pi 的 notify 转成了 QQ 消息', gotList, JSON.stringify(texts()).slice(0, 100));
  ok('notify 里带的是扩展原文', /goal/i.test(texts()), '');

  section('扩展对话框往返（select）');
  clearSent();
  say('/goal-settings');
  const asked = await waitFor(() => /^❓/.test(texts().trim()), 25000);
  ok('对话框转成了带编号的提问', asked && /1\.\s/.test(texts()), '');
  ok('提问里带了放弃方式', /\/cancel/.test(texts()), '');

  clearSent();
  say('1');
  const second = await waitFor(() => texts().trim().length > 0, 25000);
  ok('回序号后 pi 继续了流程（有后续输出）', second, JSON.stringify(texts()).slice(0, 100));

  clearSent();
  say('/cancel');
  const cancelled = await waitFor(() => /已放弃/.test(texts()), 15000);
  ok('/cancel 放弃回答', cancelled, JSON.stringify(texts()).slice(0, 80));

  section('问答不合法时的兜底');
  clearSent();
  say('/goal-settings');
  await waitFor(() => /^❓/.test(texts().trim()), 25000);
  clearSent();
  say('随便回一句不是序号的');   // input 型选项会原样接受, 这里只要求不崩、不乱发
  await sleep(6000);
  ok('乱答不会让桥接挂掉', child.exitCode === null && !/\[fatal\]|unhandledRejection/.test(log), '');
  say('/cancel');
  await sleep(3000);

  if (WITH_AGENT) {
    section('真跑一轮 /goal-direct（花额度）');
    clearSent();
    say('/goal-direct 用 bash 执行 echo done > answer.txt，然后立刻结束，不要做别的事');
    const ran = await waitFor(() => fs.existsSync(path.join(WS_DIR, 'answer.txt')), 240000);
    ok('目标真的执行了工具', ran, fs.existsSync(path.join(WS_DIR, 'answer.txt')) ? 'answer.txt 已生成' : '没等到 answer.txt');
    ok('目标期间有 QQ 输出', sent.length > 0, `${sent.length} 条`);
    say('/goal-pause');
    await sleep(5000);
  }

  section('稳定性');
  ok('bridge 进程存活', child.exitCode === null, '');
  ok('无 fatal', !/\[fatal\]/.test(log), '');
  ok('无 unhandledRejection', !/unhandledRejection|uncaughtException/.test(log), '');

  child.kill('SIGTERM');
  await sleep(800);
  try { wss.close(); } catch {}
  if (!KEEP) fs.rmSync(WORK, { recursive: true, force: true });

  const passed = results.filter((r) => r.pass).length;
  console.log(`\n\x1b[36m===== 结果 =====\x1b[0m`);
  console.log(`通过 ${passed}/${results.length}`);
  const failed = results.filter((r) => !r.pass);
  if (failed.length) console.log(`\x1b[31m失败: ${failed.map((r) => r.name).join(' | ')}\x1b[0m`);
  if (KEEP) console.log(`临时目录: ${WORK}`);
  process.exit(failed.length ? 1 : 0);
})().catch((e) => {
  console.error('测试自身出错:', e);
  child.kill('SIGKILL');
  if (!KEEP) fs.rmSync(WORK, { recursive: true, force: true });
  process.exit(1);
});
