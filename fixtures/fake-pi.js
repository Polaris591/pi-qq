#!/usr/bin/env node
'use strict';
/**
 * 假 pi: 只实现 RPC 协议的必要部分, 供集成测试使用。
 *
 * 为什么要它: 集成测试要验证「bridge 到底把什么发给了 pi」,
 * 真 pi 又慢又要模型额度。用这个替身既能断言内容, 又能秒回。
 *
 * 行为:
 *   - 收到 prompt/steer 时, 把 message 追加写入 FAKE_PI_LOG 指定的文件
 *   - 回一段固定文本 (让 bridge 的 flush 路径能跑通)
 *   - 对 get_state 等命令返回最小可用的结构
 *
 * 环境变量:
 *   FAKE_PI_LOG  记录收到的 prompt 的文件路径
 *   FAKE_PI_TEXT 回复的文本 (默认「好的，收到了。」)
 */
const fs = require('fs');

const LOG = process.env.FAKE_PI_LOG || '';
const REPLY = process.env.FAKE_PI_TEXT || '好的，收到了。';

function out(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function record(type, message) {
  if (!LOG) return;
  try { fs.appendFileSync(LOG, JSON.stringify({ type, message }) + '\n'); } catch {}
}

let buf = '';
process.stdin.on('data', (chunk) => {
  buf += chunk.toString();
  let i;
  while ((i = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, i);
    buf = buf.slice(i + 1);
    if (!line.trim()) continue;
    let r;
    try { r = JSON.parse(line); } catch { continue; }
    handle(r);
  }
});
process.stdin.on('end', () => process.exit(0));

function handle(r) {
  switch (r.type) {
    case 'prompt':
    case 'steer': {
      record(r.type, r.message);
      // 回一段文本, 让 bridge 走 flush -> sendQQ 的完整路径
      out({
        type: 'message_update',
        assistantMessageEvent: { type: 'text_delta', delta: REPLY },
      });
      break;
    }
    case 'abort':
      break;
    default:
      break;
  }

  // 所有带 id 的命令都要回一个 response, 否则 bridge 的 request() 会一直等到超时
  if (r.id !== undefined) {
    let data = {};
    if (r.type === 'get_state') {
      data = {
        sessionFile: process.env.FAKE_PI_SESSION || '',
        model: { provider: 'fake', id: 'fake-model' },
        thinkingLevel: 'off',
        messageCount: 0,
        isCompacting: false,
      };
    } else if (r.type === 'get_available_thinking_levels') {
      data = { levels: ['off', 'high'] };
    } else if (r.type === 'get_available_models') {
      data = { models: [{ provider: 'fake', id: 'fake-model' }] };
    } else if (r.type === 'get_session_stats') {
      data = { tokens: {}, contextUsage: {}, totalMessages: 0 };
    }
    out({ id: r.id, type: 'response', command: r.type, success: true, data });
  }

  // prompt 跑完要发 agent_settled, 否则 bridge 会一直认为 busy
  if (r.type === 'prompt') out({ type: 'agent_settled' });
}

// 保持进程存活, 等 stdin
setInterval(() => {}, 1 << 30);
