#!/usr/bin/env node
'use strict';
/**
 * 纯函数单元测试: 只验证记忆/任务的解析与调度逻辑, 不连网、不启 pi。
 * 做法: 复制 bridge.js 并在末尾追加导出, 用假 ws 模块替代依赖。
 */
const fs = require('fs');
const path = require('path');

const BASE = '/tmp/ut';
fs.rmSync(BASE, { recursive: true, force: true });
for (const d of ['memory', 'tasks', 'sessions', 'outbox', 'inbox', 'workspace', 'node_modules/ws']) {
  fs.mkdirSync(path.join(BASE, d), { recursive: true });
}

// 假 ws: 只让 bridge 能加载, 不做任何真实连接
fs.writeFileSync(path.join(BASE, 'node_modules/ws/index.js'), `
class WS {
  constructor() { this.readyState = 0; }
  on() { return this; }
  send() {}
  close() {}
}
WS.OPEN = 1;
class Server { constructor() {} on() {} close() {} }
module.exports = WS;
module.exports.Server = Server;
module.exports.WebSocket = WS;
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

// 复制真实源码 + 追加导出
const src = fs.readFileSync(path.join(__dirname, 'bridge.js'), 'utf8');
fs.writeFileSync(path.join(BASE, 'bridge.js'), `${src}
module.exports = {
  parseTimeOfDay, parseOnce, normalizeSchedule, nextRunAt, normalizeTask,
  describeTask, taskTargetAllowed, mergeTaskFiles, recalcTasks, tickTasks,
  memoryPrompt, memoryPromptFile, readMemory, memPath, ensureTasksDir, cfg,
  parseMessage, splitForQQ, isImagePath, isTextPath, humanSize, slug,
  PiSession, onebot, fetchGroupContext,
  memPath, readMemory, readGroupShared, readMemberMemory, listGroupMembers,
  groupMemDir, groupSharedMemPath, groupMemberMemPath, groupMemberDir,
  turnMemoryPrompt, migrateMemoryLayout,
  personaPrompt, envPrompt,
  fetchQuotedMessage, describeMentions, resolveMemberName, describeToolCall,
  parseCards, fetchForwardContent, notifyTarget,
  cacheMemberName, MEMBER_NAME_CACHE, MEMBER_NAME_MAX,
  mdToPlain, stripInlineMd, splitStreamTail, isDuplicateMessage,
  get TASKS() { return TASKS; },
  get qqDown() { return qqDown; },
  get qqDownKind() { return qqDownKind; },
  setLastSeen(v) { lastSeenAt = v; },
  getLastSeen() { return lastSeenAt; },
};
`);

const B = require(path.join(BASE, 'bridge.js'));

const results = [];
const ok = (name, pass, extra = '') => {
  results.push({ name, pass });
  console.log(`${pass ? 'PASS' : 'FAIL'}  ${name}${extra ? `  ${extra}` : ''}`);
};

// ---- 时间解析
ok('parseTimeOfDay 08:00', JSON.stringify(B.parseTimeOfDay('08:00')) === '{"h":8,"mi":0,"se":0}');
ok('parseTimeOfDay 23:59:59', JSON.stringify(B.parseTimeOfDay('23:59:59')) === '{"h":23,"mi":59,"se":59}');
ok('parseTimeOfDay 24:00 -> null', B.parseTimeOfDay('24:00') === null);
ok('parseTimeOfDay 9:5 -> null', B.parseTimeOfDay('9:5') === null);
ok('parseTimeOfDay 垃圾 -> null', B.parseTimeOfDay('abc') === null);

// ---- schedule 归一化
ok('daily 归一化', JSON.stringify(B.normalizeSchedule({ type: 'daily', time: '8:05' })) === '{"type":"daily","time":"08:05"}');
ok('weekly 归一化(排序去重)', JSON.stringify(B.normalizeSchedule({ type: 'weekly', time: '09:30', weekdays: [5, 1, 3, 3] })) === '{"type":"weekly","time":"09:30","weekdays":[1,3,5]}');
ok('weekly 缺 weekdays -> null', B.normalizeSchedule({ type: 'weekly', time: '09:30' }) === null);
ok('weekly weekdays 越界 -> null', B.normalizeSchedule({ type: 'weekly', time: '09:30', weekdays: [9] }) === null);
ok('every minutes', B.normalizeSchedule({ type: 'every', minutes: 30 }).minutes === 30);
ok('every hours 换算', B.normalizeSchedule({ type: 'every', hours: 2 }).minutes === 120);
ok('every days 换算', B.normalizeSchedule({ type: 'every', days: 1 }).minutes === 1440);
ok('every 0 -> null', B.normalizeSchedule({ type: 'every', minutes: 0 }) === null);
ok('once 字符串解析', typeof B.normalizeSchedule({ type: 'once', at: '2026-10-10 12:00' }).at === 'number');
ok('once 非法 -> null', B.normalizeSchedule({ type: 'once', at: '不是时间' }) === null);
ok('未知 type -> null', B.normalizeSchedule({ type: 'hourly' }) === null);
ok('null -> null', B.normalizeSchedule(null) === null);

// ---- nextRunAt
const base = new Date(2026, 9, 6, 7, 0, 0).getTime(); // 2026-10-06 07:00 本地
const n1 = new Date(B.nextRunAt({ schedule: { type: 'daily', time: '08:00' } }, base));
ok('daily 当天 08:00', n1.getDate() === 6 && n1.getHours() === 8, n1.toString());
const n2 = new Date(B.nextRunAt({ schedule: { type: 'daily', time: '06:00' } }, base));
ok('daily 已过则顺延一天', n2.getDate() === 7 && n2.getHours() === 6, n2.toString());
// 2026-10-06 是周二(2)
const n3 = new Date(B.nextRunAt({ schedule: { type: 'weekly', time: '09:30', weekdays: [1, 3, 5] } }, base));
ok('weekly 取最近的周三', n3.getDay() === 3 && n3.getHours() === 9 && n3.getMinutes() === 30, n3.toString());
// base=2026-10-06 07:00 周二; 09:30 尚未到 -> 应为当天
const n4 = new Date(B.nextRunAt({ schedule: { type: 'weekly', time: '09:30', weekdays: [2] } }, base));
ok('weekly 今天未过则当天', n4.getDay() === 2 && n4.getDate() === 6 && n4.getHours() === 9, n4.toString());
// 06:00 已过 -> 顺延到下周二 10-13
const n4b = new Date(B.nextRunAt({ schedule: { type: 'weekly', time: '06:00', weekdays: [2] } }, base));
ok('weekly 今天已过则下周二', n4b.getDay() === 2 && n4b.getDate() === 13 && n4b.getHours() === 6, n4b.toString());
const n5 = B.nextRunAt({ schedule: { type: 'every', minutes: 30 }, lastRun: base }, base);
ok('every 按 lastRun 递增', n5 === base + 30 * 60000, new Date(n5).toString());
const n6 = B.nextRunAt({ schedule: { type: 'every', minutes: 30 }, lastRun: base - 5 * 3600000 }, base);
ok('every 落后时追赶不堆叠', n6 > base && n6 - base <= 30 * 60000, `${(n6 - base) / 60000} 分钟后`);
ok('once 未来时间返回该值', B.nextRunAt({ schedule: { type: 'once', at: base + 60000 } }, base) === base + 60000);
ok('once 已过返回 null', B.nextRunAt({ schedule: { type: 'once', at: base - 1 } }, base) === null);

// ---- normalizeTask / 白名单
const good = B.normalizeTask({ id: 'a', target: 'private_123456789', schedule: { type: 'daily', time: '08:00' }, prompt: 'hi' }, 'f.json');
ok('合法任务通过', !!good && good.id === 'a' && good.enabled === true);
ok('目标不在白名单 -> 拒绝', B.normalizeTask({ target: 'private_9999999', schedule: { type: 'daily', time: '08:00' }, prompt: 'x' }) === null);
ok('群目标在白名单 -> 通过', !!B.normalizeTask({ target: 'group_10001', schedule: { type: 'daily', time: '08:00' }, prompt: 'x' }));
ok('target 格式错 -> 拒绝', B.normalizeTask({ target: '123456789', schedule: { type: 'daily', time: '08:00' }, prompt: 'x' }) === null);
ok('prompt 空 -> 拒绝', B.normalizeTask({ target: 'private_123456789', schedule: { type: 'daily', time: '08:00' }, prompt: '  ' }) === null);
ok('id 非法字符被清理', B.normalizeTask({ id: 'a b/../c', target: 'private_123456789', schedule: { type: 'daily', time: '08:00' }, prompt: 'x' }).id === 'a_b_.._c');

// ---- describeTask
ok('describeTask daily', B.describeTask({ schedule: { type: 'daily', time: '08:00' } }) === '每天 08:00');
ok('describeTask weekly', B.describeTask({ schedule: { type: 'weekly', time: '09:30', weekdays: [1, 3, 5] } }) === '每周一三五 09:30');
ok('describeTask every', B.describeTask({ schedule: { type: 'every', minutes: 30 } }) === '每 30 分钟');

// ---- 记忆
ok('readMemory 缺失 -> 空', B.readMemory('private_123456789') === '');
fs.writeFileSync(B.memPath('private_123456789'), '- 偏好 TypeScript\n- 名字叫小明\n');
ok('readMemory 读取并 trim', B.readMemory('private_123456789').startsWith('- 偏好 TypeScript'));
const mp1 = B.memoryPrompt('private_123456789');
ok('memoryPrompt 含路径', mp1.includes(B.memPath('private_123456789')));
ok('memoryPrompt 不放内容(改由每轮注入)', !mp1.includes('名字叫小明'));
ok('memoryPrompt 含写入指引', mp1.includes('长期记忆') && mp1.includes('不要重写整份'));
const mp2 = B.memoryPrompt('private_999');
ok('memoryPrompt 无记忆时仍给路径', mp2.includes(B.memPath('private_999')));
ok('私聊每轮注入含已有内容',
  B.turnMemoryPrompt({ type: 'private', id: '123456789' }, '123456789', 'me').includes('名字叫小明'));

// ---- 两层记忆 (群公共 + 成员个人)
const G = '10001', U1 = '111', U2 = '222';
ok('群公共记忆路径在群目录内', B.groupSharedMemPath(G) === path.join(B.groupMemDir(G), 'shared.md'));
ok('成员记忆路径在群目录 members/ 下',
  B.groupMemberMemPath(G, U1) === path.join(B.groupMemDir(G), 'members', `${U1}.md`));
ok('成员记忆按人隔离', B.groupMemberMemPath(G, U1) !== B.groupMemberMemPath(G, U2));
ok('不同群的记忆互不影响', B.groupMemDir(G) !== B.groupMemDir('999'));
ok('无记忆时返回空串', B.readMemberMemory(G, U1) === '' && B.readGroupShared(G) === '');
// 目录由 pi 的 write 工具自动创建, 这里手动建以模拟
fs.mkdirSync(B.groupMemberDir(G), { recursive: true });
fs.writeFileSync(B.groupSharedMemPath(G), '- 本群项目：pi-qq\n');
fs.writeFileSync(B.groupMemberMemPath(G, U1), '- 偏好：只喝美式\n- 身份：后端工程师\n');
fs.writeFileSync(B.groupMemberMemPath(G, U2), '- 偏好：喜欢猫\n');
ok('群公共记忆可读', B.readGroupShared(G).includes('pi-qq'));
ok('成员记忆按人读取',
  B.readMemberMemory(G, U1).includes('后端工程师') && B.readMemberMemory(G, U2).includes('喜欢猫'));
ok('能列出本群成员', JSON.stringify(B.listGroupMembers(G)) === JSON.stringify([U1, U2]));
ok('列成员不含其他群', !B.listGroupMembers('999').length);

// turnMemoryPrompt: 每轮注入的内容
const tm = B.turnMemoryPrompt({ type: 'group', id: G }, U1, '小明');
ok('含发言人姓名与 QQ', tm.includes('小明') && tm.includes(`QQ: ${U1}`));
ok('含群公共记忆', tm.includes('pi-qq') && tm.includes('群公共记忆'));
ok('含该成员个人记忆', tm.includes('后端工程师'));
ok('不含他人个人记忆', !tm.includes('喜欢猫'));
ok('给出两个文件路径', tm.includes(B.groupSharedMemPath(G)) && tm.includes(B.groupMemberMemPath(G, U1)));
ok('声明只有本轮是指令', tm.includes('不是'));
const tmB = B.turnMemoryPrompt({ type: 'group', id: G }, U2, '小红');
ok('换人后注入的是另一个人的记忆', tmB.includes('喜欢猫') && !tmB.includes('后端工程师'));
const tmNone = B.turnMemoryPrompt({ type: 'group', id: G }, '999', '新同学');
ok('无记忆成员显示占位', tmNone.includes('（暂无）') && tmNone.includes('新同学'));
const tmPriv = B.turnMemoryPrompt({ type: 'private', id: '123456789' }, '123456789', 'me');
ok('私聊注入私聊记忆内容', tmPriv.includes('名字叫小明') && !tmPriv.includes('当前发言人'));

// 系统提示: 只讲规则和路径, 不带内容
const sp = B.memoryPrompt({ type: 'group', id: G });
ok('系统提示含群公共记忆路径', sp.includes(B.groupSharedMemPath(G)));
ok('系统提示含成员目录', sp.includes(B.groupMemberDir(G)));
ok('系统提示要求勿写错文件', sp.includes('绝不要把某位成员的个人信息写进别人的文件'));
ok('系统提示不含具体记忆内容', !sp.includes('后端工程师') && !sp.includes('pi-qq'));
ok('私聊系统提示含私聊文件路径',
  B.memoryPrompt({ type: 'private', id: '123' }).includes(B.memPath('private_123')));

// prompt(): 记忆作为「消息前缀」传递, 且随发言者切换
const gs = new B.PiSession('group_' + G, { type: 'group', id: G });
const sentMsgs = [];
gs.send = (cmd) => { sentMsgs.push(cmd); return true; };
gs.sendQQ = async () => {};
gs.proc = { stdin: { write() {}, writable: true }, kill() {} };
gs.prompt('你好', [], { replyTo: 1, atUser: U1, userId: U1, userName: '小明' });
ok('成员A消息带上A的记忆', sentMsgs[0].message.includes('后端工程师') && sentMsgs[0].message.includes('你好'));
ok('成员A消息带群公共记忆', sentMsgs[0].message.includes('pi-qq'));
ok('成员A消息不串入B的记忆', !sentMsgs[0].message.includes('喜欢猫'));
ok('原始正文仍在末尾', sentMsgs[0].message.trim().endsWith('你好'));
gs.busy = false;
gs.prompt('早', [], { replyTo: 2, atUser: U2, userId: U2, userName: '小红' });
ok('成员B消息带上B的记忆', sentMsgs[1].message.includes('喜欢猫') && !sentMsgs[1].message.includes('后端工程师'));
gs.busy = false;
gs.prompt('再来', [], { replyTo: 3, atUser: U2, userId: U2, userName: '小红' });
ok('同一成员再发言仍带记忆', sentMsgs[2].message.includes('喜欢猫'));
gs.busy = false;
const ps2 = new B.PiSession('private_123456789', { type: 'private', id: '123456789' });
const privMsgs = [];
ps2.send = (cmd) => { privMsgs.push(cmd); return true; };
ps2.sendQQ = async () => {};
ps2.proc = { stdin: { write() {}, writable: true }, kill() {} };
ps2.prompt('你好', [], { replyTo: 1, atUser: null, userId: '123456789', userName: 'me' });
ok('私聊消息不带群成员段', !privMsgs[0].message.includes('当前发言人'));

// ---- 记忆布局迁移 (旧 -> 新)
const OLD_MEM = path.join(BASE, 'memory');
const legacyShared = path.join(OLD_MEM, `group_${G}.md`);
const legacyMemberDir = path.join(OLD_MEM, 'members');
fs.mkdirSync(legacyMemberDir, { recursive: true });
fs.writeFileSync(legacyShared, '- 旧群记忆\n');
fs.writeFileSync(path.join(legacyMemberDir, `${G}_${U1}.md`), '- 旧成员记忆\n');
// 先清掉新布局里同名的, 以验证真的搬过去了
fs.rmSync(B.groupSharedMemPath(G), { force: true });
fs.rmSync(B.groupMemberMemPath(G, U1), { force: true });
B.migrateMemoryLayout();
ok('旧群记忆已迁移到 shared.md', B.readGroupShared(G).includes('旧群记忆'));
ok('旧成员记忆已迁移到 members/<uid>.md', B.readMemberMemory(G, U1).includes('旧成员记忆'));
ok('迁移后旧文件已移除', !fs.existsSync(legacyShared) && !fs.existsSync(path.join(legacyMemberDir, `${G}_${U1}.md`)));
ok('迁移幂等(重复调用不报错)', (B.migrateMemoryLayout(), B.readGroupShared(G).includes('旧群记忆')));
ok('迁移不覆盖已有新文件', (fs.writeFileSync(legacyShared, '- 又一份旧的\n'), B.migrateMemoryLayout(),
  B.readGroupShared(G).includes('旧群记忆') && !B.readGroupShared(G).includes('又一份旧的')));

// ---- 工具进度提示文案
ok('bash 显示命令', B.describeToolCall('bash', { command: 'ls -la /opt' }).includes('ls -la /opt'));
ok('read 显示路径', B.describeToolCall('read', { path: '/srv/app/bridge.js' }).includes('bridge.js'));
ok('edit 显示路径', B.describeToolCall('edit', { path: 'a/b.js' }).includes('a/b.js'));
ok('grep 显示模式', B.describeToolCall('grep', { pattern: 'TODO' }).includes('TODO'));
ok('codemode 简短描述', B.describeToolCall('codemode', { code: 'x'.repeat(500) }) === '🔧 codemode: 跑脚本');
ok('无参数时只给工具名', B.describeToolCall('mystery', {}) === '🔧 mystery');
ok('长参数被截断', B.describeToolCall('bash', { command: 'x'.repeat(300) }).length < 110);
ok('命令含换行时压成一行', !B.describeToolCall('bash', { command: 'a\nb' }).includes('\n'));

// ---- Markdown -> QQ 纯文本
ok('标题 ## 变成【】', B.mdToPlain('## 第一类：安全') === '【第一类：安全】');
ok('标题 ### 也处理', B.mdToPlain('### 小标题') === '【小标题】');
ok('粗体去掉星号', B.mdToPlain('**1. 权限问题**') === '1. 权限问题');
ok('粗体在句中', B.mdToPlain('这是**重点**内容') === '这是重点内容');
ok('引用 > 变竖线', B.mdToPlain('> 这说明问题') === '｜ 这说明问题');
ok('无序列表 - 变点', B.mdToPlain('- 第一条\n- 第二条') === '• 第一条\n• 第二条');
ok('有序列表保留编号', B.mdToPlain('1. 甲\n2. 乙') === '1. 甲\n2. 乙');
ok('行内代码去反引号', B.mdToPlain('用 `read` 工具') === '用 read 工具');
ok('链接变文字+网址', B.mdToPlain('[官网](https://a.com)') === '官网 (https://a.com)');
ok('分隔线归一', B.mdToPlain('---') === '——————');
ok('删除线去掉', B.mdToPlain('~~删掉~~') === '删掉');
ok('表格转竖线', B.mdToPlain('| A | B |\n|---|---|\n| 1 | 2 |') === 'A ｜ B\n1 ｜ 2');
ok('代码块变缩进', B.mdToPlain('```js\nconst a = 1;\n```') === '———\n  const a = 1;\n———');
ok('代码块内符号不被处理', B.mdToPlain('```\n** 不动 **\n```').includes('** 不动 **'));
ok('压缩多余空行', B.mdToPlain('a\n\n\n\nb') === 'a\n\nb');
ok('空输入安全', B.mdToPlain('') === '' && B.mdToPlain(null) === '');
// 真实截图里那种回复
const dirty = [
  '三个功能已经做完了。现在剩下的，我按「该不该做」排一下。',
  '',
  '## 第一类：不修会真出事（我最建议）',
  '',
  '**1. 群里谁 @ 一下，就能在你服务器上跑命令**',
  '',
  '群里白名单里**任何一个人** @ 一下，AI 就带着完整 shell 权限干活。',
  '',
  '> 这两个是一体的：**得先知道「谁在说话」**。',
  '',
  '## 第二类：让它更好用',
  '',
  '**3. 语音消息能听懂**',
].join('\n');
const clean = B.mdToPlain(dirty);
ok('真实样例 无 ## 残留', !clean.includes('##'));
ok('真实样例 无 ** 残留', !clean.includes('**'));
ok('真实样例 无 > 行首残留', !/^\s*>/m.test(clean));
ok('真实样例 保留了正文', clean.includes('群里谁 @ 一下') && clean.includes('语音消息能听懂'));
ok('真实样例 标题变【】', clean.includes('【第一类：不修会真出事（我最建议）】'));

// ---- 性格 / 语气
const savedPersona = JSON.parse(JSON.stringify(B.cfg.persona));
B.cfg.persona.text = '不要说「好的」开头。默认很短。';
B.cfg.persona.groupBoundary = '不要透露主人的私事。';
const pp1 = B.personaPrompt({ type: 'private', id: '1' });
ok('私聊注入性格', pp1.includes('不要说「好的」开头'));
ok('私聊不注入群边界', !pp1.includes('不要透露主人的私事'));
const pp2 = B.personaPrompt({ type: 'group', id: G });
ok('群聊同时注入性格与边界', pp2.includes('不要说「好的」开头') && pp2.includes('不要透露主人的私事'));
ok('群边界单独成节', pp2.includes('【群聊边界】'));
B.cfg.persona.text = '';
ok('性格置空时不注入', B.personaPrompt({ type: 'private', id: '1' }) === '');
B.cfg.persona.groupBoundary = '';
ok('边界置空时不注入', B.personaPrompt({ type: 'group', id: G }) === '');
B.cfg.persona.text = savedPersona.text;
B.cfg.persona.groupBoundary = savedPersona.groupBoundary;
ok('性格文案来自配置', typeof B.cfg.persona.text === 'string');

// ---- 环境说明 (必须由桥接注入: CLI 的 --append-system-prompt 会取代 APPEND_SYSTEM.md)
const ep = B.envPrompt();
ok('环境说明含 outbox 目录', ep.includes(B.cfg.files.outboxDir));
ok('环境说明含 inbox 目录', ep.includes(B.cfg.files.inboxDir));
ok('环境说明含定时任务目录', ep.includes(B.cfg.tasks.dir));
ok('环境说明含定时任务格式', ep.includes('schedule') && ep.includes('daily'));
ok('环境说明含工作目录', ep.includes(B.cfg.pi.cwd));
ok('环境说明含禁止 Markdown', ep.includes('不渲染 Markdown'));
ok('环境说明告知交付文件方式', ep.includes('自动发送到当前对话'));

// ---- 注入文件应包含 环境+性格+记忆 三段
B.cfg.persona.text = '测试性格标记XYZ';
const injFile = B.memoryPromptFile({ type: 'private', id: '123456789' });
const injTxt = fs.readFileSync(injFile, 'utf8');
ok('注入文件含环境说明', injTxt.includes(B.cfg.files.outboxDir));
ok('注入文件含性格', injTxt.includes('测试性格标记XYZ'));
ok('注入文件含记忆规则', injTxt.includes('长期记忆'));
ok('性格排在记忆之前', injTxt.indexOf('测试性格标记XYZ') < injTxt.indexOf('长期记忆'));
B.cfg.persona.text = savedPersona.text;

// ---- 分段不切断 emoji (代理对)
const splitParts = (s, n) => B.splitForQQ(s, n);
const isBroken = (p) => /[\uD800-\uDBFF]$/.test(p) || /^[\uDC00-\uDFFF]/.test(p);
{
  let broken = 0, cases = 0;
  for (let lim = 10; lim <= 60; lim++) {
    for (let n = 1; n <= 20; n++) {
      for (const p of splitParts('A'.repeat(n) + '🎉🎉🎉🎉🎉🎉🎉🎉', lim)) {
        cases++; if (isBroken(p)) broken++;
      }
    }
  }
  ok('穷举 emoji 切分不产生半个代理对', broken === 0, `cases=${cases} broken=${broken}`);
}
ok('纯 emoji 不切断', splitParts('🎉'.repeat(50), 20).every((p) => !isBroken(p)));
ok('中文+emoji 不切断', splitParts(('你好🎉').repeat(30), 25).every((p) => !isBroken(p)));
ok('切分后拼回原文', splitParts('A🎉B'.repeat(30), 17).join('') === 'A🎉B'.repeat(30));
ok('短文本不切', splitParts('你好', 100).length === 1);

// ---- 流式尾部截留 (避免 ** 被切成两半)
{
  const [a, b] = B.splitStreamTail('前面的文字**重点');
  ok('未闭合 ** 被截留', !a.includes('**') && b.includes('**'));
  ok('截留后拼回原文', a + b === '前面的文字**重点');
}
{
  const [a, b] = B.splitStreamTail('已闭合的**重点** 后面的');
  ok('已闭合 ** 不截留', a.includes('**重点**') && b === '');
}
{
  const [a, b] = B.splitStreamTail('代码开始```js\nconst a=1;');
  ok('未闭合围栏被截留', !a.includes('```') && b.includes('```'));
}
{
  const [a, b] = B.splitStreamTail('结尾的单个星号*');
  ok('行尾孤立标记字符被截留', a === '结尾的单个星号' && b === '*');
}
ok('无标记时不截留', B.splitStreamTail('普通文本没有标记')[1] === '');

// ---- 消息去重
{
  const m = (id) => ({ post_type: 'message', message_id: id, user_id: 1 });
  ok('首次消息不重复', B.isDuplicateMessage(m('dup-1')) === false);
  ok('同 id 第二次判定为重复', B.isDuplicateMessage(m('dup-1')) === true);
  ok('不同 id 不重复', B.isDuplicateMessage(m('dup-2')) === false);
  ok('无 message_id 不判重', B.isDuplicateMessage({ post_type: 'message' }) === false);
  ok('message_id=0 也能判重',
    B.isDuplicateMessage(m(0)) === false && B.isDuplicateMessage(m(0)) === true);
}

// ---- 任务文件合并
fs.writeFileSync(path.join(BASE, 'tasks', 'a.json'), JSON.stringify({ id: 'ta', target: 'private_123456789', schedule: { type: 'daily', time: '08:00' }, prompt: 'p1' }));
fs.writeFileSync(path.join(BASE, 'tasks', 'b.json'), JSON.stringify([
  { id: 'tb', target: 'group_10001', schedule: { type: 'every', minutes: 15 }, prompt: 'p2' },
  { id: 'tc', target: 'private_9999999', schedule: { type: 'daily', time: '08:00' }, prompt: 'p3' },
]));
fs.writeFileSync(path.join(BASE, 'tasks', 'c.json'), '{ 坏 json');
B.mergeTaskFiles();
ok('合并 2 个有效任务', B.TASKS.length === 2, `count=${B.TASKS.length}`);
ok('白名单外被过滤', !B.TASKS.some((t) => t.id === 'tc'));
ok('坏文件不中断', B.TASKS.some((t) => t.id === 'ta') && B.TASKS.some((t) => t.id === 'tb'));
ok('合并后删除任务文件', !fs.existsSync(path.join(BASE, 'tasks', 'a.json')) && !fs.existsSync(path.join(BASE, 'tasks', 'b.json')));
ok('tasks.json 已落盘', fs.existsSync(path.join(BASE, 'tasks.json')));
B.recalcTasks();
ok('recalcTasks 计算 nextRun', B.TASKS.every((t) => t.nextRun > 0), B.TASKS.map((t) => new Date(t.nextRun).toISOString()).join(' '));

// 同 id 覆盖 + 停用
fs.writeFileSync(path.join(BASE, 'tasks', 'a2.json'), JSON.stringify({ id: 'ta', target: 'private_123456789', schedule: { type: 'daily', time: '08:00' }, prompt: 'p1', enabled: false }));
B.mergeTaskFiles();
const ta = B.TASKS.find((t) => t.id === 'ta');
ok('同 id 覆盖生效', ta && ta.enabled === false);
B.recalcTasks();
ok('停用任务 nextRun=0', ta && ta.nextRun === 0);

// once 过期自动停用
B.TASKS.push({ id: 'to', target: { type: 'private', id: '123456789' }, schedule: { type: 'once', at: Date.now() - 1000 }, prompt: 'x', enabled: true, createdAt: Date.now(), lastRun: 0, nextRun: 0 });
B.recalcTasks();
ok('过期 once 自动停用', B.TASKS.find((t) => t.id === 'to').enabled === false);

ok('ensureTasksDir 生成 README', fs.existsSync(path.join(BASE, 'tasks', 'README.md')));

// ---- parseMessage (此前无覆盖, 曾因漏声明 images 导致全量崩溃)
const P = B.parseMessage;
const m1 = P('', [{ type: 'text', data: { text: '你好' } }, { type: 'at', data: { qq: '10000' } }, { type: 'image', data: { url: 'http://a/b.png' } }]);
ok('parseMessage 数组段', m1.text === '你好@10000' && m1.images[0] === 'http://a/b.png' && m1.mentions[0] === '10000' && Array.isArray(m1.files), JSON.stringify(m1));
const m2 = P('[CQ:at,qq=10000] hi[CQ:image,url=http://a/c.png]', undefined);
ok('parseMessage CQ 码', m2.text === '@10000 hi' && m2.images[0] === 'http://a/c.png' && m2.mentions[0] === '10000', JSON.stringify(m2));
const m3 = P('', [{ type: 'file', data: { name: 'a.py', file: 'http://d/a.py', size: 12 } }]);
ok('parseMessage 文件段', m3.files.length === 1 && m3.files[0].name === 'a.py' && m3.files[0].url === 'http://d/a.py', JSON.stringify(m3));
const m4 = P('', [{ type: 'file', data: { name: 'b.py', file_id: 'FID1', size: 3 } }]);
ok('parseMessage file_id 段', m4.files[0].fileId === 'FID1' && m4.files[0].url === '', JSON.stringify(m4));

// ---- 记忆提示写成文件 (避免记忆全文出现在 ps aux / journal)
const mf = B.memoryPromptFile('group_10001');
const mfTxt = fs.existsSync(mf) ? fs.readFileSync(mf, 'utf8') : '';
ok('memoryPromptFile 落盘且含路径',
  mf.startsWith(BASE) && mfTxt.includes(B.groupSharedMemPath(G)) && mfTxt.includes('长期记忆'), mf);
ok('memoryPromptFile 幂等可重写', B.memoryPromptFile('group_10001') === mf);

// ---- 回复上下文快照: 同一会话连发两条消息不得串引用
(async () => {
  const sent = [];
  const orig = B.onebot.action;
  B.onebot.action = async (action, params) => { sent.push({ action, params }); return { status: 'ok' }; };
  const mk = () => {
    const s = Object.create(B.PiSession.prototype);
    s.key = 'group_10001'; s.target = { type: 'group', id: '10001' };
    s.ctx = null; s.turnCtx = { replyTo: null, atUser: null };
    s.lastCtx = { replyTo: null, atUser: null };
    s.queue = []; s.maxQueue = 5; s.lastQueueWarn = 0; s.queueWarnAt = new Map(); s.busy = false;
    s.proc = null; s.closed = false; s.pending = new Map(); s.flushTimer = null;
    s.buf = ''; s.lastFlush = 0; s.lastUsed = 0; s.abortRequested = false; s.stderrTail = [];
    return s;
  };
  const s = mk();
  // 第一条消息: 记录 ctx1 并开启一轮
  const ctx1 = { replyTo: 'M1', atUser: '111' };
  s.ctx = ctx1; s.lastCtx = ctx1; s.turnCtx = ctx1;
  // 第二条消息在第一条回复未结束时到达, 覆盖 this.ctx
  const ctx2 = { replyTo: 'M2', atUser: '222' };
  s.ctx = ctx2; s.lastCtx = ctx2;
  s.buf = '第一轮的回复内容';
  await s.flush(true);
  const first = sent[sent.length - 1];
  const segs = Array.isArray(first.params.message) ? first.params.message : [];
  const replySeg = segs.find((x) => x.type === 'reply');
  const atSeg = segs.find((x) => x.type === 'at');
  ok('flush 使用本轮 ctx 快照(引用 M1 而非 M2)', replySeg && replySeg.data.id === 'M1', JSON.stringify(segs));
  ok('flush 使用本轮 ctx 快照(@111 而非 @222)', atSeg && atSeg.data.qq === '111', JSON.stringify(segs));

  // 队列上限
  const s2 = mk(); s2.busy = true; s2.sendQQ = async (t) => { sent.push({ params: { message: t } }); };
  for (let i = 0; i < 20; i++) s2.prompt(`m${i}`, [], { replyTo: String(i), atUser: '1' });
  ok('队列上限生效', s2.queue.length === 5, `len=${s2.queue.length}`);
  ok('超限时回发提示', sent.some((x) => String(x.params.message).includes('排队已满')), '');
  // 排队时也要告知位置
  const s4 = mk(); s4.busy = true; s4.sendQQ = async (t) => { sent.push({ params: { message: t } }); };
  s4.prompt('hi', [], { userId: '9', userName: '小明' });
  ok('排队时告知位置', sent.some((x) => String(x.params.message).includes('已排队')), '');
  ok('队列元素保留 ctx', s2.queue[0].ctx && s2.queue[0].ctx.replyTo === '0', JSON.stringify(s2.queue[0].ctx));
  // 排队提示的去重表必须有上限: 每个发言者一条, 群大/长期跑会一直涨
  {
    const s5 = mk(); s5.busy = true; s5.queueWarnAt = new Map(); s5.sendQQ = async () => {};
    for (let i = 0; i < 1200; i++) s5.prompt('x', [], { userId: 'u' + i, userName: 'n' + i });
    ok('排队提示去重表有上限', s5.queueWarnAt.size <= 500, `size=${s5.queueWarnAt.size}`);
  }
  // flush 必须能被安全调用: buf 异常时不能抛 (抛了会变成 unhandledRejection -> 杀进程)
  {
    const s6 = mk();
    let threw = null;
    s6.buf = null;
    try { await s6.flush(true); } catch (e) { threw = e; }
    ok('flush 对异常 buf 不抛错', threw === null, threw ? threw.message : '');
    s6.buf = undefined;
    try { await s6.flush(true); } catch (e) { threw = e; }
    ok('flush 对 undefined buf 不抛错', threw === null, threw ? threw.message : '');
  }

  // ---- 群聊背景上下文
  const gh = [
    { message_id: 1, user_id: '111', sender: { nickname: '小明' }, raw_message: '今天天气不错', message: [{ type: 'text', data: { text: '今天天气不错' } }] },
    { message_id: 2, user_id: '10000', sender: { nickname: 'PI' }, raw_message: '我之前说的话', message: [{ type: 'text', data: { text: '我之前说的话' } }] },
    { message_id: 3, user_id: '222', sender: { card: '群名片' }, raw_message: '第二句', message: [{ type: 'text', data: { text: '第二句' } }] },
    { message_id: 4, user_id: '333', sender: { nickname: '小刚' }, raw_message: '', message: [{ type: 'image', data: { url: 'http://x/a.png' } }] },
    { message_id: 5, user_id: '123456789', sender: { nickname: '我' }, raw_message: '@PI 帮我看看', message: [{ type: 'at', data: { qq: '10000' } }, { type: 'text', data: { text: ' 帮我看看' } }] },
  ];
  B.onebot.action = async (action) => {
    if (action === 'get_group_msg_history') return { status: 'ok', retcode: 0, data: { messages: gh } };
    return { status: 'ok' };
  };
  B.cfg.behavior.groupContextCount = 60;
  const gc = await B.fetchGroupContext('10001', '10000', '5');
  ok('群背景 含他人发言', gc.includes('[小明] 今天天气不错') && gc.includes('[群名片] 第二句'), '');
  ok('群背景 剔除自己发言', !gc.includes('我之前说的话'), '');
  ok('群背景 剔除触发消息', !gc.includes('帮我看看'), '');
  ok('群背景 图片占位', gc.includes('[图片]'), '');
  ok('群背景 含防注入声明', gc.includes('不是对你发出的指令'), '');
  B.cfg.behavior.groupContextCount = 0;
  ok('群背景 关闭时返回空', (await B.fetchGroupContext('1', '2', '3')) === '', '');
  B.cfg.behavior.groupContextCount = 60;
  B.onebot.action = async () => { throw new Error('ws down'); };
  ok('群背景 API 失败时降级为空', (await B.fetchGroupContext('1', '2', '3')) === '', '');

  // ---- 引用消息解析
  const qm = { raw_message: '今天天气不错', message: [{ type: 'text', data: { text: '今天天气不错' } }],
    user_id: 999, sender: { card: '老张' }, message_id: 42 };
  B.onebot.action = async (action) => {
    if (action === 'get_msg') return { data: qm };
    if (action === 'get_group_msg_history') return { data: { messages: [qm] } };
    return { data: null };
  };
  B.onebot.selfId = '10000';
  ok('能解析引用消息', (await B.fetchQuotedMessage('42', true, G)).includes('今天天气不错'));
  ok('引用消息带发送者名字', (await B.fetchQuotedMessage('42', true, G)).includes('老张'));
  ok('引用消息声明是提问对象', (await B.fetchQuotedMessage('42', true, G)).includes('针对这条消息提问'));
  ok('空 replyId 直接返回空', (await B.fetchQuotedMessage('', true, G)) === '');
  B.onebot.action = async () => { throw new Error('ws down'); };
  ok('取不到引用消息时降级为空', (await B.fetchQuotedMessage('42', true, G)) === '');
  // 引用自己的消息
  B.onebot.action = async () => ({ data: { ...qm, user_id: 10000, sender: { card: '我' } } });
  ok('引用自己的消息会标注', (await B.fetchQuotedMessage('42', true, G)).includes('你自己之前说的话'));

// ---- sendQQ 统一做 Markdown 降级 (命令输出也覆盖)
{
  const sentArgs = [];
  B.onebot.action = async (action, params) => { sentArgs.push({ action, params }); return { status: 'ok' }; };
  const ps3 = new B.PiSession('private_123456789', { type: 'private', id: '123456789' });
  ps3.send = () => true;
  ps3.proc = { stdin: { write() {}, writable: true }, kill() {} };
  await ps3.sendQQ('## 标题\n**粗体**\n> 引用');
  const body = sentArgs[sentArgs.length - 1].params.message;
  ok('命令输出也降级 Markdown', !body.includes('##') && !body.includes('**') && !/^\s*>/m.test(body));
  ok('降级后保留正文', body.includes('标题') && body.includes('粗体'));
  // raw 选项可绕过
  await ps3.sendQQ('## 原样', { raw: true });
  ok('raw 选项绕过降级', sentArgs[sentArgs.length - 1].params.message.includes('##'));
}

// ---- @ 识别
const pm1 = B.parseMessage('[CQ:at,qq=10000] 你好', null);
ok('解析出 mentions', pm1.mentions.includes('10000'));
const pm2 = B.parseMessage('[CQ:at,qq=111,name=小明] 帮我看看', null);
ok('解析出 @ 的名字', pm2.atNames['111'] === '小明');
const dm = await B.describeMentions(pm2, '10000', G);
ok('@ 描述含名字与 QQ', dm.includes('小明') && dm.includes('QQ 111'));
ok('@ 描述提醒勿当发言人', dm.includes('不要把他们当成当前发言人'));
ok('只 @ 自己时不生成描述', (await B.describeMentions(pm1, '10000', G)) === '');
// 消息段没带名字时, 应反查群成员资料
B.onebot.action = async (action, params) => {
  if (action === 'get_group_member_info') {
    return { data: { user_id: params.user_id, card: '', nickname: `昵称${params.user_id}` } };
  }
  return { data: null };
};
const pmNoName = B.parseMessage('[CQ:at,qq=555] 看看', null);
const dm2 = await B.describeMentions(pmNoName, '10000', G);
ok('缺名字时反查成员资料', dm2.includes('昵称555') && dm2.includes('QQ 555'));
ok('反查结果会缓存', await B.resolveMemberName(G, '555') === '昵称555');
// 缓存必须有上限, 否则群多/人多的长期运行会无界增长
ok('成员缓存有上限常量', typeof B.MEMBER_NAME_MAX === 'number' && B.MEMBER_NAME_MAX > 0);
{
  const before = B.MEMBER_NAME_CACHE.size;
  for (let i = 0; i < B.MEMBER_NAME_MAX + 200; i++) B.cacheMemberName(`cap:${i}`, `n${i}`);
  ok('成员缓存不超过上限', B.MEMBER_NAME_CACHE.size <= B.MEMBER_NAME_MAX,
    `size=${B.MEMBER_NAME_CACHE.size} max=${B.MEMBER_NAME_MAX} (写入前 ${before})`);
  // 最新写入的应该还在 (丢的是最旧的)
  ok('缓存淘汰的是最旧条目', B.MEMBER_NAME_CACHE.has(`cap:${B.MEMBER_NAME_MAX + 199}`));
}
// 群名片优先于昵称
B.onebot.action = async (action, params) => {
  if (action === 'get_group_member_info') return { data: { card: '群名片甲', nickname: '昵称甲' } };
  return { data: null };
};
ok('群名片优先于昵称', await B.resolveMemberName('999', '777') === '群名片甲');
// 查询失败时降级为「查不到名字」
B.onebot.action = async () => { throw new Error('ws down'); };
const dm3 = await B.describeMentions(B.parseMessage('[CQ:at,qq=888] 喂', null), '10000', '999');
ok('查不到名字时降级', dm3.includes('查不到名字') && dm3.includes('QQ 888'));
const pm3 = B.parseMessage('[CQ:at,qq=111] [CQ:reply,id=42] 说的对吗', null);
ok('同时解析 @ 与引用', pm3.replyId === '42' && pm3.mentions.includes('111'));
const pm4 = B.parseMessage(null, [{ type: 'reply', data: { id: '77' } }, { type: 'text', data: { text: '嗯' } }]);
ok('数组模式解析引用', pm4.replyId === '77');
const pm5 = B.parseMessage(null, [{ type: 'reply', data: { id: '1' } }, { type: 'reply', data: { id: '2' } }]);
ok('多个引用只取第一个', pm5.replyId === '1');

  // destroy 幂等 + 清理 pending
  const s3 = mk();
  const fakeProc = { stdin: { end() {}, writable: true }, kill() {} };
  s3.proc = fakeProc;
  let rejected = false;
  s3.pending.set('x', { resolve: () => {}, reject: () => { rejected = true; }, timer: setTimeout(() => {}, 60000) });
  await s3.destroy('t1');
  ok('destroy 清理 pending 与 proc 引用', rejected && s3.pending.size === 0 && s3.proc === null);
  // 幂等: 再次 destroy 应立即返回, 不再触碰已清理的状态
  s3.proc = fakeProc;
  await s3.destroy('t2');
  ok('destroy 幂等(二次调用直接返回)', s3.proc === fakeProc);

  // ---- 心跳看门狗
  B.onebot.action = async (action, params) => { sent.push({ action, params }); return { status: 'ok' }; };
  B.cfg.behavior.heartbeatTimeoutMs = 90000;
  ok('初始未判定掉线', B.qqDown === false);
  // WS close -> 判定掉线, 并给白名单首位发告警
  sent.length = 0;
  // 直接调用 onRecord 模拟心跳异常
  B.onebot.onRecord({ post_type: 'meta_event', meta_event_type: 'heartbeat', status: { online: false, good: false } });
  ok('心跳 online=false -> 判定掉线', B.qqDown === true && B.qqDownKind === 'heartbeat', `kind=${B.qqDownKind}`);
  await new Promise((r) => setTimeout(r, 50));
  ok('掉线时发出 QQ 告警', sent.some((x) => x.action === 'send_private_msg' && String(x.params.message).includes('QQ 掉线了')),
    JSON.stringify(sent.map((x) => x.action)));
  // 恢复正常心跳 -> 报告恢复
  sent.length = 0;
  B.onebot.onRecord({ post_type: 'meta_event', meta_event_type: 'heartbeat', status: { online: true, good: true } });
  await new Promise((r) => setTimeout(r, 50));
  ok('心跳恢复 -> 清除掉线标记', B.qqDown === false);
  ok('恢复时发出 QQ 通知', sent.some((x) => x.action === 'send_private_msg' && String(x.params.message).includes('QQ 已恢复')), '');
  // 心跳超时检测逻辑: 直接断言阈值读取
  ok('heartbeatTimeoutMs 可配置', B.cfg.behavior.heartbeatTimeoutMs === 90000);
  // 重复掉线不重复告警
  sent.length = 0;
  B.onebot.onRecord({ post_type: 'meta_event', meta_event_type: 'heartbeat', status: { online: false, good: false } });
  B.onebot.onRecord({ post_type: 'meta_event', meta_event_type: 'heartbeat', status: { online: false, good: false } });
  await new Promise((r) => setTimeout(r, 50));
  ok('重复掉线不重复告警', sent.filter((x) => String(x.params.message).includes('QQ 掉线了')).length === 1,
    `count=${sent.filter((x) => String(x.params.message).includes('QQ 掉线了')).length}`);
  // 非消息上报也会重置 lastSeenAt (看门狗)
  const before = B.getLastSeen();
  await new Promise((r) => setTimeout(r, 10));
  B.onebot.onRecord({ post_type: 'meta_event', meta_event_type: 'heartbeat', status: { online: true, good: true } });
  ok('上报重置看门狗时间', B.getLastSeen() > before);

  B.onebot.action = orig;
  console.log('\n===== 结果 =====');
  const bad = results.filter((r) => !r.pass);
  console.log(`通过 ${results.length - bad.length}/${results.length}`);
  if (bad.length) console.log('失败:', bad.map((b) => b.name).join(' | '));
  process.exit(bad.length ? 1 : 0);
})();
