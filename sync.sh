#!/usr/bin/env bash
#
# 把「运行版」同步到「开源版」。
#
# 为什么要这个: 两个目录是同一份代码的两个化身 ——
#   运行版 (/opt/pi-qq)       带真实 QQ 号/群号/绝对路径, 你自己在跑
#   开源版 (/opt/pi-qq-repo)  去掉隐私、路径改成相对的, 推到 GitHub
# 改完运行版后跑一次这个脚本, 开源版就跟着更新, 不用手动合。
#
# 用法:
#   ./sync.sh           同步并显示差异
#   ./sync.sh --push    同步后直接提交并推送
#   ./sync.sh --check   只检查是否已同步 (不改动), 用于 CI/手动核对
#
set -euo pipefail

RUN_DIR="${RUN_DIR:-/opt/pi-qq}"
PUB_DIR="${PUB_DIR:-/opt/pi-qq-repo}"

# 需要通用化的文件 (运行版 -> 开源版 会做替换)
FILES=(bridge.js utest.js itest.js fixtest.js config.example.json)

# 从运行版原样复制、不做替换的文件
COPY_AS_IS=(LICENSE .gitignore)

# 原样复制的目录 (不做替换)
COPY_DIRS=(fixtures)

# ---------------------------------------------------------------- 隐私替换规则
# 顺序有意义: 先长后短, 避免子串误伤。
# 注意: 这些是「运行环境」的值, 换机器/换号时改这里。
declare -a SED_RULES=(
  # QQ 号 / 群号 -> 占位符
  "s|2452448276|123456789|g"       # 主人 QQ
  "s|317624779|10001|g"            # 群 1
  "s|648671136|10002|g"            # 群 2
  "s|3350987108|10000|g"           # 机器人自己
  # 掉线通知文案: 先整体换成通用说法, 避免 "千雪" 被替换成 "PI掉线了" 这种别扭结果。
  # 顺序重要: 必须在下面的 "千雪 -> PI" 之前。
  "s|千雪掉线了|QQ 掉线了|g"
  "s|千雪已恢复|QQ 已恢复|g"
  # 昵称 -> 通用名
  "s|千雪|PI|g"
  # 绝对路径 -> 相对安装目录
  "s|'/opt/pi-qq/workspace'|path.join(ROOT, 'workspace')|g"
  "s|'/opt/pi-qq/sessions'|path.join(ROOT, 'sessions')|g"
  "s|'/opt/pi-qq/inbox'|path.join(ROOT, 'inbox')|g"
  "s|'/opt/pi-qq/memory'|path.join(ROOT, 'memory')|g"
  "s|'/opt/pi-qq/tasks'|path.join(ROOT, 'tasks')|g"
  "s|'/opt/napcat/config/outbox'|process.env.PI_QQ_OUTBOX \|\| path.join(ROOT, 'outbox')|g"
  # 文档里的路径
  "s|/opt/pi-qq/|<安装目录>/|g"
  "s|/opt/pi-qq|<安装目录>|g"
)

log()  { printf '\033[36m%s\033[0m\n' "$*"; }
warn() { printf '\033[33m%s\033[0m\n' "$*" >&2; }
err()  { printf '\033[31m%s\033[0m\n' "$*" >&2; }

# ---------------------------------------------------------------- 前置检查

for d in "$RUN_DIR" "$PUB_DIR"; do
  if [ ! -d "$d" ]; then
    err "目录不存在: $d"
    exit 1
  fi
done

if [ ! -f "$RUN_DIR/bridge.js" ]; then
  err "$RUN_DIR 里没有 bridge.js"
  exit 1
fi

if [ ! -d "$PUB_DIR/.git" ]; then
  warn "警告: $PUB_DIR 不是 git 仓库, 同步后无法提交/推送"
fi

# ---------------------------------------------------------------- 生成通用化内容

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# 示例配置里的性格文案要清空 —— 那是使用者自己的偏好, 不该进公开模板
clear_persona() {
  node -e '
    const fs = require("fs");
    const p = process.argv[1];
    const d = JSON.parse(fs.readFileSync(p, "utf8"));
    if (d.persona) { d.persona.text = ""; d.persona.groupBoundary = ""; }
    fs.writeFileSync(p, JSON.stringify(d, null, 2) + "\n");
  ' "$1" 2>/dev/null || true
}

log "生成通用化版本..."
for f in "${FILES[@]}"; do
  if [ ! -f "$RUN_DIR/$f" ]; then
    warn "  跳过 $f (运行版没有)"
    continue
  fi
  cp "$RUN_DIR/$f" "$TMP/$f"
  for rule in "${SED_RULES[@]}"; do
    sed -i "$rule" "$TMP/$f"
  done
  printf '  ✓ %s\n' "$f"
done

# 示例配置: 清掉个人性格
if [ -f "$TMP/config.example.json" ]; then
  clear_persona "$TMP/config.example.json"
  printf '  ✓ config.example.json 性格文案已清空\n'
fi

# 原样复制的目录
for d in "${COPY_DIRS[@]}"; do
  if [ -d "$RUN_DIR/$d" ]; then
    cp -r "$RUN_DIR/$d" "$TMP/$d"
    printf '  ✓ %s/ (原样)\n' "$d"
  fi
done

# 原样复制的
for f in "${COPY_AS_IS[@]}"; do
  if [ -f "$RUN_DIR/$f" ]; then
    cp "$RUN_DIR/$f" "$TMP/$f"
    printf '  ✓ %s (原样)\n' "$f"
  fi
done

# ---------------------------------------------------------------- 隐私自检
# 这一步是关键: 宁可同步失败, 也不能把隐私推上去。

log "隐私自检..."
LEAK=0
for f in "$TMP"/*; do
  base="$(basename "$f")"
  if grep -qE '2452448276|317624779|648671136|3350987108|千雪|/opt/pi-qq|/opt/napcat' "$f" 2>/dev/null; then
    err "  ✗ $base 仍含隐私/绝对路径:"
    grep -nE '2452448276|317624779|648671136|3350987108|千雪|/opt/pi-qq|/opt/napcat' "$f" | head -5 | sed 's/^/      /'
    LEAK=1
  else
    printf '  ✓ %s 干净\n' "$base"
  fi
done

if [ "$LEAK" -ne 0 ]; then
  err ""
  err "自检未通过 —— 没有写入开源版。"
  err "请检查上面的行, 并把新出现的敏感值加进脚本的 SED_RULES。"
  exit 1
fi

# ---------------------------------------------------------------- 语法检查

log "语法检查..."
if command -v node >/dev/null 2>&1; then
  for f in bridge.js utest.js itest.js; do
    [ -f "$TMP/$f" ] || continue
    if node --check "$TMP/$f" 2>/dev/null; then
      printf '  ✓ %s\n' "$f"
    else
      err "  ✗ $f 语法错误:"
      node --check "$TMP/$f" 2>&1 | head -5 | sed 's/^/      /'
      exit 1
    fi
  done
else
  warn "  未找到 node, 跳过"
fi

# ---------------------------------------------------------------- 比对差异

CHANGED=0
for f in "$TMP"/*; do
  base="$(basename "$f")"
  target="$PUB_DIR/$base"
  if [ -d "$f" ]; then
    if [ ! -d "$target" ]; then
      printf '\033[32m新增\033[0m  %s/\n' "$base"
      CHANGED=1
    elif ! diff -rq "$f" "$target" >/dev/null 2>&1; then
      printf '\033[33m变更\033[0m  %s/\n' "$base"
      CHANGED=1
    fi
    continue
  fi
  if [ ! -f "$target" ]; then
    printf '\033[32m新增\033[0m  %s\n' "$base"
    CHANGED=1
  elif ! cmp -s "$f" "$target"; then
    n=$(diff "$target" "$f" | grep -c '^[<>]' || true)
    printf '\033[33m变更\033[0m  %s  (%s 行差异)\n' "$base" "$n"
    CHANGED=1
  fi
done

# 提交并推送。抽成函数是因为「内容无变化但仓库里有未提交的改动」也要能推上去 ——
# 以前 CHANGED=0 时直接 exit, 第二次跑 --push 就永远提交不了。
commit_and_push() {
  log ""
  log "提交并推送..."
  cd "$PUB_DIR"
  if [ -z "$(git status --porcelain)" ]; then
    log "  没有实际变化"
  else
    MSG="${1:-sync: 从运行版同步}"
    git add -A
    git commit -q -m "$MSG"
    git push -q origin "$(git rev-parse --abbrev-ref HEAD)"
    log "  已推送: $MSG"
  fi
}

if [ "$CHANGED" -eq 0 ]; then
  log ""
  log "已经是最新的, 无需同步。"
  if [ "${1:-}" = "--push" ] && [ -d "$PUB_DIR/.git" ]; then
    commit_and_push "${2:-}"
  fi
  exit 0
fi

if [ "${1:-}" = "--check" ]; then
  log ""
  log "(--check 模式, 未写入)"
  exit 1
fi

# ---------------------------------------------------------------- 写入

log ""
log "写入开源版..."
for f in "$TMP"/*; do
  base="$(basename "$f")"
  if [ -d "$f" ]; then
    rm -rf "$PUB_DIR/$base"
    cp -r "$f" "$PUB_DIR/$base"
  else
    cp "$f" "$PUB_DIR/$base"
  fi
  printf '  ✓ %s\n' "$base"
done

# ---------------------------------------------------------------- 可选: 提交推送

if [ "${1:-}" = "--push" ]; then
  commit_and_push "${2:-}"
fi

log ""
log "完成。开源版已更新: $PUB_DIR"
