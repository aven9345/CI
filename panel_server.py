#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CI 流水线结果可视化面板（零依赖，仅用 Python 标准库）

  - /               流水线总览（按 提交 分组）
  - /?project=<项目> 项目总览（只显示该仓库的报告；提交视图"返回总览"落到这里）
  - /?commit=<sha>  单次提交视图（pre-push 钩子自动弹这个）
  - POST /api/report        脚本上报
  - POST /api/report_archive 报告存档（report.py 把生成好的报告文件交给面板
                              进程落盘：面板以 bowen.dong 身份运行、本就持有
                              reports/ 写权限；pre-receive 钩子以 git 用户运行，
                              无文件系统写权限也不用开 —— 存盘统一走这里）
  - POST /api/system_hook   GitLab 系统钩子（项目删除/重命名时自动清理数据）
  - GET  /api/results       列表（content 只带预览，可用 ?commit=<sha> / ?project=<项目> 过滤）
  - GET  /api/detail        单条记录全文（?commit=<sha>&function=<功能块名>[&project=<项目>]）

存储：按项目分文件，reports/<组>/<项目>.json，与 GitLab 路径一一对应，
      各自保留最近 MAX_RECORDS_PER_PROJECT 条流水线（按 commit+push
      轮次归组，同组记录整组保留），互不影响。
      每次上报都追加新记录（不做按 key 覆盖），同一流水线/提交/功能块的
      每次上报（running → 成功/失败等）都会完整保留在时间线里。
      同一 commit 被 pre-receive 拦截后重推时 commit 不变，靠 push_seq
      （push 轮次，加载时按到达顺序推算）区分成多次"提交"，各轮互不影响。
      轮次判定优先用钩子上报里的 run_id（每次钩子执行一个），老数据
      退回终态启发式；卡在 running 的记录（被新轮次取代或超时无终态）
      按失败处理并标记 expired。

项目数据的自动清理（两条机制，互为补充）：
  1. 系统钩子：GitLab 管理后台配好 System Hook 后，项目被删除/重命名时
     面板会实时删除/改名对应数据文件；
  2. 闲置兜底：超过 IDLE_CLEANUP_DAYS 天没有新上报的项目，数据自动删除。

运行: python3 panel_server.py（前端页面 panel_dashboard.html 须与本脚本同目录）
访问: http://10.10.15.106:9000
"""
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import urlsplit, parse_qs


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    request_queue_size = 128

PORT = 9000
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "reports")          # 每个项目一个 json 文件
LEGACY_DATA_FILE = os.path.join(BASE_DIR, "reports.json")  # 旧版单文件（启动时自动迁移）
MAX_RECORDS_PER_PROJECT = 100   # 每个项目最多保留的流水线条数（按 commit+push
                                # 轮次归组，与网页总览一行同口径；同组的各功能块
                                # 记录整组保留/整组丢弃）
PREVIEW_CHARS = 300             # 列表接口 content 预览长度（全文走 /api/detail）

# 报告存档子目录：面板数据 reports/<组>/<项目>.json 与报告文件并存于 DATA_DIR，
# 报告放 reports/<组>/<项目>/（报告目录与 json 数据文件同根，便于备份与维护）。
# 存档统一走 /api/report_archive（面板进程自己写，天然有权限；
# pre-receive 钩子以 git 用户运行，无需给它开文件系统写权限）。
REPORT_ARCHIVE_DIR = os.path.join(DATA_DIR, "__reports__")

# 报告存档清理阈值（天）：不设上限时帧数无限膨胀，改成和面板数据
# 一样的"保留最近 N 天"策略；0 表示不清理。
REPORT_RETENTION_DAYS = 90

# 正式文档生成脚本（审计交付用）：与 panel_server.py 同目录部署。
# /api/report_archive 存档完成后由面板进程后台调用它，把报告重写为
# 正式版（GJB 438C 附录 N / GB/T 9386 / ISO/IEC/IEEE 29119-3 骨架，
# 状态一律文字化）并生成 docx/pdf（取决于服务器 pandoc/xelatex 环境）。
# 文档生成逻辑全部在 doc_generator.py 里，面板只负责触发；
# 生成失败只记日志，不影响已存档的原始文件与面板主流程。
DOC_GENERATOR = os.path.join(BASE_DIR, "doc_generator.py")

IDLE_CLEANUP_DAYS = 90          # 项目超过 N 天无新上报，自动删除其数据文件（0 = 关闭）
CLEANUP_INTERVAL = 24 * 3600    # 闲置清理的执行间隔（秒）

# running 中断收敛：running 超过 N 分钟仍无终态（脚本崩溃/卡死），
# 或同一 commit 已出现更新的 push 轮次（旧进程必已被取代），
# 一律按失败处理并标记 expired（0 = 关闭超时判定，只保留轮次取代判定）。
# 一轮 CI 几分钟内出结果，15 分钟无终态即可判定为中断。
RUNNING_EXPIRE_MINUTES = 15

# 系统钩子密钥：与 GitLab 管理后台 System Hook 里填的 Secret Token 一致。
# 为空则不校验（内网测试可留空，推广到公司时建议设置）。
SYSTEM_HOOK_TOKEN = os.environ.get("PANEL_HOOK_TOKEN", "")

_lock = threading.Lock()


def _project_file(project):
    """项目路径 -> 数据文件路径，按 GitLab 组/项目层级建目录。

    例如 bowen.dong/ci_test -> reports/bowen.dong/ci_test.json
    与 GitLab 页面上的项目路径一一对应，方便后期维护和查询。
    段内非法字符替换为下划线，并过滤 . / .. 防止路径穿越。

    注：报告存档也按同一分组建目录（见 save_report_archive），
    但放 DATA_DIR/__reports__ 下，不与 json 数据文件混在一处。
    """
    parts = []
    for seg in (project or "未知项目").split("/"):
        seg = re.sub(r"[^A-Za-z0-9._-]", "_", seg)
        if seg in ("", ".", ".."):
            continue
        parts.append(seg)
    if not parts:
        parts = ["未知项目"]
    *dirs, name = parts
    return os.path.join(DATA_DIR, *dirs, name + ".json")


def _report_archive_path(project, commit, stamp=""):
    """报告存档目录：DATA_DIR/__reports__/<组>/<项目>/<commit>_<时间戳>/。

    commit/stamp 都只取干净片段（十六进制 sha + 数字时间戳），防路径穿越；
    不带 stamp 时返回项目级报告根目录（清理用）。
    """
    parts = []
    for seg in (project or "未知项目").split("/"):
        seg = re.sub(r"[^A-Za-z0-9._-]", "_", seg)
        if seg in ("", ".", ".."):
            continue
        parts.append(seg)
    if not parts:
        parts = ["未知项目"]
    base = os.path.join(REPORT_ARCHIVE_DIR, *parts)
    if not commit:
        return base
    commit = re.sub(r"[^0-9a-fA-F]", "", str(commit))[:40]
    stamp = re.sub(r"[^0-9_]", "", str(stamp or ""))[:32]
    name = "%s_%s" % (commit, stamp) if stamp else commit
    return os.path.join(base, name)


def save_report_archive(project, commit, stamp, files):
    """把报告文件写入 DATA_DIR/__reports__/<组>/<项目>/<commit>_<时间戳>/。

    files: {相对文件名: 内容文本}（markdown 正文 / 或 base64 二进制附件）。
    全部由面板进程（bowen.dong）写盘 —— pre-receive 钩子（git 用户）无需
    任何文件系统写权限。写完返回目录绝对路径与文件名清单。
    """
    out_dir = _report_archive_path(project, commit, stamp)
    with _lock:
        os.makedirs(out_dir, exist_ok=True)
        made = []
        for name, payload in files.items():
            name = re.sub(r"[^A-Za-z0-9._-]", "_", str(name))
            if not name or name in (".", ".."):
                continue
            data = payload.get("data") if isinstance(payload, dict) \
                else payload
            if isinstance(payload, dict) and payload.get("encoding") == "base64":
                import base64
                raw = base64.b64decode(data)
                with open(os.path.join(out_dir, name), "wb") as fh:
                    fh.write(raw)
            else:
                with open(os.path.join(out_dir, name), "w",
                          encoding="utf-8") as fh:
                    fh.write(str(data or ""))
            made.append(name)
    return out_dir, made


def find_report_archive(project, commit, at=""):
    """定位某次提交的报告存档目录（同一 commit 每次 push 各存一份，可能有多份）。

    目录名 <commit>_<yyyymmdd_HHMMSS>，commit 双向前缀匹配（长/短 sha）。
    多份时优先选"不晚于 at（report 记录上报时间，含 10 分钟容错）的最新
    一份"——即产生该条 report 记录的那次 push 的存档；选不到再退最新一份。
    """
    base = _report_archive_path(project, "")
    if not os.path.isdir(base):
        return None
    cands = []
    for name in os.listdir(base):
        d = os.path.join(base, name)
        if not os.path.isdir(d):
            continue
        cpart = name.split("_", 1)[0]
        if _commit_match(cpart, commit):
            sfx = re.sub(r"[^0-9]", "", name.split("_", 1)[1]) \
                if "_" in name else ""
            cands.append((sfx, d))
    if not cands:
        return None
    cands.sort()   # 时间戳字典序即时间序
    stamp_at = re.sub(r"[^0-9]", "", at or "")[:14]
    if len(stamp_at) == 14:
        try:
            dt = datetime.strptime(stamp_at, "%Y%m%d%H%M%S") + timedelta(minutes=10)
            stamp_at = dt.strftime("%Y%m%d%H%M%S")
            le = [c for c in cands if c[0] and c[0] <= stamp_at]
            if le:
                return le[-1][1]
        except ValueError:
            pass
    return cands[-1][1]


def cleanup_old_report_archives(project, keep_commit=None):
    """清理该项目的过期报告目录：只保留最近 REPORT_RETENTION_DAYS 天
    且（若给了 keep_commit）属于该提交的报告目录。

    由面板进程执行，避免给钩子/脚本开文件系统写权限。
    """
    if REPORT_RETENTION_DAYS <= 0:
        return []
    base = _report_archive_path(project, "")
    if not os.path.isdir(base):
        return []
    cutoff = datetime.now() - timedelta(days=REPORT_RETENTION_DAYS)
    removed = []
    for name in os.listdir(base):
        d = os.path.join(base, name)
        if not os.path.isdir(d):
            continue
        commit = (name.split("_", 1)[0] if "_" in name else name)
        # 未过期且属于保留提交：不删
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(d))
        except OSError:
            continue
        if mtime >= cutoff and (not keep_commit or _commit_match(commit, keep_commit)):
            continue
        shutil.rmtree(d, ignore_errors=True)
        removed.append(name)
    if removed:
        print(f"[清理] 报告存档过期清理 {len(removed)} 份: {removed[:10]}")
    return removed


TERMINAL_STATUS = ("success", "failed")


def _run_doc_generator(out_dir):
    """存档后后台调用 doc_generator.py 生成正式文档（重写 report.md 为
    正式版 + 生成 docx/pdf）。

    子进程方式、后台线程执行：pandoc 转换可能耗时数十秒，不阻塞存档接口
    的 HTTP 响应；doc_generator.py 不存在时仅提示一次，面板照常工作。
    """
    if not os.path.isfile(DOC_GENERATOR):
        print("[文档] 未找到 doc_generator.py（与 panel_server.py 同目录"
              "部署后自动启用正式文档生成）")
        return

    def _job():
        try:
            proc = subprocess.run(
                [sys.executable, DOC_GENERATOR, out_dir],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                universal_newlines=True, timeout=300)
            out = (proc.stdout or "").strip()
            if proc.returncode == 0:
                print(f"[文档] 正式文档生成完成: "
                      f"{os.path.relpath(out_dir, DATA_DIR)}"
                      + (f"\n{out}" if out else ""))
            else:
                print(f"[文档] 正式文档生成失败（原始 report.md 已保留）: "
                      f"{out[-500:]}")
        except Exception as e:
            print(f"[文档] 正式文档生成异常（不影响存档）: {e}")

    threading.Thread(target=_job, daemon=True).start()


def _feed_push_seq(state, r):
    """按到达顺序给一条记录推算 push 轮次（push_seq），并推进轮次状态。

    同一 commit 被 pre-receive 拦截后重推时 commit 不变，靠轮次把多次
    push 区分开。轮次判定（只对 pre-receive 钩子上报做判定，CI 上报
    始终跟随当前轮——同一 sha 只有 push 成功才会有 CI，不会跨轮混淆）：

    1. 记录带 run_id（钩子每次执行生成一个）：首次见到该 run_id → 新一轮，
       同一 run_id 的后续上报归入同一轮 —— 精确区分，即使上一轮卡在
       running（脚本崩溃没报终态）也能分开；
    2. 老数据/不带 run_id 的上报：退回终态启发式 —— 该功能块在当前轮
       已有终态，说明上一轮已结束、这是新一轮。

    push_seq 随记录持久化（保存时已算好），加载时优先采信；仅对没有
    push_seq 字段的老数据现场推算（推算结果在下次保存时落盘）。
    """
    cur, term, run_round = state["cur"], state["term"], state["run_round"]
    proj = r.get("project") or "-"
    commit = r.get("commit") or "-"
    fn = r.get("function") or ""
    # 同一 commit 可能以全量 sha（钩子 NEW_SHA）或短 sha（CI_COMMIT_SHORT_SHA
    # 8 位；钩子里调 report.py 传的也是短 sha）混合上报：轮次状态必须共用
    # 同一把 key，按前缀双向匹配归入本项目已见过的 commit；否则长短 sha
    # 各自独立计轮，同一轮会被拆成两半（总览行数、#N 徽章都会错乱）
    known = state["commits"].setdefault(proj, [])
    for c in known:
        if _commit_match(c, commit):
            commit = c
            break
    else:
        known.append(commit)
    key = (proj, commit)
    is_hook = str(r.get("pipeline_id") or "") == "pre-receive"
    rid = str(r.get("run_id") or "") if is_hook else ""
    # 已持久化的 push_seq 直接采信为事实：running 占位在终态到达时会被
    # 删除，轮次信息只留在各记录的 push_seq 字段里；若对删过占位的数据
    # 重新推算，同一 commit 已完成的多次 push 会坍缩回同一轮（钩子的
    # run_id 随占位一起被删了，终态记录本身推不出"这是第几轮"）。
    # 只有从未落盘过 push_seq 的老数据才走到达顺序推算。
    try:
        persisted = int(r.get("push_seq") or 0)
    except (TypeError, ValueError):
        persisted = 0
    if persisted > 0:
        rnd = persisted
        if rid:
            run_round[(proj, commit, rid)] = rnd
    elif rid:
        rkey = (proj, commit, rid)
        if rkey in run_round:
            rnd = run_round[rkey]
        else:
            rnd = cur.get(key, 0) + 1
            run_round[rkey] = rnd
    else:
        rnd = cur.get(key, 0) or 1
        if is_hook and (proj, commit, fn, rnd) in term:
            rnd = cur.get(key, 0) + 1
    cur[key] = max(cur.get(key, 0), rnd)
    r["push_seq"] = rnd
    # converged 是旧版"占位收敛"方案的遗留标记（当时 running 占位不删、
    # 状态改写成终态），现方案在终态到达时直接删除占位、启动时一次性
    # 清理存量（_purge_converged），这里只为兼容尚未清理的旧文件：
    # 被取代/中断的占位不算本轮终态，否则重放推算会把它们误判成两轮
    if r.get("status") in TERMINAL_STATUS \
            and not r.get("converged") and not r.get("expired"):
        term.add((proj, commit, fn, rnd))
    return rnd


def _expire_stale_running(records, now=None):
    """running 中断收敛（原地修改，不写回文件时仅影响本次读取）：

    满足任一条件的 running 记录按失败处理并标记 expired：
      1. 同一 (project, commit) 已出现更新的 push 轮次 —— 新一轮 push 的
         到来证明旧钩子进程已死（被取代），不可能再报终态；
      2. running 超过 RUNNING_EXPIRE_MINUTES 分钟仍无终态 —— 脚本崩溃/卡死，
         用户也不一定马上重推，不能让面板永远显示"进行中"。
    """
    max_seq = {}
    for r in records:
        key = (r.get("project") or "-", r.get("commit") or "-")
        max_seq[key] = max(max_seq.get(key, 0), int(r.get("push_seq") or 1))
    now = now or datetime.now()
    for r in records:
        if r.get("status") != "running":
            continue
        key = (r.get("project") or "-", r.get("commit") or "-")
        stale = int(r.get("push_seq") or 1) < max_seq.get(key, 1)
        if not stale and RUNNING_EXPIRE_MINUTES > 0 and r.get("time"):
            try:
                t = datetime.strptime(r["time"], "%Y-%m-%d %H:%M:%S")
                stale = (now - t).total_seconds() > RUNNING_EXPIRE_MINUTES * 60
            except ValueError:
                pass   # 时间格式异常：不碰，避免误标
        if stale:
            r["status"] = "failed"
            r["expired"] = True   # 中断标记：前端显示"已中断（按失败处理）"


def _sanitize_record(r):
    """单条记录字段类型容错（原地修改）。

    数据文件可能被旧版脚本/手工 curl 写入类型异常的字段（time 是数字、
    content 是 dict、push_seq 是字符串等）。加载时统一清洗，保证下游
    int()/strptime/字符串切片不会因一条脏记录抛异常 —— handler 线程
    一旦崩掉，客户端只能看到 "Remote end closed connection"，
    所有项目的 /api/results 都会跟着瘫痪。
    """
    for k in ("function", "status", "project", "pipeline_id", "commit",
              "time"):
        v = r.get(k)
        if v is not None and not isinstance(v, str):
            r[k] = str(v)
    v = r.get("content")
    if v is not None and not isinstance(v, str):
        try:
            r["content"] = json.dumps(v, ensure_ascii=False)
        except (TypeError, ValueError):
            r["content"] = str(v)
    try:
        r["push_seq"] = int(r.get("push_seq") or 0)
    except (TypeError, ValueError):
        r["push_seq"] = 0   # 按未持久化处理，由 _feed_push_seq 重新推算


def _load_records_file(path):
    """读取单个项目数据文件：现场推算每条记录的 push 轮次，
    并把卡死的 running（被新轮次取代 / 超时无终态）收敛为失败。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            records = json.load(f)
    except Exception:
        return []
    # 只接受"dict 列表"形态的上报记录文件：DATA_DIR 下可能混入其他 json
    # （如报告存档附件被移动/拷贝到存档目录之外），顶层 dict 或含非 dict
    # 元素的文件一律跳过，不能让一个坏文件拖垮整个 /api/results
    if not isinstance(records, list):
        return []
    records = [r for r in records if isinstance(r, dict)]
    for r in records:
        _sanitize_record(r)
    state = {"cur": {}, "term": set(), "run_round": {}, "commits": {}}
    for r in records:
        _feed_push_seq(state, r)
    _expire_stale_running(records)
    return records


def _load_project_records(project):
    path = _project_file(project)
    if not os.path.exists(path):
        return []
    return _load_records_file(path)


def load_records():
    """合并所有项目的记录（前端自行过滤），支持组层级子目录。"""
    records = []
    if not os.path.isdir(DATA_DIR):
        return records
    for dirpath, dirnames, filenames in os.walk(DATA_DIR):
        # 报告存档根目录（__reports__）里放的是报告附件（report.md /
        # report_data.json 等结构化文件），不是流水线上报记录，整个跳过——
        # 否则 dict 形态的附件会被当成记录文件解析，/api/results 直接 500
        dirnames[:] = [d for d in dirnames
                       if os.path.join(dirpath, d) != REPORT_ARCHIVE_DIR]
        for name in filenames:
            if not name.endswith(".json"):
                continue
            # 单个项目文件损坏不影响其他项目（损坏时 _load_records_file 回空）
            records.extend(_load_records_file(os.path.join(dirpath, name)))
    return records


def _commit_match(record_commit, sha):
    """提交号前缀双向匹配（前端/URL 里可能是短 sha，记录里可能是全量）。"""
    a = record_commit or ""
    b = sha or ""
    return bool(a and b and a != "未知" and (a.startswith(b) or b.startswith(a)))


def _push_match(record, push):
    """push 轮次过滤：未指定不过滤；记录缺 push_seq 时按第 1 轮对待。"""
    if not push:
        return True
    try:
        return int(record.get("push_seq") or 1) == int(push)
    except (TypeError, ValueError):
        return True


def find_record(commit, function, project="", push=""):
    """按 commit+function 找最新一条记录（详情接口）。

    带上 project 时只在对应项目内找，避免不同项目的同名功能块互相串数据；
    带上 push 时只看该轮 push（同一 commit 的多次 push 互不影响）。
    """
    best = None
    for r in load_records():
        if function and r.get("function") != function:
            continue
        if project and r.get("project") != project:
            continue
        if not _push_match(r, push):
            continue
        if not _commit_match(r.get("commit"), commit):
            continue
        if best is None or (r.get("time") or "") >= (best.get("time") or ""):
            best = r
    return best


def list_history(commit, function, project="", push=""):
    """按 commit+function 列出全部历史记录（时间正序，旧→新）。

    与 find_record 同条件过滤，但返回所有匹配记录而不是最新一条：
    同一轮 push 的多次上报（running → 成功/失败）都会完整出现在时间线里，
    互不覆盖；不同轮 push 用 push 参数隔开，互不影响。

    每条记录附带 _idx（在项目数据文件里的数组下标），前端用它精确定位
    并拉取该条全文（detail 接口按 _idx 取数，避免多条同 key 记录串数据）。
    """
    project = project or ""
    # 直接在项目文件里枚举：文件内数组顺序稳定，_idx 即数组下标，
    # 避免跨项目合并（load_records/os.walk 顺序不定）导致下标错位
    recs = _load_project_records(project)
    matches = []
    for i, r in enumerate(recs):
        if function and r.get("function") != function:
            continue
        if project and r.get("project") != project:
            continue
        if not _push_match(r, push):
            continue
        if not _commit_match(r.get("commit"), commit):
            continue
        item = dict(r)
        item["_idx"] = i
        matches.append(item)
    matches.sort(key=lambda r: r.get("time") or "")
    return matches


def find_record_by_idx(project, commit, function, idx, push=""):
    """按项目文件内数组下标精确定位某条历史记录（时间线点开用）。

    idx < 0（定位失败）时退回 find_record 的最新一条，保证前端总能看到内容。
    """
    try:
        idx = int(idx)
    except (TypeError, ValueError):
        idx = -1
    recs = _load_project_records(project or "")
    if 0 <= idx < len(recs):
        r = recs[idx]
        if (not function or r.get("function") == function) \
                and _commit_match(r.get("commit"), commit) \
                and _push_match(r, push):
            return r
    return find_record(commit, function, project, push)


def save_record(record):
    """追加一条记录，不复用/覆盖旧记录。

    running → 终态替换：上报终态（success/failed）时，把同一轮 push
    （project, commit, function, push_seq）下旧的 running 占位记录
    直接删除 —— 占位只是"等待终态"的临时状态，终态一到即无价值，
    留着只会在排查时误导（卡住无终态的 running 不在这里处理，
    由 _expire_stale_running 超时判失败并标记 expired，正常展示）。

    每个项目一个文件，各自保留最近 MAX_RECORDS_PER_PROJECT 条流水线
    （按 commit+push 轮次归组，同组各功能块记录整组保留），
    项目之间互不影响。超出的旧组整组丢弃。
    """
    with _lock:
        records = _load_project_records(record["project"])
        # 推算本条记录所属的 push 轮次：同一 commit 被拦截后重推（commit
        # 不变）算新一轮；老记录先重放一遍重建轮次状态（与加载时推算幂等）
        state = {"cur": {}, "term": set(), "run_round": {}, "commits": {}}
        for r in records:
            _feed_push_seq(state, r)
        _feed_push_seq(state, record)
        # 收敛键含 push 轮次：不同轮 push 的记录互不影响（上一轮卡住的
        # running 不会被这一轮误删，它会超时被判失败并保留展示）
        def same_push_key(r, batch=record):
            if r.get("project") != batch.get("project"):
                return False
            if r.get("function") != batch.get("function"):
                return False
            if not _commit_match(r.get("commit"), batch.get("commit")):
                return False
            if (r.get("push_seq") or 1) != (batch.get("push_seq") or 1):
                return False
            return True
        if record.get("status") in TERMINAL_STATUS:
            # 删除同轮 running 占位；expired（加载时被超时判失败的占位）
            # 一并删——它只是 running 的另一种表现，终态到了同样是垃圾记录
            records = [r for r in records
                       if not (same_push_key(r)
                               and (r.get("status") == "running"
                                    or r.get("expired")))]
        records.append(record)
        # 留存按"流水线条数"计：归组键 (commit, push_seq)，与网页总览
        # 一行同口径。只保留最新 MAX_RECORDS_PER_PROJECT 组，同组的
        # 各功能块记录（running→终态、钩子+CI 各一条）整组保留或整组
        # 丢弃，不会出现"某条流水线只剩一半功能块"的残缺历史。
        last_seen = {}
        for i, r in enumerate(records):
            last_seen[(r.get("commit") or "-",
                       int(r.get("push_seq") or 1))] = i
        keep = set(sorted(last_seen, key=lambda k: last_seen[k])[
                   -MAX_RECORDS_PER_PROJECT:])
        records = [r for r in records
                   if (r.get("commit") or "-",
                       int(r.get("push_seq") or 1)) in keep]

        path = _project_file(record["project"])
        os.makedirs(os.path.dirname(path), exist_ok=True)   # 组层级子目录不存在时自动创建
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:   # 先写临时文件再替换，避免写一半断电留下坏文件
            json.dump(records, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)


def _migrate_legacy():
    """旧版 reports.json 存在且 reports/ 目录为空时，按项目拆分迁移。"""
    if not os.path.exists(LEGACY_DATA_FILE):
        return
    has_data = os.path.isdir(DATA_DIR) and any(
        n.endswith(".json") for _dp, _dn, fns in os.walk(DATA_DIR) for n in fns)
    if has_data:
        return   # 已有分项目数据，不重复迁移
    try:
        with open(LEGACY_DATA_FILE, "r", encoding="utf-8") as f:
            old = json.load(f)
    except Exception:
        return
    for r in old:
        save_record(r)
    os.rename(LEGACY_DATA_FILE, LEGACY_DATA_FILE + ".migrated")
    print(f"已将旧版 reports.json 的 {len(old)} 条记录按项目迁移到 reports/ 目录")


def _purge_converged():
    """一次性清理：删除存量数据里被终态取代的 running 占位记录。

    旧版收敛方案把占位记录改写成终态并打 converged=true 标记留在文件里，
    展示有误导性；现方案在终态到达时直接删除占位，这类记录不会再产生。
    启动时把存量文件里的 converged 记录物理删除（重放推算不受影响：
    该标记的记录本来就不计入轮次终态）。
    """
    if not os.path.isdir(DATA_DIR):
        return
    for dirpath, _dirnames, filenames in os.walk(DATA_DIR):
        for name in filenames:
            if not name.endswith(".json"):
                continue
            path = os.path.join(dirpath, name)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    records = json.load(f)
            except Exception:
                continue
            if not isinstance(records, list):
                continue
            if not all(isinstance(r, dict) for r in records):
                continue   # 非正常记录文件：不碰，避免误删
            kept = [r for r in records if not r.get("converged")]
            if len(kept) == len(records):
                continue
            with _lock:
                tmp_path = path + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(kept, f, ensure_ascii=False, indent=2)
                os.replace(tmp_path, path)
            print(f"[清理] {os.path.relpath(path, DATA_DIR)}: 删除 "
                  f"{len(records) - len(kept)} 条被终态取代的 running 占位记录")


def _prune_empty_dirs(start_dir):
    """从 start_dir 往上删除空目录，直到 DATA_DIR 为止。"""
    d = start_dir
    while d and d != DATA_DIR and os.path.abspath(d).startswith(os.path.abspath(DATA_DIR)):
        try:
            os.rmdir(d)   # 只删空目录，非空会直接抛异常停下
        except OSError:
            break
        d = os.path.dirname(d)


def delete_project_data(project):
    """删除指定项目的数据文件，并清理变空的组目录。"""
    with _lock:
        path = _project_file(project)
        if not os.path.exists(path):
            return False
        os.remove(path)
        _prune_empty_dirs(os.path.dirname(path))
        return True


def rename_project_data(old_project, new_project):
    """项目重命名/转移：把旧数据文件挪到新路径（没有旧数据则什么都不做）。"""
    with _lock:
        old_path = _project_file(old_project)
        if not os.path.exists(old_path):
            return False
        new_path = _project_file(new_project)
        os.makedirs(os.path.dirname(new_path), exist_ok=True)
        os.replace(old_path, new_path)
        _prune_empty_dirs(os.path.dirname(old_path))
        return True


def cleanup_idle_projects():
    """闲置兜底清理：超过 IDLE_CLEANUP_DAYS 天没有新上报的项目，删除其数据文件。

    以文件里最新一条记录的上报时间为准；时间解析失败的文件不碰，避免误删。
    """
    if IDLE_CLEANUP_DAYS <= 0 or not os.path.isdir(DATA_DIR):
        return
    cutoff = datetime.now() - timedelta(days=IDLE_CLEANUP_DAYS)
    for dirpath, dirnames, filenames in os.walk(DATA_DIR):
        # 报告存档目录（__reports__）里不是项目数据文件，跳过
        dirnames[:] = [d for d in dirnames
                       if os.path.join(dirpath, d) != REPORT_ARCHIVE_DIR]
        for name in filenames:
            if not name.endswith(".json"):
                continue
            path = os.path.join(dirpath, name)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    records = json.load(f)
                if not records:
                    newest = None
                else:
                    newest = max(
                        (datetime.strptime(r["time"], "%Y-%m-%d %H:%M:%S")
                         for r in records if r.get("time")),
                        default=None)
            except Exception:
                continue   # 文件损坏或时间格式异常：跳过，宁可不删
            if newest is None or newest < cutoff:
                with _lock:
                    try:
                        os.remove(path)
                        print(f"[清理] 项目数据闲置超过 {IDLE_CLEANUP_DAYS} 天，已删除: "
                              f"{os.path.relpath(path, DATA_DIR)}")
                        _prune_empty_dirs(dirpath)
                    except OSError:
                        pass


def _cleanup_loop():
    """后台线程：启动时先跑一遍，之后每天定时清理闲置项目数据。"""
    while True:
        try:
            cleanup_idle_projects()
        except Exception as e:
            print(f"[清理] 闲置清理出错（不影响服务）: {e}")
        time.sleep(CLEANUP_INTERVAL)


DASHBOARD_FILE = os.path.join(BASE_DIR, "panel_dashboard.html")   # 前端页面独立成文件，便于维护；每次请求现读，改前端不用重启服务


def load_dashboard():
    with open(DASHBOARD_FILE, "r", encoding="utf-8") as f:
        return f.read()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html, code=200):
        body = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _guard(self, fn):
        """handler 总兜底：未捕获异常回 500 并在服务端打 traceback，
        而不是让 BaseHTTPRequestHandler 裸关连接（客户端只会看到
        "Remote end closed connection without response"，排查无从下手）。"""
        try:
            fn()
        except (BrokenPipeError, ConnectionResetError):
            pass   # 客户端中途断开：无需也无法再响应
        except Exception as e:
            traceback.print_exc()
            try:
                self._send_json({"error": "internal error: %s" % e}, 500)
            except Exception:
                pass   # 响应可能已部分发出，尽力即可

    def do_GET(self):
        self._guard(self._do_GET)

    def do_POST(self):
        self._guard(self._do_POST)

    def _do_GET(self):
        split = urlsplit(self.path)
        path = split.path   # 关键修复: 只按路径路由，忽略 ?commit= 等查询参数
        query = parse_qs(split.query)
        if path == "/" or path.startswith("/index"):
            try:
                self._send_html(load_dashboard())
            except OSError:
                self._send_json({"error": "panel_dashboard.html 缺失，"
                                          "请与 panel_server.py 同目录部署"},
                                500)
        elif path.startswith("/api/results"):
            records = load_records()
            project = (query.get("project") or [""])[0]
            if project:
                # 项目总览只回本项目记录，避免把全量记录传给浏览器
                records = [r for r in records
                           if (r.get("project") or "-") == project]
            commit = (query.get("commit") or [""])[0]
            if commit:
                # commit 视图服务端直接过滤，避免每 5 秒把全量记录传给浏览器
                records = [r for r in records
                           if _commit_match(r.get("commit"), commit)]
            # 列表接口只回 content 预览，全文点开详情时走 /api/detail 按需拉取；
            # 否则单条大记录（如全量 cppcheck 明细）每 5 秒刷新都会拖垮页面
            slim = []
            for r in records:
                content = r.get("content") or ""
                item = dict(r)
                item["content"] = content[:PREVIEW_CHARS]
                item["content_full"] = len(content) > PREVIEW_CHARS
                slim.append(item)
            self._send_json(slim)
        elif path.startswith("/api/detail"):
            commit = (query.get("commit") or [""])[0]
            function = (query.get("function") or [""])[0]
            project = (query.get("project") or [""])[0]
            idx = (query.get("idx") or [""])[0]      # 可选：定位历史某条（时间线点开用）
            push = (query.get("push") or [""])[0]    # 可选：只看该轮 push
            record = find_record_by_idx(project, commit, function, idx, push) \
                if idx else find_record(commit, function, project, push)
            if record is None:
                self._send_json({"error": "not found"}, 404)
            else:
                self._send_json(record)
        elif path.startswith("/api/history"):
            # 同一轮 push 下同 key 的全部历史记录（时间正序）：时间线查看用
            commit = (query.get("commit") or [""])[0]
            function = (query.get("function") or [""])[0]
            project = (query.get("project") or [""])[0]
            push = (query.get("push") or [""])[0]    # 可选：只看该轮 push
            if not (commit and function):
                self._send_json({"error": "commit and function required"}, 400)
                return
            hist = list_history(commit, function, project, push)
            self._send_json(hist)
        elif path.startswith("/api/report_files"):
            # 报告存档文件清单（前端"质量报告汇总"卡片的下载链接用）：
            # ?project=&commit=[&at=<report记录时间>]，返回该次 push 对应
            # 存档目录里的文件列表（report.md / report.docx / report.pdf 等）
            project = (query.get("project") or [""])[0]
            commit = (query.get("commit") or [""])[0]
            at = (query.get("at") or [""])[0]
            d = find_report_archive(project, commit, at) \
                if project and commit else None
            if not d:
                self._send_json({"ok": False, "files": []})
                return
            files = []
            for name in sorted(os.listdir(d)):
                p = os.path.join(d, name)
                if os.path.isfile(p) and not name.startswith("."):
                    files.append({"name": name, "size": os.path.getsize(p)})
            self._send_json({"ok": True, "dir": os.path.basename(d),
                             "files": files})
        elif path.startswith("/api/report_download"):
            # 报告文件下载（Content-Disposition: attachment 触发浏览器下载）
            project = (query.get("project") or [""])[0]
            commit = (query.get("commit") or [""])[0]
            at = (query.get("at") or [""])[0]
            fname = re.sub(r"[^A-Za-z0-9._-]", "_",
                           (query.get("file") or [""])[0])
            d = find_report_archive(project, commit, at) \
                if project and commit and fname else None
            p = os.path.join(d, fname) if d else None
            if not p or fname.startswith(".") or not os.path.isfile(p):
                self._send_json({"error": "not found"}, 404)
                return
            ctype = {
                "md": "text/markdown; charset=utf-8",
                "pdf": "application/pdf",
                "docx": "application/vnd.openxmlformats-officedocument."
                        "wordprocessingml.document",
                "json": "application/json; charset=utf-8",
            }.get(fname.rsplit(".", 1)[-1].lower(), "application/octet-stream")
            with open(p, "rb") as fh:
                blob = fh.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Disposition",
                             "attachment; filename=\"%s\"" % fname)
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)
        else:
            self._send_json({"error": "not found"}, 404)

    def _do_POST(self):
        # 注意：/api/report 必须精确匹配（==），不能 startswith ——
        # 否则会误吞 /api/report_archive，存档请求会被当成普通上报处理
        if urlsplit(self.path).path == "/api/report":
            try:
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length)
                data = json.loads(raw.decode("utf-8"))
            except Exception as e:
                self._send_json({"error": f"invalid json: {e}"}, 400)
                return

            durations = data.get("durations")
            if not (isinstance(durations, list) and all(
                    isinstance(d, (list, tuple)) and len(d) == 2
                    and isinstance(d[1], (int, float)) for d in durations)):
                durations = None   # 格式不对就当没带，不影响主流程

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record = {
                "function": data.get("function", "unknown"),
                "status": data.get("status", "success"),
                "project": data.get("project", "-"),
                "pipeline_id": data.get("pipeline_id", "-"),
                "commit": data.get("commit", "-"),
                "content": data.get("content", ""),
                "durations": durations,
                "time": data.get("time") or now,
            }
            # 兼容旧数据：迁移前上报里带 push_time / run_id（30 分钟窗口
            # 归组方案遗留），服务端保留这些字段但不再使用它们做任何
            # 分组/过滤（归组键统一为 project + commit + function）
            for k in ("push_time", "run_id"):
                if data.get(k):
                    record[k] = data.get(k)
            _sanitize_record(record)   # 上报字段类型容错（content/time 等可能是非字符串）
            try:
                save_record(record)
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                      f"保存上报失败: {e}")
                self._send_json({"error": f"save failed: {e}"}, 500)
                return
            print(f"[{record['time']}] 收到上报: {record['function']} / {record['status']} / pipeline #{record['pipeline_id']}")
            self._send_json({"ok": True})
        elif urlsplit(self.path).path == "/api/report_archive":
            # 报告存档：report.py 把生成好的报告文件交给面板进程落盘。
            # pre-receive 钩子以 git 用户运行、无文件系统写权限，因此存档
            # 统一由这里写（面板以 bowen.dong 身份运行，本就持有 reports/ 写许可）。
            try:
                length = int(self.headers.get("Content-Length", 0))
                data = json.loads(self.rfile.read(length).decode("utf-8"))
            except Exception as e:
                self._send_json({"error": f"invalid json: {e}"}, 400)
                return
            project = data.get("project") or ""
            commit = data.get("commit") or ""
            stamp = data.get("stamp") or \
                datetime.now().strftime("%Y%m%d_%H%M%S")
            files = data.get("files") or {}
            if not project or not commit or not isinstance(files, dict) or not files:
                self._send_json({"error": "project/commit/files 必填"},
                                400)
                return
            try:
                out_dir, made = save_report_archive(
                    project, commit, stamp, files)
                cleanup_old_report_archives(project, keep_commit=commit)
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                      f"报告存档失败: {e}")
                self._send_json({"error": f"archive failed: {e}"}, 500)
                return
            # 后台生成正式文档（重写 report.md + docx/pdf），不阻塞响应
            _run_doc_generator(out_dir)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 报告已存档: "
                  f"{project} @ {commit} -> {os.path.relpath(out_dir, DATA_DIR)}")
            self._send_json({"ok": True, "dir": out_dir, "files": made,
                             "doc_gen": os.path.isfile(DOC_GENERATOR)})
        elif urlsplit(self.path).path == "/api/system_hook":
            # GitLab 系统钩子：项目删除/重命名时自动清理面板数据
            if SYSTEM_HOOK_TOKEN and self.headers.get("X-Gitlab-Token") != SYSTEM_HOOK_TOKEN:
                # HTTP/1.1 长连接下必须先读掉请求体，否则残留字节会污染下一个请求
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                self._send_json({"error": "forbidden"}, 403)
                return
            try:
                length = int(self.headers.get("Content-Length", 0))
                data = json.loads(self.rfile.read(length).decode("utf-8"))
            except Exception as e:
                self._send_json({"error": f"invalid json: {e}"}, 400)
                return

            event = data.get("event_name", "")
            project = data.get("path_with_namespace", "")
            if event == "project_destroyed" and project:
                removed = delete_project_data(project)
                print(f"[系统钩子] 项目已删除: {project}（面板数据{'已清理' if removed else '不存在，无需清理'}）")
                self._send_json({"ok": True, "removed": removed})
            elif event in ("project_renamed", "project_transferred") and project:
                old = data.get("old_path_with_namespace", "")
                moved = rename_project_data(old, project) if old else False
                print(f"[系统钩子] 项目重命名/转移: {old} -> {project}（面板数据{'已随迁' if moved else '无旧数据'}）")
                self._send_json({"ok": True, "moved": moved})
            else:
                self._send_json({"ok": True, "ignored": event or "unknown"})
        else:
            self._send_json({"error": "not found"}, 404)

    def log_message(self, fmt, *args):  # 静默静态访问日志，只保留上报日志
        pass


if __name__ == "__main__":
    if not os.path.isfile(DASHBOARD_FILE):
        print(f"[警告] 前端页面 {DASHBOARD_FILE} 不存在，"
              "请与 panel_server.py 同目录部署，否则主页无法访问")
    _migrate_legacy()
    _purge_converged()
    threading.Thread(target=_cleanup_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"面板服务已启动: http://0.0.0.0:{PORT}")
    print(f"数据目录: {DATA_DIR}（每个项目一个 json 文件）")
    print(f"闲置清理: 项目超过 {IDLE_CLEANUP_DAYS} 天无上报自动删除（每天执行一次）")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已停止")