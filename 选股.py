# -*- coding: utf-8 -*-
"""
A股「龙虎榜 + 连板 + 竞价」选股工具
================================================
日常用法：双击「打开选股工具.vbs」就行，页面里可一键刷新数据。
本文件是数据引擎，也可以单独在命令行跑（见下方用法）。

数据源（均为公开免费行情接口，主源+备源双通道）：
  · 涨停股池 / 连板 / 封单 / 炸板  → 东方财富 push2ex
  · 龙虎榜资金流向               → 东方财富 datacenter
  · 实时行情 / 市值 / 换手 / 五档 → 腾讯行情（缺失时用东财批量行情补齐）
  · 集合竞价成交额（09:25 那根）  → 新浪分时（缺失时用东财分时）
  · 日K（昨日成交额）              → 新浪日K（缺失时用腾讯日K）

用法：
    python 选股.py                # 最近一个交易日
    python 选股.py --date 2026-11-20
    python 选股.py --live         # 09:15–09:25 竞价时段，采集委买/委卖
    python 选股.py --open
    python 选股.py --refresh      # 忽略缓存

说明：
1. 行情接口有频率限制，本工具内置全局限速 + 指数退避 + 带过期时间的本地缓存；
2. 竞价委买/委卖属盘口数据，历史不可回溯，只在竞价时段采集（服务模式会自动判断时段）；
3. 妖股概率为自建多因子评分，用于排序比较，不是统计概率，不构成投资建议。
"""
import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import socket
import subprocess
import threading
import time
import urllib.parse
import urllib.request

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# 行情接口走 IPv4：本机默认解析到 IPv6 时部分接口会直接断开
_og = socket.getaddrinfo
socket.getaddrinfo = lambda h, p, f=0, t=0, pr=0, fl=0: _og(h, p, socket.AF_INET, t, pr, fl)

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE, "输出")
CACHE_DIR = os.path.join(BASE, "缓存")
TPL_PATH = os.path.join(BASE, "模板", "dashboard.html")
if not os.path.exists(TPL_PATH):  # 云端仓库里模板直接放在根目录
    alt = os.path.join(BASE, "dashboard.html")
    if os.path.exists(alt):
        TPL_PATH = alt
CFG_PATH = os.path.join(BASE, "配置.json")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120.0.0.0 Safari/537.36")

DEFAULT_CFG = {
    "阈值": {
        "竞价口径": "竞价换手率",
        "竞价下限": 29.25,
        "涨跌幅下限": 5.0,
        "总市值上限_亿": 300,
        "三日内涨停": True,
        "集合竞价委卖大于委买": True
    },
    "妖股打分权重": {
        "连板高度": 0.18, "封板强度": 0.14, "换手充分度": 0.12, "市值弹性": 0.12,
        "近3日涨停次数": 0.10, "龙虎榜资金": 0.10, "炸板惩罚": 0.10,
        "竞价活跃度": 0.08, "封板时间": 0.06
    },
    "抓取": {"最小间隔秒": 0.9, "候选上限": 150, "抓取昨日成交额": True, "精选数量": 5}
}


# ---------------------------------------------------------------- 限速 + 缓存
class Fetcher:
    BACKOFF = [15, 45, 90, 150]

    def __init__(self, min_interval=0.9, refresh=False):
        self.interval = min_interval
        self.refresh = refresh
        self._lock = threading.Lock()
        self._last = 0.0
        self.blocked_until = 0.0
        self.curl = shutil.which("curl") or shutil.which("curl.exe")

    def _pace(self):
        with self._lock:
            now = time.time()
            if now < self.blocked_until:
                time.sleep(self.blocked_until - now)
                now = time.time()
            gap = now - self._last
            if gap < self.interval:
                time.sleep(self.interval - gap)
            self._last = time.time()

    def mark_blocked(self, sec):
        with self._lock:
            self.blocked_until = max(self.blocked_until, time.time() + sec)

    def _via_curl(self, url, ref, timeout):
        args = [self.curl, "-sS", "--compressed", "-m", str(timeout), "-A", UA,
                "-H", "Accept: */*", "-H", "Accept-Language: zh-CN,zh;q=0.9"]
        if ref:
            args += ["-H", "Referer: " + ref]
        args.append(url)
        p = subprocess.run(args, capture_output=True, timeout=timeout + 5)
        if p.returncode != 0:
            raise RuntimeError("curl rc=%d" % p.returncode)
        body = p.stdout
        if not body.strip():
            raise RuntimeError("curl 返回空内容")
        return body

    def _via_urllib(self, url, ref, timeout):
        h = {"User-Agent": UA, "Accept": "*/*", "Accept-Encoding": "gzip",
             "Accept-Language": "zh-CN,zh;q=0.9"}
        if ref:
            h["Referer"] = ref
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                import gzip as _gz
                raw = _gz.decompress(raw)
        return raw

    def get(self, url, ref=None, cache=True, timeout=20, encoding="utf-8", ttl=0):
        """ttl=0 表示缓存永久有效；否则超过 ttl 秒自动重新拉取"""
        key = hashlib.sha1(url.encode("utf-8")).hexdigest()[:20]
        path = os.path.join(CACHE_DIR, key + ".txt")
        if cache and not self.refresh and os.path.exists(path):
            fresh = True
            if ttl:
                fresh = (time.time() - os.path.getmtime(path)) <= ttl
            if fresh:
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        body = f.read()
                    if body.strip():
                        return body
                except Exception:
                    pass
        last = None
        for i in range(len(self.BACKOFF) + 1):
            self._pace()
            try:
                if self.curl:
                    try:
                        raw = self._via_curl(url, ref, timeout)
                    except Exception:
                        raw = self._via_urllib(url, ref, timeout)
                else:
                    raw = self._via_urllib(url, ref, timeout)
                txt = raw.decode(encoding, "ignore")
                if ("waf.tencent.com" in txt) or ("501page" in txt) or txt.lstrip().startswith("<!DOCTYPE"):
                    raise RuntimeError("WAF 拦截")
                if cache:
                    try:
                        os.makedirs(CACHE_DIR, exist_ok=True)
                        with open(path, "w", encoding="utf-8") as f:
                            f.write(txt)
                    except Exception:
                        pass
                return txt
            except Exception as e:
                last = e
                if i < len(self.BACKOFF):
                    wait = self.BACKOFF[i]
                    print("    [等待] 请求失败（%s），%d 秒后重试（%d/%d）"
                          % (str(e)[:40], wait, i + 1, len(self.BACKOFF)))
                    self.mark_blocked(wait)
        raise last


F = None


# ---------------------------------------------------------------- 小工具
def js(txt):
    txt = txt.strip()
    if txt[:1] != "{" and txt[:1] != "[":
        i, j = txt.find("{"), txt.rfind("}")
        if i >= 0 and j > i:
            txt = txt[i:j + 1]
    return json.loads(txt)


def safe(v, d=0.0):
    try:
        return float(v)
    except Exception:
        return d


def pct(v, base):
    return (v / base * 100.0) if base else 0.0


def r2u(v):
    """按分四舍五入（half-up）。Python 的 round 是银行家舍入，会把 4.125 算成 4.12，
    而交易所涨停价是 4.13，差一分会让“是否涨停”的判断出错。"""
    return math.floor(v * 100 + 0.5) / 100.0


def load_cfg():
    cfg = json.loads(json.dumps(DEFAULT_CFG))
    if os.path.exists(CFG_PATH):
        try:
            with open(CFG_PATH, "r", encoding="utf-8") as f:
                user = json.load(f)
            for k in cfg:
                if isinstance(cfg[k], dict) and isinstance(user.get(k), dict):
                    cfg[k].update(user[k])
                elif k in user:
                    cfg[k] = user[k]
        except Exception:
            pass
    return cfg


def tx_code(code, market=None, secucode=""):
    if secucode:
        m = secucode.split(".")[-1]
        return ("sh" if m == "SH" else "sz" if m == "SZ" else "bj") + secucode.split(".")[0]
    return ("sh" if market == 1 else "sz") + code


# ---------------------------------------------------------------- 数据接口
TXPROXY = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app"
TXREF = "https://gu.qq.com/"
SINA_REF = "https://finance.sina.com.cn/"


def get_trade_days(n=10):
    """交易日历：以上证指数日K为准（新浪为主，东财为备）"""
    try:
        url = ("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
               "?symbol=sh000001&scale=240&ma=no&datalen=%d" % n)
        arr = js(F.get(url, ref=SINA_REF, ttl=600))
        if arr:
            return [x["day"] for x in arr]
    except Exception:
        pass
    url = ("https://push2.eastmoney.com/api/qt/stock/kline/get?secid=1.000001&klt=101&fqt=0"
           "&lmt=%d&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57" % n)
    d = js(F.get(url, ref="https://quote.eastmoney.com/", ttl=600))["data"]
    return [x.split(",")[0] for x in d["klines"]]


def tx_quotes(codes, chunk=35, use_cache=True, ttl=180):
    """腾讯批量行情：一次请求多只"""
    out = {}
    for i in range(0, len(codes), chunk):
        part = codes[i:i + chunk]
        url = "https://qt.gtimg.cn/q=" + ",".join(part)
        try:
            txt = F.get(url, ref="https://gu.qq.com/", cache=use_cache, encoding="gbk", ttl=ttl)
        except Exception as e:
            print("    行情批次失败（%s）" % str(e)[:40])
            continue
        for line in txt.split(";"):
            if '="' not in line:
                continue
            body = line.split('="')[1].rstrip('"')
            f = body.split("~")
            if len(f) < 46:
                continue
            code6 = f[2]
            try:
                out[code6] = {
                    "code": code6, "name": f[1], "price": safe(f[3]), "pre_close": safe(f[4]),
                    "open": safe(f[5]), "vol_hand": safe(f[6]), "time": f[30],
                    "change": safe(f[32]), "high": safe(f[33]), "low": safe(f[34]),
                    "amount": safe(f[37]) * 1e4,               # 成交额(元)
                    "hs": safe(f[38]),                          # 换手率%
                    "fcap": safe(f[44]) * 1e8,                  # 流通市值(元)
                    "mcap": safe(f[45]) * 1e8,                  # 总市值(元)
                    "limit_up": safe(f[47]),
                    "bid": sum(safe(f[10 + 2 * k]) for k in range(5)),
                    "ask": sum(safe(f[20 + 2 * k]) for k in range(5)),
                }
            except Exception:
                continue
    missing = [c for c in codes if c[2:] not in out]
    if missing:
        got = em_quotes(missing)
        for k, v in got.items():
            out.setdefault(k, v)
    return out


def em_quotes(codes, chunk=40):
    """东财批量行情（备用源）：涨跌幅 / 换手率 / 市值 / 成交额"""
    out = {}
    for i in range(0, len(codes), chunk):
        part = codes[i:i + chunk]
        secids = ",".join(("1." if c.startswith("sh") else "0.") + c[2:] for c in part)
        url = ("https://push2.eastmoney.com/api/qt/ulist.np/get?secids=%s"
               "&fields=f1,f2,f3,f5,f6,f8,f12,f14,f20,f21&fltt=2&invt=2" % secids)
        try:
            d = js(F.get(url, ref="https://quote.eastmoney.com/", ttl=180))["data"]
        except Exception:
            continue
        for r in (d or {}).get("diff") or []:
            code6 = str(r.get("f12"))
            out[code6] = {
                "code": code6, "name": r.get("f14"), "price": safe(r.get("f2")),
                "pre_close": 0.0, "open": 0.0, "vol_hand": safe(r.get("f5")), "time": "",
                "change": safe(r.get("f3")), "high": 0.0, "low": 0.0,
                "amount": safe(r.get("f6")), "hs": safe(r.get("f8")),
                "fcap": safe(r.get("f21")) * 1e8, "mcap": safe(r.get("f20")) * 1e8,
                "limit_up": 0.0, "bid": 0.0, "ask": 0.0,
            }
    return out


def tx_minute(code):
    """集合竞价成交：取分时 09:25 那一根（= 真实集合竞价撮合成交）"""
    # 主源：新浪分时（09:25 单列一根）
    try:
        url = ("https://quotes.sina.cn/cn/api/json_v2.php/CN_MinlineService.getMinlineData"
               "?symbol=%s&scale=1&datalen=12" % code)
        arr = js(F.get(url, ref=SINA_REF, ttl=180))
        if arr:
            auc = next((b for b in arr if str(b.get("m", "")).startswith("08:59")
                        or str(b.get("m", "")).startswith("09:25")), None) or arr[0]
            return {"date": "", "t": str(auc.get("m", ""))[:5],
                    "first_amt": safe(auc.get("v")) * safe(auc.get("p")),
                    "day_amt": 0.0, "bars": len(arr)}
    except Exception:
        pass
    # 备源：东财分时（首根含竞价成交）
    try:
        secid = ("1." if code.startswith("sh") else "0.") + code[2:]
        url = ("https://push2.eastmoney.com/api/qt/stock/trends2/get?secid=%s"
               "&fields1=f1,f2,f3,f4,f5,f6,f7,f8&fields2=f51,f52,f53,f54,f55,f56,f57,f58"
               "&iscr=0&ndays=1" % secid)
        d = js(F.get(url, ref="https://quote.eastmoney.com/", ttl=180))["data"]
        bars = d.get("trends") or []
        if not bars:
            return None
        p = bars[0].split(",")
        last = bars[-1].split(",")
        return {"date": "", "t": p[0].split()[-1][:5], "first_amt": safe(p[6]),
                "day_amt": safe(last[6]), "bars": len(bars)}
    except Exception:
        return None


def tx_prev_amount(code):
    """昨日成交额（成交量 × 均价，估算）"""
    try:
        url = ("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
               "?symbol=%s&scale=240&ma=no&datalen=5" % code)
        arr = js(F.get(url, ref=SINA_REF, ttl=21600))
        if len(arr) >= 2:
            x = arr[-2]
            vol = safe(x["volume"])
            avg = (safe(x["open"]) + safe(x["close"]) + safe(x["high"]) + safe(x["low"])) / 4
            return {"date": x["day"], "amount": vol * avg, "vol": vol}
    except Exception:
        pass
    try:
        d = js(F.get("%s/fqkline/get?param=%s,day,,,5,qfq" % (TXPROXY, code), ref=TXREF, ttl=21600))["data"][code]
        arr = d.get("day") or d.get("qfqday") or []
        if len(arr) >= 2:
            x = arr[-2]
            vol = safe(x[5]) * 100
            avg = (safe(x[1]) + safe(x[2]) + safe(x[3]) + safe(x[4])) / 4
            return {"date": x[0], "amount": vol * avg, "vol": vol}
    except Exception:
        return None


def get_zt_pool(day):
    """涨停股池：day=YYYYMMDD"""
    pool, pi = [], 0
    while pi <= 10:
        url = ("https://push2ex.eastmoney.com/getTopicZTPool?ut=7eea3edcaed734bea9cbfc24409ed989"
               "&dpt=wz.ztzt&Pageindex=%d&pagesize=200&sort=fbt%%3Aasc&date=%s" % (pi, day))
        try:
            d = js(F.get(url, ref="https://quote.eastmoney.com/", ttl=1800))
        except Exception as e:
            print("    涨停池 %s 失败（%s）" % (day, str(e)[:40]))
            break
        data = d.get("data") or {}
        items = data.get("pool") or []
        pool.extend(items)
        if len(pool) >= data.get("tc", 0) or not items:
            break
        pi += 1
    return pool


def get_lhb(day):
    """龙虎榜明细：day=YYYY-MM-DD"""
    rows, page = [], 1
    while page <= 10:
        flt = urllib.parse.quote("(TRADE_DATE='%s')" % day, safe="()'=")
        url = ("https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=BILLBOARD_NET_AMT"
               "&sortTypes=-1&pageSize=500&pageNumber=%d&reportName=RPT_DAILYBILLBOARD_DETAILSNEW"
               "&columns=ALL&source=WEB&client=WEB&filter=%s" % (page, flt))
        try:
            res = js(F.get(url, ttl=1800)).get("result") or {}
            rows.extend(res.get("data") or [])
            if page >= (res.get("pages") or 1):
                break
        except Exception as e:
            print("    龙虎榜 %s 第%d页失败（%s）" % (day, page, str(e)[:40]))
            break
        page += 1
    return rows


# ---------------------------------------------------------------- 妖股评分
KEY_MAP = {"连板高度": "lbc", "封板强度": "seal", "换手充分度": "turnover", "市值弹性": "mcap",
           "近3日涨停次数": "zt3", "龙虎榜资金": "lhb", "炸板惩罚": "zbc",
           "竞价活跃度": "auction", "封板时间": "fbt"}


def score_stock(s, weights):
    p = {}
    lbc = int(s.get("lbc") or 0)
    p["lbc"] = 1.0 if lbc >= 5 else {4: .85, 3: .65, 2: .45, 1: .2}.get(lbc, 0.0)
    p["seal"] = min(safe(s.get("fund")) / (safe(s.get("ltsz")) or 1) / 0.08, 1.0)

    hs = safe(s.get("hs"))
    if 8 <= hs <= 25:
        p["turnover"] = 1.0
    elif 5 <= hs < 8 or 25 < hs <= 32:
        p["turnover"] = 0.7
    elif 3 <= hs < 5 or 32 < hs <= 40:
        p["turnover"] = 0.4
    elif hs > 40:
        p["turnover"] = 0.25
    else:
        p["turnover"] = 0.15

    mc = safe(s.get("mcap_yi"))
    p["mcap"] = 1.0 if mc <= 50 else .85 if mc <= 100 else .6 if mc <= 200 else .35 if mc <= 300 else .1

    ct = int(s.get("zt3_count") or 0)
    p["zt3"] = 1.0 if ct >= 3 else .6 if ct == 2 else .3 if ct == 1 else 0.0

    net, accum = safe(s.get("lhb_net")), safe(s.get("amount"))
    if s.get("lhb_reasons"):
        p["lhb"] = 1.0 if (net > 0 and accum and net * 1e4 / accum > .05) else (.7 if net > 0 else .2)
    else:
        p["lhb"] = 0.35

    zbc = int(s.get("zbc") or 0)
    p["zbc"] = 1.0 if zbc == 0 else .6 if zbc == 1 else .3 if zbc == 2 else .05

    p["auction"] = safe(s.get("auction_rank"))

    t = str(s.get("fbt") or "")
    if t.isdigit() and len(t) == 6:
        hm = int(t[:4])
        p["fbt"] = 1.0 if hm <= 935 else .8 if hm <= 1030 else .6 if hm <= 1330 else .4 if hm <= 1430 else .2
    else:
        p["fbt"] = 0.3

    total = sum(p[KEY_MAP[k]] * v for k, v in weights.items())
    return round(total * 100, 1), {k: round(p[KEY_MAP[k]] * 100, 1) for k in weights}


def prob_label(v):
    return "高" if v >= 75 else "偏高" if v >= 60 else "中等" if v >= 45 else "偏低" if v >= 30 else "低"


def fbt_str(v):
    try:
        s = "%06d" % int(v)
        return "%s:%s:%s" % (s[:2], s[2:4], s[4:6])
    except Exception:
        return ""


# ---------------------------------------------------------------- 主流程
def _fbt_min(s):
    """首封时间 "09:32" → 分钟数；无值给很大"""
    try:
        h, m = str(s).split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 9999


def classify_roles(cands):
    """把候选分层：龙头 / 中军 / 后排（板块内比较）

    龙头：板块内连板最高（同高度则封单厚、首封早、炸板少的先），是资金主攻方向；
    中军：板块内体量最大的那只（不追高、但承接大资金，适合大仓位/看板块能不能扛住）；
    后排：跟风票，首板、封单薄、封板晚或炸过板，情绪一退最先死。
    另外单独标出全市场连板最高的「总龙头」。
    """
    groups = {}
    for x in cands:
        x["role"] = ""
        x["role_group"] = ""
        if not x.get("in_zt3"):
            continue
        if x["lbc"] <= 0:
            # 近 3 日涨停过、但今天没封板：不参与今天的梯队分层
            x["role_note"] = "今日未封板（近3日曾涨停）"
            continue
        b = x.get("board") or "-"
        if b == "-":
            # 板块标注缺失，不硬凑成一个假板块；只按连板高度给个粗略身份
            if x["lbc"] >= 3:
                x["role"] = "龙头"
                x["role_note"] = "全市场%d连板（板块未知）" % x["lbc"]
            continue
        groups.setdefault(b, []).append(x)

    def lead_key(x):
        seal = (x["fund"] / x["ltsz"]) if (x["fund"] and x["ltsz"]) else 0
        early = 1.0 if _fbt_min(x["fbt"]) <= 10 * 60 + 30 else (0.5 if _fbt_min(x["fbt"]) <= 11 * 60 else 0.0)
        return (x["lbc"] * 3 + x["zt3_count"] * 1.2 + seal * 12 + early - x["zbc"] * 0.6
                + (0.8 if x["lhb_net"] and x["lhb_net"] > 0 else 0))

    for b, g in groups.items():
        if len(g) == 1:
            # 板块里只有这一只涨停：要么是独苗龙头，要么是无梯队支撑的孤票
            x0 = g[0]
            x0["role_group"] = b
            if x0["lbc"] >= 2:
                x0["role"] = "龙头"
                x0["role_note"] = "%s板块%d连板，板块内无其他涨停（独苗龙头）" % (b, x0["lbc"])
            else:
                x0["role"] = "后排"
                x0["role_note"] = "%s板块仅1只涨停，无梯队支撑（孤票）" % b
            continue
        caps = sorted([x["mcap_yi"] for x in g])
        med = caps[len(caps) // 2]
        ranked = sorted(g, key=lambda x: -lead_key(x))
        head = ranked[0]
        head["role"], head["role_group"] = "龙头", b
        head["role_note"] = "%s板块最强：%s" % (
            b, ("%d连板" % head["lbc"]) if head["lbc"] >= 2 else "首板最早上封")
        rest = ranked[1:]
        # 中军：除龙头外体量最大的那只，且市值要在板块中位数以上（够大才叫中军）
        mid = None
        for x in sorted(rest, key=lambda x: -x["mcap_yi"]):
            if x["mcap_yi"] >= max(med, 60) or x["mcap_yi"] >= 100:
                mid = x
                break
        if mid is None and rest and rest[0]["mcap_yi"] >= 60:
            mid = sorted(rest, key=lambda x: -x["mcap_yi"])[0]
        if mid is not None:
            mid["role"], mid["role_group"] = "中军", b
            mid["role_note"] = "%s板块体量最大（%.0f亿），看它能扛住板块才有持续性" % (b, mid["mcap_yi"])
            rest = [x for x in rest if x is not mid]
        for i, x in enumerate(rest):
            x["role"], x["role_group"] = "后排", b
            tag = "首板跟风" if x["lbc"] <= 1 else "%d连板" % x["lbc"]
            if x["zbc"] > 0:
                tag += "、炸板%d次" % x["zbc"]
            if x["fund"] and x["ltsz"] and x["fund"] / x["ltsz"] < 0.01:
                tag += "、封单薄"
            x["role_note"] = "%s板块%s" % (b, tag)

    # 全市场总龙头：连板最高，同高度比封单、首封
    real = [x for x in cands if x["lbc"] > 0]
    top = None
    if real:
        top = sorted(real, key=lambda x: (-x["lbc"], -lead_key(x)))[0]
        top["top_leader"] = True

    out_groups = []
    for b, g in groups.items():
        if len(g) < 2 and max(x["lbc"] for x in g) < 2:
            continue
        members = sorted(g, key=lambda x: (-x["lbc"], -x["score"]))
        out_groups.append({
            "board": b, "zt": len(g),
            "max_lbc": max(x["lbc"] for x in g),
            "members": [{
                "code": x["code"], "name": x["name"], "role": x.get("role") or "后排",
                "lbc": x["lbc"], "change": x["change"], "price": x["price"],
                "mcap_yi": x["mcap_yi"], "seal": round((x["fund"] / x["ltsz"] * 100), 2) if (x["fund"] and x["ltsz"]) else 0,
                "lhb_net": x["lhb_net"], "score": x["score"], "prob": x["prob"],
                "zbc": x["zbc"], "fbt": x["fbt"], "note": x.get("role_note") or "",
                "top": bool(x.get("top_leader")),
            } for x in members],
        })
    out_groups.sort(key=lambda g: (-g["zt"], -g["max_lbc"]))
    return {
        "groups": out_groups[:8],
        "total": ({"code": top["code"], "name": top["name"], "lbc": top["lbc"],
                   "board": top["board"], "score": top["score"], "seal": round((top["fund"] / top["ltsz"] * 100), 2)
                   if (top["fund"] and top["ltsz"]) else 0} if top else None),
        "counts": {
            "龙头": sum(1 for x in cands if x.get("role") == "龙头"),
            "中军": sum(1 for x in cands if x.get("role") == "中军"),
            "后排": sum(1 for x in cands if x.get("role") == "后排"),
        },
    }


# 东财行业板块名有 4 字截断，补全常见尾部
IND_FIX = {"汽车零部": "汽车零部件", "自动化设": "自动化设备", "通用设": "通用设备",
           "专用设": "专用设备", "电网设": "电网设备", "光伏设": "光伏设备",
           "化学制": "化学制品", "化学原": "化学原料", "软件开": "软件开发",
           "计算": "计算机", "互联": "互联网", "证券": "证券"}


def board_of(code):
    """按代码前缀分市场：60/00 = 主板（含 002 中小板并入），30 创业，688 科创，其余北交"""
    c = str(code)
    if c.startswith("60") or c.startswith("00"):
        return "主板"
    if c.startswith("30"):
        return "创业板"
    if c.startswith("688"):
        return "科创板"
    return "北交所"


def build_main(cands, ov, base):
    """今日主板专项：涨停结构 / 连板梯队 / 炸板 / 封单 / 行业热度 / 龙虎榜"""
    zt = [x for x in cands if int(x.get("lbc") or 0) > 0]
    grp = {}
    for x in zt:
        grp.setdefault(board_of(x["code"]), []).append(x)
    total = len(zt)
    mb = grp.get("主板", [])
    counts = {"主板": len(mb), "创业板": len(grp.get("创业板", [])),
              "科创板": len(grp.get("科创板", [])), "北交所": len(grp.get("北交所", []))}
    share = (len(mb) / total * 100) if total else 0.0

    lad = {"1": 0, "2": 0, "3": 0, "4+": 0}
    for x in mb:
        n = int(x.get("lbc") or 1)
        k = "4+" if n >= 4 else str(n)
        lad[k] = lad.get(k, 0) + 1
    max_lbc = max([int(x.get("lbc") or 0) for x in mb] or [0])
    tops = [{"code": x["code"], "name": x["name"], "lbc": int(x["lbc"]),
             "chg": round(safe(x.get("change")), 2), "hs": round(safe(x.get("hs")), 2),
             "mcap_yi": safe(x.get("mcap_yi")), "board": x.get("board") or "-"}
            for x in mb if int(x.get("lbc") or 0) == max_lbc]
    tops.sort(key=lambda i: -i["mcap_yi"])

    zb = [x for x in mb if int(x.get("zbc") or 0) > 0]
    zb_rate = (len(zb) / (len(mb) + len(zb)) * 100) if (len(mb) + len(zb)) else 0.0

    seal = []
    for x in mb:
        f = safe(x.get("fund"))
        if f <= 0:
            continue
        seal.append({"code": x["code"], "name": x["name"],
                     "fund_yi": round(f / 1e8, 2),
                     "seal_pct": round(f / (safe(x.get("ltsz")) or 1e12) * 100, 2),
                     "lbc": int(x.get("lbc") or 0), "fbt": x.get("fbt") or "",
                     "chg": round(safe(x.get("change")), 2)})
    seal.sort(key=lambda i: -i["fund_yi"])

    inds = {}
    for x in mb:
        b = (x.get("board") or "").strip()
        if not b or b == "-":
            continue
        b = IND_FIX.get(b, b)
        inds[b] = inds.get(b, 0) + 1
    ind_top = [{"name": k, "n": v} for k, v in sorted(inds.items(), key=lambda kv: (-kv[1], kv[0]))[:5]]

    lhb_mb = [x for x in cands if board_of(x["code"]) == "主板" and x.get("lhb_reasons")]
    lhb_net = round(sum(safe(x.get("lhb_net")) for x in lhb_mb) / 1e4, 2)
    lhb_top = []
    for x in sorted(lhb_mb, key=lambda y: -safe(y.get("lhb_net")))[:5]:
        lhb_top.append({"code": x["code"], "name": x["name"],
                        "net_yi": round(safe(x.get("lhb_net")) / 1e4, 2),
                        "chg": round(safe(x.get("change")), 2),
                        "explain": (x.get("lhb_explain") or "")[:20]})

    avg_chg = round(sum(safe(x.get("change")) for x in mb) / len(mb), 2) if mb else 0.0
    avg_hs = round(sum(safe(x.get("hs")) for x in mb) / len(mb), 2) if mb else 0.0
    lian = sum(1 for x in mb if int(x.get("lbc") or 0) >= 2)

    facts = []
    facts.append("主板涨停 <b>%d</b> 家，占全市场 %d 家的 <b>%.1f%%</b>（创业板 %d / 科创 %d / 北交 %d）" % (
        len(mb), total, share, counts["创业板"], counts["科创板"], counts["北交所"]))
    if max_lbc >= 2:
        facts.append("主板连板 <b>%d</b> 家，最高 <b>%d</b> 板：%s" % (
            lian, max_lbc, "、".join("%s(%s)" % (t["name"], t["code"]) for t in tops[:3])))
    else:
        facts.append("主板今天全是首板，没有连板高度——接力没有打开，追高容易吃面")
    facts.append("主板炸板 <b>%d</b> 家，炸板率 <b>%.1f%%</b>；这批涨停股当前平均涨幅 %+.2f%%、平均换手 %.1f%%（跟着行情刷新）" % (
        len(zb), zb_rate, avg_chg, avg_hs))
    if seal:
        s0 = seal[0]
        facts.append("封单最硬：<b>%s</b>（%.2f 亿，占流通 %.2f%%）" % (s0["name"], s0["fund_yi"], s0["seal_pct"]))
    if lhb_mb:
        facts.append("主板龙虎榜净买 <b>%+.2f 亿</b>（%d 只上榜）" % (lhb_net, len(lhb_mb)))
    else:
        facts.append("主板今日龙虎榜暂无净买记录")

    note = ("主板是 10cm 涨停板（创业板/科创 20cm、北交 30cm）。"
            "主板涨停占比高说明资金在做低位低价、容量大的票，接力更看连板高度和封单硬度；"
            "占比低则说明资金在 20cm 弹性里搏，主板票只做龙一，别做后排。")

    return {
        "date": base, "total": total, "counts": counts,
        "mb_n": len(mb), "share": round(share, 1),
        "ladder": lad, "max_lbc": max_lbc, "tops": tops[:5], "lian": lian,
        "zb": len(zb), "zb_rate": round(zb_rate, 1),
        "avg_chg": avg_chg, "avg_hs": avg_hs,
        "seal": seal[:5], "inds": ind_top,
        "lhb": {"net_yi": lhb_net, "n": len(lhb_mb), "top": lhb_top},
        "facts": facts, "note": note
    }


def build(date_arg=None, live=False):
    cfg = load_cfg()
    th, weights, fc = cfg["阈值"], cfg["妖股打分权重"], cfg["抓取"]
    warnings = []

    days = get_trade_days(10)
    today = datetime.now().strftime("%Y-%m-%d")
    if live and today not in days:
        # 竞价时段：今日尚无收盘数据，标签用今日，统计窗口取最近 3 个已完成交易日
        td = today
        tdays = days[-3:]
    else:
        td = date_arg if date_arg and date_arg in days else days[-1]
        tdays = [d for d in days if d <= td][-3:]
    base = tdays[-1]
    print("[1/6] 基准交易日 %s ｜ 数据基准 %s ｜ 窗口 %s" % (td, base, " / ".join(tdays)))

    pools = {}
    for d in tdays:
        pools[d] = get_zt_pool(d.replace("-", ""))
        print("      涨停池 %s：%d 只" % (d, len(pools[d])))
        if not pools[d]:
            warnings.append("涨停池 %s 为空" % d)
    zt_today = {i["c"]: i for i in pools[base]}
    zt3, zt3_count = set(), {}
    for d in tdays:
        for i in pools[d]:
            zt3.add(i["c"])
            zt3_count[i["c"]] = zt3_count.get(i["c"], 0) + 1

    lhb_rows = get_lhb(base)
    print("[2/6] 龙虎榜 %s：%d 条" % (base, len(lhb_rows)))
    lhb, net3 = {}, {}
    for r in lhb_rows:
        c = r["SECURITY_CODE"]
        if c not in lhb or safe(r["BILLBOARD_DEAL_AMT"]) > safe(lhb[c]["BILLBOARD_DEAL_AMT"]):
            lhb[c] = r
        net3[c] = net3.get(c, 0) + safe(r["BILLBOARD_NET_AMT"])
    for d in tdays[:-1]:
        for r in get_lhb(d):
            net3[r["SECURITY_CODE"]] = net3.get(r["SECURITY_CODE"], 0) + safe(r["BILLBOARD_NET_AMT"])

    # 行情：只需三日内涨停股 + 当日龙虎榜股
    codes = sorted(set(zt3) | set(lhb.keys()))
    market_of = {}
    board_map = {}
    for d in tdays:
        for i in pools[d]:
            market_of[i["c"]] = i["m"]
            if i.get("hybk"):
                board_map.setdefault(i["c"], i["hybk"])
    qmap = {}
    for c in codes:
        secu = (lhb.get(c) or {}).get("SECUCODE") or ""
        qmap[c] = tx_code(c, None, secu) if secu else tx_code(c, market_of.get(c, 0))
    quotes = tx_quotes(list(qmap.values()))
    print("[3/6] 行情快照 %d/%d 只" % (len(quotes), len(codes)))
    if len(quotes) < len(codes) * 0.7:
        warnings.append("行情缺失较多：%d/%d" % (len(quotes), len(codes)))

    # 只对真正相关的标的抓分时 / 昨日成交额，控制请求量（也避免触发接口风控）
    mcap_cap = float(th["总市值上限_亿"])
    chg_floor = float(th["涨跌幅下限"]) - 1.5
    prelim = [c for c in codes
              if c in quotes and c in zt3
              and quotes[c]["mcap"] / 1e8 <= mcap_cap * 1.1
              and quotes[c]["change"] >= chg_floor]
    prelim = prelim[:int(fc.get("候选上限", 150))]
    print("[4/6] 需竞价指标 %d 只，抓取分时…" % len(prelim))

    minutes, prevs = {}, {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_m = {c: ex.submit(tx_minute, qmap[c]) for c in prelim}
        for i, (c, fu) in enumerate(fut_m.items(), 1):
            minutes[c] = fu.result()
            if i % 20 == 0:
                print("      分时进度 %d/%d" % (i, len(prelim)))
        if fc.get("抓取昨日成交额", True):
            fut_p = {c: ex.submit(tx_prev_amount, qmap[c]) for c in prelim}
            for i, (c, fu) in enumerate(fut_p.items(), 1):
                prevs[c] = fu.result()
                if i % 20 == 0:
                    print("      昨日成交额进度 %d/%d" % (i, len(prelim)))

    # 竞价盘口（live）
    live_snap, live_file = {}, os.path.join(OUT_DIR, "竞价快照_%s.json" % td.replace("-", ""))
    if live:
        watch = [tx_code(c, next((i["m"] for d in tdays for i in pools[d] if i["c"] == c), 0)) for c in codes][:200]
        print("      竞价盘口采集 %d 只" % len(watch))
        quotes_live = tx_quotes(watch, chunk=25, use_cache=False)
        for c6, q in quotes_live.items():
            live_snap[c6] = {"bid": q["bid"], "ask": q["ask"], "t": q["time"]}
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(live_file, "w", encoding="utf-8") as f:
            json.dump({"date": td, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                       "quotes": live_snap}, f, ensure_ascii=False)
        print("      已保存 %d 只竞价盘口" % len(live_snap))
    elif os.path.exists(live_file):
        with open(live_file, "r", encoding="utf-8") as f:
            live_snap = json.load(f).get("quotes", {})
        print("      复用竞价盘口快照 %d 只" % len(live_snap))

    # 组装
    cands = []
    for c in codes:
        q = quotes.get(c)
        if not q:
            continue
        z, l, m, pv = zt_today.get(c, {}), lhb.get(c, {}), minutes.get(c) or {}, prevs.get(c) or {}
        fcap = q["fcap"] or q["mcap"]
        ltsz = safe(z.get("ltsz")) or fcap
        has_m = bool(m.get("bars"))
        first_amt = safe(m.get("first_amt"))
        day_amt = safe(m.get("day_amt")) or q["amount"]
        prev_amt = pv.get("amount")
        ls = live_snap.get(c) or {}
        x = {
            "code": c, "name": q["name"], "board": board_map.get(c) or z.get("hybk") or "-", "tx": qmap.get(c, ""),
            "change": q["change"], "price": q["price"],
            "open": q["open"], "high": q["high"], "low": q["low"], "pre_close": q["pre_close"],
            "amount": day_amt, "hs": q["hs"],
            "mcap_yi": round(q["mcap"] / 1e8, 2), "fcap_yi": round(fcap / 1e8, 2),
            "is_st": "ST" in (q["name"] or ""),
            "lbc": int(z.get("lbc") or 0), "fund": safe(z.get("fund")),
            "zbc": int(z.get("zbc") or 0), "fbt": fbt_str(z.get("fbt")),
            "ltsz": ltsz, "zt3_count": zt3_count.get(c, 0), "in_zt3": c in zt3,
            "auc_first": round(first_amt / 1e4, 1) if has_m else None,
            "auc_turn": round(pct(first_amt, fcap), 3) if has_m else None,
            "auc_vs_prev": round(pct(first_amt, prev_amt), 2) if (has_m and prev_amt) else None,
            "auc_share": round(pct(first_amt, day_amt), 2) if (has_m and day_amt) else None,
            "auc_time": m.get("t") or "",
            "bid": ls.get("bid") if ls else None, "ask": ls.get("ask") if ls else None,
            "lhb_net": round(safe(l.get("BILLBOARD_NET_AMT")) / 1e4, 1),
            "lhb_buy": round(safe(l.get("BILLBOARD_BUY_AMT")) / 1e4, 1),
            "lhb_sell": round(safe(l.get("BILLBOARD_SELL_AMT")) / 1e4, 1),
            "lhb_net3": round(net3.get(c, 0) / 1e4, 1),
            "lhb_reasons": [l.get("EXPLANATION")] if l else [],
            "lhb_explain": l.get("EXPLAIN") or ""
        }
        cands.append(x)

    metric_key = {"竞价换手率": "auc_turn", "竞价量占昨日成交额": "auc_vs_prev",
                  "首根占全天成交额": "auc_share"}.get(th.get("竞价口径"), "auc_turn")
    vals = sorted([x[metric_key] for x in cands if x[metric_key] is not None]) or [0]

    def rankv(v):
        if v is None or not vals or vals[-1] <= 0:
            return 0.0
        return round(sum(1 for t in vals if t <= v) / len(vals), 3)

    for x in cands:
        x["auction_rank"] = rankv(x[metric_key])
        x["score"], x["score_parts"] = score_stock(x, weights)
        x["prob"] = prob_label(x["score"])

    roles = classify_roles(cands)
    print("      分层：龙头 %d 只 / 中军 %d 只 / 后排 %d 只 ｜ 总龙头 %s" % (
        roles["counts"]["龙头"], roles["counts"]["中军"], roles["counts"]["后排"],
        (roles["total"] or {}).get("name") or "无"))

    def hard_pass(x):
        if not x["in_zt3"]:
            return False
        if x["change"] <= float(th["涨跌幅下限"]):
            return False
        if x["mcap_yi"] > float(th["总市值上限_亿"]):
            return False
        mv = x[metric_key]
        if mv is None or mv <= float(th["竞价下限"]):
            return False
        if th.get("集合竞价委卖大于委买"):
            if x["bid"] is None or x["ask"] is None or not x["ask"] > x["bid"]:
                return False
        return True

    passed = sorted([x for x in cands if hard_pass(x)], key=lambda x: -x["score"])

    def miss(x):
        arr = [x["in_zt3"], x["change"] > float(th["涨跌幅下限"]),
               x["mcap_yi"] <= float(th["总市值上限_亿"]),
               x[metric_key] is not None and x[metric_key] > float(th["竞价下限"])]
        if x["bid"] is not None and x["ask"] is not None:
            arr.append(x["ask"] > x["bid"])
        return sum(1 for i in arr if not i)

    near = sorted([x for x in cands if x not in passed], key=lambda x: (miss(x), -x["score"]))[:15]

    ladder = {}
    for i in zt_today.values():
        k = str(i["lbc"])
        ladder[k] = ladder.get(k, 0) + 1
    prev_zt = {i["c"] for i in pools[tdays[-2]]} if len(tdays) >= 2 else set()
    prev_perf = [quotes[c]["change"] for c in prev_zt if c in quotes]
    promo = (sum(1 for c in prev_zt if c in zt_today) / len(prev_zt) * 100) if prev_zt else 0

    print("[5/6] 候选 %d 只，全条件命中 %d 只" % (len(cands), len(passed)))

    # 精选：三日内涨停 + 涨幅>下限 + 市值≤上限（竞价口径只做展示，不再当淘汰条件）
    pick_pool = [x for x in cands if x["in_zt3"] and not x["is_st"]
                 and x["change"] > float(th["涨跌幅下限"])
                 and x["mcap_yi"] <= float(th["总市值上限_亿"])]
    if not pick_pool:
        pick_pool = [x for x in cands if x["in_zt3"] and not x["is_st"]]
    print("      精选池 %d 只，计算参考技术位（MA5/止损/目标）…" % len(pick_pool))
    # 盘中打开时，行情是“今天”的、统计基准停在最近收盘日；这时参考位要钉在昨收上，
    # 否则买点会跟着现价一起漂移，看盘时判断“到没到买点”就失去意义。
    qday = ""
    for v in quotes.values():
        qday = (v.get("time") or "")[:8]
        if qday:
            break
    intraday = bool(qday) and qday != base.replace("-", "")
    picks = build_picks(pick_pool, int(fc.get("精选数量", 5)), live, anchor_prev=intraday)
    print("      精选 %d 只%s" % (len(picks), "（盘中，参考位按昨收锚定）" if intraday else ""))
    tail = build_tail(cands, 3)
    print("      尾盘短线 %d 只（%s）" % (len(tail["items"]), tail["mode"]))

    idx_panel = build_indices(base)
    if idx_panel["list"]:
        print("      大盘指数 %d 个 ｜ %s" % (len(idx_panel["list"]), idx_panel["summary"]["style"]))
    senti = build_sentiment(idx_panel, {
        "zt": len(zt_today), "zbc": sum(1 for i in zt_today.values() if safe(i.get("zbc")) > 0),
        "max_lbc": (max([int(i["lbc"]) for i in zt_today.values()] or [0])),
        "promo": promo,
        "prev_avg": (sum(prev_perf) / len(prev_perf) if prev_perf else 0.0)
    }, base, days)
    print("      情绪温度 %.1f（%s）｜仓位建议 %s" % (senti["score"], senti["zone"], senti["position"]))

    ov = {
        "zt_count": len(zt_today), "lhb_count": len(lhb),
        "lhb_net_total": round(sum(safe(r["BILLBOARD_NET_AMT"]) for r in lhb.values()) / 1e8, 2),
        "max_lbc": max([int(i["lbc"]) for i in zt_today.values()] or [0]),
        "ladder": ladder, "prev_zt_count": len(prev_zt),
        "prev_promo": round(promo, 1),
        "prev_avg": round(sum(prev_perf) / len(prev_perf), 2) if prev_perf else 0
    }
    main_panel = build_main(cands, ov, base)
    print("      主板涨停 %d 家（占 %.1f%%），最高 %d 板" % (
        main_panel["mb_n"], main_panel["share"], main_panel["max_lbc"]))

    ml_panel = build_mainline(cands)
    top_ml = ml_panel.get("top") or {}
    print("      情绪主线 %s（%s，%d 只涨停，最高 %d 板）" % (
        top_ml.get("board") or "无", ml_panel["state"], top_ml.get("zt", 0), top_ml.get("lbc_max", 0)))
    adv = build_advice(senti, idx_panel, ml_panel)
    print("      大盘建议 %s ｜ 买 %s" % (adv["stance"], (adv["buy"] or "—")[:40]))

    return {
        "meta": {
            "trade_date": td, "base_date": base, "window": tdays,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "universe": len(quotes), "universe_total": len(codes), "candidates": len(cands),
            "auction_mode": "live" if live_snap else "minute",
            "metric": metric_key, "metric_name": th.get("竞价口径"),
            "warnings": warnings,
            "source": "东财涨停池·龙虎榜 ｜ 腾讯行情 ｜ 新浪竞价分时·日K"
        },
        "overview": ov,
        "main": main_panel,
        "passed": passed, "near": near, "cands": cands, "picks": picks, "roles": roles,
        "tail": tail,
        "indexes": idx_panel, "sentiment": senti,
        "mainline": ml_panel, "advice": adv,
        "lhb_table": [x for x in cands if x["lhb_reasons"]],
        "lianban_table": [x for x in cands if x["lbc"] > 0],
        "thresholds": th, "weights": weights
    }


def tech_levels(code, price, low, high, pre_close):
    """拉 10 日K线，算出参考技术位（MA5/MA10/近5日涨幅）"""
    try:
        url = ("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
               "?symbol=%s&scale=240&ma=no&datalen=12" % code)
        arr = js(F.get(url, ref=SINA_REF, ttl=21600))
        if not arr or len(arr) < 2:
            return None
        closes = [safe(x["close"]) for x in arr]
        # 最后一根就是今天（若今天没开盘则是上一交易日），用上一根当"昨收"更稳妥
        live_close = price or closes[-1]
        prev_close = pre_close or (closes[-2] if len(closes) >= 2 else closes[-1])
        base = closes[:-1] if live_close and abs(closes[-1] - live_close) < 0.01 else closes
        ma5 = sum(base[-5:]) / min(5, len(base[-5:])) if base else 0
        ma10 = sum(base[-10:]) / min(10, len(base[-10:])) if base else 0
        ret5 = (pct(live_close, base[-5]) - 100) if len(base) >= 5 else 0
        return {"prev_close": round(prev_close, 2), "ma5": round(ma5, 2),
                "ma10": round(ma10, 2), "ret5": round(ret5, 2),
                "low": low, "high": high}
    except Exception:
        return None


def limit_pct(code, name):
    """次日涨停幅度：创业板/科创 20%，ST 5%，其余 10%"""
    if "ST" in (name or ""):
        return 5.0
    if code[:2] in ("30", "68"):
        return 20.0
    return 10.0


def tail_mode():
    """尾盘时段判断：14:28–15:00 算“尾盘实时”，收盘后算“收盘估算”"""
    now = datetime.now()
    hm = now.hour * 60 + now.minute
    if hm >= 14 * 60 + 28 and hm <= 15 * 60 + 2:
        return "尾盘实时（%s）" % now.strftime("%H:%M")
    if hm >= 15 * 60 + 2:
        return "收盘后估算（按收盘价）"
    return "盘中估算（尾盘再看一次）"


def build_tail(pool, n=3):
    """尾盘短线：从候选里挑 ≤n 只做“次日惯性”，规则打分，不预测方向"""
    rows = []
    for x in pool:
        name = x.get("name") or ""
        if "ST" in name.upper():
            continue
        px = x["price"] or 0
        if px <= 0:
            continue
        if x["mcap_yi"] and x["mcap_yi"] > 300:
            continue
        change = x["change"] or 0
        up = limit_pct(x["code"], name)
        near_limit = change >= up - 0.6
        hs = x["hs"] or 0
        ltsz = x["ltsz"] or 0
        seal = (x["fund"] / ltsz) if (x["fund"] and ltsz) else 0
        lbc = x["lbc"] or 0
        hi = x["high"] or px
        tail_pos = (px / hi - 1) * 100 if hi else 0     # 0 = 收在当日最高
        s, parts = 0.0, []

        # ① 涨幅梯度：5% 到涨停之间最好（有空间、又证明有资金）
        if near_limit:
            s += 13
            parts.append("封涨停/贴板，次日靠溢价")
        elif change >= 5:
            s += 20
            parts.append("涨 %.1f%% 未封板，留了次日空间" % change)
        elif change >= 2:
            s += 12
            parts.append("涨 %.1f%%，偏温和" % change)
        else:
            s += 4
            parts.append("涨幅弱")

        # ② 换手：8%~25% 说明筹码换过一遍
        if 8 <= hs <= 25:
            s += 16
            parts.append("换手 %.0f%% 充分" % hs)
        elif 4 <= hs < 8:
            s += 8
        elif hs > 30:
            s += 2
            parts.append("换手 %.0f%% 过热" % hs)
        else:
            s += 3

        # ③ 市值弹性
        if x["mcap_yi"] and x["mcap_yi"] <= 120:
            s += 14
            parts.append("市值 %.0f 亿，弹性够" % x["mcap_yi"])
        elif x["mcap_yi"] and x["mcap_yi"] <= 200:
            s += 9
        else:
            s += 4

        # ④ 资金：龙虎榜净买 / 封单厚度
        if x["lhb_net"] and x["lhb_net"] >= 3e7:
            s += 12
            parts.append("龙虎榜净买 %.2f 亿" % (x["lhb_net"] / 1e8))
        if seal >= 0.02:
            s += 8
            parts.append("封单占流通 %.1f%%" % (seal * 100))

        # ⑤ 连板高度：首板/二板最舒服，高位减分
        if lbc >= 3:
            s += 2
            parts.append("%d 连板，位置偏高" % lbc)
        elif lbc == 2:
            s += 10
            parts.append("2 连板，接力还在")
        elif lbc == 1:
            s += 11
            parts.append("首板")
        else:
            s += 6
            parts.append("没涨停，属半路强势")

        # ⑥ 尾盘强度：收在最高附近，说明尾盘没砸
        if -1.2 <= tail_pos <= 0.01:
            s += 10
            parts.append("收在当日最高附近")
        elif tail_pos <= -5:
            s -= 5
            parts.append("尾盘回落 %.1f%%，有出货嫌疑" % abs(tail_pos))

        # ⑦ 竞价放量
        if (x["auc_vs_prev"] or 0) >= 3:
            s += 8
            parts.append("竞价放量")

        # ⑧ 炸板惩罚
        if (x["zbc"] or 0) > 0:
            s -= 8
            parts.append("今天炸过板")

        score = max(0, min(100, round(s, 1)))
        if score < 60:          # 低于 60 分直接不入选：宁可少于 3 只，也不拿弱票凑数
            continue
        level = "强" if score >= 75 else "中"
        anchor = px
        kind = "打板型" if near_limit else ("连板型" if lbc >= 2 else "半路型")
        target_pct = 0 if near_limit else (6.0 if lbc else 5.0)
        target = r2u(anchor * (1 + target_pct / 100.0)) if target_pct else r2u(anchor * (1 + up / 100.0))
        rows.append({
            "code": x["code"], "name": name, "score": score, "kind": kind, "level": level,
            "change": round(change, 2), "price": round(px, 2), "lbc": lbc,
            "mcap_yi": x["mcap_yi"], "hs": round(hs, 2) if hs else 0,
            "lhb_net": round((x["lhb_net"] or 0) / 1e8, 2),
            "seal": round(seal * 100, 2), "tail_pos": round(tail_pos, 2),
            "anchor": round(anchor, 2),
            "buy_zone": [round(anchor * 0.98, 2), round(anchor * 1.03, 2)],
            "buy_limit": round(anchor * 1.03, 2),
            "sell_half": r2u(anchor * (1.05 if near_limit else 1.04)),
            "weak_line": r2u(anchor),
            "deadline": "14:45",
            "buy_note": "次日竞价不超过 %.2f（今收+3%%）才买；高于这个数不追" % round(anchor * 1.03, 2),
            "target": target,
            "target_note": ("次日封板 %.2f 附近减/走" % target) if near_limit else ("次日冲高 %.2f 附近就减仓，别恋战" % target),
            "stop": round(anchor * 0.95, 2),
            "stop_note": "跌破 %.2f（今收-5%%）走人，短线不扛单" % round(anchor * 0.95, 2),
            "next_limit": r2u(anchor * (1 + up / 100.0)), "limit_pct": up,
            "reasons": parts[:5],
        })
    rows.sort(key=lambda r: -r["score"])
    # 类型分散：打板型 / 连板型 / 半路型 各先取 1 只，再按分数补满
    picked, used = [], {}
    for r in rows:
        if used.get(r["kind"], 0) < 1:
            picked.append(r)
            used[r["kind"]] = used.get(r["kind"], 0) + 1
        if len(picked) >= n:
            break
    if len(picked) < n:
        for r in rows:
            if r in picked:
                continue
            if r["kind"] == "打板型" and used.get("打板型", 0) >= 1:
                continue      # 打板型最多 1 只：尾盘短线优先“半路/连板”，打板是备选
            picked.append(r)
            used[r["kind"]] = used.get(r["kind"], 0) + 1
            if len(picked) >= n:
                break
    picked.sort(key=lambda r: -r["score"])
    return {"mode": tail_mode(), "n": n, "min_score": 60, "items": picked[:n],
            "note": "入选门槛 60 分（强 ≥75 / 中 60–74，低于 60 不入选、不够 3 只就留空）；"
                    "尾盘短线只看次日惯性：竞价高于买点上沿不追、目标位不破就减、止损不扛单。"}


def build_picks(pool, n, live=False, anchor_prev=False):
    """从候选里挑 ≤n 只，并给出规则化参考买卖位"""
    pool = sorted(pool, key=lambda x: -x["score"])[:max(n * 3, 12)]
    picks = []
    for x in pool:
        sym = x.get("tx") or (("sh" if x["code"].startswith("6") else "sz") + x["code"])
        lv = tech_levels(sym, x["price"], x["low"] or x["price"], x["high"] or x["price"],
                         x["pre_close"])
        if not lv or not lv["ma5"]:
            continue
        price, ma5 = x["price"], lv["ma5"]
        L = x["low"] or price
        anchor = x["pre_close"] if (anchor_prev and x["pre_close"]) else price
        up = limit_pct(x["code"], x["name"])
        next_limit = r2u(anchor * (1 + up / 100.0))
        support = min(ma5, lv["prev_close"]) if lv["prev_close"] else ma5
        # 回踩低吸：以锚定价（收盘后=今收，盘中=昨收）为基准回撤 1.5%~4.5%
        buy_lo, buy_hi = round(anchor * 0.955, 2), round(anchor * 0.985, 2)
        # 强势跟进：高于锚定价 1.5%~4.5% 且开盘后不破均价才成立
        chase_lo, chase_hi = round(anchor * 1.015, 2), round(anchor * 1.045, 2)
        # 止损：收盘后取「当日最低价下方1%」与「锚定价下方7%」更靠上者；
        # 盘中不能用当日最低（今天的低点往往已在昨收上方），统一按锚定价下方 7%
        stop = round(anchor * 0.93, 2) if anchor_prev else max(round(L * 0.99, 2), round(anchor * 0.93, 2))
        gap = round((price / ma5 - 1) * 100, 1) if ma5 else 0
        if gap >= 15:
            ma_note = "现价已高出 5 日线 %s%%，远离均线，只适合打板不适合低吸" % gap
        elif gap >= 5:
            ma_note = "现价高出 5 日线 %s%%，处在加速段，回踩 5 日线才有安全边际" % gap
        else:
            ma_note = "现价贴近 5 日线（%s%%），跌破 5 日线即转弱" % gap
        reasons = []
        if x["lbc"] >= 2:
            reasons.append("%d连板" % x["lbc"])
        if x["fund"] and x["ltsz"] and x["fund"] / x["ltsz"] >= 0.03:
            reasons.append("封单厚")
        if x["lhb_net"] > 0:
            reasons.append("龙虎榜净买")
        if x["zbc"] == 0:
            reasons.append("未炸板")
        if x["zt3_count"] >= 2:
            reasons.append("3日%d次涨停" % x["zt3_count"])
        if (x["auc_vs_prev"] or 0) >= 3:
            reasons.append("竞价放量")
        weak = 1 if (price < ma5 * 0.97 or (lv["ret5"] is not None and lv["ret5"] < -8)) else 0
        picks.append({
            "_weak": weak,
            "code": x["code"], "name": x["name"], "score": x["score"], "prob": x["prob"],
            "change": x["change"], "price": price, "lbc": x["lbc"], "mcap_yi": x["mcap_yi"],
            "fund_yi": round(x["fund"] / 1e8, 2) if x["fund"] else 0,
            "zbc": x["zbc"], "hs": x["hs"],
            "lhb_net": x["lhb_net"], "auc_turn": x["auc_turn"], "auc_vs_prev": x["auc_vs_prev"],
            "reasons": reasons[:4],
            "role": x.get("role") or "", "role_group": x.get("role_group") or "",
            "role_note": x.get("role_note") or "", "top": bool(x.get("top_leader")),
            "lv": lv,
            "next_limit": next_limit, "limit_pct": up, "anchor": round(anchor, 2),
            "anchor_prev": bool(anchor_prev),
            "buy_zone": [buy_lo, buy_hi], "chase_zone": [chase_lo, chase_hi],
            "support": round(support, 2), "stop": stop, "ma_note": ma_note,
            "low": round(L, 2), "high": round(lv["high"] or price, 2),
            "score_parts": x["score_parts"],
        })
    # 先排「涨停后没走弱」的（现价站上 5 日线、近 5 日不深跌），再按评分从高到低
    picks.sort(key=lambda r: (r["_weak"], -r["score"]))
    for r in picks:
        r.pop("_weak", None)
    return picks[:n]


# ---------------------------------------------------------------- 大盘指数
INDEX_LIST = [
    ("上证指数", "sh000001"), ("深证成指", "sz399001"), ("创业板指", "sz399006"),
    ("科创50", "sh000688"), ("沪深300", "sh000300"), ("中证500", "sh000905"),
    ("中证1000", "sh000852"), ("北证50", "bj899050"),
]


def idx_kline(sym, n=35):
    """指数日K（新浪）：盘中当日那根不完整，这里只用已收盘的日线算均线"""
    url = ("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
           "?symbol=%s&scale=240&ma=no&datalen=%d" % (sym, n))
    try:
        arr = json.loads(F.get(url, ref="https://finance.sina.com.cn/", ttl=3600))
    except Exception as e:
        print("    指数日K失败(%s)：%s" % (sym, str(e)[:30]))
        return []
    out = []
    for x in (arr or []):
        c = safe(x.get("close"))
        if c > 0:
            out.append({"d": x.get("day", ""), "c": c, "v": safe(x.get("volume")) / 100.0,  # v: 手
                        "h": safe(x.get("high")), "l": safe(x.get("low"))})
    return out


def _ma(arr, n):
    return (sum(arr[-n:]) / float(n)) if len(arr) >= n else 0.0


def _idx_seg(series, lo, hi, w, h, pad=2):
    """把一串价格压成 SVG 折线点"""
    if len(series) < 2:
        return ""
    mn, mx = min(series), max(series)
    rng = (mx - mn) or 1e-9
    step = (w - 2 * pad) / float(len(series) - 1)
    return " ".join("%.1f,%.1f" % (pad + i * step, pad + (h - 2 * pad) * (1 - (v - mn) / rng))
                    for i, v in enumerate(series))


def build_indices(base=""):
    """大盘指数面板：点位(实时)+均线/量能(已收盘日)+风格结论"""
    q = tx_quotes([s for _, s in INDEX_LIST], chunk=20, use_cache=False)
    rows = []
    for name, sym in INDEX_LIST:
        v = q.get(sym[2:]) or {}
        px, pre = safe(v.get("price")), safe(v.get("pre_close"))
        if not px:
            print("    指数缺行情：%s" % name)
            continue
        ks = idx_kline(sym)
        closes = [k["c"] for k in ks]
        vols = [k["v"] for k in ks]
        stack = closes[-25:] + [px]                    # 含今日，算均线用
        ma5, ma10, ma20 = _ma(stack, 5), _ma(stack, 10), _ma(stack, 20)
        chg = safe(v.get("change")) or ((px / pre - 1) * 100 if pre else 0.0)
        ret5 = (px / closes[-5] - 1) * 100 if len(closes) >= 5 else 0.0
        ret20 = (px / closes[-20] - 1) * 100 if len(closes) >= 20 else 0.0
        dist20 = (px / ma20 - 1) * 100 if ma20 else 0.0
        vol_ratio = (safe(v.get("vol_hand")) / (sum(vols[-5:]) / 5.0)) if len(vols) >= 5 and sum(vols[-5:]) else 0.0
        if not ma20:
            pos = "—"
        elif px >= ma20 and ma5 >= ma10 >= ma20:
            pos = "强势多头"
        elif px >= ma20:
            pos = "20日线上方"
        elif px >= ma5:
            pos = "跌破20日线"
        else:
            pos = "20日线下方"
        series = closes[-30:] + [px]
        ma20s = []                                     # 供小图叠加 20 日线
        full = closes + [px]
        for i in range(max(0, len(full) - 30), len(full)):
            ma20s.append(_ma(full[:i + 1], 20))
        hi20 = max([k["h"] for k in ks[-20:]] or [px])
        lo20 = min([k["l"] for k in ks[-20:] if k["l"] > 0] or [px])
        rows.append({
            "name": name, "sym": sym, "code": sym[2:],
            "price": round(px, 2), "chg": round(chg, 2), "chg_pts": round(px - pre, 2),
            "pre": round(pre, 2), "high": round(safe(v.get("high")), 2), "low": round(safe(v.get("low")), 2),
            "amt_yi": round(safe(v.get("amount")) / 1e8, 1),
            "ma5": round(ma5, 2), "ma10": round(ma10, 2), "ma20": round(ma20, 2),
            "dist20": round(dist20, 2), "ret5": round(ret5, 2), "ret20": round(ret20, 2),
            "vol_ratio": round(vol_ratio, 2), "pos": pos,
            "hi20": round(hi20, 2), "lo20": round(lo20, 2),
            "series": [round(x, 2) for x in series], "ma20s": [round(x, 2) for x in ma20s],
            "path": _idx_seg(series, 0, 0, 120, 38),
            "path20": _idx_seg([x for x in ma20s if x > 0], 0, 0, 120, 38) if ma20s else "",
            "t": (v.get("time") or "")[8:12]
        })

    by = {r["name"]: r for r in rows}
    sh, sz, cy = by.get("上证指数"), by.get("深证成指"), by.get("创业板指")
    hs, z1 = by.get("沪深300"), by.get("中证1000")
    up = sum(1 for r in rows if r["chg"] > 0)
    amt2 = (sh["amt_yi"] + sz["amt_yi"]) if (sh and sz) else 0.0
    diff = (z1["chg"] - hs["chg"]) if (z1 and hs) else 0.0
    if diff >= 1.5:
        style = "小盘明显强于权重（差 %+.1f 个百分点），资金在题材/弹性里找票" % diff
    elif diff >= 0.5:
        style = "小盘略强于权重（差 %+.1f 个百分点），赚钱效应偏向中小盘" % diff
    elif diff <= -1.5:
        style = "权重明显强于小盘（差 %+.1f 个百分点），资金抱团大票避险" % diff
    elif diff <= -0.5:
        style = "权重略强于小盘（差 %+.1f 个百分点）" % diff
    else:
        style = "大小盘基本同步（差 %+.1f 个百分点），没有明显风格切换" % diff
    strongest = max(rows, key=lambda r: r["chg"]) if rows else None
    weakest = min(rows, key=lambda r: r["chg"]) if rows else None

    concl = []
    if sh:
        above = sh["dist20"] >= 0
        concl.append("上证 %+.2f%%（%+.2f 点）报 %.2f 点，%s 20 日线（距 %+.2f%%）。"
                     % (sh["chg"], sh["chg_pts"], sh["price"], "站上" if above else "跌破",
                        sh["dist20"]))
    if cy and sh:
        gap = cy["chg"] - sh["chg"]
        concl.append("创业板 %+.2f%% vs 上证 %+.2f%%，成长股%s权重（差 %+.2f 个百分点）。"
                     % (cy["chg"], sh["chg"], "强于" if gap > 0 else "弱于", gap))
    if amt2:
        vr = (sh or {}).get("vol_ratio", 0)
        concl.append("两市成交额约 %.0f 亿，沪市量比 %.2f，%s。"
                     % (amt2, vr, "放量" if vr >= 1.15 else ("明显缩量" if vr <= 0.85 else "量能持平")))
    concl.append("8 个指数里 %d 个上涨，%s最强（%+.2f%%）、%s最弱（%+.2f%%）。"
                 % (up, strongest["name"] if strongest else "—", strongest["chg"] if strongest else 0,
                    weakest["name"] if weakest else "—", weakest["chg"] if weakest else 0))

    return {"asof": datetime.now().strftime("%Y-%m-%d %H:%M"), "base": base,
            "list": rows,
            "summary": {"up": up, "total": len(rows), "amt2_yi": round(amt2, 0),
                        "style_diff": round(diff, 2), "style": style,
                        "strongest": strongest["name"] if strongest else "",
                        "weakest": weakest["name"] if weakest else "",
                        "conclusion": concl}}


def zt_counts(days):
    """逐日涨停家数（用于算情绪的分位数与 5 日均）"""
    out = {}
    for d in days:
        try:
            out[d] = len(get_zt_pool(d.replace("-", "")))
        except Exception:
            out[d] = 0
    return out


def _clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def build_sentiment(idx, stats, base, days):
    """用指数位置/量能 + 涨停家数/晋级率/炸板/连板高度 算情绪温度（0-100）和买卖点信号"""
    lst = idx.get("list") or []
    sh = next((r for r in lst if r["name"] == "上证指数"), None)
    cy = next((r for r in lst if r["name"] == "创业板指"), None)
    sm = idx.get("summary") or {}

    hist = zt_counts([d for d in days if d <= base][-7:])
    prior = [v for d, v in sorted(hist.items()) if d < base][-5:]
    avg5 = (sum(prior) / len(prior)) if prior else max(1, stats["zt"])
    zt_ratio = stats["zt"] / float(avg5) if avg5 else 1.0

    parts, score = [], 0.0

    def add(name, val, weight, note, raw):
        nonlocal score
        score += val * weight
        parts.append({"k": name, "v": round(val, 2), "w": weight, "note": note, "raw": raw})

    # 1 指数位置（距 20 日线 -6%~+6% 映射 0~1）
    if sh:
        pos = _clamp((sh["dist20"] + 8) / 12.0)
        add("指数位置", pos, 0.20, "上证 %+.2f%% 于 20 日线" % sh["dist20"], sh["dist20"])
        # 2 均线结构
        st = 1.0 if (sh["price"] >= sh["ma20"] and sh["ma5"] >= sh["ma10"] >= sh["ma20"]) else (
            0.55 if sh["price"] >= sh["ma20"] else 0.2)
        add("均线结构", st, 0.10, sh["pos"], st)
        # 3 量能（放量涨 = 热，放量跌 = 更冷更危险，缩量跌 = 温和）
        vr = sh["vol_ratio"]
        vh = _clamp((vr - 1) * 0.8 + 0.5)
        if sh["chg"] < 0:
            vh = 1 - vh
        add("量能", vh, 0.12, "量比 %.2f，指数%s" % (vr, "涨" if sh["chg"] > 0 else "跌"), vr)
    cy_pos = (_clamp((cy["dist20"] + 8) / 12.0) if cy else 0.5)
    add("成长端位置", cy_pos, 0.06, ("创业板 %+.2f%% 于 20 日线" % cy["dist20"]) if cy else "—", cy_pos)

    # 广度
    add("上涨广度", (sm.get("up", 0) / float(sm.get("total") or 1)), 0.10,
        "%d/%d 指数上涨" % (sm.get("up", 0), sm.get("total", 0)), sm.get("up", 0))
    # 涨停家数
    add("涨停家数", _clamp(zt_ratio / 1.5), 0.14, "%d 只（5 日均 %d 只，%.2f 倍）" % (stats["zt"], avg5, zt_ratio), zt_ratio)
    # 晋级率
    add("接力强度", _clamp(stats["promo"] / 40.0), 0.12, "昨涨停今晋级率 %.1f%%" % stats["promo"], stats["promo"])
    # 昨日涨停股今日表现
    add("赚钱效应", _clamp((stats["prev_avg"] + 3) / 9.0), 0.08, "昨涨停今均涨幅 %+.2f%%" % stats["prev_avg"], stats["prev_avg"])
    # 连板高度
    add("高度", _clamp(stats["max_lbc"] / 5.0), 0.04, "最高 %d 板" % stats["max_lbc"], stats["max_lbc"])
    # 炸板率（越低越健康）
    zbc_rate = (stats["zbc"] / float(stats["zt"])) if stats["zt"] else 0.0
    add("封板质量", _clamp(1 - zbc_rate / 0.4), 0.04, "曾炸板 %d 只（占涨停 %.0f%%）" % (stats["zbc"], zbc_rate * 100), zbc_rate)

    score = round(score * 100, 1)

    if score >= 78:
        zone, zone_k, tips = "过热", "high", "情绪过热：只减不加，涨停股逢强卖，别在这里接力。"
    elif score >= 62:
        zone, zone_k, tips = "偏热", "warm", "偏热：持股可以，但追高只做龙头首板，仓位 3–5 成。"
    elif score >= 42:
        zone, zone_k, tips = "中性", "mid", "中性：做自己看得懂的结构，仓位 3–5 成，别满仓赌方向。"
    elif score >= 26:
        zone, zone_k, tips = "偏冷", "cool", "偏冷：开始挑票、分批试仓，等缩量回踩确认再上。"
    else:
        zone, zone_k, tips = "冰点", "cold", "冰点：恐慌时的低吸区，指数止跌 + 涨停家数回升再加仓。"

    # 买卖点信号
    sigs = []
    if sh:
        def sig(name, state, detail, action):
            sigs.append({"name": name, "state": state, "detail": detail, "action": action})

        v_up = sh["price"] >= sh["ma20"] and sh["vol_ratio"] >= 1.15 and sh["chg"] > 0
        v_left = sh["price"] < sh["ma20"] and sh["vol_ratio"] <= 0.9 and score <= 45
        v_break = sh["hi20"] and sh["price"] >= sh["hi20"] * 0.995
        v_stop = sh["chg"] < -1.0 and sh["vol_ratio"] >= 1.15
        sig("右侧买点（放量站上 20 日线）", "触发" if v_up else "未触发",
            "上证 %.2f，MA20 %.2f，量比 %.2f" % (sh["price"], sh["ma20"], sh["vol_ratio"]),
            "站上 MA20 且放量，次日回踩不破 MA20 可跟；不追第一根大阳")
        sig("左侧低吸（缩量跌破 20 日线 + 情绪偏冷）", "触发" if v_left else "未触发",
            "距 MA20 %+.2f%%，情绪分 %.1f" % (sh["dist20"], score),
            "缩量阴跌到冰点区，分批低吸指数 ETF / 走强板块核心，破前低止损")
        sig("前高突破确认", "接近" if (v_break or (sh["hi20"] and sh["price"] >= sh["hi20"] * 0.97)) else "未触发",
            "前 20 日高 %.2f，现价 %.2f" % (sh["hi20"], sh["price"]),
            "放量破前高 = 右侧加仓信号；冲高不过则减")
        sig("放量杀跌（离场信号）", "触发" if v_stop else "未触发",
            "今日 %+.2f%%，量比 %.2f" % (sh["chg"], sh["vol_ratio"]),
            "放量跌破 MA20 先减半仓，破 20 日低点 %.2f 清仓" % (sh["lo20"] or 0))
        sig("情绪过热减仓", "触发" if score >= 78 else "未触发",
            "情绪温度 %.1f（≥78 为过热）" % score,
            "涨停家数放大 + 高度走平 → 只卖不买，等退潮")
        sig("退潮确认", "触发" if (stats["prev_avg"] <= -2 or (zbc_rate >= 0.35 and stats["prev_avg"] < 1)) else "未触发",
            "昨涨停今均 %+.2f%%，曾炸板占比 %.0f%%" % (stats["prev_avg"], zbc_rate * 100),
            "昨日涨停今天不给溢价 = 退潮，空仓等下一轮")

    # 仓位建议
    if score >= 78:
        pos_adv, pos_n = "减到 2 成以下", 0.2
    elif score >= 62:
        pos_adv, pos_n = "3–5 成，只做强龙头", 0.4
    elif score >= 42:
        pos_adv, pos_n = "3–5 成，按结构做", 0.4
    elif score >= 26:
        pos_adv, pos_n = "2–3 成试仓", 0.25
    else:
        pos_adv, pos_n = "1–2 成分批低吸", 0.15

    return {"score": score, "zone": zone, "zone_key": zone_k, "tips": tips,
            "position": pos_adv, "position_n": pos_n, "parts": parts, "signals": sigs,
            "zt_hist": hist, "zt_avg5": round(avg5, 1), "zt_ratio": round(zt_ratio, 2),
            "zbc_rate": round(zbc_rate, 3),
            "key": ({"ma20": sh["ma20"], "hi20": sh["hi20"], "lo20": sh["lo20"], "price": sh["price"]} if sh else {})}


# ---------------------------------------------------------------- 情绪主线 / 大盘建议
def build_mainline(cands):
    """情绪主线：按行业板块聚合基准日涨停数/连板高度/龙虎榜净买/封单，判断资金主攻方向"""
    boards = {}
    for x in cands:
        lbc = int(x.get("lbc") or 0)
        if lbc <= 0:          # 只看基准日涨停的票
            continue
        b = (x.get("board") or "").strip() or "未分类"
        r = boards.setdefault(b, {"board": b, "zt": 0, "lbc_max": 0, "n2": 0,
                                  "net": 0.0, "seal": 0.0, "lead": None})
        r["zt"] += 1
        r["n2"] += 1 if lbc >= 2 else 0
        r["net"] += safe(x.get("lhb_net"))
        fv = safe(x.get("fund"))
        r["seal"] += fv
        r["lbc_max"] = max(r["lbc_max"], lbc)
        if r["lead"] is None or (lbc, fv) > (r["lead"]["lbc"], r["lead"]["fund"]):
            r["lead"] = {"name": x.get("name"), "code": x.get("code"), "lbc": lbc,
                         "fund": fv, "fund_yi": round(fv / 1e8, 2)}
    total = sum(r["zt"] for r in boards.values())
    rows = sorted(boards.values(), key=lambda r: (r["zt"], r["lbc_max"], r["net"]), reverse=True)
    out = []
    for r in rows[:6]:
        out.append({"board": r["board"], "zt": r["zt"], "lbc_max": r["lbc_max"], "n2": r["n2"],
                    "net_yi": round(r["net"] / 1e4, 2), "seal_yi": round(r["seal"] / 1e8, 2),
                    "lead": r["lead"],
                    "share": round(r["zt"] * 100.0 / total, 1) if total else 0})
    top = out[0] if out else None
    if top:
        if top["zt"] >= 5 and top["lbc_max"] >= 3 and top["n2"] >= 2:
            state, sk, note = "强化", "hot", "主线在强化：涨停家数、连板高度、梯队同时到位，回踩主线龙头/中军可以继续做。"
        elif top["zt"] >= 4 and top["lbc_max"] >= 2:
            state, sk, note = "延续", "mid", "主线延续但高度一般：只做板块内龙头和中军，后排跟风票不碰。"
        elif top["zt"] >= 2:
            state, sk, note = "分歧", "warm", "主线有分歧：涨停家数不够、高度也不高，等板块重新聚齐再上，追高容易吃面。"
        else:
            state, sk, note = "散乱", "cold", "没有明确主线：资金各打各的，宁可空仓或只低吸指数，不硬找票。"
    else:
        state, sk, note = "无数据", "cold", "今日无涨停数据，无法判断主线。"
    return {"top": top, "rows": out, "state": state, "state_key": sk, "note": note,
            "total_zt": total,
            "unique": bool(top and len(out) > 1 and top["zt"] >= out[1]["zt"] * 2)}


def build_advice(senti, idx, mainline):
    """大盘买卖建议：把情绪温度 + 指数位置 + 主线状态压成一句结论和执行清单"""
    score = senti.get("score", 50)
    K = senti.get("key") or {}
    ma20, hi20, lo20, px = (safe(K.get("ma20")), safe(K.get("hi20")),
                            safe(K.get("lo20")), safe(K.get("price")))
    ml = (mainline or {}).get("top") or {}
    ml_state = (mainline or {}).get("state", "")

    if score >= 78:
        stance, sk, commit = "减仓 · 防守", "high", "只卖不买，仓位压到 2 成以下"
    elif score >= 62:
        stance, sk, commit = "进攻 · 只做主线", "warm", "仓位 3–5 成，只买主线内龙头/中军"
    elif score >= 42:
        stance, sk, commit = "中性 · 轻仓做结构", "mid", "仓位 3 成左右，等主线重新聚齐再加"
    elif score >= 26:
        stance, sk, commit = "试探 · 分批低吸", "cool", "先 2 成试仓，跌破买点就止损"
    else:
        stance, sk, commit = "冰点 · 分批建仓", "cold", "分 2–3 次建到 3 成，别一次打满"

    # 指数买卖点（点位都用上证，条件写清楚前置信号）
    buy_txt = sell_txt = ""
    d_ma = d_hi = d_lo = 0
    if ma20 and px:
        d_ma, d_hi = round(ma20 - px), round((hi20 or ma20 * 1.05) - px)
        d_lo = round((lo20 or ma20 * 0.95) - px)
        if px >= ma20:
            buy_txt = "空仓的：回踩 %.0f 点（MA20，距现价 %+d 点）缩量不破就买，放量站上 %.0f 点（%+d 点）可追加" % (
                ma20, d_ma, hi20 or ma20 * 1.03, d_hi)
            sell_txt = "持仓的：冲高到 %.0f 点（%+d 点）不过就减，放量跌破 %.0f 点减半，跌破 %.0f 点（%+d 点）清仓" % (
                hi20 or ma20 * 1.05, d_hi, ma20, lo20 or ma20 * 0.95, d_lo)
        else:
            buy_txt = "空仓的：先别买，等收盘站回 %.0f 点（MA20，现价上方 %.0f 点）再动手；反抽不过 %.0f 点不追" % (
                ma20, abs(d_ma), ma20 * 1.02)
            sell_txt = "持仓的：反抽到 %.0f 点（上方 %.0f 点）不过就减，跌破 %.0f 点（下方 %.0f 点）清掉短线仓" % (
                ma20, abs(d_ma), lo20 or ma20 * 0.95, abs(d_lo))

    if ml and ml_state in ("强化", "延续"):
        what = "只买 %s 板块的龙头/中军（%s 连板）" % (ml["board"], "%d" % ml["lbc_max"])
    elif ml and ml_state == "分歧":
        what = "%s 板块已分歧，只留龙头，后排一律不碰" % ml["board"]
    elif ml:
        what = "主线散乱（%s 只有 %d 只涨停），宁可空仓等主线" % (ml["board"], ml["zt"])
    else:
        what = "等主线出现再买"

    checklist = [
        {"k": "仓位", "v": senti.get("position") or "—", "d": commit},
        {"k": "买什么", "v": what, "d": "主线 = 涨停家数最多、高度最高的那个板块"},
        {"k": "买点", "v": buy_txt or "—", "d": "指数只决定仓位，个股到价再看个股买点"},
        {"k": "卖点", "v": sell_txt or "—", "d": "先减后清的顺序，别一次砍完"},
    ]
    return {"stance": stance, "stance_key": sk, "commit": commit, "score": score,
            "buy": buy_txt, "sell": sell_txt, "checklist": checklist,
            "main_board": ml.get("board") if ml else "", "main_state": ml_state,
            "ma20": ma20, "hi20": hi20, "lo20": lo20, "price": px,
            "pts": {"ma20": d_ma, "hi20": d_hi, "lo20": d_lo}}


# ---------------------------------------------------------------- 输出
HEAD = ["代码", "名称", "涨跌幅%", "现价", "换手率%", "竞价换手率%", "竞价/昨成交%", "首根/全天%",
        "总市值(亿)", "连板", "3日涨停", "炸板", "首封", "封单(万)", "龙虎榜净买(万)",
        "3日龙虎榜(万)", "妖股评分", "概率档"]
KEYS = ["code", "name", "change", "price", "hs", "auc_turn", "auc_vs_prev", "auc_share",
        "mcap_yi", "lbc", "zt3_count", "zbc", "fbt", "fund", "lhb_net", "lhb_net3", "score", "prob"]


def export_csv(data):
    os.makedirs(OUT_DIR, exist_ok=True)
    td = data["meta"]["trade_date"].replace("-", "")
    for name, rows in [("筛选通过_%s" % td, data["passed"]),
                       ("最接近_%s" % td, data["near"]),
                       ("连板数据_%s" % td, data["lianban_table"]),
                       ("龙虎榜_%s" % td, data["lhb_table"])]:
        if not rows:
            continue
        with open(os.path.join(OUT_DIR, name + ".csv"), "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(HEAD)
            for x in rows:
                vals = []
                for k in KEYS:
                    v = x.get(k)
                    if k == "fund":
                        v = round((v or 0) / 1e4, 1)
                    vals.append(v if v is not None else "")
                w.writerow(vals)


def render(data):
    with open(TPL_PATH, "r", encoding="utf-8") as f:
        tpl = f.read()
    html = tpl.replace("__DATA_JSON__", json.dumps(data, ensure_ascii=False, separators=(",", ":")))
    out = os.path.join(BASE, "选股看板.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--open", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--render-only", action="store_true", help="只按已有 data.json 重出看板")
    a = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    if not os.path.exists(CFG_PATH):
        with open(CFG_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CFG, f, ensure_ascii=False, indent=2)

    cfg = load_cfg()
    global F
    F = Fetcher(min_interval=float(cfg["抓取"].get("最小间隔秒", 0.9)), refresh=a.refresh)

    if a.render_only:
        with open(os.path.join(OUT_DIR, "data.json"), "r", encoding="utf-8") as f:
            data = json.load(f)
        print("按已有数据重出看板：%s" % render(data))
        return

    data = build(a.date, a.live)
    export_csv(data)
    with open(os.path.join(OUT_DIR, "data.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    path = render(data)
    print("[6/6] 看板已生成：%s" % path)
    for w in data["meta"]["warnings"]:
        print("      提示：" + w)
    if a.open:
        os.startfile(path)


if __name__ == "__main__":
    main()
