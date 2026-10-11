# -*- coding: utf-8 -*-
"""fetch_all.py — GitHub Actions runner 上运行：东财分钟+日线四档资金流分片采集

用法: SHARD=0 NSHARD=5 python fetch_all.py
输出: out/mfmin_shard{N}.parquet   (klt=1,  当日240条, 当日累计曲线)
      out/mfdaily_shard{N}.parquet (klt=101, 近260交易日, 每日净额)
      out/failures_shard{N}.txt
限速: 0.34s/请求(≈2.9/s, 每分片独立runner独立IP)
字段: klines行 = "YYYY-MM-DD[ HH:MM],f52主力,f53小单,f54中单,f55大单,f56超大单" (元)
列:   code,date(yyyymmdd int),time(HHMM int),zl_net,xs_net,zx_net,dd_net,cdd_net
"""
import os
import time
import requests
import pandas as pd

SHARD = int(os.environ.get("SHARD", 0))
NSHARD = int(os.environ.get("NSHARD", 1))
WANT_MINUTE = os.environ.get("WANT_MINUTE", "1") != "0"
API = "https://push2his.eastmoney.com/api/qt/stock/fflow/kline/get"
HDR = {"Referer": "https://quote.eastmoney.com",
       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
RATE = 0.34

S = requests.Session()
S.headers.update(HDR)


def all_codes():
    """优先东财clist(单页pz=6000), 失败fallback仓库codes_backup.txt"""
    try:
        r = S.get("https://push2.eastmoney.com/api/qt/clist/get", params={
            "pn": 1, "pz": 6000, "po": 1, "np": 1, "fltt": 2, "invt": 2,
            "fid": "f12", "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
            "fields": "f12,f13"}, timeout=20)
        diff = (r.json().get("data") or {}).get("diff") or []
        out = [(str(it["f13"]), str(it["f12"]).zfill(6)) for it in diff]
        if len(out) > 1000:
            print(f"clist: {len(out)} codes", flush=True)
            return out
        print(f"clist suspiciously small: {len(out)}", flush=True)
    except Exception as e:
        print(f"clist fail: {e}", flush=True)
    codes = [l.strip() for l in open("codes_backup.txt", encoding="utf-8") if l.strip()]
    out = [(("1" if c[0] == "6" else "0") + "." + c, c) for c in codes]
    print(f"backup list: {len(out)} codes", flush=True)
    return out


def parse_klines(kl, klt):
    rows = []
    for k in kl:
        p = k.split(",")
        if len(p) < 6:
            continue
        if klt == 1:
            tm, d = p[0].split(" ")
            rows.append((d.replace("-", ""), int(tm.replace(":", "")[:4])))
        else:
            rows.append((p[0].replace("-", ""), 0))
        rows[-1] = rows[-1] + (float(p[1]), float(p[2]), float(p[3]),
                               float(p[4]), float(p[5]))
    return rows


def fetch_one(secid, klt, lmt):
    for attempt in range(3):
        try:
            r = S.get(API, params={"secid": secid, "fields1": "f1,f2,f3,f7",
                                   "fields2": "f51,f52,f53,f54,f55,f56",
                                   "klt": str(klt), "lmt": str(lmt)}, timeout=15)
            kl = (r.json().get("data") or {}).get("klines") or []
            return parse_klines(kl, klt)
        except Exception:
            if attempt == 2:
                return None
            time.sleep(1.5)


def main():
    todo = all_codes()[SHARD::NSHARD]
    print(f"shard {SHARD}/{NSHARD}: {len(todo)} codes", flush=True)
    os.makedirs("out", exist_ok=True)
    rows_min, rows_day, fails = [], [], []
    t0 = time.time()
    for i, (secid, code) in enumerate(todo, 1):
        r1 = fetch_one(secid, 1, 0) if WANT_MINUTE else []
        time.sleep(RATE)
        r2 = fetch_one(secid, 101, 260)
        time.sleep(RATE)
        if r1 is None and r2 is None:
            fails.append(code)
        if r1:
            rows_min.extend([(code,) + tuple(x) for x in r1])
        if r2:
            rows_day.extend([(code,) + tuple(x) for x in r2])
        if i % 200 == 0:
            print(f"  {i}/{len(todo)} min={len(rows_min)} day={len(rows_day)} "
                  f"fail={len(fails)} {time.time()-t0:.0f}s", flush=True)
    cols = ["code", "date", "time", "zl_net", "xs_net", "zx_net", "dd_net", "cdd_net"]
    dtypes = {"code": str, "date": "int32", "time": "int32",
              "zl_net": "float64", "xs_net": "float64", "zx_net": "float64",
              "dd_net": "float64", "cdd_net": "float64"}
    if rows_min:
        pd.DataFrame(rows_min, columns=cols).astype(dtypes).to_parquet(
            f"out/mfmin_shard{SHARD}.parquet", index=False)
    if rows_day:
        pd.DataFrame(rows_day, columns=cols).astype(dtypes).to_parquet(
            f"out/mfdaily_shard{SHARD}.parquet", index=False)
    with open(f"out/failures_shard{SHARD}.txt", "w") as f:
        f.write("\n".join(fails))
    print(f"DONE shard{SHARD}: min={len(rows_min)} day={len(rows_day)} fail={len(fails)} "
          f"{time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
