#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import random

now = datetime.now()
print("now =", now)
print("type(now) =", type(now))


now = datetime.utcnow()
print("now =", now)
print("type(now) =", type(now))
print(f"{now.tzname()}")


def generate_random_int_list(n, start, end):
    # 生成包含 n 个在 [start, end] 范围内的随机整数的列表
    random_int_list = [random.randint(start, end) for _ in range(n)]
    return random_int_list


# 示例：生成包含 10 个在 1 到 100 之间的随机整数的列表
n = 10
start = 1
end = 100
random_integers = generate_random_int_list(n, start, end)
print(random_integers)


org = {"abc": "company_name"}
for key, value in org.items():
    print(f"key={key}, value={value}")

tt = ("china", "usa")
aa = ("aa", "bb", "china", "dd", "usa")
bb = {cc: {} for cc in aa}
print(f"bb={bb}")
cc = "usa"
if cc in aa:
    print("cc is in aa")
else:
    print("cc is not in aa")

mark = "[glb]"
print(f"'{mark[:1]}', '{mark[-1:]}', '{mark[1:-1]}'")

field = "tagx,chn500,y2023"
fields = field.split(",")
ft = tuple(fields)
print(f"tuple={ft}")
for f in ft:
    print(f"f={f}")

key_path = "user_info.location.state"
keys = key_path.split(".")
final_key = keys[-1]
print(f"keys={keys}, final_key={final_key}, keys[:-1]={keys[:-1]}")


dtnow = datetime.now()
dtnow_str = dtnow.strftime("%Y-%m-%d %H:%M:%S")
print(f"now={datetime.now()}, str={dtnow_str}")

dt = datetime(2025, 2, 12, 12, 12, 12)
print(f"dt={dt}")


tuple1 = ("soe", "city", "prov")
list1 = [f"[{keys}]" for keys in tuple1]
print(f"list1={list1}")
list1 = list1 + list(tuple1)
print(f"list1={list1}")

mark = "soe"
tmp = mark if mark[:1] == "[" else ("[" + mark + "]")
print(f"tmp={tmp}")
mark = "[abc]"
tmp = mark if mark[:1] == "[" else ("[" + mark + "]")
print(f"tmp={tmp}")

stock = {
    "东方通信": {
        "usn": "",
        "code": {
            "900941": {
                "region": "chn-mainland",
                "section": "沪市主板",
                "eff_date": "",
                "exp_date": "",
                "doc_ops": {},
            },
            "600776": {
                "region": "chn-mainland",
                "section": "沪市主板",
                "eff_date": "",
                "exp_date": "",
                "doc_ops": {},
            },
        },
    }
}

for k, v in stock.items():
    print(f"k={k}, v={v}")
    print(f"v['code']={v['code']}")
    for k1, v1 in v["code"].items():
        print(f"k1={k1}, v1['region']={v1['region']}")

tagx = {
    "egc_tagx": {
        "glb500": {"y2023": "nf-404"},
        "chn500": {"y2023": "nf-404", "y2024": "nf-404"},
        "soe": {"y2023": "nf-404"},
        "plc": {"y2023": "nf-404"},
        "nht": {"y2023": "nf-404"},
        "isc100": {"y2023": "nf-404"},
        "fic100_svc": {"y2023": "nf-404"},
        "fic500_all": {"y2023": "nf-404"},
        "fic500_mfg": {"y2023": "nf-404"},
    }
}

print(
    f"len::egc_tagx={len(tagx)}, tagx['egc_tagx']={len(tagx['egc_tagx'])}, tagx['egc_tagx']['glb500']={len(tagx['egc_tagx']['glb500'])}, tagx['egc_tagx']['chn500']={len(tagx['egc_tagx']['chn500'])}"
)
