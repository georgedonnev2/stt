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
