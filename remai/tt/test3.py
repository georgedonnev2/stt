#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


now = datetime.now()
print("now =", now)
print("type(now) =", type(now))


now = datetime.utcnow()
print("now =", now)
print("type(now) =", type(now))
print(f"{now.tzname()}")

dt = datetime(2020, 10, 31, 12, tzinfo=ZoneInfo("America/Los_Angeles"))
print(f"{dt}, {dt.tzname()}")
