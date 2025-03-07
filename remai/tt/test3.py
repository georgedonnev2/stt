#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import random

f1 = 1.534
print(f"v={f1}, t={type(f1)}")
f2 = round(f1, 2)
print(f"v={f2}, t={type(f2)}")
f2 = round(f1, 0)
print(f"v={f2}, t={type(f2)}")
f2 = int(round(f1, 0))
print(f"v={f2}, t={type(f2)}")
# map_db_f = {
#     "name": {"file": "name", "key": True},
#     "rank": {"file": "rank", "format": "int"},
#     "revenue": {"file": "revenue", "format": "float2"},
#     "profit": {"file": "profit", "format": "float1"},
# }

# list_xls = [
#     {"name": "com-a", "rank": "1", "revenue": "123.4", "profit": 111},
#     {"name": "com-b", "rank": 2, "revenue": 345.6, "profit": 222},
# ]

# fmt = {"int": "f'{xls[v[\"file\"]]:d}'"}
# print(f"int={fmt['int']}")

# tmp = {}

# for xls in list_xls:
#     tmp_v = {}
#     tmp_k = ""
#     for k, v in map_db_f.items():
#         if v.get("format", "") not in ["str", "int", "float", "float1", "float2", ""]:
#             raise ValueError("str /int /float /float1 /float2 expected.")

#         print(f"type(xls[v['file']]={type(xls[v['file']])}")

#         if v.get("key", "") == True:

#             match v.get("format", ""):
#                 case "int":
#                     tmp_k = (
#                         int(xls[v["file"]])
#                         if type(xls[v["file"]]) == str
#                         else xls[v["file"]]
#                     )
#                 case "float":
#                     tmp_k = f"{xls[v['file']]:.0f}"
#                 case "float1":
#                     tmp_k = f"{xls[v['file']]:.1f}"
#                 case "float2":
#                     tmp_k = f"{xls[v['file']]:.2f}"
#                 case "str":
#                     tmp_k = f"{xls[v['file']]}"
#                 case "":
#                     tmp_k = xls[v["file"]]

#         else:
#             match v.get("format", ""):
#                 case "int":
#                     tmp_v[k] = int(f"{xls[v['file']]:d}")
#                 case "float":
#                     tmp_v[k] = f"{xls[v['file']]:.0f}"
#                 case "float1":
#                     tmp_v[k] = f"{xls[v['file']]:.1f}"
#                 case "float2":
#                     tmp_v[k] = f"{xls[v['file']]:.2f}"
#                 case "str":
#                     tmp_v[k] = f"{xls[v['file']]}"
#                 case "":
#                     tmp_v[k] = xls[v["file"]]

#     print(f"tmp_v={tmp_v}")
#     tmp.update({tmp_k: tmp_v})

# print(f"tmp={tmp}")


# now = datetime.now()
# print("now =", now)
# print("type(now) =", type(now))


# now = datetime.utcnow()
# print("now =", now)
# print("type(now) =", type(now))
# print(f"{now.tzname()}")


# def generate_random_int_list(n, start, end):
#     # 生成包含 n 个在 [start, end] 范围内的随机整数的列表
#     random_int_list = [random.randint(start, end) for _ in range(n)]
#     return random_int_list


# # 示例：生成包含 10 个在 1 到 100 之间的随机整数的列表
# n = 10
# start = 1
# end = 100
# random_integers = generate_random_int_list(n, start, end)
# print(random_integers)


# org = {"abc": "company_name"}
# for key, value in org.items():
#     print(f"key={key}, value={value}")

# tt = ("china", "usa")
# aa = ("aa", "bb", "china", "dd", "usa")
# bb = {cc: {} for cc in aa}
# print(f"bb={bb}")
# cc = "usa"
# if cc in aa:
#     print("cc is in aa")
# else:
#     print("cc is not in aa")

# mark = "[glb]"
# print(f"'{mark[:1]}', '{mark[-1:]}', '{mark[1:-1]}'")

# field = "tagx,chn500,y2023"
# fields = field.split(",")
# ft = tuple(fields)
# print(f"tuple={ft}")
# for f in ft:
#     print(f"f={f}")

# key_path = "user_info.location.state"
# keys = key_path.split(".")
# final_key = keys[-1]
# print(f"keys={keys}, final_key={final_key}, keys[:-1]={keys[:-1]}")


# dtnow = datetime.now()
# dtnow_str = dtnow.strftime("%Y-%m-%d %H:%M:%S")
# print(f"now={datetime.now()}, str={dtnow_str}")

# dt = datetime(2025, 2, 12, 12, 12, 12)
# print(f"dt={dt}")


# tuple1 = ("soe", "city", "prov")
# list1 = [f"[{keys}]" for keys in tuple1]
# print(f"list1={list1}")
# list1 = list1 + list(tuple1)
# print(f"list1={list1}")

# mark = "soe"
# tmp = mark if mark[:1] == "[" else ("[" + mark + "]")
# print(f"tmp={tmp}")
# mark = "[abc]"
# tmp = mark if mark[:1] == "[" else ("[" + mark + "]")
# print(f"tmp={tmp}")

# stock = {
#     "东方通信": {
#         "usn": "",
#         "code": {
#             "900941": {
#                 "region": "chn-mainland",
#                 "section": "沪市主板",
#                 "eff_date": "",
#                 "exp_date": "",
#                 "doc_ops": {},
#             },
#             "600776": {
#                 "region": "chn-mainland",
#                 "section": "沪市主板",
#                 "eff_date": "",
#                 "exp_date": "",
#                 "doc_ops": {},
#             },
#         },
#     }
# }

# for k, v in stock.items():
#     print(f"k={k}, v={v}")
#     print(f"v['code']={v['code']}")
#     for k1, v1 in v["code"].items():
#         print(f"k1={k1}, v1['region']={v1['region']}")

# tagx = {
#     "egc_tagx": {
#         "glb500": {"y2023": "nf-404"},
#         "chn500": {"y2023": "nf-404", "y2024": "nf-404"},
#         "soe": {"y2023": "nf-404"},
#         "plc": {"y2023": "nf-404"},
#         "nht": {"y2023": "nf-404"},
#         "isc100": {"y2023": "nf-404"},
#         "fic100_svc": {"y2023": "nf-404"},
#         "fic500_all": {"y2023": "nf-404"},
#         "fic500_mfg": {"y2023": "nf-404"},
#     }
# }

# print(
#     f"len::egc_tagx={len(tagx)}, tagx['egc_tagx']={len(tagx['egc_tagx'])}, tagx['egc_tagx']['glb500']={len(tagx['egc_tagx']['glb500'])}, tagx['egc_tagx']['chn500']={len(tagx['egc_tagx']['chn500'])}"
# )

import pandas as pd

# excel_path = "【爱企查】深圳市腾讯计算机系统有限公司工商注册信息.xls"
# excel_path = "test1.xlsx"
excel_path = "tx.xlsx"

# 使用 ExcelFile 打开文件
# df = pd.read_excel(excel_path, engine="openpyxl")
df = pd.read_excel(excel_path)

# 打印所有工作表的名称
# print(df)
# print(f"shape={df.shape}")
# for index, row in df.iterrows():
#     for col in df.columns:
#         print(f"Row {index}, Column {col}: {row[col]}")


# excel_path = "【爱企查】华为技术有限公司股东信息.xlsx"
# df = pd.read_excel(excel_path)
# print(f"shape={df.shape}")
# for index, row in df.iterrows():
#     for col in df.columns:
#         print(f"Row {index}, Column {col}: {row[col]}")


# # 读取 .xls 文件
# xls_file_path = "【爱企查】深圳市腾讯计算机系统有限公司工商注册信息.xls"
# # df = pd.read_excel(xls_file_path, engine="openpyxl")  # 使用 'xlrd' 引擎读取 .xls 文件
# df = pd.read_excel(xls_file_path, engine="xlrd")  # 使用 'xlrd' 引擎读取 .xls 文件
# print(f"shape={df.shape}")
# for index, row in df.iterrows():
#     for col in df.columns:
#         print(f"Row {index}, Column {col}: {row[col]}")


# file_path = "/Users/george1442/gdpvt/work/jnu/rem/2024届就业方案导出-戴士焱-250217.xlsx"
# # df = pd.read_excel(xls_file_path, engine="openpyxl")  # 使用 'xlrd' 引擎读取 .xls 文件
# df = pd.read_excel(
#     file_path,
#     usecols=[
#         "学号",
#         "姓名",
#         "性别",
#         "生源地名称",
#         "生源地代码",
#         "民族名称",
#         "政治面貌",
#         "城乡生源",
#         "学历层次",
#         "专业名称",
#         "专业方向",
#         "专业代码",
#         "入学年月",
#         "毕业日期",
#         "培养方式",
#         "委培单位",
#         "学习形式",
#         "毕业去向类别代码",
#         "毕业去向",
#         "签约日期",
#         "单位名称",
#         "统一社会信用代码",
#         "留学院校外文名称",
#         "单位性质代码",
#         "单位性质",
#         "单位行业代码",
#         "单位行业",
#         "单位所在地代码",
#         "单位所在地",
#         "单位地址",
#         "工作职位类别代码",
#         "工作职位类别",
#     ],
# )  # 使用 'xlrd' 引擎读取 .xls 文件
# rows = 0
# columns = 0
# print(f"shape={df.shape}")
# req_columns = df[["学号", "姓名"]]
# print(f"req_columns={req_columns}")
# df_list_of_series = req_columns.iterrows()  # 这是一个生成器，产生 (index, Series) 对
# df_list_of_dicts = [
#     row.to_dict() for index, row in req_columns.iterrows()
# ]  # 每行转换为字典
# print(f"df_list_of_dicts={df_list_of_dicts}")
# count = 1
# for ll in df_list_of_dicts:
#     print(f"#{count}::学号={ll['学号']},姓名={ll['姓名']}")
#     count += 1

# print("*" * 36)
# df_to_dict_of_values = df.to_dict(orient="records")
# print(f"df_to_dict_of_values={df_to_dict_of_values}")
# count = 1
# for student in df_to_dict_of_values:
#     print("-" * 36)
#     tmp = f"{student['单位行业代码']:.0f}"
#     print(f"#{count}::{student},{tmp},{student['单位所在地代码']:.0f}")

#     count += 1
# for index, row in df.iterrows():
#     rows += 1
#     # print(f"rows={rows}")
#     if rows > 10:
#         # print(f"rows={rows},break")
#         break
#     columns = 0
#     print(f"df.columns={df.columns}")
#     print(f"df.columns.to_list={df.columns.to_list()}")
#     for col in df.columns:
#         columns += 1
#         # print(f"columns={columns}")
#         if columns > 10:
#             # print(f"columns={columns},break")
#             break
#         # if col == "姓名":
#         print(f"Row {index}, Column {col}: {row[col]}")
#         print(f"df.columns[col]={df.columns[0]}")
#         # colstr = row[col].split(":")
#         # print(f"colstr={colstr}")

# 写入 .xlsx 文件
# xlsx_file_path = "tx1.xlsx"
# df.to_excel(xlsx_file_path, index=False)  # 将 DataFrame 写入 .xlsx 文件，不写入行索引

# print(f"成功将 {xls_file_path} 转换为 {xlsx_file_path}")

# # 读取 data.xlsx 文件
# df = pd.read_excel("【爱企查】深圳市腾讯计算机系统有限公司工商注册信息.xls")

# # 打印读取的 DataFrame
# print(df)

# # 临时使用
# pip install -i https://pypi.tuna.tsinghua.edu.cn/simple openpyxl


# # 永久配置
# pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# dtnow = datetime.now()
# dtnow_str = dtnow.strftime("%Y%m%d%H%M")
# print(f"dtnow={dtnow_str}")


# data = {
#     "glb500": {"y2023": "nf-404"},
#     "chn500": {"y2023": "nf-404"},
#     "soe": {"y2023": "nf-404", "y2024": "nf-404"},
#     "plc": {"y2023": "nf-404", "y2024": "nf-404"},
#     "nht": {"y2023": "nht", "y2024": "nht"},
#     "isc100": {"y2023": "nf-404"},
#     "fic100_svc": {"y2023": "nf-404"},
#     "fic500_all": {"y2023": "nf-404"},
#     "fic500_mfg": {"y2023": "nf-404"},
# }

# # 创建一个新字典来存储只包含 'y2024' 键的条目
# y2024_only_data = {
#     key: {"y2024": value["y2024"]} for key, value in data.items() if "y2024" in value
# }

# # 打印结果
# print(y2024_only_data)
