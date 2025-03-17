#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, datetime, timedelta, timezone
import pandas as pd
from pathlib import Path
import random
import re
import sys
from pymongo import MongoClient, UpdateOne, InsertOne
from pypinyin import pinyin, Style

import logging

logging.basicConfig(level=logging.INFO)

### --------------------------------------------------------------------------------------------------------------------
### some definition
### --------------------------------------------------------------------------------------------------------------------

FV_NOT_FOUND = "nf-404"
FV_ROOT_NODE = "i-root"
FV_NOT_APP = "not-applicable"


###
def int_to_base36(num):
    if num < 0:
        raise ValueError("The function does not support negative numbers.")

    # Characters used to represent base 36
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    if num == 0:
        return "0"

    result = []
    while num > 0:
        num, remainder = divmod(num, 36)
        result.append(chars[remainder])

    # The result list contains the base-36 digits in reverse order
    result.reverse()

    return "".join(result)


# ------------------------------------------------------------------------------
# Remai: base class
# ------------------------------------------------------------------------------
class Remai:
    pass


# ------------------------------------------------------------------------------
# RemaiTreeDB: base class
# ------------------------------------------------------------------------------
class RemaiTreeDB:

    ###
    def __init__(
        self,
        yearx="",
        tagx="",
        markx=("",),
        schemax={},
        treemapx={},
        collx="",
        tag_root=True,
    ):
        self.__year = yearx
        self.__tag = tagx
        self.__tag_mark = markx
        self.__schema_map = schemax
        self.__tree_map = treemapx
        self.__db_coll = collx
        self.__tag_root = tag_root
        self.__tree = {}

        return

    ###
    def set_usn(self, organizationx):

        orgx = {}
        for usn, v in organizationx.tree.items():
            orgx[v["name_fr"]] = usn

        nf_404 = []
        for organization_fr, v in self.tree.items():
            v["usn"] = orgx.get(organization_fr, "")
            if v["usn"] == "":
                nf_404.append(organization_fr)

        return nf_404

    ### show value of tree
    # shows: 0 < < 1, percent; =1, all(100%), >1, numbers
    def show(self, shows=1):

        keys2show = {}
        size2show = 0
        if shows == 1:
            keys2show = list(self.tree.keys())
        if (shows > 0) and (shows < 1):
            size2show = int(self.size * shows)
        if shows > 1:
            # print(f"shows is greater than 1.")
            size2show = min(shows, self.size)

        if (shows > 0) and (shows != 1):
            int_list = [random.randint(1, self.size) for _ in range(int(size2show))]
            key_list = list(self.tree.keys())
            for index in int_list:
                node = {key_list[index]: {}}
                keys2show.update(node)

        count = 1
        for node in keys2show:
            print(f"#{count}, node={node}, value={self.tree[node]}")
            count = count + 1

        return

    ###
    def set_mark(self, k_org, v_org):
        if v_org["name_fr"] in self.tree:
            return self.mark[0]

        return ""

    ###
    def wash(self, key2wash=[]):

        # logging.info(f"coll '{self.coll}'::washing to check whether all are unique ...")

        client = MongoClient()
        db = client.get_database("remai")

        result_tree = {}

        # key2wash = ["field1.field2.field3", "filed4"]
        # e.g. key2wash = ["root_node.name_fr", "usn", "name_fr"]
        for k2w in key2wash:

            value_tree = {}
            result_tree[k2w] = {}
            # print(f"result_tree={result_tree}")

            query = {}
            projection = {"_id": 0, k2w: 1}
            cursor = db.get_collection(self.coll).find(query, projection)
            count = 0
            for docx in cursor:
                count = count + 1

                v = docx  # make a copy
                for k in k2w.split("."):
                    v = v[k]  # deal with nested field. v is the value finally.

                if v in value_tree:  # if value is duplicated
                    result_tree[k2w].update({v: {}})
                    # logging.warning(
                    #     f"{key2wash[0]}={v} is NOT unique. Fix it then try again."
                    # )

                value_tree[v] = {}  # add value to dict anyway, as key of dic is unique.

            cursor.close()

        client.close()

        return result_tree

    ### property

    @property
    def coll(self):
        return self.__db_coll

    @property
    def mark(self):
        return self.__tag_mark

    @mark.setter
    def mark(self, m):
        self.__tag_mark = m
        return

    @property
    def schema(self):
        return self.__schema_map

    @property
    def size(self):
        return len(self.__tree)

    @property
    def tag(self):
        return self.__tag

    @tag.setter
    def tag(self, tagx):
        self.__tag = tagx

    @property
    def tag_root(self):
        return self.__tag_root

    @tag_root.setter
    def tag_root(self, v):
        self.__tag_root = v
        return

    @property
    def tree(self):
        return self.__tree

    @tree.setter
    def tree(self, tree):
        if type(tree) == dict:
            self.__tree = tree
        else:
            raise TypeError("dict expected such as {}.")

    @property
    def treemap(self):
        return self.__tree_map

    @property
    def year(self):
        return self.__year

    @year.setter
    def year(self, year):
        if (type(year) == str) and (year != ""):
            if (len(year) != 5) or (int(year[1:]) <= 2000):
                raise ValueError(
                    "string expected such as 'y2024' (greater than year 2000)."
                )
            else:
                self.__year = year
        else:
            raise TypeError("string expected such as 'y2024'.")

        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class CertNationalHteDB(RemaiTreeDB):

    def __init__(self, yearx, colly=None):

        # __coll = colly if colly is not None else ("list_chn500" + "_" + yearx)
        # print(f"list_chn500::coll={__coll}")

        super().__init__(
            yearx,
            tagx="nht",
            markx=("nht",),
            schemax={},
            treemapx={},
            collx=colly,
            tag_root=False,
        )
        return

    ### load from organization
    def load(self, org):
        self.tree = {}

        for usn, vx in org.tree.items():
            self.tree[usn] = {"name_fr": vx["name_fr"], "nht_period": vx["nht_period"]}

        # count = 0
        # for k, v in self.tree.items():
        #     print(f">>>nht::k={k}, v={v}")
        #     count += 1
        #     if count >= 10:
        #         break

        return

    ###
    def set_mark(self, k_org, v_org):

        ydate = date.fromisoformat(self.year[1:] + "0101")  # year = y2024

        vx = self.tree.get(k_org, {})

        if vx == {}:
            return ""

        for eff_date, exp_date in vx["nht_period"].items():
            if eff_date != "":
                if (date.fromisoformat(eff_date) <= ydate) and (
                    ydate <= date.fromisoformat(exp_date)
                ):
                    return self.mark[0]
        return ""


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListChn500YearDB(RemaiTreeDB):

    def __init__(self, yearx, colly=None):

        __coll = colly if colly is not None else ("list_chn500" + "_" + yearx)
        # print(f"list_chn500::coll={__coll}")

        super().__init__(
            yearx,
            tagx="chn500",
            markx=("chn",),
            schemax={
                "rank": "int",  # 排名（来自榜单）
                "enterprise": "str",  # 企业名称（来自榜单）
                "revenue": "float",  # 年收入（来自榜单）
                "profit": "float",  # 利润（来自榜单）
                "organization_fr": "str",  # 工商注册的企业名称，通常手工输入或调整
                "usn": "str",  # 集合 organization 中对每个组织的唯一编号，36进制
                "doc_ops": {
                    "create_time": "str",  # 文档创建时间，年月日时分秒
                    "create_by": "str",  # 由谁创建的
                    "update_time": "str",  # 文档的最后更新时间，年月日时分秒
                    "update_by": "str",  # 由谁更新的
                },
            },
            treemapx={},
            collx=__coll,
        )
        return

    ###
    def load(self, org=None):

        self.tree = {}

        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            self.tree[docx["organization_fr"]] = {
                "usn": docx.get("usn", ""),
                "doc_ops": docx.get("doc_ops", {}),
            }

        cursor.close()
        client.close()

        return

    ###
    def post(self):

        list_chn500 = ListChn500YearDB(self.year)
        list_chn500.load()

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        not_change = 0
        for kx, vx in self.tree.items():

            if list_chn500.tree.get(kx, {}).get("usn", "") == vx["usn"]:
                not_change = not_change + 1
                continue

            bulk_ops.append(
                UpdateOne(
                    {"organization_fr": kx},
                    {
                        "$set": {
                            "usn": vx["usn"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_time": datetime.now(),
                            "doc_ops.update_by": "back-end-gdv2",
                            # "doc_ops": {
                            #     "update_time": datetime.now(),
                            #     # "update_time": datetime(2025,2,12,12,12,12),
                            #     # "update_time": datetime.now().strftime(
                            #     # "%Y-%m-%d %H:%M:%S"
                            #     # ),
                            #     "update_by": "back-end-gdv2",
                            # },
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        print(f"list_chn500::not_change={not_change}, year={self.year}")

        client.close()

        return result


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListFic100SvcYearDB(RemaiTreeDB):

    ###
    def __init__(self, yearx, colly=None):

        __coll = colly if colly is not None else ("list_fic100_svc" + "_" + yearx)
        # print(f"list_chn500::coll={__coll}")

        super().__init__(
            yearx,
            tagx="fic100_svc",
            markx=("fic",),
            schemax={
                "rank": "int",  # 排名（来自榜单）
                "name": "str",  # 企业名称（来自榜单）
                "hq_location": "str",  # 所属省份（来自榜单）
                "industry": "str",  # 行业（来自榜单）
                "revenue": "float",  # 收入（来自榜单）
                "name_fr": "str",  # 工商注册的企业名称，通常手工输入或调整
                "usn": "str",  # 集合 organization 中对每个组织的唯一编号，36进制
                "doc_ops": {
                    "create_time": "str",  # 文档创建时间，年月日时分秒
                    "create_by": "str",  # 由谁创建的
                    "update_time": "str",  # 文档的最后更新时间，年月日时分秒
                    "update_by": "str",  # 由谁更新的
                },
            },
            treemapx={},
            collx=__coll,
        )

        return

    ###
    ###
    def load(self, org=None):

        self.tree = {}

        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            self.tree[docx["name_fr"]] = {
                "usn": docx.get("usn", ""),
                "doc_ops": docx.get("doc_ops", {}),
            }

        cursor.close()
        client.close()

        return

    ###
    def post(self):

        list4db = ListFic100SvcYearDB(self.year)  # change to correct class if necessary
        list4db.load()

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        change2be = 0
        for kx, vx in self.tree.items():

            if list4db.tree.get(kx, {}).get("usn", "") == vx["usn"]:
                continue

            change2be = change2be + 1
            bulk_ops.append(
                UpdateOne(
                    {"name_fr": kx},
                    {
                        "$set": {
                            "usn": vx["usn"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_by": "back-end-gdv2",
                            "doc_ops.update_time": datetime.now(),
                            # "doc_ops.create_by": "back-end-gdv2",
                            # "doc_ops.create_time": datetime(2025, 2, 12, 12, 12, 12),
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        # print(f"list_chn500::not_change={not_change}, year={self.year}")

        client.close()

        return result

    ###
    def set_usn(self, listx):

        orgx = {}  # listx is an instance of organization.
        for usn, v in listx.tree.items():
            orgx[v["name_fr"]] = usn

        nf_404 = []
        for organization_fr, v in self.tree.items():
            v["usn"] = orgx.get(organization_fr, "")
            if v["usn"] == "":
                v["usn"] = FV_NOT_FOUND
                nf_404.append(organization_fr)

        return nf_404


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListFic500AllYearDB(RemaiTreeDB):

    ###
    def __init__(self, yearx, colly=None):

        __coll = colly if colly is not None else ("list_fic500_all" + "_" + yearx)
        # print(f"list_chn500::coll={__coll}")

        super().__init__(
            yearx,
            tagx="fic500_all",
            markx=("fic",),
            schemax={
                "rank": "int",  # 排名（来自榜单）
                "name": "str",  # 企业名称（来自榜单）
                "hq_location": "str",  # 所属省份（来自榜单）
                "industry": "str",  # 行业（来自榜单）
                "revenue": "float",  # 收入（来自榜单）
                "name_fr": "str",  # 工商注册的企业名称，通常手工输入或调整
                "usn": "str",  # 集合 organization 中对每个组织的唯一编号，36进制
                "doc_ops": {
                    "create_time": "str",  # 文档创建时间，年月日时分秒
                    "create_by": "str",  # 由谁创建的
                    "update_time": "str",  # 文档的最后更新时间，年月日时分秒
                    "update_by": "str",  # 由谁更新的
                },
            },
            treemapx={},
            collx=__coll,
        )

        return

    ###
    ###
    def load(self, org=None):

        self.tree = {}

        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            self.tree[docx["name_fr"]] = {
                "usn": docx.get("usn", ""),
                "doc_ops": docx.get("doc_ops", {}),
            }

        cursor.close()
        client.close()

        return

    ###
    def post(self):

        list4db = ListFic500AllYearDB(self.year)
        list4db.load()

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        change2be = 0
        for kx, vx in self.tree.items():

            if list4db.tree.get(kx, {}).get("usn", "") == vx["usn"]:
                continue

            change2be = change2be + 1
            bulk_ops.append(
                UpdateOne(
                    {"name_fr": kx},
                    {
                        "$set": {
                            "usn": vx["usn"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_by": "back-end-gdv2",
                            "doc_ops.update_time": datetime.now(),
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        # print(f"list_chn500::not_change={not_change}, year={self.year}")

        client.close()

        return result

    ###
    def set_usn(self, listx):

        orgx = {}  # listx is an instance of organization.
        for usn, v in listx.tree.items():
            orgx[v["name_fr"]] = usn

        nf_404 = []
        for organization_fr, v in self.tree.items():
            v["usn"] = orgx.get(organization_fr, "")
            if v["usn"] == "":
                v["usn"] = FV_NOT_FOUND
                nf_404.append(organization_fr)

        return nf_404


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListFic500MfgYearDB(RemaiTreeDB):

    ###
    def __init__(self, yearx, colly=None):

        __coll = colly if colly is not None else ("list_fic500_mfg" + "_" + yearx)
        # print(f"list_chn500::coll={__coll}")

        super().__init__(
            yearx,
            tagx="fic500_mfg",
            markx=("fic",),
            schemax={
                "rank": "int",  # 排名（来自榜单）
                "name": "str",  # 企业名称（来自榜单）
                "hq_location": "str",  # 所属省份（来自榜单）
                "industry": "str",  # 行业（来自榜单）
                "revenue": "float",  # 收入（来自榜单）
                "name_fr": "str",  # 工商注册的企业名称，通常手工输入或调整
                "usn": "str",  # 集合 organization 中对每个组织的唯一编号，36进制
                "doc_ops": {
                    "create_time": "str",  # 文档创建时间，年月日时分秒
                    "create_by": "str",  # 由谁创建的
                    "update_time": "str",  # 文档的最后更新时间，年月日时分秒
                    "update_by": "str",  # 由谁更新的
                },
            },
            treemapx={},
            collx=__coll,
        )

        return

    ###
    ###
    def load(self, org=None):

        self.tree = {}

        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            self.tree[docx["name_fr"]] = {
                "usn": docx.get("usn", ""),
                "doc_ops": docx.get("doc_ops", {}),
            }

        cursor.close()
        client.close()

        return

    ###
    def post(self):

        list4db = ListFic500MfgYearDB(self.year)  # change to correct class if necessary
        list4db.load()

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        change2be = 0
        for kx, vx in self.tree.items():

            if list4db.tree.get(kx, {}).get("usn", "") == vx["usn"]:
                continue

            change2be = change2be + 1
            bulk_ops.append(
                UpdateOne(
                    {"name_fr": kx},
                    {
                        "$set": {
                            "usn": vx["usn"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_by": "back-end-gdv2",
                            "doc_ops.update_time": datetime.now(),
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        # print(f"list_chn500::not_change={not_change}, year={self.year}")

        client.close()

        return result

    ###
    def set_usn(self, listx):

        orgx = {}  # listx is an instance of organization.
        for usn, v in listx.tree.items():
            orgx[v["name_fr"]] = usn

        nf_404 = []
        for organization_fr, v in self.tree.items():
            v["usn"] = orgx.get(organization_fr, "")
            if v["usn"] == "":
                v["usn"] = FV_NOT_FOUND
                nf_404.append(organization_fr)

        return nf_404


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListGlb500YearDB(RemaiTreeDB):

    ###
    def __init__(self, yearx, colly=None):

        __coll = colly if colly is not None else ("list_glb500" + "_" + yearx)
        # print(f"list_chn500::coll={__coll}")

        super().__init__(
            yearx,
            tagx="glb500",
            markx=("glb",),
            schemax={
                "rank": "int",  # 排名（来自榜单）
                "enterprise": "str",  # 企业名称（来自榜单）
                "revenue": "float",  # 年收入（来自榜单）
                "profit": "float",  # 利润（来自榜单）
                "country": "str",  # 属于哪个国家（来自磅蛋糕）
                "organization_fr": "str",  # 工商注册的企业名称，通常手工输入或调整
                "usn": "str",  # 集合 organization 中对每个组织的唯一编号，36进制
                "doc_ops": {
                    "create_time": "str",  # 文档创建时间，年月日时分秒
                    "create_by": "str",  # 由谁创建的
                    "update_time": "str",  # 文档的最后更新时间，年月日时分秒
                    "update_by": "str",  # 由谁更新的
                },
            },
            treemapx={},
            collx=__coll,
        )

        return

    ###
    def load(self, org=None):

        self.tree = {}

        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            self.tree[docx["organization_fr"]] = {
                "usn": docx.get("usn", ""),
                "country": docx.get("country", ""),
                "doc_ops": docx.get("doc_ops", {}),
            }

        cursor.close()
        client.close()

        return

    ###
    def post(self):

        list4db = ListGlb500YearDB(self.year)
        list4db.load()

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        change2be = 0
        for kx, vx in self.tree.items():

            if list4db.tree.get(kx, {}).get("usn", "") == vx["usn"]:
                continue

            change2be = change2be + 1
            bulk_ops.append(
                UpdateOne(
                    {"organization_fr": kx},
                    {
                        "$set": {
                            "usn": vx["usn"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_by": "back-end-gdv2",
                            "doc_ops.update_time": datetime.now(),
                            # "doc_ops.create_by": "back-end-gdv2",
                            # "doc_ops.create_time": datetime(2025, 2, 12, 12, 12, 12),
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        # print(f"list_chn500::not_change={not_change}, year={self.year}")

        client.close()

        return result

    ###
    def set_usn(self, listx):

        orgx = {}  # listx is an instance of organization.
        for usn, v in listx.tree.items():
            orgx[v["name_fr"]] = usn

        nf_404 = []
        for organization_fr, v in self.tree.items():
            v["usn"] = orgx.get(organization_fr, "")
            if v["usn"] == "":
                v["usn"] = FV_NOT_FOUND
                if v["country"] == "中国":
                    nf_404.append(organization_fr)

        return nf_404


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListIsc100YearDB(RemaiTreeDB):

    ###
    def __init__(self, yearx, colly=None):

        __coll = colly if colly is not None else ("list_isc100" + "_" + yearx)
        # print(f"list_chn500::coll={__coll}")

        super().__init__(
            yearx,
            tagx="isc100",
            markx=("isc",),
            schemax={
                "no": "int",  # 编号（来自榜单）
                "name": "str",  # 企业名称（来自榜单）
                "business_brand": "str",  # 主要业务以及品牌（来自榜单）
                "hq_location": "str",  # 所属省份（来自榜单）
                "name_fr": "str",  # 工商注册的企业名称，通常手工输入或调整
                "usn": "str",  # 集合 organization 中对每个组织的唯一编号，36进制
                "doc_ops": {
                    "create_time": "str",  # 文档创建时间，年月日时分秒
                    "create_by": "str",  # 由谁创建的
                    "update_time": "str",  # 文档的最后更新时间，年月日时分秒
                    "update_by": "str",  # 由谁更新的
                },
            },
            treemapx={},
            collx=__coll,
        )

        return

    ###
    def load(self, org=None):

        self.tree = {}

        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            self.tree[docx["name_fr"]] = {
                "usn": docx.get("usn", ""),
                "doc_ops": docx.get("doc_ops", {}),
            }

        cursor.close()
        client.close()

        return

    ###
    def post(self):

        list4db = ListIsc100YearDB(self.year)
        list4db.load()

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        change2be = 0
        for kx, vx in self.tree.items():

            if list4db.tree.get(kx, {}).get("usn", "") == vx["usn"]:
                continue

            change2be = change2be + 1
            bulk_ops.append(
                UpdateOne(
                    {"name_fr": kx},
                    {
                        "$set": {
                            "usn": vx["usn"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_by": "back-end-gdv2",
                            "doc_ops.update_time": datetime.now(),
                            # "doc_ops.create_by": "back-end-gdv2",
                            # "doc_ops.create_time": datetime(2025, 2, 12, 12, 12, 12),
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        # print(f"list_chn500::not_change={not_change}, year={self.year}")

        client.close()

        return result

    ###
    def set_usn(self, listx):

        orgx = {}  # listx is an instance of organization.
        for usn, v in listx.tree.items():
            orgx[v["name_fr"]] = usn

        nf_404 = []
        for organization_fr, v in self.tree.items():
            v["usn"] = orgx.get(organization_fr, "")
            if v["usn"] == "":
                v["usn"] = FV_NOT_FOUND
                nf_404.append(organization_fr)

        return nf_404


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListPlcDB(RemaiTreeDB):

    def __init__(self, yearx, colly=None):

        __coll = colly if colly is not None else ("list_plc")

        super().__init__(
            yearx,
            tagx="plc",
            markx=("plc", "lcx"),
            schemax={
                "code": "str",  # 代码
                "name_aka": "str",  # 股票名称
                "name": "str",  # 企业名称
                "section": "float",  # 板块
                "region": "str",  # 在哪里上市的。比如 chn-mainland。
                "name_fr": "str",  # 工商注册的企业名称，通常手工输入或调整
                "eff_date": "datetime",  # 上市日期
                "exp_date": "datetime",  # 退市日期（如有）
                "usn": "str",  # 集合 organization 中对每个组织的唯一编号，36进制
                "doc_ops": {
                    "create_time": "str",  # 文档创建时间，年月日时分秒
                    "create_by": "str",  # 由谁创建的
                    "update_time": "str",  # 文档的最后更新时间，年月日时分秒
                    "update_by": "str",  # 由谁更新的
                },
            },
            treemapx={},
            collx=__coll,
            tag_root=False,
        )
        return

    ###
    def load(self, org=None):

        self.tree = {}

        client = MongoClient()
        db = client.get_database("remai")

        tmp = {}
        cursor = db.get_collection(self.coll).find({}, {"_id": 0})
        for docx in cursor:
            if docx["name_fr"] in self.tree:  # a company can have more than 1 stock.
                tmp1 = {
                    str(docx["code"]): {
                        "region": docx["region"],
                        "section": docx["section"],
                        "eff_date": docx["eff_date"],
                        "exp_date": docx["exp_date"],
                        "doc_ops": docx.get("doc_ops", {}),
                    },
                }
                self.tree[docx["name_fr"]]["code"].update(tmp1)
                tmp[str(docx["code"])] = docx["name_fr"]

            else:
                self.tree[docx["name_fr"]] = {
                    "usn": docx.get("usn", ""),
                    "code": {
                        str(docx["code"]): {
                            "region": docx["region"],
                            "section": docx["section"],
                            "eff_date": docx["eff_date"],
                            "exp_date": docx["exp_date"],
                            "doc_ops": docx.get("doc_ops", {}),
                        },
                    },
                }

        client.close()

        return tmp

    ###
    """
    def post(self):

        list_chn500 = ListChn500YearDB(self.year)
        list_chn500.load()

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        not_change = 0
        for kx, vx in self.tree.items():

            if list_chn500.tree.get(kx, {}).get("usn", "") == vx["usn"]:
                not_change = not_change + 1
                continue

            bulk_ops.append(
                UpdateOne(
                    {"organization_fr": kx},
                    {
                        "$set": {
                            "usn": vx["usn"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_time": datetime.now(),
                            "doc_ops.update_by": "back-end-gdv2",
                            # "doc_ops": {
                            #     "update_time": datetime.now(),
                            #     # "update_time": datetime(2025,2,12,12,12,12),
                            #     # "update_time": datetime.now().strftime(
                            #     # "%Y-%m-%d %H:%M:%S"
                            #     # ),
                            #     "update_by": "back-end-gdv2",
                            # },
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        print(f"list_chn500::not_change={not_change}, year={self.year}")

        client.close()

        return result
    """

    ###
    def set_mark(self, k_org, v_org):

        __mark = ""
        v = self.tree.get(v_org["name_fr"], {})

        if v != {}:
            for k1, v1 in v["code"].items():
                if v1["region"] == "chn-mainland":
                    __mark = self.mark[0]
                elif v1["region"] != "chn-mainland":
                    if __mark == "":
                        __mark = self.mark[1]
        return __mark


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListSoeDB(RemaiTreeDB):

    ###
    def __init__(self, yearx, colly=None):

        __coll = colly if colly is not None else ("list_soe")
        # print(f"list_chn500::coll={__coll}")

        super().__init__(
            yearx,
            tagx="soe",
            markx=("soe", "prov", "city"),
            schemax={
                "sn": "int",  # 编号
                "organization": "str",  # 企业名称
                "category": "str",  # 分类。比如"中央文化企业"。
                "supervisor": "str",  # 监管部门。国务院国资委或财政部。
                "administrative": "str",  # 主管部门。主要用于中央文化企业。
                "source": "str",  # 名录来源
                "organization_fr": "str",  # 工商注册的企业名称，通常手工输入或调整
                "usn": "str",  # 集合 organization 中对每个组织的唯一编号，36进制
                "doc_ops": {
                    "create_time": "str",  # 文档创建时间，年月日时分秒
                    "create_by": "str",  # 由谁创建的
                    "update_time": "str",  # 文档的最后更新时间，年月日时分秒
                    "update_by": "str",  # 由谁更新的
                },
            },
            treemapx={},
            collx=__coll,
        )

        return

    ###
    def load(self, org=None):

        self.tree = {}

        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            self.tree[docx["organization_fr"]] = {
                "usn": docx.get("usn", ""),
                "category": docx.get("category", ""),
                "doc_ops": docx.get("doc_ops", {}),
            }

        cursor.close()
        client.close()

        return

    ###
    def post(self):

        list4db = ListSoeDB(self.year)
        list4db.load()

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        change2be = 0
        for kx, vx in self.tree.items():

            if list4db.tree.get(kx, {}).get("usn", "") == vx["usn"]:
                continue

            change2be = change2be + 1
            bulk_ops.append(
                UpdateOne(
                    {"organization_fr": kx},
                    {
                        "$set": {
                            "usn": vx["usn"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_by": "back-end-gdv2",
                            "doc_ops.update_time": datetime.now(),
                            # "doc_ops.create_by": "back-end-gdv2",
                            # "doc_ops.create_time": datetime(2025, 2, 12, 12, 12, 12),
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        # print(f"list_chn500::not_change={not_change}, year={self.year}")

        client.close()

        return result

    ###
    def set_usn(self, listx):

        orgx = {}  # listx is an instance of organization.
        for usn, v in listx.tree.items():
            orgx[v["name_fr"]] = usn

        nf_404 = []
        for organization_fr, v in self.tree.items():
            v["usn"] = orgx.get(organization_fr, "")
            if v["usn"] == "":
                v["usn"] = FV_NOT_FOUND
                if v["category"] != "中央文化企业":
                    nf_404.append(organization_fr)

        return nf_404


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class OrganizationDB(RemaiTreeDB):

    ###
    def __init__(self, yearx, colly="organization"):

        self.__tag_count = {"hit": 0, "h_set": 0, "nf_404": 0}

        super().__init__(
            yearx,
            tagx="",
            markx=("",),
            schemax={
                "usn": "str",  # 组织的不重复编号，36进制。
                "name": "str",  # 从外部导入/输入的组织名称
                "name_fr": "str",  # 正式的工商注册名称
                "name_aka": "str",  # 保留
                "category": "str",  # 组织类别，比如企业ent、政府机关gpo，等。
                "h_psn": "str",  # 上级组织的usn。如果没有上级组织，就填写 root-node。
                "h_comments": "str",  # 组织的层次架构的相关信息参考。
                "root_node": {  # 根节点的信息
                    "usn": "str",
                    "name_fr": "str",
                },
                "child_node": {},  # 子节点的 usn。比如 {"abc":{}, "bef": {}}，或者为空（即没有子节点）。
                "nht_period": {},  # 高新技术企业的有效期信息。{eff_date1:exp_date1, eff_date2: exp_date2,...}
                "egc_tagx": {  # 企业的标签。egc = enterprise group company，正好同成长/新兴企业 Emerging Growth Company
                    # value of each tag is {year: tag}. e.g. {"y2023": "glb", "y2024":"nf-404"}
                    "glb500": {},  # 《财富》世界500强
                    "chn500": {},
                    "soe": {},
                    "plc": {},
                    "nht": {},
                    "isc100": {},
                    "fic100_svc": {},
                    "fic500_all": {},
                    "fic500_mfg": {},
                },
            },
            treemapx={},
            collx=colly,
        )

        return

    ### load from db
    def load(self, keep_all=False):

        self.tree = {}
        client = MongoClient()
        db = client.get_database("remai")

        cursor = db.get_collection(self.coll).find({}, {"_id": 0})
        for docx in cursor:
            if keep_all == True:
                self.tree[docx["usn"]] = {
                    "name_fr": docx["name_fr"],
                    "category": docx["category"],
                    "h_psn": docx["h_psn"],
                    "nht_period": docx.get("nht_period", {}),
                    "egc_tagx": docx.get("egc_tagx", {}),
                    "root_node": docx.get("root_node", {}),
                    "child_node": docx.get("child_node", {}),
                    "doc_ops": docx.get("doc_ops", {}),
                }
            else:
                self.tree[docx["usn"]] = {
                    "name_fr": docx["name_fr"],
                    "category": docx["category"],
                    "h_psn": docx["h_psn"],
                    "nht_period": docx.get("nht_period", {}),
                    "doc_ops": docx.get("doc_ops", {}),
                    "egc_tagx": {},
                    "root_node": {},
                    "child_node": {},
                }

        client.close()
        return

    ###
    def post(self):

        list4db = OrganizationDB(self.year)
        list4db.load(keep_all=True)  # load egc_tagx etc. from db

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        change2be = 0
        countx = 1
        for kx, vx in self.tree.items():

            # if list4db.tree.get(kx, {}).get("usn", "") == vx["usn"]:
            #     continue

            # identical betwee mem and db : usn, root_node, egc_tgax?
            __identical = 0

            if list4db.tree.get(kx, {}).get("child_node", "") == vx["child_node"]:
                __identical += 1

            if list4db.tree.get(kx, {}).get("root_node", "") == vx["root_node"]:
                __identical += 2

            __tmark_eq = True
            egc_tagx = list4db.tree.get(kx, {}).get("egc_tagx", {})
            yr_egc_tagx = {
                k: {self.year: v[self.year]}
                for k, v in egc_tagx.items()
                if self.year in v
            }  # get tagx of this year ONLY!
            if len(yr_egc_tagx) == len(vx["egc_tagx"]):
                for tag, mark in vx["egc_tagx"].items():
                    if yr_egc_tagx.get(tag, {}).get(self.year) != mark[self.year]:
                        __tmark_eq = False
            else:
                # print(
                #     f">>>#{countx}len-egc_tagx::kx={kx}, db={len(yr_egc_tagx)}, mem={len(vx['egc_tagx'])},db={yr_egc_tagx},mem={vx['egc_tagx']}"
                # )
                # countx += 1
                __tmark_eq = False
            if __tmark_eq == True:
                __identical += 4

            if __identical == 7:  # yes!
                continue

            field2update = {
                "$set": {
                    "root_node": vx["root_node"],
                    "child_node": vx["child_node"],
                    # specify field to be updated with '.' chain to avoid wrong overide.
                    "doc_ops.update_by": "back-end-gdv2",
                    "doc_ops.update_time": datetime.now(),
                }
            }

            for tag, mark in vx["egc_tagx"].items():
                tmp = {("egc_tagx" + "." + tag + "." + self.year): mark[self.year]}
                field2update["$set"].update(tmp)
            # tmp = {
            #     ("egc_tagx" + "." + "glb500" + "." + self.year): vx["egc_tagx"][
            #         "glb500"
            #     ][self.year]
            # }
            # field2update["$set"].update(tmp)

            change2be = change2be + 1
            bulk_ops.append(UpdateOne({"usn": kx}, field2update))

        # count = 1
        # for b in bulk_ops:
        #     print(f">>> #{count}, b={b}")
        #     count += 1
        #     if count > 10:
        #         break

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        client.close()

        return result

    ### set field 'child_node'
    def setf_child_node(self):

        psn_nf_404 = {}

        for key, value in self.tree.items():
            psn = str(value.get("h_psn"))
            if psn != FV_ROOT_NODE:  # if not 'root-node'
                if psn in self.tree:
                    self.tree.get(psn)["child_node"][key] = {}
                else:
                    psn_nf_404[psn] = {}

        return psn_nf_404

    ### set field 'root_node' for further statiscs, such as how many students employed by China Mobile.
    def setf_root_node(self, node, root_node):

        self.tree[node]["root_node"] = {
            "usn": root_node,
            "name_fr": self.tree[root_node]["name_fr"],
        }

        for child in self.tree[node]["child_node"]:
            self.setf_root_node(child, root_node)

        return

    ### set rest (still blank, not set yet)
    def setf_egc_tagx_rest(self, node, mark):

        listx_mark2 = [f"[{key}]" for key in self.mark]  # add []
        listx_mark2 = listx_mark2 + list(self.mark)  # e.g., ["soe","[soe]"]
        tmp = ""

        if self.get_mark_nty(node) in listx_mark2:
            tmp = self.get_mark_nty(node)
            self.__tag_count["hit"] += 1
        elif mark in listx_mark2:
            tmp = mark if (mark[:1] == "[") else ("[" + mark + "]")
            self.__tag_count["h_set"] += 1
        else:
            tmp = FV_NOT_FOUND
            self.__tag_count["nf_404"] += 1

        self.set_mark_nty(node, tmp)  # set mark = tmp

        for child in self.tree[node]["child_node"]:
            self.setf_egc_tagx_rest(child, self.get_mark_nty(node))

        return

    ### set field 'egc_tagx' of root node
    def setf_egc_tagx_root(self):

        tmp = {}

        for node, value in self.tree.items():
            if value.get("h_psn") == FV_ROOT_NODE:  # if node is root node
                if self.__get_egc_tagx_child(node) == True:
                    if self.get_mark_nty(node) != self.mark[0]:
                        self.set_mark_nty(node, "[" + self.mark[0] + "]")
                        # tmp.update({node: value["name_fr"]})
                        tmp[node] = value["name_fr"]
        return tmp

    def __get_egc_tagx_child(self, node):

        if self.get_mark_nty(node) in self.mark:
            return True

        for child in self.tree[node]["child_node"]:
            if self.__get_egc_tagx_child(child) == True:
                return True

        return False

    # get/set mark according to node & tag & year
    def get_mark_nty(self, node):
        return self.tree[node]["egc_tagx"].get(self.tag, {}).get(self.year, "")

    def set_mark_nty(self, node, m):
        if self.tree[node]["egc_tagx"].get(self.tag, {}) == {}:
            self.tree[node]["egc_tagx"][self.tag] = {}
        self.tree[node]["egc_tagx"][self.tag][self.year] = m
        return

    ###
    @property
    def tag_count(self):
        return self.__tag_count

    @tag_count.setter
    def tag_count(self, v):
        self.__tag_count["hit"] = 0
        self.__tag_count["h_set"] = 0
        self.__tag_count["nf_404"] = 0
        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class AddOrganization:

    ###
    def __init__(self, arg):
        self.__coll = arg["coll"]

        self.__flist = []  # list of file path
        self.__tree = {}  # {} or []
        self.__xlist = []  # list of doc

        self.__map_db2t = arg.get("map_db2t", {})

        self.__map_t2fp = arg.get("map_t2fp", {})
        self.__to_fpath = arg.get("to_fpath", "export.xlsx")
        self.__to_sheet = arg.get("to_sheet", "sheet1")

        return

    ### get list of files in folder
    # file_key in ["工商注册信息.xls", "股东信息.xls"]
    def get_flist(self, folder, file_key):

        if folder == "":
            raise ValueError("Name of folder is empty. Fix it and try again.")

        folder_path = Path(folder)
        for fp in folder_path.iterdir():

            if fp.is_file() == False:
                continue

            if file_key in fp.name:
                self.flist.append(fp)

        return

    ### get value of nested nodex from dictx
    # e.g., nodex = "ecg_tagx.glb500.y2024"
    def get_nested_node(self, nodex, dictx):

        node_list = nodex.split(".")
        __value = dictx

        for __nodex in node_list:
            __value = __value.get(__nodex, None)
            if __value == None:
                break

        return __value

    ### insert into db
    # key_field : name of key field
    # tree4chk : to compare with to check if data already exist in db.
    def insert2db(self, key_field, tree4chk, tree4post=None):

        client = MongoClient()
        db = client.get_database("remai")

        ops4db = []
        # result : of db operation,
        # exist : already in db (skip and not inserted.)
        result = {"ops4db": False, "result": None, "exist": 0}
        if tree4post == None:
            __treex = self.tree
        else:
            __treex = tree4post

        for k, v in __treex.items():

            if k in tree4chk:
                result["exist"] += 1
                continue

            doc2db = {}
            doc2db[key_field] = k
            for k1, v1 in v.items():
                doc2db[k1] = v1

            doc2db["doc_ops"] = {}
            doc2db["doc_ops"]["update_by"] = "be-gdv2"
            doc2db["doc_ops"]["create_by"] = "be-gdv2"
            doc2db["doc_ops"]["update_time"] = datetime.now()
            doc2db["doc_ops"]["create_time"] = datetime.now()

            ops4db.append(InsertOne(doc2db))

        if len(ops4db) > 0:
            result["result"] = db.get_collection(self.coll).bulk_write(ops4db)
            result["ops4db"] = True

        client.close()
        return result

    ###
    def load_db(self, queryx={}):

        self.tree = {}  # clear
        mapx = self.map_db2t

        client = MongoClient()
        db = client.get_database("remai")

        query = queryx
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:

            __tmp_v = {}
            for kmap, vmap in mapx.items():

                __v_dst = self.map_v2v(
                    docx.get(vmap["source"], None), vmap.get("format", None)
                )
                if vmap.get("key", False) == True:
                    __k_dst = __v_dst
                else:
                    __tmp_v[kmap] = __v_dst

            self.tree.update({__k_dst: __tmp_v})

        client.close()
        return

    ### map source to destination, and return destination
    def map_v2v(self, source, formatx=None, is_key=False):

        __typev = type(source)
        __tmp_dst = ""

        match formatx:
            case "int" if __typev == int:
                __tmp_dst = source
            case "int" if __typev == float:
                __tmp_dst = int(round(source, 0))
            case "int" if __typev == str:
                __tmp_dst = int(source)
            case "float" if __typev == float:
                __tmp_dst = source
            case "float" if __typev == str:
                __tmp_dst = float(source)
            case "str":
                __tmp_dst = f"{source}"
            case None:
                __tmp_dst = source

        return __tmp_dst

    ###
    # tree4pick = {usn1:usn_root_node1,usn2:usn_root_node2,...}
    def post2fp(self, tree4pick=None):

        ###
        if self.size == 0:
            return 0

        ###
        self.set_field2fp()  # define in child class

        # docs exported by sorted key. normally not sorted by usn, by name_fr.
        sorted_key = self.set_sort4fp()  # define in clid class
        if sorted_key == None:  # if set_sort4fp() not defined in child class
            sorted_key = self.tree

        ### tree to list
        # tree : {sid1:{"name":name1,"gender":male, ...}, sid2:{"name":name2,"gender":male, ...}}
        # list : [{"sid":sid1,"name":name1,...},{"sid":sid2,"name":name2,...} ]
        # tree ==> list

        list4export = []
        for k in sorted_key:
            v = self.tree[k]
            # {k:v} = {sid: {"name":xx, "gender":xx,...}}
            docx = {}
            for kmap, vmap in self.map_t2fp.items():
                if vmap.get("key", False) == True:
                    # e.g., "sid"(file): {"source": "sid"(tree), "key": True},
                    # kmap = "sid"(file), k = value of sid
                    # i.e., docx[kmap] = k  ==> {"sid": "xx"}
                    docx[kmap] = k
                else:
                    # e.g., "name"(file): {"source": "name"(tree)},
                    # docx[kmap] ==> docx["name"(file)]
                    # v[vmap["source"]] ==> v["name"(tree)] ==> "xx"
                    # docx[kmap] = v[vmap["source"]] ==> {"name": "xx"}
                    docx[kmap] = self.get_nested_node(vmap["source"], v)
                    # docx[kmap] = v[vmap["source"]]

            list4export.append(docx)

        ### pick some of list4export to export(list2fp)
        list2fp = list4export

        if tree4pick != None:  # tree4pick = {usn:{root_node:{usn:xx,name_fr:xx}},...}

            # set list of root_node.usn for pick
            root4pick = {}  # {root_node.usn:{}}
            for k, v in tree4pick.items():
                if v.get("root_node", None) == None:
                    continue
                root4pick[v["root_node"]["usn"]] = {}

            #
            key_fp = ""
            for k, v in self.map_t2fp.items():
                if v.get("key", False) == True:
                    key_fp = k
                    break

            # pick some of list4export to export(list2fp)
            list2fp = []
            for docx in list4export:
                if docx[key_fp] in tree4pick:
                    # print(f"#{count} :: docx[key_field]={docx[key_field]}")
                    # count += 1
                    # if count > 10:
                    #     break
                    docx["hit"] = "hit"
                else:
                    docx["hit"] = FV_NOT_FOUND

                if docx["snrt"] not in root4pick:
                    continue

                list2fp.append(docx)
        # count = 1
        # for docx in list2fp:
        #     print(f"#{count} :: doc = {docx}")
        #     count += 1
        #     if count > 10:
        #         break
        ### list to excel
        df = pd.DataFrame(list2fp)
        df.to_excel(self.to_fpath, index=False, sheet_name=self.to_sheet)

        return df.shape[0]  # (#) of docs

    ###
    def set_sort4fp(self):
        return None

    ###
    def setf_usn(self, treex):

        __not_found = 0
        for k, v in self.tree.items():
            if k in treex:
                v["usn"] = treex[k]["usn"]
            else:
                v["usn"] = FV_NOT_FOUND
                __not_found += 1

        return __not_found

    ###
    def show_tree(self, countx=10):
        print("=" * 36)
        count = 1
        for k, v in self.tree.items():
            print(f"{self.coll} :: #{count}, k = {k}, v = {v}")
            print("-" * 36)
            count += 1
            if count > countx:
                break
        return

    ###
    def show_xlist(self, countx=10):
        print("=" * 36)
        count = 1
        for docx in self.xlist:
            print(f"{self.coll} :: #{count}, doc = {docx}")
            print("-" * 36)
            count += 1
            if count > countx:
                break
        return

    ###
    def update2db(self, key_field, tree4chk, tree4post=None):

        client = MongoClient()
        db = client.get_database("remai")

        ops4db = []
        # result : of db operation,
        # exist : already in db (skip and not inserted.)
        result = {"ops4db": False, "result": None, "exist": 0}
        if tree4post == None:
            __treex = self.tree
        else:
            __treex = tree4post

        for k, v in __treex.items():

            # k should in. that's fine if not.
            if (k in tree4chk) == False:
                continue

            __size = 0
            __identical = 0
            for k1, v1 in v.items():
                __size += 1
                v4chk = tree4chk[k].get(k1, None)
                if v1 == v4chk:
                    __identical += 1
            # print(f"size={__size}, identical = {__identical}")
            if __size == __identical:
                result["exist"] += 1

                continue

            doc2db = {"$set": {}}
            for k1, v1 in v.items():
                doc2db["$set"][k1] = v1

            doc2db["$set"]["doc_ops.update_by"] = "be-gdv2"
            doc2db["$set"]["doc_ops.update_time"] = datetime.now()

            ops4db.append(UpdateOne({key_field: k}, doc2db))

        if len(ops4db) > 0:
            result["result"] = db.get_collection(self.coll).bulk_write(ops4db)
            result["ops4db"] = True

        client.close()
        return result

    ### property

    @property
    def coll(self):
        return self.__coll

    @property
    def xlist(self):
        return self.__xlist

    @xlist.setter
    def xlist(self, l):
        self.__xlist = l
        return

    @property
    def map_db2t(self):
        return self.__map_db2t

    @property
    def map_t2fp(self):
        return self.__map_t2fp

    @property
    def flist(self):
        return self.__flist

    @property
    def to_fpath(self):
        return self.__to_fpath

    @property
    def to_sheet(self):
        return self.__to_sheet

    @property
    def tree(self):
        return self.__tree

    @tree.setter
    def tree(self, t):
        self.__tree = t
        return

    @property
    def size(self):
        return len(self.tree)


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class OrganizationV2(AddOrganization):

    ###
    def __init__(self):

        arg = {
            "coll": "organization",
            "map_db2t": {
                "name_fr": {"source": "name_fr", "key": True},
                "usn": {"source": "usn"},
            },
        }

        super().__init__(arg)
        return

    ###
    def get_max_usn(self):

        client = MongoClient()
        db = client.get_database("remai")

        pipeline = [{"$project": {"_id": 0, "usn": 1}}, {"$sort": {"usn": -1}}]
        cursor = db.get_collection(self.coll).aggregate(pipeline)

        for docx in cursor:
            max_usn = docx["usn"]
            # print(docx)
            break

        return max_usn


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class Stu4Pick(AddOrganization):
    ###
    def __init__(self, yearx):

        collx = "student_" + yearx
        arg = {
            "coll": collx,
            "map_db2t": {
                "usn": {"source": "usn", "key": True},
                "root_node": {"source": "root_node"},
            },
        }

        self.__year = yearx

        super().__init__(arg)
        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class StuV2Export(AddOrganization):

    ###
    def __init__(self, yearx):

        collx = "student_" + yearx
        arg = {
            "coll": collx,
            "map_db2t": {
                # bio
                "sid": {"source": "sid", "key": True},  # key
                "name": {"source": "name"},
                "gender": {"source": "gender"},
                "student_from_code": {"source": "student_from_code"},
                "student_from": {"source": "student_from"},
                # major
                "major": {"source": "major"},
                "specialization": {"source": "specialization"},
                "major_code": {"source": "major_code"},
                "degree_year": {"source": "degree_year"},
                # path-after-graduate
                "path_after_graduate_code": {"source": "path_after_graduate_code"},
                "path_after_graduate": {"source": "path_after_graduate"},
                "path_fr": {"source": "path_fr"},
                "position_type_code": {"source": "position_type_code"},
                "position_type": {"source": "position_type"},
                # organization
                "organization": {"source": "organization"},
                "organization_fr": {"source": "organization_fr"},
                "category": {"source": "category"},  # ent, gpo, etc.
                "usci_org": {"source": "usci_org"},
                "type_org_code": {"source": "type_org_code"},
                "type_org": {"source": "type_org"},
                "industry_org_code": {"source": "industry_org_code"},
                "industry_org": {"source": "industry_org"},
                "location_org_code": {"source": "location_org_code"},
                "location_org": {"source": "location_org"},
                "address_org": {"source": "address_org"},
                "usn": {"source": "usn"},
                "egc_tagx": {"source": "egc_tagx"},  # glb, chn, etc.
                "root_node": {"source": "root_node"},
            },
            "map_t2fp": {
                # source: tree in memory; destination: file
                # bio
                "sid": {"source": "sid", "key": True},
                "name": {"source": "name"},
                "gender": {"source": "gender"},
                "student_from_code": {"source": "student_from_code"},
                "student_from": {"source": "student_from"},
                # major
                "degree_year": {"source": "degree_year"},
                # path-after-graduate
                "path_after_graduate_code": {"source": "path_after_graduate_code"},
                "path_after_graduate": {"source": "path_after_graduate"},
                "path_fr": {"source": "path_fr"},
                "position_type_code": {"source": "position_type_code"},
                "position_type": {"source": "position_type"},
                # organization
                "organization": {"source": "organization"},
                "organization_fr": {"source": "organization_fr"},
                "category": {"source": "category"},  # ent, gpo, etc.
                "usci_org": {"source": "usci_org"},
                "location_org_code": {"source": "location_org_code"},
                "location_org": {"source": "location_org"},
                "address_org": {"source": "address_org"},
                "usn": {"source": "usn"},
                # orgnaization.egc_tagx
                "glb": {"source": "egc_tagx.glb500"},
                "chn": {"source": "egc_tagx.chn500"},
                "soe": {"source": "egc_tagx.soe"},
                "plc": {"source": "egc_tagx.plc"},
                "nht": {"source": "egc_tagx.nht"},
                "isc": {"source": "egc_tagx.isc100"},
                "fic": {"source": "egc_tagx.fic500_all"},
                "mfg": {"source": "egc_tagx.fic500_mfg"},
                "svc": {"source": "egc_tagx.fic100_svc"},
                # organization.root_node
                "usn_root": {"source": "root_node.usn"},
                "name_root": {"source": "root_node.name_fr"},
            },
        }

        self.__year = yearx

        super().__init__(arg)
        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class OrgV3Export(AddOrganization):

    ###
    def __init__(self, yearx_list):

        arg = {
            "coll": "organization",
            "years": yearx_list.sort(),
            "to_fpath": "exp-organization-"
            + datetime.now().strftime("%Y%m%d%H%M")
            + ".xlsx",
            "map_db2t": {
                "usn": {"source": "usn", "key": True},
                "name_fr": {"source": "name_fr"},
                "usci_org": {"source": "usci_org"},
                "category": {"source": "category"},
                "h_psn": {"source": "h_psn"},
                "egc_tagx": {"source": "egc_tagx"},
                "child_node": {"source": "child_node"},
                "root_node": {"source": "root_node"},
            },
            "map_t2fp": {
                "usn": {"source": "usn", "key": True},
                "name_fr": {"source": "name_fr"},
                # "py": {"source": "pinyin"},  # check sorted order only
                "usci": {"source": "usci_org"},
                "cat": {"source": "category"},
                "psn": {"source": "h_psn"},
                "snrt": {"source": "root_node.usn"},
                "namert": {"source": "root_node.name_fr"},
                "loh": {"source": "loh"},  # not from db
                "csz": {"source": "child_size"},  # not from db
            },
        }

        ### add egc_tagx to map_t2fp
        __map_egc_tagx = {
            "glb": "glb500",
            "chn": "chn500",
            "soe": "soe",
            "plc": "plc",
            "nht": "nht",
            "isc": "isc100",
            "fic": "fic500_all",
            "mfg": "fic500_mfg",
            "svc": "fic100_svc",
        }
        yearx_list.sort()  # e.g., yearx_list = ["y2021", "y2022"]

        for __to_file, __in_tree in __map_egc_tagx.items():
            for yearx in yearx_list:
                arg["map_t2fp"][__to_file + yearx[-2:]] = {
                    "source": "egc_tagx." + __in_tree + "." + yearx
                }

        super().__init__(arg)
        return

    ###
    # set loh, child_size
    def set_field2fp(self):

        def __set_loh(nodex, lohx=1):
            self.tree[nodex]["loh"] = lohx
            for child, v in self.tree[nodex]["child_node"].items():
                __set_loh(child, lohx + 1)
            return

        ###
        for k, v in self.tree.items():

            # set loh (for tree)
            if v["h_psn"] == FV_ROOT_NODE:
                __set_loh(k, lohx=1)

            # set child_size (for tree)
            v["child_size"] = len(v["child_node"])

        return

    ###
    def set_sort4fp(self):
        # print("i am child.")

        def __set_list4sort(usn, pathx, lohx=1):
            # print(f"node = {node}, loh={lohx}, path={pathx}")
            pathx = pathx + "." + self.tree[usn]["pinyin"]
            docx = []
            docx = [
                pathx,
                usn,
                self.tree[usn]["name_fr"],
                self.tree[usn]["pinyin"],
                lohx,
            ]
            list4sort.append(docx)

            # if self.tree[node]["h_psn"] != FV_ROOT_NODE:
            # pathx = pathx + "." + self.tree[node]["pinyin"]
            for child in self.tree[usn]["child_node"]:
                # print(f"child = {child}, loh={lohx + 1}, path={pathx}")
                __set_list4sort(child, pathx)

            return

        # set pinyin
        for k, v in self.tree.items():
            d2lists = pinyin(v["name_fr"], style=Style.NORMAL)  # 2 dimension list
            flat_list = [item for sublist in d2lists for item in sublist]
            pyx = "".join(flat_list)
            v["pinyin"] = pyx.lower()

        # set list for sort
        list4sort = []
        for k, v in self.tree.items():  # k = usn
            if v["h_psn"] != FV_ROOT_NODE:
                continue
            __set_list4sort(k, "")

        # sort list4sort
        list4sort = sorted(list4sort, key=lambda x: (x[0],))

        # return sorted_key
        sorted_key = []
        for itemx in list4sort:
            sorted_key.append(itemx[1])  # usn is 2nd item, i.e., [1]

        # count = 1
        # for k in sorted_key:
        #     print(f"#{count} :: {k}")
        #     count += 1
        #     if count > 10:
        #         break

        return sorted_key


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class OrgV2Export(AddOrganization):  # 废弃

    ###
    def __init__(self, yearx="y2024"):

        arg = {
            "coll": "organization",
            "map_db2t": {
                "usn": {"source": "usn", "key": True},
                "name_fr": {"source": "name_fr"},
                "usci_org": {"source": "usci_org"},
                "category": {"source": "category"},
                "h_psn": {"source": "h_psn"},
                "egc_tagx": {"source": "egc_tagx"},
                "child_node": {"source": "child_node"},
                "root_node": {"source": "root_node"},
            },
            # "map_t2fp": {
            #     "usn": {"source": "usn", "key": True},
            #     "name_fr": {"source": "name_fr"},
            #     "h_psn": {"source": "h_psn"},
            #     "child_node": {"source": "child_node"},
            #     "root_node": {"source": "root_node"},
            # },
        }

        self.__year = yearx
        self.__fpath = (
            "remai-" + self.year + "-" + datetime.now().strftime("%Y%m%d%H%M") + ".xlsx"
        )
        self.__sheet = "org-" + self.year

        super().__init__(arg)
        return

    ###
    # tree4pick = {usn:usn_root_node}
    def export2xlsx(self, key_field, yearx=None, tree4pick=None):

        list2export = []
        count = 1
        for itemx in self.xlist:
            # print(f"#{count} :: item = {itemx}")
            # count += 1
            # if count > 10:
            #     break

            # [[path, loh, pinyin, name_fr, usn]]
            usn = itemx[4]
            loh = itemx[1]

            # for k, v in self.tree.items():
            # usn, name_fr, category

            docx = {}

            docx[key_field] = usn
            docx["name_fr"] = self.tree[usn]["name_fr"]
            docx["usci_org"] = self.tree[usn].get("usci_org", "")
            docx["category"] = self.tree[usn]["category"]
            docx["h_psn"] = self.tree[usn]["h_psn"]
            docx["root_usn"] = self.tree[usn]["root_node"]["usn"]
            docx["root_name"] = self.tree[usn]["root_node"]["name_fr"]

            docx["loh"] = loh
            docx["child"] = len(self.tree[usn]["child_node"])

            for k, v in self.tree[usn]["egc_tagx"].items():
                docx[k] = v[self.year]

            list2export.append(docx)

        ### pick some to export
        if tree4pick != None:

            root4pick = {}
            for k, v in tree4pick.items():
                if v.get("root_node", None) == None:
                    continue
                root4pick[v["root_node"]["usn"]] = {}

            list_picked = []
            count = 1
            for docx in list2export:

                if docx[key_field] in tree4pick:
                    # print(f"#{count} :: docx[key_field]={docx[key_field]}")
                    # count += 1
                    # if count > 10:
                    #     break
                    docx["hit"] = "hit"
                else:
                    docx["hit"] = FV_NOT_FOUND

                if docx["root_usn"] not in root4pick:
                    continue

                list_picked.append(docx)

            __sheet_name = self.sheet
        else:
            list_picked = list2export
            __sheet_name = "all"

        df = pd.DataFrame(list_picked)

        # 将DataFrame写入Excel文件
        df.to_excel(self.fpath, index=False, sheet_name=__sheet_name)

        return df.shape[0]

    ###
    def set_xlist(self):
        # xlist = [
        #     (path, loh, name_fr),
        #     (path, loh, name_fr),
        # ]

        def __set_xlist_child(node, pathx, lohx=1):
            # print(f"node = {node}, loh={lohx}, path={pathx}")
            pathx = pathx + "." + self.tree[node]["pinyin"]
            docx = []
            docx = [
                pathx,
                lohx,
                self.tree[node]["pinyin"],
                self.tree[node]["name_fr"],
                node,
            ]
            self.xlist.append(docx)

            # if self.tree[node]["h_psn"] != FV_ROOT_NODE:
            # pathx = pathx + "." + self.tree[node]["pinyin"]
            for child in self.tree[node]["child_node"]:
                # print(f"child = {child}, loh={lohx + 1}, path={pathx}")
                __set_xlist_child(child, pathx, lohx + 1)

            return

        self.xlist = []  # clear
        count = 0
        for k, v in self.tree.items():  # k = usn
            if v["h_psn"] != FV_ROOT_NODE:
                continue
            __set_xlist_child(k, "", lohx=1)
            # count += 1
            # if count >= 100:
            #     break
        return

    ###
    def sort_xlist(self):
        # sort by : path,loh,name_fr
        # self.xlist = sorted(self.xlist, key=lambda x: (x[0]，x[1]))
        self.xlist = sorted(self.xlist, key=lambda x: (x[0],))
        return

    ###
    # def set_xlist(self):

    #     __sorted_list = self.xlist[:]  # make a copy
    #     self.xlist = []

    #     for item in __sorted_list:
    #         usn = item[0]
    #         name_fr = item[1]

    #         self.__set_xlist_child(usn, lohx=1)

    #     return

    # def __set_xlist_child(self, node, lohx=1):

    #     docx = []
    #     docx = [
    #         self.tree[node]["name_fr"],
    #         node,
    #         lohx,
    #         self.tree[node]["h_psn"],
    #         self.tree[node]["root_node"]["usn"],
    #     ]
    #     self.xlist.append(docx)

    #     for child in self.tree[node]["child_node"]:
    #         self.__set_xlist_child(child, lohx + 1)

    #     return

    # ###
    # def setf_loh(self):

    #     for k, v in self.tree.items():
    #         if v["h_psn"] != FV_ROOT_NODE:
    #             continue
    #         self.__setf_loh_child(k)
    #     return

    # def __setf_loh_child(self, node, lohx=1):
    #     self.tree[node]["loh"] = lohx

    #     for child in self.tree[node]["child_node"]:
    #         self.__setf_loh_child(child, lohx + 1)
    #     return

    # ###
    def setf_pinyin(self):
        for k, v in self.tree.items():
            d2lists = pinyin(v["name_fr"], style=Style.NORMAL)  # 2 dimension list
            flat_list = [item for sublist in d2lists for item in sublist]
            v["pinyin"] = "".join(flat_list)

            # d2lists = pinyin(
            #     v["root_node"]["name_fr"], style=Style.NORMAL
            # )  # 2 dimension list
            # flat_list = [item for sublist in d2lists for item in sublist]
            # v["root_node"]["pinyin"] = "".join(flat_list)
        return

    # ###

    # def sort_xlist(self):

    #     def sort_key(item):
    #         usn, name_fr, name_fr_root, loh, pinyin_root = item
    #         # 将公司名字转换为拼音，并拼接成一个字符串（不带声调）
    #         # pinyin_root = "".join(pinyin(name_fr_root, style=Style.NORMAL))
    #         return (pinyin_root, loh)

    #     self.xlist = []
    #     for k, v in self.tree.items():
    #         if v["h_psn"] != FV_ROOT_NODE:
    #             continue
    #         docx = []
    #         docx.append(k)
    #         docx.append(v["name_fr"])
    #         docx.append(v["root_node"]["name_fr"])
    #         docx.append(v["loh"])
    #         docx.append(v["root_node"]["pinyin"])

    #         self.xlist.append(docx)

    #     # 使用sorted()函数进行排序，并指定key参数
    #     self.xlist = sorted(self.xlist, key=sort_key)

    #     return

    @property
    def fpath(self):
        return self.__fpath

    @property
    def year(self):
        return self.__year

    @property
    def sheet(self):
        return self.__sheet


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class AddOrgBizReg(AddOrganization):

    ###
    def __init__(self):

        arg = {
            "coll": "org_bizreg",
            "map_db2t": {
                "name_fr": {"source": "name_fr", "key": True},
                "usn": {"source": "usn"},
                "category": {"source": "category"},  # post to 'org_asis2b'
                "usci_org": {"source": "usci_org"},  # post to 'org_asis2b'
            },
        }

        self.__map_fp2t = {
            # source: file, destination : tree in memory
            # key : name_fr
            "name_fr": {"source": "企业名称", "format": "str", "key": True},
            # "organization_fr": {"source": "企业名称", "format": "str"},
            "usci_org": {"source": "统一社会信用代码", "format": "str"},
            "tmp0001": {"source": "法定代表人", "format": "str"},
            "est_date": {"source": "成立日期", "format": "str"},
            "operation_status": {"source": "经营状态", "format": "str"},
            "tmp0002": {"source": "注册资本", "format": "str"},
            "location_org": {"source": "行政区划分", "format": "str"},
            "type1_org": {"source": "企业类型", "format": "str"},
            "tmp0003": {"source": "实缴资本", "format": "str"},
            "rn4b": {"source": "工商注册号", "format": "str"},
            "industry_org": {"source": "所属行业", "format": "str"},
            "tin": {"source": "纳税人识别号", "format": "str"},
            "code_org": {"source": "组织机构代码", "format": "str"},
            "tmp0004": {"source": "纳税人资质", "format": "str"},
            "tmp0005": {"source": "营业期限", "format": "str"},
            "tmp0006": {"source": "核准日期", "format": "str"},
            "reg_authority": {"source": "登记机关", "format": "str"},
            "tmp0007": {"source": "参保人数", "format": "str"},
            "former_name": {"source": "曾用名", "format": "str"},
            "address_org": {"source": "注册地址", "format": "str"},
            "business_scope": {"source": "经营范围", "format": "str"},
            # "usn": "xx",
            # "parent_node": {
            #     "name_fr": "xx",
            #     "usn": "xx",
            # },
        }

        super().__init__(arg)
        return

    ###
    def get_flist(self, folder):
        file_key = "工商注册信息.xls"
        super().get_flist(folder, file_key)
        return

    ###
    def load_fp(self):

        count_fp = 0
        if len(self.flist) == 0:
            return count_fp

        self.tree = {}
        for fp in self.flist:

            org = {}
            df = pd.read_excel(fp)
            row_list = df.to_dict(orient="records")
            for row in row_list:
                for __col_index in range(0, 4):  # 'Unnamed: 0' - 'Unnamed: 3'

                    k = row[f"Unnamed: {__col_index}"]
                    if type(k) == str:
                        if k.startswith("下载数据日期"):
                            dt = re.findall(
                                r"\d{4}-(?:0[1-9]|1[0-2]|[1-9])-(?:0[1-9]|[12][0-9]|3[01]|[1-9])",
                                k,
                            )
                            org["aqc_download_date"] = dt[0]

                    for kmap, vmap in self.map_fp2t.items():
                        if vmap["source"] == k:
                            # get value of next column, based on content of xls file.
                            __v_dst = self.map_v2v(
                                row[f"Unnamed: {__col_index + 1}"],
                                vmap.get("format", None),
                            )
                            if vmap.get("key", False) == True:
                                __k_dst = __v_dst
                            else:
                                org[kmap] = __v_dst

            count_fp += 1
            self.tree.update({__k_dst: org})

        return count_fp

    ### set 'category' after load file
    def setf_category(self):

        category_unknown = {}
        for k, v in self.tree.items():
            if v["type1_org"] == "-":
                if "事业单位" in v["reg_authority"]:
                    v["category"] = "pin"  # set as 事业单位
                elif "未公示" in v["reg_authority"]:
                    v["category"] = "gpo"  # set as 党政机关
                else:
                    category_unknown[k] = {}
            elif "公司" in v["type1_org"]:
                v["category"] = "ent"  # set as 企业
            else:
                category_unknown[k] = {}

        return category_unknown

    @property
    def map_fp2t(self):
        return self.__map_fp2t

    ###
    def setf_parent_node(self, treex):

        # treex = org_pchild.tree
        # treex = {
        #     "name_fr": {"usn": "xx", "parent_node": {"usn": "xx", "name_fr": "xx"}}
        # }

        # copy 'parent_node' from 'org_pchild'
        for k, v in self.tree.items():
            v["parent_node"] = {}
            if k in treex:
                v["parent_node"] = treex[k]["parent_node"]

        # set 'parent_node.usn'
        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            if v["parent_node"]["usn"] == FV_NOT_FOUND:
                v["parent_node"]["usn"] = self.tree[v["parent_node"]["name_fr"]]["usn"]

        # copy 'h_psn' from 'parent_node.usn'
        for k, v in self.tree.items():
            v["h_psn"] = FV_ROOT_NODE
            if v["parent_node"] != {}:
                v["h_psn"] = v["parent_node"]["usn"]

        __pnode_usn_not_found = {}
        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            if v["parent_node"]["usn"] == FV_NOT_FOUND:
                __pnode_usn_not_found[k] = {
                    "usn": v["usn"],
                    "parent_node.name_fr": v["parent_node"]["name-fr"],
                }

        return __pnode_usn_not_found

    ###
    def setf_usn4nf(self, max_usn36):

        max_usn10 = int(max_usn36, base=36)

        for k, v in self.tree.items():
            if v["usn"] == FV_NOT_FOUND:
                max_usn10 += 1
                __usn36 = int_to_base36(max_usn10).lower()
                v["usn"] = __usn36

        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class AddOrgPChild(AddOrganization):

    ###
    def __init__(self):

        arg = {
            "coll": "org_pchild",
            "map_db2t": {
                "name_fr": {"source": "name_fr", "key": True},
                "usn": {"source": "usn"},
                "parent_node": {"source": "parent_node"},
            },
        }

        super().__init__(arg)
        return

    ###
    def get_flist(self, folder):
        file_key = "股东信息.xls"
        super().get_flist(folder, file_key)
        return

    ###
    def load_fp(self):

        count_fp = 0
        if len(self.flist) == 0:
            return count_fp

        self.tree = {}
        for fp in self.flist:

            __v_dst = {}
            __v_dst["parent_node"] = {}
            __v_dst["parent_node"]["usn"] = ""
            fn = fp.name
            __org = fn[len("【爱企查】") : -len("股东信息.xls")]

            df = pd.read_excel(fp)
            rows = df.to_dict(orient="records")
            for row in rows:

                # 'Unnamed: 0' - 'Unnamed: 4', only focus 0 & 1
                for __col_index in range(0, 1):

                    k = row[f"Unnamed: {__col_index}"]
                    if type(k) == str:
                        if k.startswith("下载数据日期"):
                            dt = re.findall(
                                r"\d{4}-(?:0[1-9]|1[0-2]|[1-9])-(?:0[1-9]|[12][0-9]|3[01]|[1-9])",
                                k,
                            )
                            __v_dst["aqc_download_date"] = dt[0]

                    if type(k) == str:
                        if k == "1":
                            __v_dst["parent_node"]["name_fr"] = row[
                                f"Unnamed: {__col_index + 1}"
                            ]

            count_fp += 1
            self.tree.update({__org: __v_dst})

        return count_fp

    ###
    def setf_usn(self, treex):

        __not_found = 0
        for k, v in self.tree.items():

            if k in treex:
                v["usn"] = treex[k]["usn"]
            else:
                v["usn"] = FV_NOT_FOUND
                __not_found += 1

            __name_pnode = v["parent_node"]["name_fr"]
            if __name_pnode in treex:
                v["parent_node"]["usn"] = treex[__name_pnode]["usn"]
            else:
                v["parent_node"]["usn"] = FV_NOT_FOUND
                __not_found += 1

        return __not_found


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class AddOrgAsIs2b(AddOrganization):

    ###
    def __init__(self):

        arg = {
            "coll": "org_asis2b",
            "map_db2t": {  # load and insert into 'organization'
                "name_fr": {"source": "name_fr", "key": True},
                "usn": {"source": "usn"},
                "category": {"source": "category"},
                "usci_org": {"source": "usci_org"},
                "h_psn": {"source": "h_psn"},
                "parent_node": {"source": "parent_node"},
            },
        }

        super().__init__(arg)
        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class OrganizationFR2B:

    ###
    def __init__(self):

        self.__coll_br = "org_fr_br"  # business-registration (from aiqicha.com)
        self.__coll_pc = "org_fr_pc"  # parent-child (from aichqi.com)
        self.__coll_2b = "org_fr_2b"  # to be merged into 'organization'
        self.__coll = "organization"

        # data of tree is from file
        self.__fptree_br = []  # [{k1:v11,k2:v12,...}, {k1:v21,k2:v22,...}, ...]
        self.__fptree_pc = []
        # self.__tree_2b = []

        # data of tree is from db
        self.__dbtree_br = {}
        self.__dbtree_pc = {}
        self.__dbtree_2b = {}
        self.__dbtree = {}

        # tree for checking if data is same as that in db.
        self.__tchk_br = {}  # {k1:v1, k2:v2, ...}
        self.__tchk_pc = {}
        self.__tchk_2b = {}
        self.__tchk = {}  # 'organization'

        self.__flist = {}  # {path-to-file:type}

        # mapping : file to db, source = file
        self.__f2db_br = {
            "name_fr": {"source": "企业名称", "format": "str"},
            # "organization_fr": {"source": "企业名称", "format": "str"},
            "usci_org": {"source": "统一社会信用代码", "format": "str"},
            "tmp0001": {"source": "法定代表人", "format": "str"},
            "est_date": {"source": "成立日期", "format": "str"},
            "operation_status": {"source": "经营状态", "format": "str"},
            "tmp0002": {"source": "注册资本", "format": "str"},
            "location_org": {"source": "行政区划分", "format": "str"},
            "type1_org": {"source": "企业类型", "format": "str"},
            "tmp0003": {"source": "实缴资本", "format": "str"},
            "rn4b": {"source": "工商注册号", "format": "str"},
            "industry_org": {"source": "所属行业", "format": "str"},
            "tin": {"source": "纳税人识别号", "format": "str"},
            "code_org": {"source": "组织机构代码", "format": "str"},
            "tmp0004": {"source": "纳税人资质", "format": "str"},
            "tmp0005": {"source": "营业期限", "format": "str"},
            "tmp0006": {"source": "核准日期", "format": "str"},
            "reg_authority": {"source": "登记机关", "format": "str"},
            "tmp0007": {"source": "参保人数", "format": "str"},
            "former_name": {"source": "曾用名", "format": "str"},
            "address_org": {"source": "注册地址", "format": "str"},
            "business_scope": {"source": "经营范围", "format": "str"},
            # "usn": "xx",
            # "parent_node": {
            #     "name_fr": "xx",
            #     "usn": "xx",
            # },
        }

        self.__db2tchk_br = {
            # source is db
            "name_fr": {"source": "name_fr", "key": True},
            "category": {"source": "category"},
            "usn": {"source": "usn"},
        }

        self.__db2tchk_pc = {
            # source is db
            "name_fr": {"source": "name_fr", "key": True},
            "name_fr_pnode": {"source": "name_fr_pnode"},
        }

        # organization
        self.__db2tchk = {
            # source is db
            "name_fr": {"source": "name_fr", "key": True},
            "usn": {"source": "usn"},
        }

        # org_fr_2b
        self.__map_db2tree_2b = {
            # source is db
            "name_fr": {"source": "name_fr", "key": True},
            "usn": {"source": "usn"},
            "h_psn": {"source": "h_psn"},
            "parent_node": {"source": "parent_node"},
        }

        # org_fr_2b :: to insert into 'organization'
        self.__map_db2tins_2b = {
            # "name_fr": {"source": "name_fr", "key": True},
            "name_fr": {"source": "name_fr"},
            "name": {"source": "name"},
            "usn": {"source": "usn"},
            "category": {"source": "category"},
            "usci_org": {"source": "usci_org", "format": "str"},
            "h_psn": {"source": "h_psn"},
        }

        self.__map_db2tree_pc = {
            # source is db
            "name_fr": {"source": "name_fr", "key": True},
            "name_fr_pnode": {"source": "name_fr_pnode"},
        }

        return

    ### get list of files in folder, and set file type
    def get_flist(self, folder):

        if folder == "":
            raise ValueError("Folder name is empty. Fix it and try again.")

        folder_path = Path(folder)
        __file_not_unknown = []

        for fp in folder_path.iterdir():

            if fp.is_file() == False:
                __file_not_unknown.append(fp.name)
                continue

            if "工商注册信息.xls" in fp.name:
                self.__flist[fp] = "business-registration"
            elif "股东信息.xls" in fp.name:
                self.__flist[fp] = "parent-child"
            else:
                __file_not_unknown.append(fp.name)
                continue

        return __file_not_unknown

    ### load from db to create tree for checking if data is same as that in db.
    def load_db4chk_br(self):
        self.tchk_br = self.__load_dbx(self.coll_br, self.db2tchk_br)
        return

    def load_db4chk_pc(self):
        self.tchk_pc = self.__load_dbx(self.coll_pc, self.db2tchk_pc)
        return

    def load_db4chk_2b(self):
        self.tchk_2b = self.__load_dbx(self.coll_2b, self.map_db2tree_2b)
        return

    def load_db_br(self):
        self.dbtree_br = self.__load_dbx(self.coll_br, self.db2tchk_br)
        return

    def load_db_pc(self):
        self.dbtree_pc = self.__load_dbx(self.coll_pc, self.map_db2tree_pc)
        return

    def load_db_2b(self):
        self.dbtree_2b = self.__load_dbx(self.coll_2b, self.map_db2tree_2b)
        return

    # load from 'organization'
    def load_db(self):
        self.dbtree = self.__load_dbx(self.coll, self.db2tchk)
        return

    def __load_dbx(self, collx, mapx):

        client = MongoClient()
        db = client.get_database("remai")

        __has_key = False
        for k, v in mapx.items():
            if v.get("key", False) == True:
                __has_key = True
        if __has_key == True:
            treex = {}  # clear
        else:
            treex = []

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(collx).find(query, projection)
        for docx in cursor:
            # print(f"docx={docx}")

            __tmp_k = ""
            __tmp_v = {}
            for kmap, vmap in mapx.items():

                __value_db = docx.get(vmap["source"], None)
                __type_v = type(__value_db)
                if vmap.get("key", False) == True:  # it is key
                    match vmap.get("format", None):
                        case "int" if __type_v == int:
                            __tmp_k = __value_db
                        case "int" if __type_v == float:
                            __tmp_k = int(round(__value_db, 0))
                        case "int" if __type_v == str:
                            __tmp_k = int(__value_db)
                        case "float" if __type_v == float:
                            __tmp_k = __value_db
                        case "float" if __type_v == str:
                            __tmp_k = float(__value_db)
                        case "str":
                            __tmp_k = f"{__value_db}"
                        case None:
                            __tmp_k = __value_db
                else:
                    match vmap.get("format", None):
                        case "int" if __type_v == int:
                            __tmp_v[kmap] = __value_db
                        case "int" if __type_v == float:
                            __tmp_v[kmap] = int(round(__value_db, 0))
                        case "int" if __type_v == str:
                            __tmp_v[kmap] = int(__value_db)
                        case "float" if __type_v == float:
                            __tmp_v[kmap] = __value_db
                        case "float" if __type_v == str:
                            __tmp_v[kmap] = float(__value_db)
                        case "str":
                            __tmp_v[kmap] = f"{__value_db}"
                        case None:
                            __tmp_v[kmap] = __value_db
                        case "str":
                            __tmp_v[kmap] = f"{__value_db}"
                        case None:
                            __tmp_v[kmap] = __value_db

            if __has_key == True:
                treex.update({__tmp_k: __tmp_v})
            else:
                treex.append(__tmp_v)
            # if no key , then self.tree_db = [] and append(tmp_v)
            # self.tree[docx["organization_fr"]] = {
            #     "usn": docx.get("usn", ""),
            #     "category": docx.get("category", ""),
            #     "doc_ops": docx.get("doc_ops", {}),
            # }

        # for k, v in treex.items():
        #     print(f"load_db2 :: k={k}, v={v}")

        # cursor.close()
        client.close()

        return treex

    ### insert into db

    # insert into 'org_fr_br': data from excel file, business-registration
    def insert2db_br(self):
        result = self.__insert2dbx(
            self.coll_br,  # collection to insert
            self.fptree_br,  # tree have data to insert
            self.tchk_br,  # tree for checking if data is same as that in db.
            self.db2tchk_br,  # map for tchk_br (db to tree)
        )

        self.tchk_br = {}  # clear

        return result

    # insert into 'org_fr_pc': data from excel file, parent-child
    def insert2db_pc(self):
        result = self.__insert2dbx(
            self.coll_pc,  # collection to insert
            self.fptree_pc,  # tree have data to insert
            self.tchk_pc,  # tree for checking if data is same as that in db.
            self.db2tchk_pc,  # map for tchk_br (db to tree)
        )

        self.tchk_pc = {}  # clear
        return result

    def __insert2dbx(self, collx, treex, tchkx, mapx):

        client = MongoClient()
        db = client.get_database("remai")

        key_field = ""
        for k, v in mapx.items():
            if v.get("key", False) == True:
                key_field = v["source"]
        # print(f">>>key_field = {key_field}")

        ops4db = []
        for docx in treex:
            # print(f">>>doc={docx}")
            if docx[key_field] in tchkx:  # same as that in db.
                continue

            doc2db = {}
            count = 1
            for k, v in docx.items():
                # if k.startswith("tmp000"):
                #     continue
                doc2db[k] = v

            doc2db["doc_ops"] = {}
            doc2db["doc_ops"]["update_by"] = "be-gdv2"
            doc2db["doc_ops"]["create_by"] = "be-gdv2"
            doc2db["doc_ops"]["update_time"] = datetime.now()
            doc2db["doc_ops"]["create_time"] = datetime.now()

            ops4db.append(InsertOne(doc2db))

        if len(ops4db) > 0:
            db_result = db.get_collection(collx).bulk_write(ops4db)
            result = {"ops4db": True, "result": db_result}
        else:
            result = {"ops4db": False, "result": None}

        client.close()
        return result

    ### update to db
    def update2db_br(self):

        result = self.__update2dbx(
            self.coll_br, self.dbtree_br, self.tchk_br, self.db2tchk_br
        )

        self.tchk_br = {}  # clear
        return result

    def update2db_2b(self):

        result = self.__update2dbx(
            self.coll_2b, self.dbtree_2b, self.tchk_2b, self.map_db2tree_2b
        )

        self.tchk_2b = {}  # clear
        return result

    def __update2dbx(self, collx, treex, tchkx, mapx):
        # collx : colletion to update
        # treex : {}, data to update to db
        # tchkx : data from collx to check if data is same as that in db.
        # mapx  : db map to tree. get key_field used in update
        client = MongoClient()
        db = client.get_database("remai")

        # key_field = ""
        # for k, v in mapx.items():
        #     if v.get("key", False) == True:
        #         key_field = v["source"]
        # print(f"key_field = {key_field}")

        ops4db = []
        for kx, vx in treex.items():

            __identical = 0
            for kmap, vmap in mapx.items():
                if (vmap.get("key", False) == True) and (kx in tchkx):
                    __identical += 1
                    continue
                if tchkx[kx][vmap["source"]] == vx[kmap]:
                    __identical += 1
            # print(f"__identical={__identical}")
            if __identical == len(mapx):
                continue  # data of treex is same as that in db, continue.

            query = {}
            doc2db = {"$set": {}}
            count = 1
            for k, v in mapx.items():
                if v.get("key", False) == True:
                    query[v["source"]] = kx
                else:
                    doc2db["$set"][v["source"]] = vx[k]

            doc2db["$set"]["doc_ops.update_by"] = "be-gdv2"
            doc2db["$set"]["doc_ops.update_time"] = datetime.now()

            ops4db.append(UpdateOne(query, doc2db))

        if len(ops4db) > 0:
            db_result = db.get_collection(collx).bulk_write(ops4db)
            result = {"ops4db": True, "result": db_result}
        else:
            result = {"ops4db": False, "result": None}

        client.close()
        return result

    ### load from excel (.xls) file from aiqicha.com
    def load_fp(self):

        count_fp = {"business-registration": 0, "parent-child": 0}

        if len(self.flist) == 0:
            return count_fp

        for fp, ftype in self.flist.items():

            if ftype == "business-registration":
                df = pd.read_excel(fp)
                tmp = df.to_dict(orient="records")
                self.__loadf_br(tmp)

            elif ftype == "parent-child":
                fn = fp.name
                # print(f'org = [{fn[len("【爱企查】"):-len("股东信息.xls")]}]')
                org = fn[len("【爱企查】") : -len("股东信息.xls")]
                df = pd.read_excel(fp)
                tmp = df.to_dict(orient="records")
                self.__loadf_pc(tmp, org)

            else:
                raise ValueError(
                    "Wrong file type. 'business-registration' or 'parent-child' expected."
                )

            count_fp[ftype] += 1

        return count_fp

    ### load from excel (.xls) file from aiqicha.com, business-registration
    def __loadf_br(self, list_xls):

        org = {}
        for xls in list_xls:
            for __colindex in range(0, 4):  # 'Unnamed: 0' - 'Unnamed: 3'
                k = xls[f"Unnamed: {__colindex}"]

                if type(k) == str:
                    if k.startswith("下载数据日期"):
                        org["aqc_download_date"] = re.findall(
                            r"\d{4}-(?:0[1-9]|1[0-2]|[1-9])-(?:0[1-9]|[12][0-9]|3[01]|[1-9])",
                            k,
                        )[0]

                for kmap, vmap in self.f2db_br.items():
                    if vmap["source"] == k:
                        # get value of next column
                        org[kmap] = xls[f"Unnamed: {__colindex + 1}"]

        self.fptree_br.append(org)
        return

    ### load from excel (.xls) file from aiqicha.com, parent-child
    def __loadf_pc(self, list_xls, orgx):

        org = {}
        for xls in list_xls:
            for __colindex in range(
                0, 1
            ):  # 'Unnamed: 0' - 'Unnamed: 4', only focus 0 & 1
                k = xls[f"Unnamed: {__colindex}"]

                if type(k) == str:
                    if k.startswith("下载数据日期"):
                        aqc_download_date = re.findall(
                            r"\d{4}-(?:0[1-9]|1[0-2]|[1-9])-(?:0[1-9]|[12][0-9]|3[01]|[1-9])",
                            k,
                        )[0]

                if type(k) == str:
                    if k == "1":
                        shareholder = xls[f"Unnamed: {__colindex + 1}"]

        org["name_fr"] = orgx
        org["aqc_download_date"] = aqc_download_date
        org["name_fr_pnode"] = shareholder
        self.fptree_pc.append(org)

        return

    ###
    def load_db_2b2org(self):

        srcx = {"coll": self.coll_2b, "map_db2tree": self.map_db2tins_2b}
        dstx = {"coll": self.coll, "map_db2tree": self.db2tchk}

        srcx_tree, dstx_tree, result = self.__load_db2dbx(srcx, dstx)

        return srcx_tree, dstx_tree, result

    def __load_db2dbx(self, srcx, dstx):

        # destx = {"coll": "xx", "map": "xx"}

        srcx_tree = self.__load_dbx(srcx["coll"], srcx["map_db2tree"])
        dstx_tree = self.__load_dbx(
            dstx["coll"], dstx["map_db2tree"]
        )  # duplicate check

        # count = 1
        # for docx in srcx_tree:
        #     print(f">>> #{count} :: docx = {docx}")
        #     count += 1
        result = self.__insert2dbx(
            dstx["coll"], srcx_tree, dstx_tree, dstx["map_db2tree"]
        )

        return srcx_tree, dstx_tree, result

    ### 'org_fr_br' :: set category of organization
    def setf_category(self):

        category_unknown = {}
        for org in self.fptree_br:
            if org["type1_org"] == "-":
                if "事业单位" in org["reg_authority"]:
                    org["category"] = "pin"  # set as 事业单位
                elif "未公示" in org["reg_authority"]:
                    org["category"] = "gpo"  # set as 党政机关
                else:
                    category_unknown[org["name_fr"]] = {}
            elif "公司" in org["type1_org"]:
                org["category"] = "ent"  # set as 企业
            else:
                category_unknown[org["name_fr"]] = {}

        return category_unknown

    ### 'org_fr_br':: set 'usn'
    def setf_usn(self):

        for __name_fr, v in self.dbtree_br.items():  # name_fr in 'org_fr_br'
            if __name_fr in self.dbtree:  # if in 'organization'
                v["usn"] = self.dbtree[__name_fr]["usn"]
            else:
                v["usn"] = FV_NOT_FOUND

        return

    ### 'org_fr_2b' :: set 'parent_node' / 'h_psn', based on 'org_fr_pc'
    def setf_parent_node(self):

        # set 'parent_node.name_fr'
        for __name_fr, v_2b in self.dbtree_2b.items():  # name_fr in 'org_fr_2b'
            if __name_fr in self.dbtree_pc:  # if in 'org_fr_pc'
                v_2b["parent_node"] = {}
                v_2b["parent_node"]["usn"] = ""
                v_2b["parent_node"]["name_fr"] = self.dbtree_pc[__name_fr][
                    "name_fr_pnode"
                ]
            else:
                v_2b["parent_node"] = {}  # clear

        # set 'parent_node.usn', based on self ('org_fr_2b')
        for __name_fr, v_2b in self.dbtree_2b.items():
            # print(f"v_2b={v_2b}")
            if v_2b.get("parent_node", {}) == {}:
                continue
            __name_fr_pnode = v_2b["parent_node"]["name_fr"]
            # if __name_fr_pnode == None:
            #     continue
            if __name_fr_pnode in self.dbtree_2b:
                v_2b["parent_node"]["usn"] = self.dbtree_2b[__name_fr_pnode]["usn"]

        # set 'parent_node.usn', based on 'organizaiton'
        count = 0
        for k, v in self.dbtree_2b.items():
            print(
                f">>>k={k}, p.usn='{v['parent_node'].get('usn')}', parent_node={v['parent_node']}"
            )
            # print(f">>> v={v}")
            if v["parent_node"] == {}:
                continue
            if v["parent_node"]["usn"] == "":
                count += 1
        if count == 0:
            print(f"{self.coll_2b} :: 'parent_node.usn' all set.")
        else:
            # set 'parent_node.usn', based on 'organizaiton'
            logging.warning(f"{self.coll_2b} :: {count} 'parent_node.usn' not set.")
            pass

        # set 'h_psn'
        for k, v in self.dbtree_2b.items():
            if v.get("parent_node", {}) == {}:
                v["parent_node"] = {}
                v["h_psn"] = FV_ROOT_NODE
            else:
                v["h_psn"] = v["parent_node"]["usn"]

        return

    ### collection name

    @property
    def coll_br(self):
        return self.__coll_br

    @property
    def coll_pc(self):
        return self.__coll_pc

    @property
    def coll_2b(self):
        return self.__coll_2b

    @property
    def coll(self):
        return self.__coll

    ### tree, data of that from file or db

    @property
    def fptree_br(self):
        return self.__fptree_br

    @property
    def fptree_pc(self):
        return self.__fptree_pc

    @property
    def dbtree_br(self):
        return self.__dbtree_br

    @dbtree_br.setter
    def dbtree_br(self, t):
        self.__dbtree_br = t
        return

    @property
    def dbtree_pc(self):
        return self.__dbtree_pc

    @dbtree_pc.setter
    def dbtree_pc(self, t):
        self.__dbtree_pc = t
        return

    @property
    def dbtree_2b(self):
        return self.__dbtree_2b

    @dbtree_2b.setter
    def dbtree_2b(self, t):
        self.__dbtree_2b = t
        return

    @property
    def dbtree(self):
        return self.__dbtree

    @dbtree.setter
    def dbtree(self, t):
        self.__dbtree = t
        return

    ### tree for checking if data is same as that in db.

    @property
    def tchk_br(self):
        return self.__tchk_br

    @tchk_br.setter
    def tchk_br(self, t):
        self.__tchk_br = t
        return

    @property
    def tchk_pc(self):
        return self.__tchk_pc

    @tchk_pc.setter
    def tchk_pc(self, t):
        self.__tchk_pc = t
        return

    @property
    def tchk_2b(self):
        return self.__tchk_2b

    @tchk_2b.setter
    def tchk_2b(self, t):
        self.__tchk_2b = t
        return

    @property
    def tchk(self):
        return self.__tchk

    @tchk.setter
    def tchk(self, t):
        self.__tchk = t
        return

    @property
    def flist(self):
        return self.__flist

    ### mapping

    @property
    def f2db_br(self):
        return self.__f2db_br

    @property
    def db2tchk_br(self):
        return self.__db2tchk_br

    @property
    def db2tchk_pc(self):
        return self.__db2tchk_pc

    @property
    def db2tchk(self):
        return self.__db2tchk

    @property
    def map_db2tree_2b(self):
        return self.__map_db2tree_2b

    @property
    def map_db2tins_2b(self):
        return self.__map_db2tins_2b

    @property
    def map_db2tree_pc(self):
        return self.__map_db2tree_pc


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class OrganizationVer:

    ###
    def __init__(self, yearx=""):
        self.__coll = "org_fr_xls"
        self.__coll_h = "org_fr_h"
        self.__coll_fr = "org_fr"
        self.__fpath = ""
        self.__sheet = ""
        self.__size = 0
        self.__flist = {}
        self.__tree_db = {}
        self.__tree_db_h = {}
        self.__tree = []
        self.__tree_p = {}
        self.__tree_h = []
        self.__tree_fr = []
        self.__db_map_f = {
            "name_fr": {"file": "企业名称", "format": "str"},
            # "organization_fr": {"file": "企业名称", "format": "str"},
            "usci_org": {"file": "统一社会信用代码", "format": "str"},
            "tmp0001": {"file": "法定代表人", "format": "str"},
            "est_date": {"file": "成立日期", "format": "str"},
            "operation_status": {"file": "经营状态", "format": "str"},
            "tmp0002": {"file": "注册资本", "format": "str"},
            "location_org": {"file": "行政区划分", "format": "str"},
            "type1_org": {"file": "企业类型", "format": "str"},
            "tmp0003": {"file": "实缴资本", "format": "str"},
            "rn4b": {"file": "工商注册号", "format": "str"},
            "industry_org": {"file": "所属行业", "format": "str"},
            "tin": {"file": "纳税人识别号", "format": "str"},
            "code_org": {"file": "组织机构代码", "format": "str"},
            "tmp0004": {"file": "纳税人资质", "format": "str"},
            "tmp0005": {"file": "营业期限", "format": "str"},
            "tmp0006": {"file": "核准日期", "format": "str"},
            "reg_authority": {"file": "登记机关", "format": "str"},
            "tmp0007": {"file": "参保人数", "format": "str"},
            "former_name": {"file": "曾用名", "format": "str"},
            "address_org": {"file": "注册地址", "format": "str"},
            "business_scope": {"file": "经营范围", "format": "str"},
            # "usn": "xx",
            # "parent_node": {
            #     "name_fr": "xx",
            #     "usn": "xx",
            # },
        }

        self.__db2tree = {
            # source is db
            "name_fr": {"source": "name_fr", "key": True},
            "category": {"source": "category"},
        }

        self.__db2tree_h = {
            # source is db
            "name_fr": {"source": "name_fr", "key": True},
            "name_fr_pnode": {"source": "name_fr_pnode"},
        }

        return

    ### get file list and set file type
    def get_flist(self, fp):

        if fp == "":
            raise ValueError("Folder name is empty. Fix it and try again.")

        folder_path = Path(fp)
        __file_not_unknown = []

        for fp in folder_path.iterdir():

            if fp.is_file() == False:
                __file_not_unknown.append(fp.name)
                continue

            if "工商注册信息.xls" in fp.name:
                self.__flist[fp] = "business-registration"
                # print(f">>>>>> 工商注册文件: {fn}")
            elif "股东信息.xls" in fp.name:
                self.__flist[fp] = "parent-shareholder"
                # print(f"****** 股东信息文件: {fn}")
            else:
                __file_not_unknown.append(fp.name)
                # print(f"------ unknown file: {fn}")
                continue

        return __file_not_unknown

    ### load from excel (.xls) file from aiqicha.com
    def load_fp(self):

        count_fp = {"business-registration": 0, "parent-shareholder": 0}

        if len(self.flist) == 0:
            return count_fp

        for fp, ftype in self.flist.items():

            if ftype == "business-registration":
                df = pd.read_excel(fp)
                tmp = df.to_dict(orient="records")
                self.__load_fbz(tmp)
                count_fp["business-registration"] += 1

            elif ftype == "parent-shareholder":
                fn = fp.name
                # print(f'org = [{fn[len("【爱企查】"):-len("股东信息.xls")]}]')
                org = fn[len("【爱企查】") : -len("股东信息.xls")]
                df = pd.read_excel(fp)
                tmp = df.to_dict(orient="records")
                self.__load_fsh(tmp, org)
                count_fp["parent-shareholder"] += 1

        # self.__setf_parent_node()
        result = self.__setf_category()
        if len(result) > 0:
            logging.warning(
                f"category is not set for {len(result)} organizations : \n{result}"
            )

        return count_fp

    ### load from excel (.xls) file from aiqicha.com, business-registration
    def __load_fbz(self, list_xls):

        org = {}
        for xls in list_xls:
            for __colindex in range(0, 4):  # 'Unnamed: 0' - 'Unnamed: 3'
                k = xls[f"Unnamed: {__colindex}"]

                if type(k) == str:
                    if k.startswith("下载数据日期"):
                        org["aqc_download_date"] = re.findall(
                            r"\d{4}-(?:0[1-9]|1[0-2]|[1-9])-(?:0[1-9]|[12][0-9]|3[01]|[1-9])",
                            k,
                        )[0]

                for kmap, vmap in self.db_map_f.items():
                    if vmap["file"] == k:
                        # get value of next column
                        org[kmap] = xls[f"Unnamed: {__colindex + 1}"]

        self.tree.append(org)
        return

    ### load from excel (.xls) file from aiqicha.com, parent-shareholder
    def __load_fsh(self, list_xls, orgx):
        org = {}
        for xls in list_xls:
            for __colindex in range(0, 1):  # 'Unnamed: 0' - 'Unnamed: 4'
                k = xls[f"Unnamed: {__colindex}"]

                if type(k) == str:
                    if k.startswith("下载数据日期"):
                        aqc_download_date = re.findall(
                            r"\d{4}-(?:0[1-9]|1[0-2]|[1-9])-(?:0[1-9]|[12][0-9]|3[01]|[1-9])",
                            k,
                        )[0]

                if type(k) == str:
                    if k == "1":
                        shareholder = xls[f"Unnamed: {__colindex + 1}"]

        # self.tree_p[orgx] = {}
        # self.tree_p[orgx]["name_fr_pnode"] = shareholder
        # self.tree_p[orgx]["aqc_download_date"] = aqc_download_date

        org["name_fr"] = orgx
        org["aqc_download_date"] = aqc_download_date
        org["name_fr_pnode"] = shareholder
        self.tree_h.append(org)

        return

    ###
    def __setf_parent_node(self):

        for docx in self.tree:

            if docx["name_fr"] in self.tree_p:
                docx["parent_node"] = {}
                docx["parent_node"]["name_fr"] = self.tree_p.get(docx["name_fr"]).get(
                    "name_fr_pnode"
                )

        return

    ###
    def __setf_category(self):
        category_unknown = {}
        for org in self.tree:
            if org["type1_org"] == "-":
                if "事业单位" in org["reg_authority"]:
                    org["category"] = "pin"  # set as 事业单位
                elif "未公示" in org["reg_authority"]:
                    org["category"] = "gpo"  # set as 党政机关
                else:
                    category_unknown[org["name_fr"]] = {}
            elif "公司" in org["type1_org"]:
                org["category"] = "ent"  # set as 企业
            else:
                category_unknown[org["name_fr"]] = {}

        return category_unknown

    ### load from db
    def load_db(self):

        self.tree_db = {}  # clear
        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            # print(f"docx={docx}")

            __tmp_k = ""
            __tmp_v = {}
            for kmap, vmap in self.db2tree.items():

                __value_db = docx.get(vmap["source"], None)
                if vmap.get("key", False) == True:  # it is key
                    __tmp_k = __value_db
                else:
                    __tmp_v[kmap] = __value_db
            self.tree_db.update({__tmp_k: __tmp_v})
            # if no key , then self.tree_db = [] and append(tmp_v)
            # self.tree[docx["organization_fr"]] = {
            #     "usn": docx.get("usn", ""),
            #     "category": docx.get("category", ""),
            #     "doc_ops": docx.get("doc_ops", {}),
            # }

        # cursor.close()
        client.close()

        return

    def load_db2(self, choice):

        treex = {}  # clear
        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(collx).find(query, projection)
        for docx in cursor:
            # print(f"docx={docx}")

            __tmp_k = ""
            __tmp_v = {}
            for kmap, vmap in db2treex.items():

                __value_db = docx.get(vmap["source"], None)
                if vmap.get("key", False) == True:  # it is key
                    __tmp_k = __value_db
                else:
                    __tmp_v[kmap] = __value_db
            treex.update({__tmp_k: __tmp_v})
            # if no key , then self.tree_db = [] and append(tmp_v)
            # self.tree[docx["organization_fr"]] = {
            #     "usn": docx.get("usn", ""),
            #     "category": docx.get("category", ""),
            #     "doc_ops": docx.get("doc_ops", {}),
            # }

        for k, v in treex.items():
            print(f"load_db2 :: k={k}, v={v}")

        # cursor.close()
        client.close()

        return

    def load(self):
        return self.load_db()

    ### post/write to db
    def post2db(self):

        # load from db, and put into tree_db.
        self.load_db()
        # count = 1
        # for k, v in self.tree_db.items():
        #     print(f"#{count} :: k={k}, v={v}")
        #     count += 1

        client = MongoClient()
        db = client.get_database("remai")

        key_field = ""
        for k, v in self.db2tree.items():
            if v.get("key", False) == True:
                key_field = v["source"]
        # print(f"key_field = {key_field}")

        bulk_ops = []
        for docx in self.tree:

            if docx[key_field] in self.tree_db:
                continue

            doc2db = {}
            count = 1
            for k, v in docx.items():
                if k.startswith("tmp000"):
                    continue
                doc2db[k] = v

            doc2db["doc_ops"] = {}
            doc2db["doc_ops"]["update_by"] = "be-gdv2"
            doc2db["doc_ops"]["create_by"] = "be-gdv2"
            doc2db["doc_ops"]["update_time"] = datetime.now()
            doc2db["doc_ops"]["create_time"] = datetime.now()

            bulk_ops.append(InsertOne(doc2db))

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        client.close()
        return result

    def post2db_h(self):

        # load from db, and put into tree_db.
        self.load_db()
        # count = 1
        # for k, v in self.tree_db.items():
        #     print(f"#{count} :: k={k}, v={v}")
        #     count += 1

        client = MongoClient()
        db = client.get_database("remai")

        key_field = ""
        for k, v in self.db2tree.items():
            if v.get("key", False) == True:
                key_field = v["source"]
        # print(f"key_field = {key_field}")

        bulk_ops = []
        for docx in self.tree:

            if docx[key_field] in self.tree_db:
                continue

            doc2db = {}
            count = 1
            for k, v in docx.items():
                if k.startswith("tmp000"):
                    continue
                doc2db[k] = v

            doc2db["doc_ops"] = {}
            doc2db["doc_ops"]["update_by"] = "be-gdv2"
            doc2db["doc_ops"]["create_by"] = "be-gdv2"
            doc2db["doc_ops"]["update_time"] = datetime.now()
            doc2db["doc_ops"]["create_time"] = datetime.now()

            bulk_ops.append(InsertOne(doc2db))

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        client.close()
        return result

    #
    def set_pnode(self, org, org_parent):
        if self.tree_p.get(org, {}) == {}:
            self.tree_p[org] = {}
        self.tree_p[org]["name_fr_pnode"] = org_parent

        return

    ###
    def set_usn(self, organizationx):
        orgx = {}
        for usn, v in organizationx.tree.items():
            orgx[v["name_fr"]] = usn

        nf_404 = []
        for docx in self.tree:
            docx["usn"] = orgx.get(docx["name_fr"], None)
            if docx["usn"] == None:
                nf_404.append(docx["name_fr"])

        return nf_404

    @property
    def coll(self):
        return self.__coll

    @property
    def coll_h(self):
        return self.__coll_h

    @property
    def db2tree(self):
        return self.__db2tree

    @property
    def db2tree_h(self):
        return self.__db2tree_h

    @property
    def db_map_f(self):
        return self.__db_map_f

    @property
    def flist(self):
        return self.__flist

    @property
    def fpath(self):
        return self.__fpath

    @fpath.setter
    def fpath(self, fp):
        self.__fpath = fp
        return

    @property
    def sheet(self):
        return self.__sheet

    @sheet.setter
    def sheet(self, st):
        self.__sheet = st
        return

    @property
    def size(self):
        return len(self.tree)

    @property
    def tree(self):
        return self.__tree

    @property
    def tree_db(self):
        return self.__tree_db

    @tree_db.setter
    def tree_db(self, t):
        self.__tree_db = t
        return

    @property
    def tree_db_h(self):
        return self.__tree_db_h

    @tree_db_h.setter
    def tree_db_h(self, t):
        self.__tree_db_h = t
        return

    @property
    def tree_h(self):
        return self.__tree_h

    @property
    def tree_p(self):
        return self.__tree_p


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class StudentYearDB(RemaiTreeDB):

    ###
    def __init__(self, yearx, colly="student"):

        super().__init__(
            yearx,
            tagx="",
            markx="",
            schemax={},
            treemapx={},
            collx=colly + "_" + yearx,
        )

        return

    ###
    def load(self):
        self.tree = {}
        client = MongoClient()
        db = client.get_database("remai")

        cursor = db.get_collection(self.coll).find({}, {"_id": 0})
        for docx in cursor:
            self.tree[docx["sid"]] = {
                "organization_fr": docx["organization_fr"],
                "name": docx["name"],
                "degree_year": docx["degree_year"],
                "path_fr": docx["path_fr"],
                "egc_tagx": docx.get("egc_tagx", {}),
                "usn": docx.get("usn", ""),
                "category": docx.get("category", ""),
                "root_node": docx.get("root_node", {}),
                "doc_ops": docx.get("doc_ops", {}),
            }

        client.close()
        return

    ###
    def post(self):

        list4db = StudentYearDB(self.year)
        list4db.load()

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        __change2be = 0
        for kx, vx in self.tree.items():

            # identical betwee mem and db : usn, root_node, egc_tgax?
            __identical = 0

            if list4db.tree.get(kx, {}).get("usn", "") == vx["usn"]:
                __identical += 1
            if list4db.tree.get(kx, {}).get("root_node", "") == vx["root_node"]:
                __identical += 2
            if list4db.tree.get(kx, {}).get("category", "") == vx["category"]:
                __identical += 4

            __tmark_eq = True
            egc_tagx = list4db.tree.get(kx, {}).get("egc_tagx", {})
            if len(egc_tagx) == len(vx["egc_tagx"]):
                for tag, mark in vx["egc_tagx"].items():
                    if egc_tagx.get(tag, "") != mark:
                        __tmark_eq = False
            else:
                __tmark_eq = False
            if __tmark_eq == True:
                __identical += 8

            if __identical == 15:  # yes!
                continue

            __change2be += 1
            bulk_ops.append(
                UpdateOne(
                    {"sid": kx},
                    {
                        "$set": {
                            "egc_tagx": vx["egc_tagx"],
                            "usn": vx["usn"],
                            "category": vx["category"],
                            "root_node": vx["root_node"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_by": "back-end-gdv2",
                            "doc_ops.update_time": datetime.now(),
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        client.close()

        return result

    ### copy field egc_tagx + usn + root_node from 'organization'
    def copyf_tag_h(self, org):

        __tag_copied = {"got-job": 0, "match": 0, "not-found": {}, "egc_tagx": {}}

        for kx, vx in self.tree.items():
            if vx["path_fr"] == "got-job":
                __tag_copied["got-job"] += 1
            else:
                continue

            __match = False
            for k_org, v_org in org.tree.items():
                if vx["organization_fr"] == v_org["name_fr"]:
                    __tag_copied["match"] += 1
                    __match = True

                    vx["usn"] = k_org
                    vx["root_node"] = v_org["root_node"]
                    vx["category"] = v_org["category"]

                    for tag, mark in v_org["egc_tagx"].items():
                        vx["egc_tagx"][tag] = mark[self.year]

                        # count mark + [mark] for each tag
                        if vx["egc_tagx"][tag] != FV_NOT_FOUND:
                            if __tag_copied["egc_tagx"].get(tag, 0) == 0:  # not exist
                                __tag_copied["egc_tagx"][tag] = 0
                            __tag_copied["egc_tagx"][tag] += 1

            if __match == False:
                vx["usn"] = FV_NOT_FOUND
                __tag_copied["not-found"][kx] = vx["organization_fr"]

        return __tag_copied


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class UploadStudentYear:

    #
    def __init__(self, year):
        self.__year = year
        self.__coll = "student_" + year + "_" + datetime.now().strftime("%Y%m%d%H%M")
        self.__fpath = ""
        self.__tree = {}

    #
    def loadf(self, list_std):

        count = 0
        for std in list_std:
            s = {}

            # set degree_year
            degree_year = ""
            if std["学历层次"] in ["二学位毕业", "本科生毕业"]:
                degree_year = "bachelor"
            elif std["学历层次"] == "硕士生毕业":
                degree_year = "master"
            elif std["学历层次"] == "博士生毕业":
                degree_year = "doctor"

            if degree_year == "":
                raise ValueError(
                    "学号={std['学号']}的学历层次取值有误，期望是'二学位毕业,'本科生毕业','硕士生毕业','博士生毕业'。"
                )
            else:
                degree_year += "-" + self.year  # -， not _

            # set path_fr
            if f"{std['毕业去向类别代码']:.0f}" in [
                "10",
                "11",
                "12",
                "46",
                "76",
                "271",
                "502",
                "503",
                "512",
                "519",
            ]:
                path_fr = "got-job"
            elif f"{std['毕业去向类别代码']:.0f}" in ["85", "801", "802"]:
                path_fr = "further-study"
            else:
                raise ValueError(
                    "学号={std['学号']}的'毕业去向类别代码'取值有误，期望是'10,11,12,271,46,502,503,512,519,76,801,802,85'。"
                )

            # 10	签就业协议形式就业
            # 11	签劳动合同形式就业
            # 12	其他录用形式就业
            # 46	应征义务兵
            # 76	自由职业
            # 85	境外留学
            # 271	科研助理、管理助理—科研助理、管理助理
            # 502	国家基层项目—三支一扶
            # 503	国家基层项目—西部计划
            # 512	地方基层项目—地方选调生
            # 519	地方基层项目—其他地方基层项目
            # 801	境内升学—研究生
            # 802	境内升学—第二学士学位

            s[f"{std['学号']:.0f}"] = {
                "organization_fr": std["单位名称"],
                "name": std["姓名"],
                "degree_year": degree_year,
                "path_fr": path_fr,
                "egc_tagx": {},
                "usn": "",
                "category": "",
                "root_node": {},
                "doc_ops": {},
            }

            self.tree.update(s)
            count += 1
            # print(f"#{count}, s={s}")

            # self.tree[docx["sid"]] = {
            #     "organization_fr": docx["organization_fr"],
            #     "name": docx["name"],
            #     "degree_year": docx["degree_year"],
            #     "path_fr": docx["path_fr"],
            #     "egc_tagx": docx.get("egc_tagx", {}),
            #     "usn": docx.get("usn", ""),
            #     "category": docx.get("category", ""),
            #     "root_node": docx.get("root_node", {}),
            #     "doc_ops": docx.get("doc_ops", {}),
            # }

            #     "学号",
            #     "姓名",
            #     "性别",
            #     "生源地名称",
            #     "生源地代码",
            #     "民族名称",
            #     "政治面貌",
            #     "城乡生源",
            #     "学历层次",
            #     "专业名称",
            #     "专业方向",
            #     "专业代码",
            #     "入学年月",
            #     "毕业日期",
            #     "培养方式",
            #     "委培单位",
            #     "学习形式",
            #     "毕业去向类别代码",
            #     "毕业去向",
            #     "签约日期",
            #     "单位名称",
            #     "统一社会信用代码",
            #     "留学院校外文名称",
            #     "单位性质代码",
            #     "单位性质",
            #     "单位行业代码",
            #     "单位行业",
            #     "单位所在地代码",
            #     "单位所在地",
            #     "单位地址",
            #     "工作职位类别代码",
            #     "工作职位类别",
            # ],
        return count

    ### post to db
    def post(self):

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        for kx, vx in self.tree.items():

            bulk_ops.append(
                InsertOne(
                    {
                        "sid": kx,
                        "organization_fr": vx["organization_fr"],
                        "name": vx["name"],
                        "degree_year": vx["degree_year"],
                        "path_fr": vx["path_fr"],
                        "egc_tagx": vx["egc_tagx"],
                        "usn": vx["usn"],
                        "category": vx["category"],
                        "root_node": vx["root_node"],
                        # specify field to be updated with '.' chain to avoid wrong overide.
                        "doc_ops": {
                            "update_by": "back-end-gdv2",
                            "update_time": datetime.now(),
                            "create_by": "back-end-gdv2",
                            "creat_time": datetime.now(),
                        },
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)

            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        client.close()

        return result

    @property
    def coll(self):
        return self.__coll

    @property
    def fpath(self):
        return self.__fpath

    @fpath.setter
    def fpath(self, fpath):
        self.__fpath = fpath
        return

    @property
    def tree(self):
        return self.__tree

    @property
    def year(self):
        return self.__year


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class X2DBYear:

    ###
    def __init__(self, yearx, collx="", db_map_fx={}):
        self.__year = yearx
        self.__coll = collx
        self.__db_map_f = db_map_fx
        self.__fpath = ""
        self.__sheet = ""
        self.__tree = []  # list instead of dict

    ###
    def loadf(self, list_xls):
        # list_xls: [{k1:v11,k2:v12,...},{k1:v21,k2:v22,...},...]

        count = 0
        for xls in list_xls:
            # xls = {k1:v11, k2:v12, ...}

            tmp_k = ""
            tmp_v = {}
            for kmap, vmap in self.db_map_f.items():

                # format is valid ?
                if vmap.get("format", "") not in [
                    "str",
                    "int",
                    "float",
                    "",
                ]:
                    raise ValueError("str /int /float expected.")

                value_xls = xls[vmap["file"]]
                type_xls = type(value_xls)  # type of value in xls

                if vmap.get("key", False) == True:  # if it is the key
                    match vmap.get("format", ""):
                        case "int" if type_xls == int:
                            tmp_k = value_xls
                        case "int" if type_xls == float:
                            tmp_k = int(round(value_xls, 0))
                        case "int" if type_xls == str:
                            tmp_k = int(value_xls)
                        case "float" if type_xls == float:
                            tmp_k = value_xls
                        case "float" if type_xls == str:
                            tmp_k = float(value_xls)
                        case "str":
                            tmp_k = f"{value_xls}"
                        case "":
                            tmp_k = value_xls
                else:  # else {k:v}
                    match vmap.get("format", ""):
                        case "int" if type_xls == int:
                            tmp_v[kmap] = value_xls
                        case "int" if type_xls == float:
                            tmp_v[kmap] = int(round(value_xls, 0))
                        case "int" if type_xls == str:
                            tmp_v[kmap] = int(value_xls)
                        case "float" if type_xls == float:
                            tmp_v[kmap] = value_xls
                        case "float" if type_xls == str:
                            tmp_v[kmap] = float(value_xls)
                        case "str":
                            tmp_v[kmap] = f"{value_xls}"
                        case "":
                            tmp_v[kmap] = value_xls

            # print(f"tmp_k={tmp_k}, tmp_v={tmp_v}")

            # add to tree
            # self.tree.update({tmp_k: tmp_v})
            self.tree.append(tmp_v)
            count += 1

        return count

    ###
    def post(self):

        client = MongoClient()
        db = client.get_database("remai")

        bulk_ops = []
        for docx in self.tree:

            doc2db = {}
            for k, v in docx.items():
                doc2db[k] = v
            doc2db["doc_ops"] = {}
            doc2db["doc_ops"]["update_by"] = "back-end-gdv2"
            doc2db["doc_ops"]["create_by"] = "back-end-gdv2"
            doc2db["doc_ops"]["update_time"] = datetime.now()
            doc2db["doc_ops"]["create_time"] = datetime.now()

            bulk_ops.append(InsertOne(doc2db))

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        client.close()
        return result

    @property
    def coll(self):
        return self.__coll

    @coll.setter
    def coll(self, collx):
        self.__coll = collx
        return

    @property
    def db_map_f(self):
        return self.__db_map_f

    @property
    def fpath(self):
        return self.__fpath

    @fpath.setter
    def fpath(self, fp):
        self.__fpath = fp
        return

    @property
    def sheet(self):
        return self.__sheet

    @sheet.setter
    def sheet(self, st):
        self.__sheet = st
        return

    @property
    def tree(self):
        return self.__tree

    @tree.setter
    def tree(self, t):
        self.__tree = t
        return

    @property
    def year(self):
        return self.__year


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class X2DbChn500(X2DBYear):

    ###
    def __init__(self, yearx):
        colly = (
            "list_chn500" + "_" + yearx + "_" + datetime.now().strftime("%Y%m%d%H%M")
        )

        super().__init__(
            yearx,
            collx=colly,
            db_map_fx={
                "rank": {"file": "rank", "format": "int"},
                "enterprise": {"file": "enterprise"},
                "revenue": {"file": "revenue", "format": "float"},
                "profit": {"file": "profit", "format": "float"},
                "organization_fr": {"file": "organization_fr"},
            },
        )
        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class X2DbFic100Svc(X2DBYear):

    ###
    def __init__(self, yearx, db_map_fy=None):
        colly = (
            "list_fic100"
            + "_svc_"
            + yearx
            + "_"
            + datetime.now().strftime("%Y%m%d%H%M")
        )

        if db_map_fy == None:
            db_map_fy = {
                "rank": {"file": "序号", "format": "int"},
                "name": {"file": "企业名称"},
                "hq_location": {"file": "省份"},
                "revenue": {"file": "营收总额（万元）", "format": "int"},
                "name_fr": {"file": "name_fr"},
            }

        super().__init__(
            yearx,
            collx=colly,
            db_map_fx=db_map_fy,
        )

        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class X2DbFic500All(X2DBYear):

    ###
    def __init__(self, yearx, db_map_fy=None):
        colly = (
            "list_fic500"
            + "_all_"
            + yearx
            + "_"
            + datetime.now().strftime("%Y%m%d%H%M")
        )

        if db_map_fy == None:
            db_map_fy = {
                "rank": {"file": "序号", "format": "int"},
                "name": {"file": "企业名称"},
                "hq_location": {"file": "省份"},
                "revenue": {"file": "营收总额（万元）", "format": "int"},
                "name_fr": {"file": "name_fr"},
            }

        super().__init__(
            yearx,
            collx=colly,
            db_map_fx=db_map_fy,
        )

        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class X2DbFic500Mfg(X2DBYear):

    ###
    def __init__(self, yearx, db_map_fy=None):
        colly = (
            "list_fic500"
            + "_mfg_"
            + yearx
            + "_"
            + datetime.now().strftime("%Y%m%d%H%M")
        )

        if db_map_fy == None:
            db_map_fy = {
                "rank": {"file": "序号", "format": "int"},
                "name": {"file": "企业名称"},
                "hq_location": {"file": "省份"},
                "revenue": {"file": "营收总额（万元）", "format": "int"},
                "name_fr": {"file": "name_fr"},
            }

        super().__init__(
            yearx,
            collx=colly,
            db_map_fx=db_map_fy,
        )

        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class X2DbGlb500(X2DBYear):

    ###
    def __init__(self, yearx):
        colly = (
            "list_glb500" + "_" + yearx + "_" + datetime.now().strftime("%Y%m%d%H%M")
        )

        super().__init__(
            yearx,
            collx=colly,
            db_map_fx={
                "rank": {"file": "rank", "format": "int"},
                "enterprise": {"file": "enterprise", "format": "str"},
                "revenue": {"file": "revenue", "format": "float"},
                "profit": {"file": "profit", "format": "float"},
                "country": {"file": "country"},
                "organization_fr": {"file": "organization_fr"},
            },
        )
        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class X2DbIsc100(X2DBYear):

    ###
    def __init__(self, yearx):
        colly = (
            "list_isc100" + "_" + yearx + "_" + datetime.now().strftime("%Y%m%d%H%M")
        )

        super().__init__(
            yearx,
            collx=colly,
            db_map_fx={
                "no": {"file": "no", "format": "int"},
                "name": {"file": "name"},
                "business_brand": {"file": "business_brand"},
                "hq_location": {"file": "hq_location"},
                "name_fr": {"file": "name_fr"},
            },
        )
        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class X2DbStudent(X2DBYear):

    ###
    def __init__(self, yearx, colly=None, db_map_fy=None):

        if colly == None:
            colly = (
                "student" + "_ir_" + yearx + "_" + datetime.now().strftime("%Y%m%d%H%M")
            )

        if db_map_fy == None:
            db_map_fy = {
                "sid": {"file": "学号", "format": "str"},
                "tmp000": {"file": "考生号", "format": "str"},
                "name": {"file": "姓名", "format": "str"},
                "gender": {"file": "性别", "format": "str"},
                "birth_date": {"file": "出生日期", "format": "str"},
                "graduate_year": {"file": "毕业年份", "format": "str"},
                "student_from_code": {"file": "生源地代码", "format": "str"},
                "student_from": {"file": "生源地名称", "format": "str"},
                "nationality": {"file": "民族名称", "format": "str"},
                "tmp000": {"file": "政治面貌", "format": "str"},
                "tmp000": {"file": "城乡生源", "format": "str"},
                "degree": {"file": "学历层次", "format": "str"},
                "major": {"file": "专业名称", "format": "str"},
                "specialization": {"file": "专业方向", "format": "str"},
                "major_code": {"file": "专业代码", "format": "str"},
                "tmp000": {"file": "入学年月", "format": "str"},
                "tmp000": {"file": "毕业日期", "format": "str"},
                "tmp000": {"file": "培养方式", "format": "str"},
                "tmp000": {"file": "委培单位", "format": "str"},
                "tmp000": {"file": "学习形式", "format": "str"},
                "path_after_graduate_code": {
                    "file": "毕业去向类别代码",
                    "format": "str",
                },
                "path_after_graduate": {"file": "毕业去向", "format": "str"},
                "tmp000": {"file": "签约日期", "format": "str"},
                "organization": {"file": "单位名称", "format": "str"},
                "usci_org": {"file": "统一社会信用代码", "format": "str"},
                "tmp000": {"file": "留学国家地区", "format": "str"},
                "tmp000": {"file": "留学院校中文名称", "format": "str"},
                "tmp000": {"file": "留学院校外文名称", "format": "str"},
                "tmp000": {"file": "留学专业中文名称", "format": "str"},
                "tmp000": {"file": "留学专业外文名称", "format": "str"},
                "type_org_code": {"file": "单位性质代码", "format": "str"},
                "type_org": {"file": "单位性质", "format": "str"},
                "industry_org_code": {"file": "单位行业代码", "format": "str"},
                "industry_org": {"file": "单位行业", "format": "str"},
                "location_org_code": {"file": "单位所在地代码", "format": "str"},
                "location_org": {"file": "单位所在地", "format": "str"},
                "address_org": {"file": "单位地址", "format": "str"},
                "position_type_code": {"file": "工作职位类别代码", "format": "str"},
                "position_type": {"file": "工作职位类别", "format": "str"},
            }

        super().__init__(
            yearx,
            collx=colly,
            db_map_fx=db_map_fy,
        )
        return

    ###
    def update_fields(self, collx=None):

        if collx == None:
            self.coll = input("Input collection to be updated (Enter for default) : ")
        else:
            self.coll = "student" + "_ir_" + self.year

        client = MongoClient()
        db = client.get_database("remai")

        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)

        self.tree = []
        list_db = []
        for docx in cursor:

            list_db.append(docx)

            s = {}
            s["sid"] = docx["sid"]

            # set organization_fr = organization
            if (
                docx["path_after_graduate_code"] == "76"
                or docx["path_after_graduate"] == "自由职业"
            ):
                s["organization_fr"] = "fl-org"  # freelance
            elif docx["path_after_graduate_code"] == "46":
                s["organization_fr"] = "应征义务兵[46]"
            elif docx["path_after_graduate_code"] == "503":
                s["organization_fr"] = "西部计划志愿者[503]"
            else:
                s["organization_fr"] = docx["organization"]

            # set degree_year
            s["degree_year"] = ""
            if docx["degree"] in ["二学位毕业", "本科生毕业"]:
                s["degree_year"] = "bachelor"
            elif docx["degree"] == "硕士生毕业":
                s["degree_year"] = "master"
            elif docx["degree"] == "博士生毕业":
                s["degree_year"] = "doctor"

            if s["degree_year"] == "":
                raise ValueError(
                    f"学号={docx['sid']}的学历层次取值有误，期望是'二学位毕业,'本科生毕业','硕士生毕业','博士生毕业'。"
                )
            else:
                s["degree_year"] += "-" + self.year  # -， not _

            # set path_fr
            if docx["path_after_graduate_code"] in [
                "10",
                "11",
                "12",
                "46",
                "76",
                "271",
                "502",
                "503",
                "512",
                "519",
            ]:
                s["path_fr"] = "got-job"
            elif docx["path_after_graduate_code"] in ["85", "801", "802"]:
                s["path_fr"] = "further-study"
            else:
                raise ValueError(
                    f"学号={docx['sid']}的'毕业去向类别代码'取值有误，期望是'10,11,12,271,46,502,503,512,519,76,801,802,85'。"
                )

            self.tree.append(s)

        #
        # count = 1
        # for docx in self.tree:
        #     print(f"#{count}:: docx = {docx}")
        #     count += 1

        # update db
        # 250223: compare before update. to be amended.
        bulk_ops = []
        change2be = 0
        for docx in self.tree:

            __identical = 0
            for docdb in list_db:
                if docx["sid"] == docdb["sid"]:

                    if docx["organization_fr"] == docdb.get("organization_fr", ""):
                        __identical += 1
                    if docx["degree_year"] == docdb.get("degree_year", ""):
                        __identical += 2
                    if docx["path_fr"] == docdb.get("path_fr", ""):
                        __identical += 4

                    # print(
                    #     f'sid={docx["sid"]}/{docdb["sid"]}，org={docx["organization_fr"]}/{docdb.get("organization_fr", "")}, year={docx["degree_year"]}/{docdb.get("degree_year", "")}, path={docx["path_fr"]}/{docdb.get("path_fr", "")}'
                    # )
                    break
            if __identical == 7:
                continue

            s = {}
            change2be = change2be + 1

            bulk_ops.append(
                UpdateOne(
                    {"sid": docx["sid"]},
                    {
                        "$set": {
                            "organization_fr": docx["organization_fr"],
                            "degree_year": docx["degree_year"],
                            "path_fr": docx["path_fr"],
                            # specify field to be updated with '.' chain to avoid wrong overide.
                            "doc_ops.update_by": "back-end-gdv2",
                            "doc_ops.update_time": datetime.now(),
                            # "doc_ops.create_by": "back-end-gdv2",
                            # "doc_ops.create_time": datetime(2025, 2, 12, 12, 12, 12),
                        }
                    },
                )
            )

        if len(bulk_ops) > 0:
            bulk_ops_result = db.get_collection(self.coll).bulk_write(bulk_ops)
            result = {"bulk_ops": 1, "result": bulk_ops_result}
        else:
            result = {"bulk_ops": 0, "result": ""}

        client.close()
        return result


### --------------------------------------------------------------------------------------------------------------------
### tools
### --------------------------------------------------------------------------------------------------------------------
#


def add_organization():
    ###
    org_bizreg = AddOrgBizReg()
    org_pchild = AddOrgPChild()
    org_asis2b = AddOrgAsIs2b()

    ### get file list
    fp = input("Folder for files from aiqicha.com (/path/to/folder, Enter to skip) : ")
    if fp != "":

        ### step #1 : load (工商注册信息) and insert into db

        # logging.info("-" * 36)
        # logging.info(f"load (工商注册信息) files and insert into '{org_bizreg.coll}'.")
        # logging.info("-" * 36)

        org_bizreg.get_flist(fp)
        logging.info(f" {len(org_bizreg.flist)} file(s)(工商注册信息) found.")

        org_pchild.get_flist(fp)
        logging.info(f" {len(org_pchild.flist)} file(s)(股东信息) found.")

        result = org_bizreg.load_fp()
        logging.info(f" read {result} file(s)(工商注册信息) done.")
        # org_bizreg.show_tree()

        result = org_pchild.load_fp()
        logging.info(f" read {result} file(s)(股东信息) done.")

        # set 'category' for 'org_bizreg'
        result = org_bizreg.setf_category()
        if len(result) > 0:
            logging.warning(
                f"{org_bizreg.coll} :: 'category' unknown for  {len(result)} doc(s) : {result} "
            )

        # insert into 'org_bizreg'
        tmp = AddOrgBizReg()
        tmp.load_db()
        result = org_bizreg.insert2db("name_fr", tmp.tree)
        if result["ops4db"] == True:
            logging.info(
                f"{org_bizreg.coll} :: insert done, [{result['result'].inserted_count}] inserted, [{len(tmp.tree)}] before operation."
            )
        else:
            logging.info(f"{org_bizreg.coll} :: insert done, no doc posted to db.")

        # insert into 'org_pchild'
        tmp = AddOrgPChild()
        tmp.load_db()
        result = org_pchild.insert2db("name_fr", tmp.tree)
        if result["ops4db"] == True:
            logging.info(
                f"{org_pchild.coll} :: insert done, [{result['result'].inserted_count}] inserted, [{len(tmp.tree)}] before operation."
            )
        else:
            logging.info(f"{org_pchild.coll} :: insert done, no doc posted to db.")

    ### step #2 ： set 'usn' for 'org_bizreg' & 'org_pchild'

    orgv2 = OrganizationV2()
    orgv2.load_db()

    # for 'org_bizreg'
    org_bizreg.load_db()
    logging.info(f"{org_bizreg.coll} :: set field 'usn' based on '{orgv2.coll}'")
    result = org_bizreg.setf_usn(orgv2.tree)
    if result > 0:
        logging.warning(f"{org_bizreg.coll} :: 'usn' of {result} doc(s) not found.")

    tmp = AddOrgBizReg()
    tmp.load_db()
    result = org_bizreg.update2db("name_fr", tmp.tree)
    if result["ops4db"] == True:
        logging.info(
            f"{org_bizreg.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified."
        )
    else:
        logging.info(f"{org_bizreg.coll} :: update done, no doc changed in db.")

    # for 'org_pchild'
    org_pchild.load_db()
    logging.info(f"{org_pchild.coll} :: set field 'usn' based on '{orgv2.coll}'")
    result = org_pchild.setf_usn(orgv2.tree)
    if result > 0:
        logging.warning(f"{org_pchild.coll} :: 'usn' of {result} doc(s) not found.")

    tmp = AddOrgPChild()
    tmp.load_db()

    # print("tmp.tree::")
    # tmp.show_tree()
    # print("org_pchild.tree::")
    # org_pchild.show_tree()

    result = org_pchild.update2db("name_fr", tmp.tree)
    if result["ops4db"] == True:
        logging.info(
            f"{org_pchild.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified."
        )
    else:
        logging.info(f"{org_pchild.coll} :: update done, no doc changed in db.")

    #
    __usn_not_found = {}
    __usn_nf_404_pc = {}

    for k, v in org_bizreg.tree.items():
        if v["usn"] == FV_NOT_FOUND:
            __usn_not_found[k] = {}

    for k, v in org_pchild.tree.items():
        if v["usn"] == FV_NOT_FOUND:
            if k in __usn_not_found:
                continue
            __usn_nf_404_pc[k] = {}

        if v["parent_node"]["usn"] == FV_NOT_FOUND:
            if v["parent_node"]["name_fr"] in __usn_not_found:
                continue
            __usn_nf_404_pc[v["parent_node"]["name_fr"]] = {}

    if len(__usn_nf_404_pc) > 0:
        logging.warning(
            f"  [{len(__usn_nf_404_pc)}] more 'usn' not found in '{org_pchild.coll}', fix it and try again. not-found : {__usn_nf_404_pc}"
        )

    ###
    logging.info("*" * 72)

    choice = input(
        "[Step #2] Load docs ('usn' not found) from 'org_bizreg' to 'org_asis2b', go ？(Y/n) : "
    )
    logging.info("*" * 72)
    if choice in ["Y", "y"]:

        # print(f"size = {org_bizreg.size} (before)")
        org_bizreg.load_db({"usn": "nf-404"})
        # print(f"size = {org_bizreg.size} (after)")
        # org_bizreg.show_tree()

        tmp = OrganizationV2()
        max_usn = tmp.get_max_usn()
        # print(f"{tmp.coll} :: max_usn = {max_usn}")
        tmp = ""

        # set usn for usn not found
        org_bizreg.setf_usn4nf(max_usn)
        # org_bizreg.show_tree()

        # set parent_node
        tmp = AddOrgPChild()
        tmp.load_db()
        result = org_bizreg.setf_parent_node(tmp.tree)
        if len(result) > 0:
            logging.warning(
                f"{org_bizreg.coll} :: [{len(result)}] parent_node's 'usn' not found : {result}"
            )
        # org_bizreg.show_tree()

        # insert into 'org_asis2b'
        tmp = AddOrgAsIs2b()
        tmp.load_db()
        # if tmp.size > 0:
        #     logging.warning(
        #         f"{tmp.coll} :: [{tmp.size}] doc(s) in db. empty it and try again."
        #     )
        #     return

        result = tmp.insert2db("name_fr", tmp.tree, org_bizreg.tree)
        if result["ops4db"] == True:
            logging.info(
                f"{tmp.coll} :: insert done, [{result['result'].inserted_count}] inserted."
            )
        else:
            logging.info(f"{tmp.coll} :: insert done, no doc posted to db.")

        # update 'org_asis2b' after insertion
        tmp = AddOrgAsIs2b()
        tmp.load_db()
        result = tmp.update2db("name_fr", tmp.tree, org_bizreg.tree)
        if result["ops4db"] == True:
            logging.info(
                f"{tmp.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified."
            )
        else:
            logging.info(f"{tmp.coll} :: update done, no doc changed in db.")

    ### load from 'org_asis2b' and insert into 'organization'
    logging.info("*" * 72)
    choice = input(
        "[Step #3] Insert into 'organization' from 'org_asis2b', go ？(Y/n) : "
    )
    logging.info("*" * 72)
    if choice not in ["Y", "y"]:
        return

    org_asis2b = AddOrgAsIs2b()
    org_asis2b.load_db()

    # insert into 'organization' from 'org_asis2b'
    tmp = OrganizationV2()
    tmp.load_db()
    result = tmp.insert2db("name_fr", tmp.tree, org_asis2b.tree)
    if result["ops4db"] == True:
        logging.info(
            f"{tmp.coll} :: insert done, [{result['result'].inserted_count}] inserted."
        )
    else:
        logging.info(f"{tmp.coll} :: insert done, no doc posted to db.")

    ###
    # flist = [org_bizreg.flist, org_pchild.flist]
    # for listx in flist:
    #     print("-" * 36)
    #     count = 1
    #     for v in listx:
    #         print(f"#{count} :: v={v}")
    #         count += 1

    # tmp1 = tmp
    # print(f"coll={tmp1.coll}, map_db2t = {tmp1.map_db2t}")

    return


### add more organization to the system
def add_organization1():

    org_fr2b = OrganizationFR2B()

    fp = input("Folder for files from aiqicha.com (/path/to/folder) : ")

    if fp != "":

        org_fr2b.get_flist(fp)
        count_fp = org_fr2b.load_fp()
        logging.info(
            f"done with {count_fp['business-registration']} 'business-registration' and {count_fp['parent-child']} 'parent-child' file(s)."
        )

        ### set field 'category'
        logging.info(f"{org_fr2b.coll_br} :: set field 'category'.")
        result = org_fr2b.setf_category()
        if len(result) > 0:
            logging.warning(
                f"unknown category for {len(result)} organization(s) : {result}"
            )

        ### insert into 'org_fr_br'
        logging.info(f"{org_fr2b.coll_br} :: insert into db.")

        # load from db to create tree for checking if data is same as that in db.
        org_fr2b.load_db4chk_br()

        result = org_fr2b.insert2db_br()
        # print(f"result={result}")
        if result["ops4db"] == True:
            logging.info(
                f"{org_fr2b.coll_br} :: {result['result'].inserted_count} inserted."
            )
        else:
            logging.info(
                f"{org_fr2b.coll_br} :: 0 inserted. organizations in files might already exist in db. "
            )

        ### insert into 'org_fr_pc'
        logging.info(f"{org_fr2b.coll_pc} :: insert into db.")
        # load from db to create tree for checking if data is same as that in db.
        org_fr2b.load_db4chk_pc()
        result = org_fr2b.insert2db_pc()
        # print(f"result={result}")
        if result["ops4db"] == True:
            logging.info(
                f"{org_fr2b.coll_pc} :: {result['result'].inserted_count} inserted."
            )
        else:
            logging.info(
                f"{org_fr2b.coll_pc} :: 0 inserted. parent-child in files might already exist in db. "
            )

    ### copy field 'usn' from 'organization' to 'org_fr_br'
    org_fr2b.load_db()
    # count = 1
    # for k, v in org_fr2b.dbtree.items():
    #     print(f"#{count} :: k={k}, v={v}")
    #     count += 1

    org_fr2b.load_db_br()
    org_fr2b.setf_usn()

    org_fr2b.load_db4chk_br()
    result = org_fr2b.update2db_br()
    if result["ops4db"] == True:
        logging.info(
            f"{org_fr2b.coll_br} :: {result['result'].matched_count} matched, {result['result'].modified_count} modified."
        )
    else:
        logging.info(f"{org_fr2b.coll_br} :: no data changed, so no db updated.")

    ### set h_psn for 'org_fr_2b', based on parent-child in 'org_fr_pc'

    logging.info(f"{org_fr2b.coll_2b} :: set parent_node")
    org_fr2b.load_db_2b()
    org_fr2b.load_db_pc()
    org_fr2b.setf_parent_node()

    org_fr2b.load_db4chk_2b()
    result = org_fr2b.update2db_2b()
    if result["ops4db"] == True:
        logging.info(
            f"{org_fr2b.coll_2b} :: {result['result'].matched_count} matched, {result['result'].modified_count} modified."
        )
    else:
        logging.info(f"{org_fr2b.coll_2b} :: no data changed, so no db updated.")

    ### load from 'org_fr_2b' and insert into 'organization'
    # srcx_tree, dstx_tree, result = org_fr2b.load_db_2b2org()
    # if result["ops4db"] == True:
    #     logging.info(
    #         f"{org_fr2b.coll} :: {result['result'].inserted_count} inserted (from '{org_fr2b.coll_2b}')."
    #     )
    # else:
    #     logging.info(
    #         f"{org_fr2b.coll} :: 0 inserted. organization from '{org_fr2b.coll_2b}' might already exist in db. "
    #     )
    return


###
def upload_frxx(yearx):

    while True:
        print(" " * 36)
        print("Upload Excel File to DB. Make a choice:")
        print("-" * 36)
        print("1. glb500 - 世界 500 强")
        print("2. chn500 - 中国 500 强")
        print("3. isc100 - 互联网 100 强")
        print("41. fic500all - 民营企业 500 强")
        print("42. fic500mfg - 制造业民营企业 500 强")
        print("43. fic100svc - 服务业民营企业 100 强")
        print("100. student - 学生信息")
        print("101. 更新student字段")

        print("-" * 36)

        choice_list = input("Your choice (1-2, other to return):")
        if choice_list not in ["1", "2", "3", "41", "42", "43", "100", "101"]:
            return

        if choice_list == "1":
            list_x2db = X2DbGlb500(yearx)
        elif choice_list == "2":
            list_x2db = X2DbChn500(yearx)
        elif choice_list == "3":
            list_x2db = X2DbIsc100(yearx)
        elif choice_list == "41":
            list_x2db = X2DbFic500All(yearx)
        elif choice_list == "42":
            list_x2db = X2DbFic500Mfg(yearx)
        elif choice_list == "43":
            list_x2db = X2DbFic100Svc(yearx)
        elif choice_list in ["100", "101"]:
            list_x2db = X2DbStudent(yearx)
        else:
            continue

        #
        if choice_list == "101":
            result = list_x2db.update_fields()

            if result["bulk_ops"] == 0:
                logging.info(f"{list_x2db.coll}::no fields updated..")
            else:
                logging.info(
                    f"{list_x2db.coll}::{result['result'].matched_count} matched, {result['result'].modified_count} modified."
                )

            continue

        fpath = input("Excel file path to upload to db (/path/to/file) : ")
        sheet = input("Sheet name to read (Enter to skip) : ")
        print(f"fpath={fpath}, sheet={sheet}")
        choice = input("Correct and go  (Y/n) ? : ")
        if choice not in ["Y", "y"]:
            continue

        list_x2db.fpath = fpath
        list_x2db.sheet = sheet

        # read excel file
        logging.info(f"{list_x2db.coll} :: read file : {list_x2db.fpath}.")
        if list_x2db.sheet == "":
            df = pd.read_excel(list_x2db.fpath)
        else:
            df = pd.read_excel(list_x2db.fpath, sheet_name=list_x2db.sheet)

        tmp = df.to_dict(orient="records")
        count = list_x2db.loadf(tmp)
        logging.info(f"{list_x2db.coll} :: {count} record(s) ready to post to db.")

        logging.info(f"{list_x2db.coll} :: insert into db.")
        result = list_x2db.post()
        # bulk_ops_result.inserted_count
        logging.info(f"{list_x2db.coll} :: {result['result'].inserted_count} inserted.")

    return
    load_glb500 = X2DbGlb500(yearx)
    # load_glb500.fpath = "/Users/george1442/stt/remai/listxx/glb500.xlsx"
    load_glb500.fpath = input("Excel file path to upload to db (/path/to/file): ")

    print(
        f"year={load_glb500.year}, db_map_f={load_glb500.db_map_f}, coll={load_glb500.coll}"
    )

    # return

    logging.info(f"read file: {load_glb500.fpath}")
    df = pd.read_excel(load_glb500.fpath)

    list_xls = df.to_dict(orient="records")
    count = load_glb500.loadf(list_xls)
    logging.info(f"{count} record(s) ready to update to db.")

    logging.info(f"{load_glb500.coll}::post to db.")
    result = load_glb500.post()
    # bulk_ops_result.inserted_count
    logging.info(f"{load_glb500.coll}::{result['result'].inserted_count} posted to db.")

    # count = 1
    # for docx in load_glb500.tree:
    #     print(f"#{count}::docs={docx}")
    #     count += 1

    return

    upload_student = UploadStudentYear(yearx)
    print(f"{upload_student.coll}::year={upload_student.year}")

    choice = input("Excel file path to upload to db (/path/to/file): ")
    upload_student.fpath = choice

    #
    logging.info(f"get student information from file: {upload_student.fpath}")
    df = pd.read_excel(
        upload_student.fpath,
        usecols=[
            "学号",
            "姓名",
            "性别",
            "生源地名称",
            "生源地代码",
            "民族名称",
            "政治面貌",
            "城乡生源",
            "学历层次",
            "专业名称",
            "专业方向",
            "专业代码",
            "入学年月",
            "毕业日期",
            "培养方式",
            "委培单位",
            "学习形式",
            "毕业去向类别代码",
            "毕业去向",
            "签约日期",
            "单位名称",
            "统一社会信用代码",
            "留学院校外文名称",
            "单位性质代码",
            "单位性质",
            "单位行业代码",
            "单位行业",
            "单位所在地代码",
            "单位所在地",
            "单位地址",
            "工作职位类别代码",
            "工作职位类别",
        ],
    )
    # req_column = df[["姓名","学号"]]

    # list of dict, some key-value pairs in each dict.
    # e.g., {'学号': 7201607003, '姓名': '谢润山', '性别': '男',...}.

    ll = df.to_dict(orient="records")
    count = upload_student.loadf(ll)
    logging.info(f"{count} student(s) ready to update to db.")

    # count = 1
    # for sid, value in upload_student.tree.items():
    #     print("-" * 36)
    #     print(f"#{count}::sid={sid}, v={value}")
    #     count +=1

    # post to db
    logging.info(f"{upload_student.coll}::post to db.")
    result = upload_student.post()
    # bulk_ops_result.inserted_count
    logging.info(
        f"{upload_student.coll}::{result['result'].inserted_count} posted to db."
    )

    logging.info("*" * 36)
    logging.info(
        f"next step:: rename collection '{upload_student.coll} to {'student_' + upload_student.year}."
    )
    return


def show_kvp(listx):
    for k, v in listx.items():
        print(f"k={k}, v={v}")

        if isinstance(v, dict):
            show_kvp(v)
    return


###
def set_tag(yearx):

    # print(f"yearx={yearx},yearx[1:]={yearx[1:]}")

    if isinstance(yearx, str) is False:
        raise TypeError("string expected. e.g., 'y2024'(' not included, y2024 only)")
        return
    elif (len(yearx) != 5) or (not yearx[1:].isdigit()):
        raise ValueError(
            "y followed by 4-digit year expected. e.g., 'y2024'(' not included, y2024 only)"
        )
        return

    """
    client = MongoClient()
    db = client.get_database("remai")

    bulk_ops = []
    bulk_ops.append(
        UpdateOne(
            {"usn": "a09"},
            {
                "$set": {
                    # specify field to be updated with '.' chain to avoid wrong overide.
                    "doc_ops.update_time": datetime.now(),
                    "doc_ops.update_by": "back-end-gdv2",
                    "egc_tagx.glb500.y2023": "glb",
                }
            },
        )
    )

    for b in bulk_ops:
        print(f"b={b}")

    bulk_ops_result = db.get_collection("organization").bulk_write(bulk_ops)
    bulk_ops_result.matched_count
    print(
        f"{bulk_ops_result.matched_count} matched, {bulk_ops_result.modified_count} modified."
    )

    return
    """

    # gdtree = ListPlcDB(yearx)
    # tmp = gdtree.load()
    # print(f"tmp={tmp}")
    # count = 0
    # for k, v in gdtree.tree.items():
    #     print(f"k={k}, v={v}")
    #     count += 1
    #     if count >= 10:
    #         break
    # print(f"600776, v={gdtree.tree['东方通信股份有限公司']}")
    # return

    ### set_tag start
    logging.info("*" * 36)
    organization = OrganizationDB(yearx)
    student = StudentYearDB(yearx)
    logging.info(
        f"{yearx}::set tags for '{organization.coll}', and then copy tags to from '{organization.coll}' to '{student.coll}'."
    )

    ### 1. organization::load from db
    logging.info(f"{organization.coll}::load from db.")
    organization.load()
    logging.info("loaded.")

    ### 2. organization::set field 'child_node'
    logging.info(f"{organization.coll}::set field 'child_node'.")
    tmp = organization.setf_child_node()
    if len(tmp) == 0:
        logging.info("set done.")
    else:
        logging.info("-" * 16)
        logging.warning(
            f"{organization.coll}::{len(tmp)} usn(s) in field 'h_psn' are not found, fix it and try again. not-found: {tmp}"
        )
        logging.info("-" * 16)

    ### 3. organization::set field 'root_node'
    logging.info(f"{organization.coll}::set field 'root_node'.")
    for node, vx in organization.tree.items():
        psn = str(vx.get("h_psn"))
        if psn == FV_ROOT_NODE:  # if it is root-node
            organization.setf_root_node(node, node)
    logging.info(f"set done.")

    # usn = "aue"
    # print(f"organization::check value, usn='{usn}', value={organization.tree.get(usn)}")
    # usn = "aud"
    # print(f"organization::check value, usn='{usn}', value={organization.tree.get(usn)}")
    # usn = "a03"
    # print(f"organization::check value, usn='{usn}', value={organization.tree.get(usn)}")

    ### 4. organization::set field 'ecg_tagx'

    list2ref = [
        ListGlb500YearDB(yearx),
        ListChn500YearDB(yearx),
        ListSoeDB(yearx),
        ListPlcDB(yearx),
        CertNationalHteDB(yearx),
        ListIsc100YearDB(yearx),
        ListFic100SvcYearDB(yearx),
        ListFic500AllYearDB(yearx),
        ListFic500MfgYearDB(yearx),
    ]

    for gdtree in list2ref:

        # load from db
        logging.info("-" * 36)
        logging.info(f"{gdtree.coll}::load from db.")
        gdtree.load(organization)
        if gdtree.size > 0:
            logging.info("loaded")
        else:
            logging.info("-" * 16)
            logging.warning(f"{gdtree.coll}::nothing loaded, fix it and try again.")
            logging.info("-" * 16)
            continue  # if empty or non-exist, skip.

        # organization::set field 'egc_tagx'
        organization.tag = gdtree.tag  # copy tag to organization
        organization.mark = gdtree.mark  # copy mark to organization
        organization.tag_root = gdtree.tag_root  # copy tag_root to organization

        logging.info(
            f"{organization.coll}::set field 'egc_tagx', tag='{organization.tag}'."
        )

        print(
            f">>>{organization.coll}::year={organization.year}, tag={organization.tag}, mark={organization.mark}, tag_root={organization.tag_root}"
        )

        # 1/3: set mark refer to listx
        tmp_result = {"node": {}, "mark": {}}
        for node, value in organization.tree.items():
            tmp = gdtree.set_mark(node, value)
            if len(tmp) > 0:
                organization.set_mark_nty(node, tmp)

                tmp_result["node"][node] = {}
                tmp_result["mark"][tmp] = {}

        tmp = list(tmp_result["mark"].keys())
        logging.info(
            f"{organization.coll}::{len(tmp_result['node'])} doc(s) are marked as {tmp}."
        )

        # 2/3: set [mark] for root node if applicable
        if organization.tag_root == True:
            tmp = organization.setf_egc_tagx_root()
            if len(tmp) > 0:
                logging.info(
                    f"{len(tmp)} root node(s) are set as [{organization.mark[0]}] : {tmp}."
                )
            else:
                logging.info(f"no root node is set as [{organization.mark[0]}].")
        else:
            logging.info("set root node is not applicable.")

        # 3/3 : set field 'egc_tagx' for the rest (still blank, not set yet)
        organization.tag_count = 0  # any value will be reset tag_count
        for kx, vx in organization.tree.items():
            psn = str(vx.get("h_psn"))
            if psn == FV_ROOT_NODE:
                organization.setf_egc_tagx_rest(kx, organization.get_mark_nty(kx))
        logging.info(
            f"{organization.coll}::set tag={organization.tag}, {organization.tag_count}."
        )

        # organization.setf_egc_tagx(gdtree)
        logging.info("set done.")

    # usn = "aue"
    # print(f"organization::check value, usn='{usn}', value={organization.tree.get(usn)}")
    # usn = "aud"
    # print(f"organization::check value, usn='{usn}', value={organization.tree.get(usn)}")
    # usn = "a03"
    # print(f"organization::check value, usn='{usn}', value={organization.tree.get(usn)}")

    # print(" " * 36)
    # print("*" * 36)
    # print(f"{organization.coll}::check value")
    # organization.show(10)

    logging.info(f"{organization.coll}::update to db.")
    tmp_list = organization.post()
    if tmp_list["bulk_ops"] == 0:
        logging.info(
            f"{organization.coll}::no tag/root_node/child_node changed ,then no db updated."
        )
    else:
        logging.info(
            f"{organization.coll}::{tmp_list['result'].matched_count} matched, {tmp_list['result'].modified_count} modified."
        )

    ### 5. copy tags to student from organization

    # load from db
    print(" " * 36)
    print("*" * 36)
    student = StudentYearDB(yearx)
    logging.info(f"{student.coll}::load from db.")
    student.load()
    logging.info("loaded.")

    # copy tags
    logging.info(f"{student.coll}::copy tags from {organization.coll}.")
    tmp = student.copyf_tag_h(organization)
    if tmp["got-job"] == tmp["match"]:
        logging.info(f"{tmp['got-job']} student(s) got job, {tmp['match']} matched.")
    else:
        logging.warning(
            f"{tmp['got-job']} student(s) got job, {tmp['match']} matched, {tmp['got-job'] - tmp['match']} not found. Fix it and try again."
        )
        logging.warning(f"not-found = {tmp['not-found']}")
    logging.info(f"tags copied (excl. 'nf-404') : {tmp['egc_tagx']}")

    # update to db
    logging.info(f"{student.coll}::update to db.")
    tmp_list = student.post()
    if tmp_list["bulk_ops"] == 0:
        logging.info(
            f"{student.coll}::no tag/usn/root_node changed, then no db updated."
        )
    else:
        logging.info(
            f"{student.coll}::{tmp_list['result'].matched_count} matched, {tmp_list['result'].modified_count} modified."
        )

    return


###
def set_usn(yearx):

    # print(f"yearx={yearx},yearx[1:]={yearx[1:]}")

    ###
    print("*" * 36)
    organization = OrganizationDB(yearx)
    logging.info(f"coll={organization.coll}::loading from db.")
    organization.load()
    logging.info("loaded.")

    list2set = [
        ListGlb500YearDB(yearx),
        ListChn500YearDB(yearx),
        ListSoeDB(yearx),
        ListIsc100YearDB(yearx),
        ListFic100SvcYearDB(yearx),
        ListFic500AllYearDB(yearx),
        ListFic500MfgYearDB(yearx),
        OrganizationVer(yearx),
    ]

    for gdtree in list2set:

        print("*" * 36)
        # gdtree = ListGlb500YearDB(yearx)
        logging.info(f"coll={gdtree.coll}::loading from db.")
        gdtree.load()
        logging.info("loaded.")

        logging.info(
            f"coll={gdtree.coll}::copying field 'usn' from coll='organization'."
        )
        tmp_list = gdtree.set_usn(organization)
        if len(tmp_list) == 0:
            logging.info(
                f"coll={gdtree.coll}::copy field 'usn' from coll='organization', done."
            )
        else:
            logging.warning(
                f"{len(tmp_list)} are NOT found in 'organization. Fix it then try again. NOT-found : {tmp_list}'"
            )

        logging.info(f"coll={gdtree.coll}::updating 'usn' to db.")
        tmp_list = gdtree.post()
        if tmp_list["bulk_ops"] == 0:
            logging.info(
                f"coll={gdtree.coll}::db not updated cause all 'usn' are identical."
            )
        else:
            logging.info(
                f"coll={gdtree.coll}::{tmp_list['result'].matched_count} matched, {tmp_list['result'].modified_count} modified."
            )

    return


###
def set_usn1():

    gd_client = MongoClient()
    gd_db = gd_client.get_database("remai")

    # query = {}
    # projection = {"_id": 0}
    # gd_cursor = gd_db.get_collection("list_chn500_y2023").find(query, projection)
    # for gd_doc in gd_cursor:
    #     print(f">>>doc={gd_doc}")

    gd_years = ("y2023",)
    query = {}
    projection = {"_id": 0}
    gd_cursor = gd_db.get_collection("list_chn500_y2023").find(query, projection)
    tmp_list = []
    count = 1
    for gd_doc in gd_cursor:
        tmp_list.append(gd_doc)
        print(f">>>doc={gd_doc}, list={tmp_list}")
        count = count + 1
        if count >= 10:
            break

    for doc in tmp_list:
        print(f"doc={doc}")

    logging.info("show kvp")
    show_kvp(list_chn500_yxxxx_schema)

    list_chn500_db = ListChn500YearDB("y2023")
    tmp = ListChn500YearDB(yearx="y2024", colly="list_chn555")
    logging.info(f"list_chn500::loading from db.")
    # list_chn500_db.load(tmp_list, list_chn500_tree, list_chn500_yxxxx_schema)
    logging.info("loaded.")

    logging.info("list_chn500::check value.")
    list_chn500_db.show(3)

    organization = OrganizationDB(gd_years[0])
    logging.info("org::check value")
    logging.info(
        f"org::year={organization.year},schema={organization.schema}, coll={organization.coll}"
    )

    #
    organization.load()
    count = 1
    for k, v in organization.tree.items():
        print(f"org::k={k}, v={v}")
        count = count + 1
        if count >= 10:
            break

    #
    # organization.wash()

    gd_client.close()

    return


###
def wash_data(yearx):

    # print(f"yearx={yearx},yearx[1:]={yearx[1:]}")

    if isinstance(yearx, str) is False:
        raise TypeError("string expected. e.g., 'y2024'(' not included, y2024 only)")
        return
    elif (len(yearx) != 5) or (not yearx[1:].isdigit()):
        raise ValueError(
            "y followed by 4-digit year expected. e.g., 'y2024'(' not included, y2024 only)"
        )
        return

    ### organization::wash 'usn' + 'name_fr' to confirm whether all are unique.
    gdtree = OrganizationDB(yearx)
    # organization.wash()
    key2wash = ["usn", "name_fr"]
    logging.info(f"coll={gdtree.coll}::washing data, key-to-wash={key2wash}")
    result_tree = gdtree.wash(key2wash)
    for key in key2wash:
        if len(result_tree[key]) > 0:
            logging.warning(
                f"coll={gdtree.coll}::field={key}, value is NOT unique. Fix it then try again. {len(result_tree[key])} value(s) are duplicated: {result_tree[key]}"
            )
        else:
            logging.info(f"coll={gdtree.coll}::field={key}, all value are unique.")

    ### student::wash 'sid'  to confirm whether all are unique.
    gdtree = StudentYearDB(yearx)
    key2wash = ["sid"]
    # key2wash = ["sid", "name", "organization_fr"]
    logging.info(f"coll={gdtree.coll}::washing data, key-to-wash={key2wash}")
    result_tree = gdtree.wash(key2wash)
    for key in key2wash:
        if len(result_tree[key]) > 0:
            logging.warning(
                f"coll={gdtree.coll}::field={key}, value is NOT unique. Fix it then try again. {len(result_tree[key])} value(s) are duplicated: {result_tree[key]}"
            )
        else:
            logging.info(f"coll={gdtree.coll}::field={key}, all value are unique.")

    return


###
def export_data(yearx):
    ###

    orgv2_export = OrgV2Export()
    logging.info(f"{orgv2_export.coll} :: export")
    orgv2_export.load_db()
    logging.info(f"{orgv2_export.coll} :: load db done.")
    orgv2_export.setf_pinyin()
    logging.info(f"{orgv2_export.coll} :: set pinyin done.")
    orgv2_export.set_xlist()
    orgv2_export.sort_xlist()
    # orgv2_export.show_xlist(countx=100)

    stuv2_export = StuV2Export(yearx)
    stuv2_export.load_db()
    # print(f"size={stuv2_export.size}")
    # stuv2_export.show_tree(countx=10)

    result = orgv2_export.export2xlsx("usn")
    # result = orgv2_export.export2xlsx("usn", yearx, stuv2_export.tree)
    logging.info(
        f"{orgv2_export.coll} :: {result} doc(s) exported to '{orgv2_export.fpath}', sheet '{orgv2_export.sheet}'."
    )

    return


###
def main():

    # yearx_list = ["y2025", "y2024", "y2020", "y2021", "y2022", "y2023"]
    yearx_list = ["y2024"]

    org4exp = OrgV3Export(yearx_list)
    logging.info(f"{org4exp.coll} :: exporting docs to excel ...'")
    org4exp.load_db()
    print(f"{org4exp.coll} :: size = {org4exp.size}")

    stu4pick = Stu4Pick("y2024")
    stu4pick.load_db({"degree_year": "bachelor-y2024"})

    result = org4exp.post2fp(stu4pick.tree)
    logging.info(f"{org4exp.coll} :: {result} doc(s) export to '{org4exp.to_fpath}.'")

    return

    yearx = "y2024"

    stu4exp = StuV2Export(yearx)
    logging.info(f"{stu4exp.coll} :: exporting docs to excel ...'")
    stu4exp.load_db()
    print(f"{stu4exp.coll} :: size = {stu4exp.size}")
    result = stu4exp.post2fp()
    logging.info(f"{stu4exp.coll} :: {result} doc(s) export to '{stu4exp.to_fpath}.'")

    return

    ###

    while True:
        print(" " * 36)
        print("Welcome to Remai. Make a choice:")
        print("=" * 36)
        print("1. set tags for organization")
        print("2. upload excel to db")
        print("3. copy field 'usn' from 'organization')")
        print("4. wash data")
        print("5. add more organization")
        print("6. export data")
        print("=" * 36)

        choice = input("Your choice(1/2/3/4/5/6, q=quit): ")
        if choice not in ["1", "2", "3", "4", "5", "6", "q", "Q"]:
            continue

        if choice in ["q", "Q"]:
            logging.info(">>> The End <<<")
            return

        if choice in ["1", "2", "3", "4", "6"]:
            yearx = input("Input year (y followed by 4-digit, e.g. y2024): ")
            if isinstance(yearx, str) is False:
                raise TypeError(
                    "string expected. e.g., 'y2024'(' not included, y2024 only)"
                )
                return
            elif (len(yearx) != 5) or (not yearx[1:].isdigit()):
                raise ValueError(
                    "y followed by 4-digit year expected. e.g., 'y2024'(' not included, y2024 only)"
                )
                return

        if choice == "1":
            set_tag(yearx)

        elif choice == "2":
            upload_frxx(yearx)

        elif choice == "3":
            set_usn(yearx)

        elif choice == "4":
            wash_data(yearx)

        elif choice == "5":
            add_organization()

        elif choice == "6":
            export_data(yearx)


###
if __name__ == "__main__":
    main()


"""
    db.old_collection.aggregate([
    {
        $project: {
        // 指定要复制到新集合的字段，并进行类型转换
        field1String: { $toString: "$field1" }, // 将 field1 字段从数字转换成字符串
        field2String: { $toString: "$field2" }, // 将 field2 字段从数字转换成字符串
        // 如果还有其他字段需要保留且不做转换，可以直接指定
        otherField: 1,
        // _id 字段的处理，根据需要选择是否包含
        _id: 0 // 如果不想包含 _id 字段，则设置为 0
        // 或者 _id: 1 // 如果想包含 _id 字段，则设置为 1
        }
    },
    {
        $out: "new_collection" // 将转换后的结果输出到新集合
    }
    ])
"""
"""
///mongodb shell
db.my_collection.aggregate([
    {
        $group: {
            _id: "$my_field", // 按 my_field 字段的值进行分组
            count: { $sum: 1 } // 计算每个组的文档数量
        }
    }
])

"""
### amend something for collection 'organization'
# """

# make a copy
# db.organization.aggregate([{ $match: {} }, { $out: "bk_organization_250211" }])

# bk_organization_250211:: sn --> usn
# db.bk_organization_250211.updateMany({},{$rename:{"sn":"usn"}})

# bk_organization_250211:: hierarchy_psn --> h_psn
# db.bk_organization_250211.updateMany({},{$rename:{"hierarchy_psn":"h_psn"}})

# bk_organization_250211:: update value of h_psn
# db.bk_organization_250211.updateMany({"h_psn": 0},{$set:{"h_psn":"root-node"}})
# db.bk_organization_250211.updateMany({"h_psn": ""},{$set:{"h_psn":"root-node"}})

# bk_organization_250211:: move sn of root_node to usn
# db.bk_organization_250211.updateMany({},{$rename:{"root_node.sn":"root_node.usn"}})

# bk_organization_250211:: copy and transfer to bk_organization_250211_1

# update student_y2023
# db.student_y2023.aggregate([{ $match: {} }, { $out: "student_y2023_250215" }])
# db.student_y2023.updateMany({},{$set:{"doc_ops.update_by":"back-end-gdv2", "doc_ops.update_time":ISODate('2025-02-12T12:12:12.000Z')}})
# db.student_y2023.updateMany({},{$set:{"doc_ops.create_by":"back-end-gdv2", "doc_ops.create_time":ISODate('2025-02-12T12:12:12.000Z')}})


"""
db.bk_organization_250211.aggregate([
    {
        $project: {
            "usn":1,
            "name":1,
            "name_fr":1,
            "name_aka":1,
            "category":1,
            "h_psn":1,
            "h_comments":1,
            "root_node":1,
            "nht_period":1,
            
            "egc_tagx.glb500": "$glb500",
            "egc_tagx.chn500": "$chn500",
            "egc_tagx.soe": "$soe",
            "egc_tagx.plc": "$plc",
            "egc_tagx.nht": "$nht",
            "egc_tagx.isc100":"$isc100",
            "egc_tagx.fic100_svc": "$fic100_svc",
            "egc_tagx.fic500_all": "$fic500_all",
            "egc_tagx.fic500_mfg": "$fic500_mfg",

            "_id": 0
        }
    },
    {
        $out: "bk_organization_250211_1" 
    }
]);
"""

# organization::update
# db.organization.aggregate([{ $match: {} }, { $out: "bk_organization_0_250211" }])
# db.bk_organization_250211_1.aggregate([{ $match: {} }, { $out: "organization" }])

# find
# db.list_chn500_y2023.find( {"doc_ops.update_time": {$gt: ISODate('2025-02-12T16:14:00Z')}})


"""
# delimiter is tab instead of comma, which will be good for exporting field 'h_comments' -- 不支持哈

mongoexport --db remai --collection organization --type=csv --out=export-organization-250215.csv --fields=usn,name,name_fr,category,h_psn,root_node.usn,root_node.name_fr,nht_period,egc_tagx.glb500.y2023,egc_tagx.chn500.y2023,egc_tagx.soe.y2023,egc_tagx.plc.y2023,egc_tagx.nht.y2023,egc_tagx.isc100.y2023,egc_tagx.fic500_all.y2023,egc_tagx.fic500_mfg.y2023,egc_tagx.fic100_svc.y2023,h_comments

mongoexport --db remai --collection organization --type=json --out=export-organization-250213.json --fields=usn,name,name_fr,category,h_psn,root_node.usn,root_node.name_fr,nht_period,egc_tagx.glb500.y2023,h_comments

mongoexport --db remai --collection org_fr_br --type=csv --out=export-org-fr-br-250227.csv --query='{"usn": "nf-404"}' --fields=usn,name_fr,usci_org,category

mongoexport --db remai --collection org_fr_2b --type=csv --out=export-org-fr-2b-250226-to-organization.csv  --fields=usn,name,name_fr,usci_org,category,h_psn

mongoexport --db remai --collection org_fr_2b --type=csv --out=export-org-fr-2b-250226-root.csv --query='{"h_psn": "i-root"}' --fields=usn,name,name_fr,usci_org,category,h_psn,parent_node

mongoexport --db remai --collection org_fr_2b --type=csv --out=export-org-fr-2b-250226-child.csv --query='{"h_psn": {"$ne":"i-root"}}' --fields=usn,name,name_fr,usci_org,category,h_psn,parent_node.name_fr,parent_node.usn

mongoimport --db=remai --collection=org_fr_2b --type=csv --headerline --file=org-fr-2b-250226.csv
mongoimport --db=remai --collection=org_fr_2b --type=csv --headerline --file=in-org-fr-2b-250227.csv
mongoexport --uri="mongodb://username:password@localhost:27017/" --db=yourDatabase --collection=users  --out=output.json
"""


# // 在目标集合上执行聚合查询
"""
db.organization.aggregate([
    {
        $match: {}
    },
    {
        $project: {
            _id: 0,
            "usn": 1
            
        }
    },
    {
        $sort: {
            "usn": -1
        }
    }
]);
"""
"""
db.organization.aggregate([
    {
        $match: {}
    },
    {
        $project: {
            _id: 0,"usn":1,"name_fr":1,"h_psn":1,"parent_node":1,
        }
    },
    {
        $sort: {
            "usn": -1
        }
    }
]);

db.org_pchild.aggregate([
    {
        $match: {}
    },
    {
        $project: {
            _id: 0,"usn":1,"name_fr":1,"parent_node":1,
        }
    },
    {
        $sort: {
            "name_fr": 1
        },
        {
        $collation: {
            locale: "zh", 
            strength: 2,   
        }
    }
]);

db.organization.aggregate([
    {
        $project: {
            "name_fr":1,
            "usn":1,
            _id: 0 
        }
    },
    {
        $sort: {
           "name_fr": 1 
        }
    },
    {
        $collation: {
            locale: "zh", 
            strength: 2   
        }
    }
]);
"""
##
# 250219：y2024， 第2次运行，organization 也被全部更新了。期望：是不必更新db的。待查bug。
# 已解决。抽取某一年的数据，即可。
