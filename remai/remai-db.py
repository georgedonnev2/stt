#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, datetime, timedelta, timezone
import pandas as pd
import random
import sys
from pymongo import MongoClient, UpdateOne, InsertOne

import logging

logging.basicConfig(level=logging.INFO)

### --------------------------------------------------------------------------------------------------------------------
### some definition
### --------------------------------------------------------------------------------------------------------------------

FV_NOT_FOUND = "nf-404"
FV_ROOT_NODE = "i-root"


# 《财富》中国500强企业
list_chn500_yxxxx_schema = {
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
}


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
                v["usn"] = NOT_FOUND
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
                v["usn"] = NOT_FOUND
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
                    "nht_period": docx["nht_period"],
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
            if len(egc_tagx) == len(vx["egc_tagx"]):
                for tag, mark in vx["egc_tagx"].items():
                    if egc_tagx.get(tag, {}).get(self.year) != mark[self.year]:
                        __tmark_eq = False
            else:
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


### --------------------------------------------------------------------------------------------------------------------
### tools
### --------------------------------------------------------------------------------------------------------------------
#


###
def upload_frxx(yearx):

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
def main():

    # upload_frxx()
    # return

    # set_tag("y2023")
    # return

    while True:
        print(" " * 36)
        print("Welcome to Remai. Make a choice:")
        print("=" * 36)
        print("1. set tags for organization")
        print("2. upload excel to db.")
        print("3. copy field 'usn' from 'organization')")
        print("4. wash data")
        print("=" * 36)

        choice = input("Your choice(1/2/3/4, q=quit): ")
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

        if choice == "2":
            upload_frxx(yearx)

        if choice == "3":
            set_usn(yearx)

        elif choice == "4":
            wash_data(yearx)

        elif choice in ["q", "Q"]:
            logging.info(">>> The End <<<")
            return


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
    ])
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

"""


##
# 250219：y2024， 第2次运行，organization 也被全部更新了。期望：是不必更新db的。待查bug。
