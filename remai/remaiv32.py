#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, datetime, timedelta, timezone
import json
import os
import pandas as pd
from pathlib import Path
import argparse

import random
import re
import sys
from pymongo import MongoClient, UpdateOne, InsertOne
from pypinyin import pinyin, Style

import logging
from logging.handlers import RotatingFileHandler

# ------------------------------------------------------------------------------

### config logging
current_directory = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(current_directory, "log/remai.log")

# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
formatter = logging.Formatter(
    fmt="%(asctime)s - %(lineno)d - %(levelname)s - %(message)s",
    datefmt="%m-%d %H:%M:%S",
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# 100MB max. for each log file, 5 files at most, then 1st file is to be overrided.
rotating_file_handler = RotatingFileHandler(
    log_file_path, maxBytes=100 * 1024 * 1024, backupCount=5
)
rotating_file_handler.setLevel(logging.INFO)
rotating_file_handler.setFormatter(formatter)

rlog = logging.getLogger(__name__)
rlog.setLevel(logging.INFO)
rlog.addHandler(console_handler)
rlog.addHandler(rotating_file_handler)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

FV_NOT_FOUND = "nf-404"
FV_ROOT_NODE = "i-root"
FV_NOT_APP = "not-applicable"

HOME_DB = "remaiv3"


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


def show_t(tree, count=10, show="all", cr="yes"):
    tree_info = ""
    # print("=" * 36)
    __count = 1
    for k, v in tree.items():
        if cr == "yes":
            tree_info += "\n"

        match show:
            case "k":
                tree_info += f"[#{__count}]::k={k} "
            case "v":
                tree_info += f"[#{__count}]::v={v} "
            case _:
                tree_info += f"[#{__count}]::k={k},v={v} "

        if cr == "yes":
            tree_info += "\n"
            tree_info += "-" * 72

        __count += 1
        if __count > count:
            break
    return tree_info


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class RemaiConfig:

    ###
    def __init__(self):

        file_path = __file__
        file_name_with_ext = os.path.basename(file_path)
        file_name_without_ext, file_extension = os.path.splitext(file_name_with_ext)

        self.__fn = file_name_without_ext + ".json"
        self.__config = {}

        return

    ###
    def load(self):
        with open(self.fn, encoding="utf-8") as file:
            self.__config = json.load(file)

        # print(f"config=#{self.__config}#")
        return

    @property
    def expx_fpath(self):
        return self.__config["toxls"]["expx_fpath"]

    @property
    def fn(self):
        return self.__fn

    @fn.setter
    def fn(self, v):
        self.__fn = v
        return

    @property
    def listx_fpath(self):
        return self.__config["washd"]["listx_fpath"]

    @property
    def toxls_pick(self):
        return self.__config["toxls"]["pick"]

    @property
    def toxls_xyears(self):
        return self.__config["toxls"]["xyears"]

    @property
    def stu_fpath(self):
        return self.__config["washd"]["stu_fpath"]

    @property
    def usci_fpath(self):
        return self.__config["washd"]["usci_fpath"]

    @property
    def qbank_fpath(self):
        return self.__config["washd"]["qbank_fpath"]

    @property
    def year(self):
        return self.__config["year"]


###
remaicfg = RemaiConfig()


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class RemAi:

    ###
    def __init__(self, arg):

        self.__coll = arg["coll"]
        self.__year = arg.get("year", None)
        self.__tree = {}  # {}

        # tag 'organization' & 'student'
        self.__tag = arg.get("tag", None)
        self.__mark = arg.get("mark", None)
        self.__tag_root = arg.get("tag_root", True)
        self.__tag_count = {"hit": {}, "h_set": 0, "nf_404": 0}

        # add organization
        self.__flist = []  # list of file path
        self.__map_fp2t = arg.get("map_fp2t", {})

        self.__xlist = []  # list of doc

        self.__map_db2t = arg.get("map_db2t", {})

        self.__map_t2fp = arg.get("map_t2fp", {})
        self.__to_fpath = arg.get("to_fpath", "export.xlsx")
        self.__to_sheet = arg.get("to_sheet", "sheet1")

        self.__map_db2db = arg.get("map_db2db", {})

        return

    ### get list of files in folder
    # file_key in ["工商注册信息.xls", "股东信息.xls"]
    def get_flist(self, folder, file_key):

        if folder == "":
            raise ValueError("Name of folder is empty. Fix it and try again.")

        folder_path = Path(folder)
        # print(f"folder={folder}, folder_path={folder_path}")
        for fp in folder_path.iterdir():
            if fp.is_file() == False:
                continue
            if fp.name[:1] == "~":  # ignore office temp file
                continue
            if file_key in fp.name:
                self.flist.append(fp)

        return

    ###
    def get_mark(self, k_org, v_org):
        if v_org["oname_fr"] in self.tree:
            return self.mark[0]
        return ""

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

    # fake definition if no defintion in child class
    def get_year_fp(self, xls=None):
        return self.year

    ### insert into db
    # key_field : name of key field
    # tree4chk : to compare with to check if data already exist in db.
    def insert2db(self, key_field, tree4chk, tree4post=None):

        client = MongoClient()
        db = client.get_database(HOME_DB)
        # print(f"db={db}")

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

    ### load into tree from db
    def load_db(self, queryx={}, xmap=None, tree4load=None):

        # print(f"load_db :: query = #{queryx}#, tree4load = #{tree4load}#")
        self.tree = {}  # clear
        if xmap is None:
            mapx = self.map_db2t
        else:
            mapx = xmap

        client = MongoClient()
        db = client.get_database(HOME_DB)

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

    ###
    def load_fp(self):
        # list_xls: [{k1:v11,k2:v12,...},{k1:v21,k2:v22,...},...]

        c4fp = 0
        fp = 0
        if len(self.flist) == 0:
            print(f"len-of-flist={len(self.flist)}")
            return c4fp, fp

        self.tree = {}

        for fp in self.flist:
            # print(f">>> fp = {fp}")
            df_temp = pd.read_excel(fp, nrows=0)
            converters_dict = {col: str for col in df_temp.columns}

            df = pd.read_excel(fp, converters=converters_dict)
            # print("-" * 144)
            # print(f"df={df}")
            # print("-" * 144)

            df = df.fillna("")  # to be amended for different value for diff column.
            xls = df.to_dict(orient="records")

            #
            year_fp = self.get_year_fp(xls)
            if year_fp != self.year:
                logging.warning(
                    f"{fp.name} :: skipped, as '{year_fp}' (file) not match '{self.year}'."
                )
                continue

            # __fp_the_year = fp

            # print(f"read done!")
            # print(f"size of row = {len(xls)}")
            # count = 1
            for row in xls:
                # if count <= 10:
                #     print(f">>> [{count}]row=#{row}#")
                #     count += 1
                # for k, v in row.items():
                #     if v == "nan":
                #         print(f"k={k}, v is na.")
                # row = {k1:v11, k2:v12, ...}

                tmp_v = {}
                __k_dst = ""
                for kmap, vmap in self.map_fp2t.items():
                    __v_dst = self.map_v2v(
                        row[vmap["source"]],
                        vmap.get("format", None),
                    )

                    if vmap.get("key", False) == True:
                        __k_dst = __v_dst
                    else:
                        # tmp_v[kmap] = __v_dst
                        self.set_nested_node(tmp_v, kmap, __v_dst)
                        # tmp_v.update(tmp)

                c4fp += 1
                self.tree.update({__k_dst: tmp_v})

            break  # one file a year. that's it.

        return c4fp, fp

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

    ### export tree to file
    # tree4pick = {usn1:usn_root_node1,usn2:usn_root_node2,...}
    def post2fp(self, tree4pick=None):

        #
        if self.size == 0:
            return 0

        #
        self.set_field2fp()  # define in child class

        # docs exported by sorted key. normally not sorted by usn, by name_fr.
        sorted_key = self.set_sort4fp()  # define in clid class
        if sorted_key == None:  # if set_sort4fp() not defined in child class
            sorted_key = self.tree

        # tree to list
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

        # pick some of list4export to export(list2fp)
        list2fp = list4export

        root4pick = {}  # {root_node.usn:{}}
        if tree4pick != None:  # tree4pick = {usn:{root_node:{usn:xx,name_fr:xx}},...}
            for year, tree in tree4pick.items():

                # set list of root_node.usn for pick
                for k, v in tree.items():
                    if v.get("root_node", None) == None:
                        continue
                    if root4pick.get(year, {}) == {}:
                        root4pick[year] = {}
                    root4pick[year][v["root_node"]["usn"]] = {}

                #
                key_fp = ""
                for k, v in self.map_t2fp.items():
                    if v.get("key", False) == True:
                        key_fp = k
                        break

                # pick some of list4export to export(list2fp)
                list2fp = []
                for docx in list4export:
                    if docx[key_fp] in tree:
                        # print(f"#{count} :: docx[key_field]={docx[key_field]}")
                        # count += 1
                        # if count > 10:
                        #     break
                        docx["hit" + year[-2:]] = "hit"
                    else:
                        docx["hit" + year[-2:]] = FV_NOT_FOUND

                    if docx["root_usn"] not in root4pick[year]:
                        continue

        if tree4pick == None:
            list2fp == list4export
        else:
            for docx in list4export:
                for year, tree in tree4pick.items():
                    if docx["root_usn"] in root4pick[year]:
                        # (docx["hit" + year[-2:]] == "hit")
                        # or (docx["hit" + year[-2:]] == FV_NOT_FOUND)
                        list2fp.append(docx)
                        break

        # count = 1
        # for docx in list2fp:
        #     print(f"#{count} :: doc = {docx}")
        #     count += 1
        #     if count > 10:
        #         break

        # list to excel
        df = pd.DataFrame(list2fp)
        df.to_excel(self.to_fpath, index=False, sheet_name=self.to_sheet)

        return df.shape[0]  # (#) of docs

    ### fake definition if no definition in child class
    def set_field2fp(self):
        pass

    ###
    def set_nested_node(self, nested_node, nested_key, value2set):

        # nested_key likes 'bio.sname'
        keys = nested_key.split(".")
        nlevel = nested_node  # node to be set with nested structure and value

        # set nested structure
        for k in keys[:-1]:  # exclude the last key
            if k not in nlevel:
                nlevel[k] = {}
            nlevel = nlevel[k]

        last_key = keys[-1]
        nlevel[last_key] = value2set

        return

    ### fake definition if no definition in child class
    def set_sort4fp(self):
        return None

    ###
    def setf_egc_tagx(self):
        pass

    ###
    # key = oname_fr
    def setf_usn(self, treex):

        __not_found = {}
        for k, v in self.tree.items():
            if k in treex:
                v["usn"] = treex[k]["usn"]
            else:
                v["usn"] = FV_NOT_FOUND
                # print(f"k={k} not in treex.")
                __not_found[k] = {}

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
    def show_h(self, node):

        root_node = self.tree[node]["root_node"]["usn"]
        tree_info = ""
        tree_info += self.__show_h(root_node)
        return tree_info

    def __show_h(self, node, loh=1):

        tree_info = ""

        match loh:
            case 1:
                head_info = "*"
            case 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9:
                if len(self.tree[node]["child_node"]) == 0:
                    head_info = "|" * loh
                else:
                    head_info = "|" + "+" * (loh - 1)
        tree_info += head_info + " "

        tree_info += f"({node}){self.tree[node]['oname_fr']}" + "\n"
        for child in self.tree[node]["child_node"]:
            tree_info += self.__show_h(child, loh=loh + 1)

        return tree_info

    ###
    def show_t(self, count=10, show="all", cr="yes"):
        tree_info = ""
        # print("=" * 36)
        __count = 1
        for k, v in self.tree.items():
            if cr == "yes":
                tree_info += "\n"

            match show:
                case "k":
                    tree_info += f"[#{__count}]::k={k} "
                case "v":
                    tree_info += f"[#{__count}]::v={v} "
                case _:
                    tree_info += f"[#{__count}]::k={k},v={v} "

            if cr == "yes":
                tree_info += "\n"
                tree_info += "-" * 72

            __count += 1
            if __count > count:
                break
        return tree_info

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
    def update2db(self, key_field, tree4chk=None, tree4post=None):

        client = MongoClient()
        db = client.get_database(HOME_DB)

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
            if tree4chk != None:
                if (k in tree4chk) == False:
                    print(f"update2db:: k={k} not in tree4ck.")
                    continue

                __size = 0
                __identical = 0
                for k1, v1 in v.items():
                    __size += 1
                    v4chk = tree4chk[k].get(k1, None)
                    if v1 == v4chk:
                        __identical += 1

                if __size == __identical:
                    result["exist"] += 1
                    # print(f"== size={__size}, k={k}, vvv={v}，chk={tree4chk[k]}")
                    continue
                else:
                    # print(
                    #     f"xx size={__size}, identical = {__identical}, vvv={v}， chk={tree4chk[k]}"
                    # )
                    pass

            doc2db = {"$set": {}}
            for k1, v1 in v.items():
                doc2db["$set"][k1] = v1

            doc2db["$set"]["doc_ops.update_by"] = "be-gdv2"
            doc2db["$set"]["doc_ops.update_time"] = datetime.now()

            ops4db.append(UpdateOne({key_field: k}, doc2db))

        # count = 1
        # for docx in ops4db:
        #     print(f"#{count} :: {self.coll} :: update2db, docx={docx}.")
        #     count += 1
        #     break

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
    def size(self):
        return len(self.tree)

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

    @year.setter
    def year(self, y):
        self.__year = y
        return

    # --------------------------------------------------------------------------
    @property
    def mark(self):
        return self.__mark

    @property
    def tag(self):
        return self.__tag

    @property
    def tag_count(self):
        return self.__tag_count

    @tag_count.setter
    def tag_count(self, c):
        self.__tag_count = c
        return

    @property
    def tag_root(self):
        return self.__tag_root

    # --------------------------------------------------------------------------
    @property
    def flist(self):
        return self.__flist

    @flist.setter
    def flist(self, fl):
        self.__flist = fl
        return

    @property
    def map_fp2t(self):
        return self.__map_fp2t

    # --------------------------------------------------------------------------
    # --------------------------------------------------------------------------
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
    def to_fpath(self):
        return self.__to_fpath

    @property
    def to_sheet(self):
        return self.__to_sheet

    @property
    def map_db2db(self):
        return self.__map_db2db


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class RemAiv3:

    ###
    def __init__(self, args):

        # some args such as {"coll": "xx", "year":"y2024"}
        self.__args = args

        # xtags. others are in args.
        self.__tree = {}  # {}
        self.__tag_count = {"hit": {}, "h_set": 0, "nf_404": 0}

        # washd..
        self.__flist = []  # list of file path

        self.__xlist = []  # list of doc

        return

    ### get list of files in folder
    # file_key in ["工商注册信息.xls", "股东信息.xls"]
    def get_flist(self, folder, file_key=None):

        if folder == "":
            raise ValueError("Name of folder is empty. Fix it and try again.")
        if file_key is None:
            file_key = self.fpkey

        folder_path = Path(folder)
        # print(f"folder={folder}, folder_path={folder_path}")
        for fp in folder_path.iterdir():
            if fp.is_file() == False:
                continue
            if fp.name[:1] == "~":  # ignore office temp file
                continue
            if file_key in fp.name:
                self.flist.append(fp)

        return

    ###
    def get_mark(self, k_org, v_org):
        if v_org["oname_fr"] in self.tree:
            return self.mark[0]
        return ""

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

    # fake definition if no defintion in child class
    def get_year_fp(self, xls=None):
        return self.year

    ### insert into db
    # key_field : name of key field
    # tree4chk : to compare with to check if data already exist in db.
    def insert2db(self, key_field, tree4chk, tree4post=None):

        client = MongoClient()
        db = client.get_database(HOME_DB)
        # print(f"db={db}")

        ops4db = []
        # result : of db operation,
        # exist : already in db (skip and not inserted.)
        result = {"ops4db": False, "result": None, "exist": 0, "message": ""}
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
            result["message"] = (
                f"{self.coll} :: insert done, [{result['result'].inserted_count}] inserted."
            )
        else:
            result["message"] = f"{self.coll} :: insert done, no doc posted to db."

        client.close()
        return result

    ### load into tree from db
    def load_db(self, queryx={}, xmap=None, tree4load=None):

        # print(f"load_db :: query = #{queryx}#, tree4load = #{tree4load}#")
        self.tree = {}  # clear
        if xmap is None:
            mapx = self.map_db2t
        else:
            mapx = xmap

        client = MongoClient()
        db = client.get_database(HOME_DB)

        query = queryx
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            # print(f"docx={docx}")

            __tmp_v = {}
            for kmap, vmap in mapx.items():
                __v_dst = self.get_nested_node(vmap["source"], docx)
                # __v_dst = self.map_v2v(
                #     docx.get(vmap["source"], None), vmap.get("format", None)
                # )
                if vmap.get("key", False) == True:
                    __k_dst = __v_dst
                else:
                    # __tmp_v[kmap] = __v_dst
                    self.set_nested_node(__tmp_v, kmap, __v_dst)

            # print(f"k={__k_dst}, v={__tmp_v}")
            self.tree.update({__k_dst: __tmp_v})

        client.close()
        return

    ###
    def load_fp(self):
        # list_xls: [{k1:v11,k2:v12,...},{k1:v21,k2:v22,...},...]

        c4fp = 0
        fp = 0
        if len(self.flist) == 0:
            # print(f"len-of-flist={len(self.flist)}")
            return c4fp, fp

        self.tree = {}

        for fp in self.flist:
            # print(f">>> fp = {fp}")
            df_temp = pd.read_excel(fp, nrows=0)
            converters_dict = {col: str for col in df_temp.columns}

            if self.sheet == "":
                df = pd.read_excel(fp, converters=converters_dict)
            else:
                try:
                    df = pd.read_excel(
                        fp, sheet_name=self.sheet, converters=converters_dict
                    )
                except ValueError as e:
                    rlog.exception(f"read {fp} error: {e}\nfix it, then try again.\n")
                    return 0, 0  # c4fp =0, fp=0

            # print("-" * 144)
            # print(f"df={df}")
            # print("-" * 144)

            df = df.fillna("")  # to be amended for different value for diff column.
            xls = df.to_dict(orient="records")

            #
            year_fp = self.get_year_fp(xls)
            if year_fp != self.year:
                logging.warning(
                    f"{fp.name} :: skipped, as '{year_fp}' (file) not match '{self.year}'."
                )
                continue

            # __fp_the_year = fp

            # print(f"read done!")
            # print(f"size of row = {len(xls)}")
            # count = 1
            for row in xls:

                # if count <= 10:
                #     print(f">>> [{count}]row=#{row}#")
                #     count += 1
                # for k, v in row.items():
                #     if v == "nan":
                #         print(f"k={k}, v is na.")
                # row = {k1:v11, k2:v12, ...}
                # print(f"row={row}")

                tmp_v = {}
                __k_dst = ""
                for kmap, vmap in self.map_fp2t.items():
                    # __v_dst = self.map_v2v(
                    #     row[vmap["source"]],
                    #     vmap.get("format", None),
                    # )
                    __v_dst = row.get(vmap["source"], vmap.get("default", None))

                    if vmap.get("key", False) == True:
                        __k_dst = __v_dst
                    else:
                        # tmp_v[kmap] = __v_dst
                        self.set_nested_node(tmp_v, kmap, __v_dst)
                        # tmp_v.update(tmp)

                c4fp += 1
                self.tree.update({__k_dst: tmp_v})

            break  # one file a year. that's it.

        return c4fp, fp

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

    ### export tree to file
    # tree4pick = {usn1:usn_root_node1,usn2:usn_root_node2,...}
    def post2fp(self, tree4pick=None):

        #
        if self.size == 0:
            return 0

        #
        self.set_field2fp()  # define in child class

        # docs exported by sorted key. normally not sorted by usn, by name_fr.
        sorted_key = self.set_sort4fp()  # define in clid class
        if sorted_key == None:  # if set_sort4fp() not defined in child class
            sorted_key = self.tree

        # tree to list
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

        # pick some of list4export to export(list2fp)
        list2fp = list4export

        root4pick = {}  # {root_node.usn:{}}
        if tree4pick != None:  # tree4pick = {usn:{root_node:{usn:xx,name_fr:xx}},...}
            # print(f"tree4pick={tree4pick}")
            for year, tree in tree4pick.items():

                # set list of root_node.usn for pick
                for k, v in tree.items():
                    if v.get("root_node", None) == None:
                        continue
                    if root4pick.get(year, {}) == {}:
                        root4pick[year] = {}

                    print(f"k={k}, v={v}")
                    root4pick[year][v["root_node"]["usn"]] = {}
                # print(f"root4pick={root4pick}")
                #
                key_fp = ""
                for k, v in self.map_t2fp.items():
                    if v.get("key", False) == True:
                        key_fp = k
                        break

                # pick some of list4export to export(list2fp)
                list2fp = []
                for docx in list4export:
                    if docx[key_fp] in tree:
                        # print(f"#{count} :: docx[key_field]={docx[key_field]}")
                        # count += 1
                        # if count > 10:
                        #     break
                        docx["hit" + year[-2:]] = "hit"
                    else:
                        docx["hit" + year[-2:]] = FV_NOT_FOUND

                    if docx["root_usn"] not in root4pick[year]:
                        continue

        if tree4pick == None:
            list2fp == list4export
        else:
            for docx in list4export:
                for year, tree in tree4pick.items():
                    if docx["root_usn"] in root4pick[year]:
                        # (docx["hit" + year[-2:]] == "hit")
                        # or (docx["hit" + year[-2:]] == FV_NOT_FOUND)
                        list2fp.append(docx)
                        break

        # count = 1
        # for docx in list2fp:
        #     print(f"#{count} :: doc = {docx}")
        #     count += 1
        #     if count > 10:
        #         break

        # list to excel
        df = pd.DataFrame(list2fp)
        df.to_excel(self.to_fpath, index=False, sheet_name=self.to_sheet)

        return df.shape[0]  # (#) of docs

    ### fake definition if no definition in child class
    def set_field2fp(self):
        pass

    ###
    def set_nested_node(self, nested_node, nested_key, value2set):

        # nested_key likes 'bio.sname'
        keys = nested_key.split(".")
        nlevel = nested_node  # node to be set with nested structure and value

        # set nested structure
        for k in keys[:-1]:  # exclude the last key
            if k not in nlevel:
                nlevel[k] = {}
            nlevel = nlevel[k]

        last_key = keys[-1]
        nlevel[last_key] = value2set

        return

    ### fake definition if no definition in child class
    def set_sort4fp(self):
        return None

    ###
    def setf_egc_tagx(self):
        pass

    ###
    # key = oname_fr
    def setf_usn(self, treex):

        __not_found = {}
        for k, v in self.tree.items():
            if k in treex:
                v["usn"] = treex[k]["usn"]
            else:
                v["usn"] = FV_NOT_FOUND
                # print(f"k={k} not in treex.")
                __not_found[k] = {}

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
    def show_h(self, node):

        root_node = self.tree[node]["root_node"]["usn"]
        tree_info = ""
        tree_info += self.__show_h(root_node)
        return tree_info

    def __show_h(self, node, loh=1):

        tree_info = ""

        match loh:
            case 1:
                head_info = "*"
            case 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9:
                if len(self.tree[node]["child_node"]) == 0:
                    head_info = "|" * loh
                else:
                    head_info = "|" + "+" * (loh - 1)
        tree_info += head_info + " "

        tree_info += f"({node}){self.tree[node]['oname_fr']}" + "\n"
        for child in self.tree[node]["child_node"]:
            tree_info += self.__show_h(child, loh=loh + 1)

        return tree_info

    ###
    def show_t(self, count=10, show="all", cr="yes"):
        tree_info = ""
        # print("=" * 36)
        __count = 1
        for k, v in self.tree.items():
            if cr == "yes":
                tree_info += "\n"

            match show:
                case "k":
                    tree_info += f"[#{__count}]::k={k} "
                case "v":
                    tree_info += f"[#{__count}]::v={v} "
                case _:
                    tree_info += f"[#{__count}]::k={k},v={v} "

            if cr == "yes":
                tree_info += "\n"
                tree_info += "-" * 72

            __count += 1
            if __count > count:
                break
        return tree_info

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
    def update2db(self, key_field, tree4chk=None, tree4post=None):

        client = MongoClient()
        db = client.get_database(HOME_DB)

        ops4db = []
        # result : of db operation,
        # exist : already in db (skip and not inserted.)
        result = {"ops4db": False, "result": None, "exist": 0, "message": ""}
        if tree4post == None:
            __treex = self.tree
        else:
            __treex = tree4post

        for k, v in __treex.items():

            # k should in. that's fine if not.
            if tree4chk != None:
                if (k in tree4chk) == False:
                    print(f"update2db:: k={k} not in tree4ck.")
                    continue

                __size = 0
                __identical = 0
                for k1, v1 in v.items():
                    __size += 1
                    v4chk = tree4chk[k].get(k1, None)
                    if v1 == v4chk:
                        __identical += 1

                if __size == __identical:
                    result["exist"] += 1
                    # print(f"== size={__size}, k={k}, vvv={v}，chk={tree4chk[k]}")
                    continue
                else:
                    # print(
                    #     f"xx size={__size}, identical = {__identical}, vvv={v}， chk={tree4chk[k]}"
                    # )
                    pass

            doc2db = {"$set": {}}
            for k1, v1 in v.items():
                doc2db["$set"][k1] = v1

            doc2db["$set"]["doc_ops.update_by"] = "be-gdv2"
            doc2db["$set"]["doc_ops.update_time"] = datetime.now()

            ops4db.append(UpdateOne({key_field: k}, doc2db))

        # count = 1
        # for docx in ops4db:
        #     print(f"#{count} :: {self.coll} :: update2db, docx={docx}.")
        #     count += 1
        #     break

        if len(ops4db) > 0:
            result["result"] = db.get_collection(self.coll).bulk_write(ops4db)
            result["ops4db"] = True
            result["message"] = (
                f"{self.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified."
            )
        else:
            result["message"] = f"{self.coll} ::  update done, no doc changed in db.\n"

        client.close()
        return result

    ### property
    # --------------------------------------------------------------------------
    @property
    def coll(self):
        return self.__args.get("coll", "")

    @coll.setter
    def coll(self, v):
        self.__args["coll"] = v

    @property
    def size(self):
        return len(self.tree)

    @property
    def tree(self):
        return self.__tree

    @tree.setter
    def tree(self, t):
        self.__tree = t
        return

    @property
    def year(self):
        return self.__args.get("year", "")

    @year.setter
    def year(self, y):
        self.__args["year"] = y
        return

    # --------------------------------------------------------------------------
    # xtags

    @property
    def mark(self):
        return self.__args.get("mark", "")

    @property
    def tag(self):
        return self.__args.get("tag", "")

    @property
    def tag_count(self):
        return self.__tag_count

    @tag_count.setter
    def tag_count(self, c):
        self.__tag_count = c
        return

    @property
    def tag_root(self):
        return self.__args.get("tag_root", True)

    # --------------------------------------------------------------------------
    # washd

    @property
    def coll_base(self):
        return self.__args.get("coll_base", "")

    @property
    def flist(self):
        return self.__flist

    @flist.setter
    def flist(self, v):
        self.__flist = v
        return

    @property
    def fpkey(self):
        return self.__args.get("fpkey", "")

    @fpkey.setter
    def fpkey(self, v):
        self.__args["fpkey"] = v

    @property
    def map_fp2t(self):
        return self.__args.get("map_fp2t", {})

    @property
    def sheet(self):
        return self.__args.get("sheet", "")

    @sheet.setter
    def sheet(self, v):
        self.__args["sheet"] = v

    # --------------------------------------------------------------------------
    @property
    def xlist(self):
        return self.__xlist

    @xlist.setter
    def xlist(self, l):
        self.__xlist = l
        return

    @property
    def map_db2t(self):
        return self.__args.get("map_db2t", {})

    @property
    def map_t2fp(self):
        return self.__args.get("map_t2fp", {})

    @map_t2fp.setter
    def map_t2fp(self, v):
        self.__args["map_t2fp"] = v

    @property
    def to_fpath(self):
        return self.__args.get("to_fpath", "export.xlsx")

    @property
    def to_sheet(self):
        return self.__args.get("to_sheet", "sheet1")

    @property
    def map_db2db(self):
        return self.__args.get("map_db2db", {})


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListChn500(RemAiv3):
    ###
    def __init__(self, year, argx=None):

        if argx is not None:
            __coll = argx.get("coll", "list_chn500_" + year)
        else:
            __coll = "list_chn500_" + year

        arg = {
            "coll": __coll,
            "year": year,
            "tag": "chn500",
            "mark": ("chn",),
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
            },
            #
            "map_fp2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "rank": {"source": "rank", "format": "int"},
                "oname": {"source": "oname"},
                "revenue": {"source": "revenue", "format": "float"},
                "profit": {"source": "profit", "format": "float"},
            },
        }

        super().__init__(arg)
        return

    ###
    def update2db(self, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().update2db("oname_fr", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListChn500Imp(ListChn500):

    ###
    def __init__(self, year, argx=None):
        argx = {"coll": "list_chn500_" + year + "_" + datetime.now().strftime("%y%m")}
        super().__init__(year, argx)

    ###
    def get_flist(self, folder):
        file_key = "list_chn500_" + self.year + ".xlsx"
        return super().get_flist(folder, file_key)

    def insert2db(self, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().insert2db("oname_fr", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListFic100Svc(RemAiv3):
    ###
    def __init__(self, year):

        __coll = "list_fic100_svc_" + year
        arg = {
            "coll": __coll,
            "year": year,
            "tag": "fic100_svc",
            "mark": ("fic",),
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
            },
            #
            "map_fp2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "rank": {"source": "rank"},
                "hq_location": {"source": "hq_location"},
                "revenue": {"source": "revenue"},
                "oname": {"source": "oname"},
                "list_year": {"source": "list_year"},
                # "usn": {},
            },
        }

        super().__init__(arg)
        return

    ###
    def insert2db(self, key_field=None, tree4chk=None, tree4post=None):
        if key_field is None:
            key_field = "oname_fr"
        if tree4chk is None:
            tmp = self.__class__(self.year)
            tmp.load_db()
            tree4chk = tmp.tree
        return super().insert2db(key_field, tree4chk, tree4post)

    ###
    def update2db(self, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().update2db("oname_fr", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListFic500All(RemAiv3):
    ###
    def __init__(self, year):

        __coll = "list_fic500_all_" + year
        arg = {
            "coll": __coll,
            "year": year,
            "tag": "fic500_all",
            "mark": ("fic",),
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
            },
            #
            "map_fp2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "rank": {"source": "rank"},
                "hq_location": {"source": "hq_location"},
                "revenue": {"source": "revenue"},
                "oname": {"source": "oname"},
                "list_year": {"source": "list_year"},
                # "usn": {},
            },
        }

        super().__init__(arg)
        return

    ###
    def insert2db(self, key_field=None, tree4chk=None, tree4post=None):
        if key_field is None:
            key_field = "oname_fr"
        if tree4chk is None:
            tmp = self.__class__(self.year)
            tmp.load_db()
            tree4chk = tmp.tree
        return super().insert2db(key_field, tree4chk, tree4post)

    ###
    def update2db(self, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().update2db("oname_fr", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListFic500Mfg(RemAiv3):
    ###
    def __init__(self, year):

        __coll = "list_fic500_mfg_" + year
        arg = {
            "coll": __coll,
            "year": year,
            "tag": "fic500_mfg",
            "mark": ("fic",),
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
            },
            #
            "map_fp2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "rank": {"source": "rank"},
                "hq_location": {"source": "hq_location"},
                "revenue": {"source": "revenue"},
                "oname": {"source": "oname"},
                "list_year": {"source": "list_year"},
                # "usn": {},
            },
        }

        super().__init__(arg)
        return

    ###
    def insert2db(self, key_field=None, tree4chk=None, tree4post=None):
        if key_field is None:
            key_field = "oname_fr"
        if tree4chk is None:
            tmp = self.__class__(self.year)
            tmp.load_db()
            tree4chk = tmp.tree
        return super().insert2db(key_field, tree4chk, tree4post)

    ###
    def update2db(self, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().update2db("oname_fr", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListGlb500(RemAiv3):
    ###
    def __init__(self, year, argx=None):

        if argx is not None:
            __coll = argx.get("coll", "list_glb500_" + year)
        else:
            __coll = "list_glb500_" + year

        arg = {
            "coll": __coll,
            "year": year,
            "tag": "glb500",
            "mark": ("glb",),
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
                "country": {"source": "country"},
            },
            #
            "map_fp2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "rank": {"source": "rank", "format": "int"},
                "oname": {"source": "oname"},
                "country": {"source": "country"},
                "revenue": {"source": "revenue", "format": "float"},
                "profit": {"source": "profit", "format": "float"},
            },
        }

        super().__init__(arg)
        return

    ###
    # key = oname_fr
    def setf_usn(self, treex):

        __not_found = {}
        # count = 1
        for k, v in self.tree.items():
            # print(f"#{count}::k={k},v={v}")
            # count += 1

            if k in treex:
                v["usn"] = treex[k]["usn"]

            else:
                # print(f"k=#{k}#,country={v['country']}")
                v["usn"] = FV_NOT_FOUND
                if v["country"] == "中国":
                    __not_found[k] = {}
                # print(f"k={k} not in treex.")

        return __not_found

    ###
    def update2db(self, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().update2db("oname_fr", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListGlb500Imp(ListGlb500):

    ###
    def __init__(self, year, argx=None):
        argx = {"coll": "list_glb500_" + year + "_" + datetime.now().strftime("%y%m")}
        super().__init__(year, argx)

    ###
    def get_flist(self, folder):
        file_key = "list_glb500_" + self.year + ".xlsx"
        return super().get_flist(folder, file_key)

    def insert2db(self, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().insert2db("oname_fr", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListIsc100(RemAiv3):
    ###
    def __init__(self, year):

        __coll = "list_isc100_" + year
        arg = {
            "coll": __coll,
            "year": year,
            "tag": "isc100",
            "mark": ("isc",),
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
            },
            #
            "map_fp2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "no": {"source": "no"},
                "business_brand": {"source": "business_brand"},
                "hq_location": {"source": "hq_location"},
                "oname": {"source": "oname"},
                "list_year": {"source": "list_year"},
                # "usn": {},
            },
        }

        super().__init__(arg)
        return

    ###
    def insert2db(self, key_field=None, tree4chk=None, tree4post=None):
        if key_field is None:
            key_field = "oname_fr"
        if tree4chk is None:
            tmp = self.__class__(self.year)
            tmp.load_db()
            tree4chk = tmp.tree
        return super().insert2db(key_field, tree4chk, tree4post)

    ###
    def update2db(self, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().update2db("oname_fr", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListNae(RemAi):
    # Named Account of Employee / Enterprise

    ###
    def __init__(self, year):

        arg = {
            "coll": "list_nae",
            "year": year,
            "tag": "nae",
            "mark": ("nae",),
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
                "category": {"source": "category"},
            },
        }

        super().__init__(arg)
        return


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListPlc(RemAiv3):
    ###
    def __init__(self, year):

        arg = {
            "coll": "list_plc",
            "year": year,
            "tag": "plc",
            "mark": ("plc", "lcx"),
            "tag_root": False,
            # to be amended as one company might has 2 code. //250308
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "code": {"source": "code"},
                "name_aka": {"source": "name_aka"},
                "section": {"source": "section"},
                "region": {"source": "region"},
                "eff_date": {"source": "eff_date"},
                "exp_date": {"source": "exp_date"},
            },
        }

        super().__init__(arg)
        return


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListSoe(RemAiv3):
    ###
    def __init__(self, year):

        arg = {
            "coll": "list_soe",
            "year": year,
            "tag": "soe",
            "mark": ("soe", "prov", "city"),
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
                "category": {"source": "category"},
            },
        }

        super().__init__(arg)
        return

    ###
    # key = oname_fr
    def setf_usn(self, treex):

        __not_found = {}
        for k, v in self.tree.items():

            if k in treex:
                v["usn"] = treex[k]["usn"]
            elif v["category"] == "中央文化企业":
                continue
            else:
                v["usn"] = FV_NOT_FOUND
                # print(f"k={k} not in treex.")
                __not_found[k] = {}

        return __not_found

    ###
    def update2db(self, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().update2db("oname_fr", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class NationalHte(RemAiv3):
    ###
    def __init__(self, year):

        arg = {
            "coll": None,
            "year": year,
            "tag": "nht",
            "mark": ("nht",),
            "tag_root": False,
            #
            "map_db2t": {},
        }

        super().__init__(arg)
        return

    ###
    def get_mark(self, k_org, v_org):  # override parent class
        ydate = date.fromisoformat(self.year[1:] + "0101")  # year = y2024

        v = self.tree.get(k_org, {})
        if v == {}:
            return ""
        if v.get("nht_period", {}) == {}:
            return ""
        if v.get("nht_period", None) is None:
            return ""

        # print(f"k={k_org}, v={v}, v_org={v_org}")
        for eff_date, exp_date in v["nht_period"].items():
            if eff_date != "":
                if (date.fromisoformat(eff_date) <= ydate) and (
                    ydate <= date.fromisoformat(exp_date)
                ):
                    return self.mark[0]
        return ""

    ###
    def load_db(self, tree4load=None):  # override parent class
        self.tree = {}

        for usn, v in tree4load.items():
            # print(f"k={usn}, v={v}")
            self.tree[usn] = {
                "oname_fr": v["oname_fr"],
                "nht_period": v.get("nht_period", {}),
            }

        return


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------


class Organization(RemAi):

    ###
    def __init__(self, *xyears):

        # self.__tag_count = {"hit": 0, "h_set": 0, "nf_404": 0}

        arg = {
            "coll": "organization",
            "years": xyears,
            "year": xyears[0],
            "to_fpath": "exp-organization-" + datetime.now().strftime("%m%d") + ".xlsx",
            "map_db2t": {
                "usn": {"source": "usn", "key": True},  # f01
                "oname": {"source": "oname"},  # f02
                "oname_fr": {"source": "oname_fr"},  # f03
                "usci_org_fr": {"source": "usci_org_fr"},  # f04
                "category": {"source": "category"},  # f05
                "category_fr": {"source": "category_fr"},  # f06
                "type_org_fr": {"source": "type_org_fr"},  # f07
                "industry_org_fr": {"source": "industry_org_fr"},  # f08
                "location_org_fr": {"source": "location_org_fr"},  # f09
                "former_name_fr": {"source": "former_name_fr"},
                "parent_node": {"source": "parent_node"},  # f10 - f11
                "root_node": {"source": "root_node"},  # f12 - f13
                "child_node": {"source": "child_node"},  # f15 (len)
                "egc_tagx": {"source": "egc_tagx"},  # fxx
                "h_psn": {"source": "h_psn"},
                "nht_period": {"source": "nht_period"},
            },
            "map_t2fp": {
                "usn": {"source": "usn", "key": True},
                "oname": {"source": "oname"},
                "oname_fr": {"source": "oname_fr"},
                "usci_org_fr": {"source": "usci_org_fr"},
                "category": {"source": "category"},
                "category_fr": {"source": "category_fr"},
                "type_org_fr": {"source": "type_org_fr"},
                "industry_org_fr": {"source": "industry_org_fr"},
                "location_org_fr": {"source": "location_org_fr"},
                "parent_usn": {"source": "parent_node.usn"},
                "parent_oname_fr": {"source": "parent_node.oname_fr"},
                "root_usn": {"source": "root_node.usn"},
                "root_oname_fr": {"source": "root_node.oname_fr"},
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

        yearx_list = list(xyears)
        # print(f"xyears='{xyears}'")
        # print(f"yearx_list='{yearx_list}'")
        yearx_list.sort()  # e.g., yearx_list = ["y2021", "y2022"]
        # print(f"yearx_list='{yearx_list}'")

        for __to_file, __in_tree in __map_egc_tagx.items():
            for yearx in yearx_list:
                # print(f"yearx='{yearx}'")
                arg["map_t2fp"][__to_file + yearx[-2:]] = {
                    "source": "egc_tagx." + __in_tree + "." + yearx
                }

        super().__init__(arg)
        return

    #
    def insert2db(self, tree4post=None):
        org4chk = Organization(self.year)
        org4chk.load_db()
        return super().insert2db("usn", org4chk.tree, tree4post)

    ###
    def setf_egc_tagx(self, node, tag, mark):
        if self.tree[node]["egc_tagx"] is None:
            self.tree[node]["egc_tagx"] = {}
        if self.tree[node]["egc_tagx"].get(tag, {}) == {}:
            self.tree[node]["egc_tagx"][tag] = {}
        self.tree[node]["egc_tagx"][tag][self.year] = mark
        return

    def get_egc_tagx(self, node, tag):
        return self.tree[node]["egc_tagx"].get(tag, {}).get(self.year, "")

    ### set field 'egc_tagx' of root node
    def setf_egc_tagx_root(self, tag, xmark):

        def __setf_egc_tagx_root(node, root_node, tag, xmark):
            # e.g., xmark = ["soe", "prov", "city"]
            mark = self.get_egc_tagx(node, tag)
            if mark in xmark:
                if self.get_egc_tagx(root_node, tag) != xmark[0]:
                    self.setf_egc_tagx(root_node, tag, "[" + mark + "]")
            for child in self.tree[node]["child_node"]:
                __setf_egc_tagx_root(child, root_node, tag, xmark)
            return

        result = {}
        for k, v in self.tree.items():
            if v["h_psn"] == FV_ROOT_NODE:  # if node is root node
                __mark_before = self.get_egc_tagx(k, tag)
                __setf_egc_tagx_root(k, k, tag, xmark)
                if self.get_egc_tagx(k, tag) != __mark_before:
                    result[k] = v["oname_fr"]

        return result

    ### set rest (still blank, not set yet)
    def setf_egc_tagx_rest(self, node, tag, mark, xmark):

        xmark2 = [f"[{key}]" for key in xmark]  # add []
        xmark2 = xmark2 + list(xmark)  # e.g., ["soe","[soe]"]
        __mark = ""

        if self.get_egc_tagx(node, tag) in xmark:
            __mark = self.get_egc_tagx(node, tag)
            self.tag_count["hit"][node] = {}
        elif mark in xmark2:
            __mark = mark if (mark[:1] == "[") else ("[" + mark + "]")
            self.tag_count["h_set"] += 1
        else:
            __mark = FV_NOT_FOUND
            self.tag_count["nf_404"] += 1

        self.setf_egc_tagx(node, tag, __mark)

        for child in self.tree[node]["child_node"]:
            self.setf_egc_tagx_rest(child, tag, self.get_egc_tagx(node, tag), xmark)

        return

    ### set some additional fields (to post to db)
    def setf_more2db(self):

        #
        def __setf_root_node(node, root_node):

            self.tree[node]["root_node"] = {
                "usn": root_node,
                "oname_fr": self.tree[root_node]["oname_fr"],
            }

            for child in self.tree[node]["child_node"]:
                __setf_root_node(child, root_node)

            return

        #
        result = {"psn_nf_404": {}}

        # set field 'child_node'
        logging.info("set field 'child_node'")

        for k, v in self.tree.items():  # !!! clear 'child_node'
            v["child_node"] = {}

        count = 0
        for k, v in self.tree.items():
            psn = v["h_psn"]
            if psn != FV_ROOT_NODE:  # if not 'root-node'
                if psn in self.tree:
                    self.tree[psn]["child_node"][k] = {}
                else:
                    if count < 10:
                        print(f"k={k}, v={v}")
                        count += 1
                    result["psn_nf_404"][psn] = {}

        # then, set field 'root_node' after 'child_node' set.
        logging.info("set field 'root_node'")

        for k, v in self.tree.items():  # !!! clear 'root_node'
            v["root_node"] = {}

        for k, v in self.tree.items():
            psn = v["h_psn"]
            if psn == FV_ROOT_NODE:
                __setf_root_node(k, k)

        return result

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
                self.tree[usn]["oname_fr"],
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
            d2lists = pinyin(v["oname_fr"], style=Style.NORMAL)  # 2 dimension list
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

    ###
    def update2db(self, tree4post=None):
        org4chk = Organization(self.year)
        org4chk.load_db()
        return super().update2db("usn", org4chk.tree, tree4post)

    # @property
    def getf_former_name_fr(self, node):
        return self.tree[node]["former_name_fr"]

    def setf_former_name_fr(self, node, v):
        self.tree[node]["former_name_fr"] = v


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------


class OrganizationV3(RemAiv3):

    ###
    def __init__(self, xyears, xargs=None):

        # self.__tag_count = {"hit": 0, "h_set": 0, "nf_404": 0}

        arg = {
            "coll": "organization",
            "years": xyears,
            "year": xyears,  #
            "to_fpath": "exp-organization-" + datetime.now().strftime("%m%d") + ".xlsx",
            #
            "map_db2t": {
                "usn": {"source": "usn", "key": True},
                "oname_fr": {"source": "oname_fr"},
                "usci_org_fr": {"source": "usci_org_fr"},
                "category": {"source": "category"},
                "bio": {"source": "bio"},  # {}
                # bio: {oname, oname_aka, former_name_fr:{},category_fr,type_org_fr,industry_org_fr,location_org_fr}
                "misc": {"source": "misc"},  # {}
                "nht_period": {"source": "nht_period"},  # {}
                "egc_tagx": {"source": "egc_tagx"},  # {}
                "parent_node": {"source": "parent_node"},  # {}
                "root_node": {"source": "root_node"},  # {}
                "child_node": {"source": "child_node"},  # {}
                "h_comments": {"source": "h_comments"},
            },
            #
            "map_t2fp": {
                "usn": {"source": "usn", "key": True},
                "oname": {"source": "bio.oname"},
                "oname_fr": {"source": "oname_fr"},
                "usci_org_fr": {"source": "usci_org_fr"},
                "category": {"source": "category"},
                "category_fr": {"source": "bio.category_fr"},
                "type_org_fr": {"source": "bio.type_org_fr"},
                "industry_org_fr": {"source": "bio.industry_org_fr"},
                "location_org_fr": {"source": "bio.location_org_fr"},
                "province": {"source": "bio.province"},
                "city": {"source": "bio.city"},
                "district": {"source": "bio.district"},
                #
                "parent_usn": {"source": "parent_node.usn"},
                "parent_oname_fr": {"source": "parent_node.oname_fr"},
                "root_usn": {"source": "root_node.usn"},
                "root_oname_fr": {"source": "root_node.oname_fr"},
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

        """
        yearx_list = list(xyears)
        print(f"xyears='{xyears}'")
        print(f"yearx_list='{yearx_list}'")
        yearx_list.sort()  # e.g., yearx_list = ["y2021", "y2022"]
        # print(f"yearx_list='{yearx_list}'")

        for __to_file, __in_tree in __map_egc_tagx.items():
            for yearx in yearx_list:
                # print(f"yearx='{yearx}'")
                arg["map_t2fp"][__to_file + yearx[-2:]] = {
                    "source": "egc_tagx." + __in_tree + "." + yearx
                }
        """

        if xargs is not None:
            arg.update(xargs)

        super().__init__(arg)
        return

    #
    def check_usn_oname_fr(self):

        client = MongoClient()
        db = client.get_database(HOME_DB)

        result = {"usn_dup": {}, "oname_fr_dup": {}}

        # check if usn is unique
        tree_usn = {}
        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            # print(f"docx={docx}")
            usn = docx["usn"]
            if usn in tree_usn:
                result["usn_dup"][usn] = docx["oname_fr"]
            else:
                tree_usn[usn] = {}

        # check if oname_fr is unique
        tree_oname_fr = {}
        query = {}
        projection = {"_id": 0}
        cursor = db.get_collection(self.coll).find(query, projection)
        for docx in cursor:
            oname_fr = docx["oname_fr"]
            if oname_fr in tree_oname_fr:
                result["oname_fr_dup"][oname_fr] = docx["usn"]
            else:
                tree_oname_fr[oname_fr] = {}

        client.close()
        return result

    #
    def insert2db(self, key_field=None, tree4chk=None, tree4post=None):
        if key_field is None:
            key_field = "usn"
        if tree4chk is None:
            tmp = self.__class__(self.year)
            tmp.load_db()
            tree4chk = tmp.tree
        return super().insert2db(key_field, tree4chk, tree4post)

    ###
    def setf_egc_tagx(self, node, tag, mark):
        if self.tree[node]["egc_tagx"] is None:
            self.tree[node]["egc_tagx"] = {}
        if self.tree[node]["egc_tagx"].get(tag, {}) == {}:
            self.tree[node]["egc_tagx"][tag] = {}
        self.tree[node]["egc_tagx"][tag][self.year] = mark
        return

    def get_egc_tagx(self, node, tag):
        return self.tree[node]["egc_tagx"].get(tag, {}).get(self.year, "")

    ### set field 'egc_tagx' of root node
    def setf_egc_tagx_root(self, tag, xmark):

        def __setf_egc_tagx_root(node, root_node, tag, xmark):
            # e.g., xmark = ["soe", "prov", "city"]
            mark = self.get_egc_tagx(node, tag)
            if mark in xmark:
                if self.get_egc_tagx(root_node, tag) != xmark[0]:
                    self.setf_egc_tagx(root_node, tag, "[" + mark + "]")
            for child in self.tree[node]["child_node"]:
                __setf_egc_tagx_root(child, root_node, tag, xmark)
            return

        result = {}
        for k, v in self.tree.items():
            if v["parent_node"] == {}:  # if node is root node
                __mark_before = self.get_egc_tagx(k, tag)
                __setf_egc_tagx_root(k, k, tag, xmark)
                if self.get_egc_tagx(k, tag) != __mark_before:
                    result[k] = v["oname_fr"]

        return result

    ### set rest (still blank, not set yet)
    def setf_egc_tagx_rest(self, node, tag, mark, xmark):

        xmark2 = [f"[{key}]" for key in xmark]  # add []
        xmark2 = xmark2 + list(xmark)  # e.g., ["soe","[soe]"]
        __mark = ""

        if self.get_egc_tagx(node, tag) in xmark:
            __mark = self.get_egc_tagx(node, tag)
            self.tag_count["hit"][node] = {}
        elif mark in xmark2:
            __mark = mark if (mark[:1] == "[") else ("[" + mark + "]")
            self.tag_count["h_set"] += 1
        else:
            __mark = FV_NOT_FOUND
            self.tag_count["nf_404"] += 1

        self.setf_egc_tagx(node, tag, __mark)

        for child in self.tree[node]["child_node"]:
            self.setf_egc_tagx_rest(child, tag, self.get_egc_tagx(node, tag), xmark)

        return

    ### set some additional fields (to post to db)
    def setf_more2db(self):

        #
        def __setf_root_node(node, root_node):

            self.tree[node]["root_node"] = {
                "usn": root_node,
                "oname_fr": self.tree[root_node]["oname_fr"],
            }

            for child in self.tree[node]["child_node"]:
                __setf_root_node(child, root_node)

            return

        #
        result = {"psn_nf_404": {}}

        # set field 'child_node'
        rlog.info("set field 'child_node'")

        for k, v in self.tree.items():  # !!! clear 'child_node'
            v["child_node"] = {}

        count = 0
        for k, v in self.tree.items():
            pnode = v["parent_node"]
            if pnode != {}:  # if not 'root-node'
                if pnode["usn"] in self.tree:
                    self.tree[pnode["usn"]]["child_node"][k] = {}
                else:
                    if count < 10:
                        print(f"k={k}, v={v}")
                        count += 1
                    result["psn_nf_404"][psn] = {}

        # then, set field 'root_node' after 'child_node' set.
        rlog.info("set field 'root_node'")

        for k, v in self.tree.items():  # !!! clear 'root_node'
            v["root_node"] = {}

        for k, v in self.tree.items():
            pnode = v["parent_node"]
            if pnode == {}:
                __setf_root_node(k, k)

        return result

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
            if v["parent_node"] == {}:
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
                self.tree[usn]["oname_fr"],
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
            d2lists = pinyin(v["oname_fr"], style=Style.NORMAL)  # 2 dimension list
            flat_list = [item for sublist in d2lists for item in sublist]
            pyx = "".join(flat_list)
            v["pinyin"] = pyx.lower()

        # set list for sort
        list4sort = []
        for k, v in self.tree.items():  # k = usn
            if v["parent_node"] != {}:
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

    ###
    def update2db(self, key_field=None, tree4chk=None, tree4post=None):
        if key_field is None:
            key_field = "usn"
        if tree4chk is None:
            tmp = self.__class__(self.year)
            tmp.load_db()
            tree4chk = tmp.tree
        return super().update2db(key_field, tree4chk, tree4post)

    # --------------------------------------------------------------------------

    ###
    # category
    def putf_category(self, node, v):
        self.tree[node]["category"] = v

    # usci_org_fr
    def getf_usci_org_fr(self, node):
        return self.tree[node]["usci_org_fr"]

    ### bio
    # category_fr
    def getf_category_fr(self, node):
        return self.tree[node]["bio"].get("category_fr", {})

    def putf_category_fr(self, node, v):
        if self.tree[node].get("bio", None) is None:
            # print(f"k={node} bio is none")
            self.tree[node]["bio"] = {}
        self.tree[node]["bio"]["category_fr"] = v

    # former_name_fr
    def getf_nformer(self, node):
        return self.tree[node]["bio"].get("former_name_fr", None)

    def putf_nformer(self, node, v):
        self.tree[node]["bio"]["former_name_fr"] = v


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class Org4Usn(RemAiv3):

    ###
    def __init__(self):

        arg = {
            "coll": "organization",
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
                "former_name_fr": {"source": "former_name_fr"},
                "pnode_name": {"source": "parent_node.oname_fr"},
            },
        }

        super().__init__(arg)
        return

    ###
    def get_max_usn(self):

        client = MongoClient()
        db = client.get_database(HOME_DB)

        pipeline = [{"$project": {"_id": 0, "usn": 1}}, {"$sort": {"usn": -1}}]
        cursor = db.get_collection(self.coll).aggregate(pipeline)

        for docx in cursor:
            max_usn = docx["usn"]
            # print(docx)
            break  # get a doc, that is max usn.

        return max_usn

    ###
    def load_db(self, queryx={}, xmap=None, tree4load=None):
        result = super().load_db(queryx, xmap, tree4load)
        # print(f"{self.coll} :: size = {self.size}")

        # tree_fn = {}
        # # add 'former_name_fr' to tree
        # for k, v in self.tree.items():
        #     if v["former_name_fr"] == {}:
        #         continue
        #     print(f"k={k}, former={v['former_name_fr']}")
        #     for kfn in v["former_name_fr"].keys():
        #         tree_fn[kfn] = {"usn": v["usn"], "pnode_name": v["pnode_name"]}

        # self.tree.update(tree_fn)
        # print(f"{self.coll} :: size = {self.size}")
        return result


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class OrgBizReg(RemAi):
    ###
    def __init__(self, year=""):

        arg = {
            "coll": "org_bizreg",
            "year": year,
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "oname": {"source": "oname"},
                "usn": {"source": "usn"},
                "usci_org_fr": {"source": "usci_org_fr"},
                "category": {"source": "category"},
                "category_fr": {"source": "category_fr"},
                "location_org_fr": {"source": "location_org_fr"},
                "type_org_fr": {"source": "type_org_fr"},
                "industry_org_fr": {"source": "industry_org_fr"},
                "h_psn": {
                    "source": "h_psn"
                },  # to be deleted, use 'parent_node' instead.
                "parent_node": {"source": "parent_node"},
            },
            "map_fp2t": {
                # source: file, destination : tree in memory
                # key information
                "oname_fr": {"source": "企业名称", "format": "str", "key": True},
                "oname": {"source": "企业名称", "format": "str"},
                "usci_org_fr": {"source": "统一社会信用代码", "format": "str"},
                "location_org_fr": {"source": "行政区划分", "format": "str"},
                "type_org_fr": {"source": "企业类型", "format": "str"},
                "industry_org_fr": {"source": "所属行业", "format": "str"},
                "former_name_fr": {"source": "曾用名", "format": "str"},
                # 'category': set
                # other inforamtion
                "legal_person": {"source": "法定代表人", "format": "str"},
                "est_date": {"source": "成立日期", "format": "str"},
                "operation_status": {"source": "经营状态", "format": "str"},
                "registered_capital": {"source": "注册资本", "format": "str"},
                "paidin_capital": {"source": "实缴资本", "format": "str"},
                "rn4b": {"source": "工商注册号", "format": "str"},
                "tin": {"source": "纳税人识别号", "format": "str"},
                "code_org": {"source": "组织机构代码", "format": "str"},
                "taxpayer_qualification": {"source": "纳税人资质", "format": "str"},
                "operational_period": {"source": "营业期限", "format": "str"},
                "approval_date": {"source": "核准日期", "format": "str"},
                "reg_authority": {"source": "登记机关", "format": "str"},
                "insured_persons": {"source": "参保人数", "format": "str"},
                "address_org": {"source": "注册地址", "format": "str"},
                "business_scope": {"source": "经营范围", "format": "str"},
                # "usn": "xx",
                # "parent_node": {
                #     "name_fr": "xx",
                #     "usn": "xx",
                # },
            },
            "map_db2db": {
                "usn": {"source": "usn", "key": True},
                "oname_fr": {"source": "oname_fr"},
                "oname": {"source": "oname"},
                "usci_org_fr": {"source": "usci_org_fr"},
                "category_fr": {"source": "category_fr"},
                "category": {"source": "category"},
                "type_org_fr": {"source": "type_org_fr", "default": ""},
                "industry_org_fr": {"source": "industry_org_fr", "default": ""},
                "location_org_fr": {"source": "location_org_fr"},
                "former_name_fr": {"source": "former_name_fr", "default": ""},
                # "h_psn": {
                #     "source": "h_psn"
                # },  # to be deleted, use 'parent_node' instead.
                "parent_node": {"source": "parent_node"},
            },
        }

        super().__init__(arg)
        return

    ###
    def get_flist(self, folder):
        file_key = "工商注册信息.xls"
        super().get_flist(folder, file_key)
        return

    ###
    def insert2db(self, tree4post=None):
        orgbr4chk = OrgBizReg()
        orgbr4chk.load_db()
        return super().insert2db("oname_fr", orgbr4chk.tree, tree4post)

    ###
    def load_db2db(self):
        return super().load_db(xmap=self.map_db2db)

    ###
    def load_fp(self):

        c4fp = 0
        if len(self.flist) == 0:
            return c4fp

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

            c4fp += 1
            self.tree.update({__k_dst: org})

        return c4fp

    ### set 'category' after load file
    def setf_category(self):

        usci_category = {
            # from GB32100-2015
            # 1: 登记管理部门是 '机构编制'
            "11": "机关[11]",
            "12": "事业单位[12]",
            "13": "中央编办直接管理机构编制的群众团体[13]",
            "19": "其他[19]",
            # 5: 登记管理部门是 '民政'
            "51": "社会团体[21]",
            "52": "民办非企业单位[22]",
            "53": "基金会[23]",
            "59": "其他[29]",
            # 9: 登记管理部门是 '工商'
            "91": "企业[91]",
            "92": "个体工商户[92]",
            "93": "农民专业合作社[93]",
            # Y: 登记管理部门是 '其他'
            "Y1": "其他[Y1]",
        }

        category_unknown = {}
        for k, v in self.tree.items():
            # print(f"usci={v['usci_org_fr']}")
            if v["usci_org_fr"][:2] in usci_category:
                v["category_fr"] = usci_category[v["usci_org_fr"][:2]]
            else:

                category_unknown[k] = {}

            match v["category_fr"]:
                case "机关[11]":
                    v["category"] = "gpo"  # Party and government organs
                case "事业单位[12]":
                    v["category"] = "pin"  # public institution
                case "民办非企业单位[22]":
                    v["category"] = "pne"  # private non-enterprise unit
                case "企业[91]":
                    v["category"] = "egc"  # enterprise, group, compan
                case _:
                    v["category"] = FV_NOT_FOUND
                    category_unknown[k] = {}

        # for k, v in self.tree.items():
        #     if v["type1_org"] == "-":
        #         if "事业单位" in v["reg_authority"]:
        #             v["category"] = "pin"  # set as 事业单位
        #         elif "未公示" in v["reg_authority"]:
        #             v["category"] = "gpo"  # set as 党政机关
        #         else:
        #             v["category"] = ""
        #             category_unknown[k] = {}
        #     elif "公司" in v["type1_org"]:
        #         v["category"] = "ent"  # set as 企业
        #     else:
        #         v["category"] = "org"
        #         category_unknown[k] = {}

        return category_unknown

    ###

    def setf_parent_node(self, treex):
        # 2 treex, one from file, one from 'organization'
        # treex = {child:parent}

        result = {"set": 0, "skip": 0}

        # set "" / {} if null
        for k, v in treex.items():
            # print(f"k={k},v={v}")
            if v["pnode_name"] is None:
                v["pnode_name"] = ""
        for k, v in self.tree.items():
            if v["parent_node"] is None:
                v["parent_node"] = {}

        for k, v in self.tree.items():
            if k in treex:
                if v["parent_node"].get("oname_fr", "") == treex[k]["pnode_name"]:
                    result["skip"] += 1
                    continue  # both empty or both have something
                if treex[k]["pnode_name"] != "":  # tree has something but self is empty
                    result["set"] += 1
                    # print(
                    #     f"k=#{k}#,self=#{v['parent_node'].get('oname_fr','')}#, tree={treex[k]['pnode_name']}"
                    # )
                    v["parent_node"]["oname_fr"] = treex[k]["pnode_name"]

        return result

    def setf_parent_node1(self, treex):  # 废弃

        # treex = org_pchild.tree
        # treex = {
        #     "oname_fr": {"usn": "xx", "parent_node": {"usn": "xx", "name_fr": "xx"}}
        # }

        # copy 'parent_node' from 'org_pchild'
        for k, v in self.tree.items():  # k = oname_fr
            v["parent_node"] = {}
            if k in treex:
                v["parent_node"] = treex[k]["parent_node"]

        # set 'parent_node.usn'
        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            if v["parent_node"]["usn"] == FV_NOT_FOUND:
                if v["parent_node"]["oname_fr"] in self.tree:
                    v["parent_node"]["usn"] = self.tree[v["parent_node"]["oname_fr"]][
                        "usn"
                    ]

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
                    "parent_node.oname_fr": v["parent_node"]["oname_fr"],
                }

        return __pnode_usn_not_found

    ###
    def setf_usn(self, treex):
        # treex = organization.tree = {oname_fr:usn}

        # set  {} if null
        for k, v in self.tree.items():
            if v["parent_node"] is None:
                v["parent_node"] = {}

        __not_found = {}

        for k, v in self.tree.items():
            if k in treex:
                v["usn"] = treex[k]["usn"]
            else:
                # v["usn"] = FV_NOT_FOUND
                __not_found[k] = {}

        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            kpn = v["parent_node"]["oname_fr"]
            if kpn in treex:
                v["parent_node"]["usn"] = treex[kpn]["usn"]
            else:
                # v["parent_node"]["usn"] = FV_NOT_FOUND
                if kpn in __not_found:
                    continue  # should in
                __not_found[kpn] = {}
                print(f"ERROR :: k={k}, kpn={kpn}, 'usn' not found for kpn.\n")

            # print(f"k={k}, parent_node=#{v['parent_node']}#")

            # pnode = v["parent_node"]
            # if pnode["oname_fr"] in treex:
            #     pnode["usn"] = treex[pnode["oname_fr"]]["usn"]
            # else:
            #     pnode["usn"] = FV_NOT_FOUND
            #     __not_found[pnode["oname_fr"]] = {}

        return __not_found

    ###
    def setf_usn4nf(self, max_usn36):

        __max_usn10 = int(max_usn36, base=36)
        usn10_4nf = __max_usn10
        result = {"set": 0, "set_docs": {}, "to_be_copied": 0, "to_be_copied_docs": {}}
        # print(f"max={max_usn36} / {__max_usn10}")

        # setf 'usn' for those not found (i.e., none)
        for k, v in self.tree.items():
            # print(f"usn = {v['usn']}, k={k}")
            # if v["usn"] == FV_NOT_FOUND:
            if v["usn"] is None:
                # print(f"usn={v['usn']}, k={k}")
                usn10_4nf += 1
                __usn36 = int_to_base36(usn10_4nf).lower()
                v["usn"] = __usn36

                result["set"] += 1
                result["set_docs"][v["usn"]] = k
            else:
                __usn10 = int(v["usn"], base=36)
                # print(f"usn={v['usn']}/{__usn10}, k = {k}")
                if __usn10 > __max_usn10:
                    result["to_be_copied"] += 1
                    result["to_be_copied_docs"][v["usn"]] = k

        # setf 'parent_node.usn' for those not found (i.e., none)
        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            kpn = v["parent_node"]["oname_fr"]
            if v["parent_node"].get("usn", None) is None:
                if kpn in self.tree:
                    v["parent_node"]["usn"] = self.tree[kpn]["usn"]

        return result

    ###
    def update2db(self):
        orgbr4chk = OrgBizReg()
        orgbr4chk.load_db()
        return super().update2db("oname_fr", orgbr4chk.tree, tree4post=None)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class OrgBizRegV3(OrganizationV3):

    ###
    def __init__(self, year=""):

        arg = {
            "coll": "org_bizreg",
            "year": year,
            #
            "map_db2t": {
                "usn": {"source": "usn"},
                "oname_fr": {"source": "oname_fr", "key": True},
                "usci_org_fr": {"source": "usci_org_fr"},
                "category": {"source": "category"},
                #
                ### bio
                "bio": {"source": "bio"},  # {}
                # bio: {oname, oname_aka, former_name_fr, category_fr,type_org_fr, industry_org_fr,location_org_fr }
                #
                "misc": {"source": "misc"},  # {}
                #
                # "aqc_download_date":{}
                "parent_node": {"source": "parent_node"},
            },
            ###
            ###
            ###
            "map_fp2t": {
                # "usn": {}
                "oname_fr": {"source": "企业名称", "key": True},
                "usci_org_fr": {"source": "统一社会信用代码"},
                # "category": {}
                #
                ### bio
                "bio.oname": {"source": "企业名称"},
                "bio.oname_aka": {"source": "企业名称"},
                "bio.former_name_fr": {"source": "曾用名"},
                # "bio.category_fr": {}
                "bio.type_org_fr": {"source": "企业类型"},
                "bio.industry_org_fr": {"source": "所属行业"},
                "bio.location_org_fr": {"source": "行政区划分"},
                #
                ### misc
                "misc.legal_person": {"source": "法定代表人"},
                "misc.est_date": {"source": "成立日期"},
                "misc.operation_status": {"source": "经营状态"},
                "misc.registered_capital": {"source": "注册资本"},
                "misc.paidin_capital": {"source": "实缴资本"},
                "misc.rn4b": {"source": "工商注册号"},
                "misc.tin": {"source": "纳税人识别号"},
                "misc.code_org": {"source": "组织机构代码"},
                "misc.taxpayer_qualification": {"source": "纳税人资质"},
                "misc.operational_period": {"source": "营业期限"},
                "misc.approval_date": {"source": "核准日期"},
                "misc.reg_authority": {"source": "登记机关"},
                "misc.insured_persons": {"source": "参保人数"},
                "misc.address_org": {"source": "注册地址"},
                "misc.business_scope": {"source": "经营范围"},
                #
                # "aqc_download_date":{}
                # "parent_node":{} # {}
            },
            "map_db2db": {
                "usn": {"source": "usn", "key": True},
                "oname_fr": {"source": "oname_fr"},
                "usci_org_fr": {"source": "usci_org_fr"},
                "category": {"source": "category"},
                "bio": {"source": "bio"},  # {}
                # bio: {oname, oname_aka, former_name_fr:{},category_fr,type_org_fr,industry_org_fr,location_org_fr}
                "misc": {"source": "misc"},  # {}
                "parent_node": {"source": "parent_node"},
            },
        }

        super().__init__(year, xargs=arg)
        return

    ###
    def get_flist(self, folder):
        file_key = "工商注册信息.xls"
        super().get_flist(folder, file_key)
        return

    ###
    def insert2db(self, tree4post=None):
        orgbr4chk = self.__class__()
        orgbr4chk.load_db()
        return super().insert2db("oname_fr", orgbr4chk.tree, tree4post)

    ###
    def load_db2db(self):
        return super().load_db(xmap=self.map_db2db)

    ###
    def load_fp(self):

        c4fp = 0
        if len(self.flist) == 0:
            return c4fp

        self.tree = {}
        for fp in self.flist:

            org = {}
            df_temp = pd.read_excel(fp, nrows=0)
            converters_dict = {col: str for col in df_temp.columns}
            df = pd.read_excel(fp, converters=converters_dict)
            df = df.fillna("")
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
                                # org[kmap] = __v_dst
                                self.set_nested_node(org, kmap, __v_dst)

            c4fp += 1
            self.tree.update({__k_dst: org})

        return c4fp

    ### set 'category' after load file
    def setf_category(self):

        usci_category = {
            # from GB32100-2015
            # 1: 登记管理部门是 '机构编制'
            "11": "机关[11]",
            "12": "事业单位[12]",
            "13": "中央编办直接管理机构编制的群众团体[13]",
            "19": "其他[19]",
            # 5: 登记管理部门是 '民政'
            "51": "社会团体[51]",
            "52": "民办非企业单位[52]",
            "53": "基金会[53]",
            "59": "其他[59]",
            # 9: 登记管理部门是 '工商'
            "91": "企业[91]",
            "92": "个体工商户[92]",
            "93": "农民专业合作社[93]",
            # Y: 登记管理部门是 '其他'
            "Y1": "其他[Y1]",
        }

        category_unknown = {}
        for k, v in self.tree.items():
            # print(
            #     f"k={k}, v={v}, bio is none={v['bio'] is None}, bio={self.tree[k]['bio']}"
            # )
            # print(f"k={k},v={v}")
            usci_org_fr = self.getf_usci_org_fr(k)
            if usci_org_fr[:2] in usci_category:
                self.putf_category_fr(k, usci_category[usci_org_fr[:2]])
            else:
                self.putf_category_fr(k, FV_NOT_FOUND)
                category_unknown[k] = {}

            match self.getf_category_fr(k):
                case "机关[11]":
                    self.putf_category(k, "gpo")  # Party and government organs
                case "事业单位[12]":
                    self.putf_category(k, "pin")  # public institution
                case "民办非企业单位[22]":
                    self.putf_category(k, "pne")  # private non-enterprise unit
                case "企业[91]":
                    self.putf_category(k, "egc")  # enterprise, group, company
                case _:
                    self.putf_category(k, "FV_NOT_FOUND")
                    category_unknown[k] = {}

        # for k, v in self.tree.items():
        #     if v["type1_org"] == "-":
        #         if "事业单位" in v["reg_authority"]:
        #             v["category"] = "pin"  # set as 事业单位
        #         elif "未公示" in v["reg_authority"]:
        #             v["category"] = "gpo"  # set as 党政机关
        #         else:
        #             v["category"] = ""
        #             category_unknown[k] = {}
        #     elif "公司" in v["type1_org"]:
        #         v["category"] = "ent"  # set as 企业
        #     else:
        #         v["category"] = "org"
        #         category_unknown[k] = {}

        return category_unknown

    ###

    def setf_parent_node(self, treex):
        # 2 treex, one from file, one from 'organization'
        # treex = {child:parent}

        result = {"set": 0, "skip": 0, "diff": {}, "pc": {}}

        # set "" / {} if null
        for k, v in treex.items():
            # print(f"k={k},v={v}")
            if v["pnode_name"] is None:
                v["pnode_name"] = ""
        for k, v in self.tree.items():
            if v["parent_node"] is None:
                v["parent_node"] = {}

        for k, v in self.tree.items():
            if k in treex:
                if v["parent_node"].get("oname_fr", "") == treex[k]["pnode_name"]:
                    result["skip"] += 1
                    continue  # both empty or both have something
                if treex[k]["pnode_name"] != "":  # tree has something but self is empty
                    if v["parent_node"] != {}:
                        result["diff"][k] = v["parent_node"]["oname_fr"]
                    result["set"] += 1
                    # print(
                    #     f"k=#{k}#,self=#{v['parent_node'].get('oname_fr','')}#, tree={treex[k]['pnode_name']}"
                    # )
                    v["parent_node"]["oname_fr"] = treex[k]["pnode_name"]
                    result["pc"][k] = treex[k]["pnode_name"]

        return result

    ###
    def setf_parent_node1(self, treex):  # 废弃

        # treex = org_pchild.tree
        # treex = {
        #     "oname_fr": {"usn": "xx", "parent_node": {"usn": "xx", "name_fr": "xx"}}
        # }

        # copy 'parent_node' from 'org_pchild'
        for k, v in self.tree.items():  # k = oname_fr
            v["parent_node"] = {}
            if k in treex:
                v["parent_node"] = treex[k]["parent_node"]

        # set 'parent_node.usn'
        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            if v["parent_node"]["usn"] == FV_NOT_FOUND:
                if v["parent_node"]["oname_fr"] in self.tree:
                    v["parent_node"]["usn"] = self.tree[v["parent_node"]["oname_fr"]][
                        "usn"
                    ]

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
                    "parent_node.oname_fr": v["parent_node"]["oname_fr"],
                }

        return __pnode_usn_not_found

    ###
    def setf_usn(self, treex):
        # treex = organization.tree = {oname_fr:usn}

        # set  {} if null
        for k, v in self.tree.items():
            if v["parent_node"] is None:
                v["parent_node"] = {}

        __not_found = {}

        for k, v in self.tree.items():
            if k in treex:
                v["usn"] = treex[k]["usn"]
            else:
                # v["usn"] = FV_NOT_FOUND
                __not_found[k] = {}

        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            kpn = v["parent_node"]["oname_fr"]
            if kpn in treex:
                v["parent_node"]["usn"] = treex[kpn]["usn"]
            else:
                # v["parent_node"]["usn"] = FV_NOT_FOUND
                if kpn in __not_found:
                    continue  # should in
                __not_found[kpn] = {}
                print(f"ERROR :: k={k}, kpn={kpn}, 'usn' not found for kpn.\n")

            # print(f"k={k}, parent_node=#{v['parent_node']}#")

            # pnode = v["parent_node"]
            # if pnode["oname_fr"] in treex:
            #     pnode["usn"] = treex[pnode["oname_fr"]]["usn"]
            # else:
            #     pnode["usn"] = FV_NOT_FOUND
            #     __not_found[pnode["oname_fr"]] = {}

        return __not_found

    ###
    def setf_usn4nf(self, max_usn36):

        __max_usn10 = int(max_usn36, base=36)
        usn10_4nf = __max_usn10
        result = {"set": 0, "set_docs": {}, "to_be_copied": 0, "to_be_copied_docs": {}}
        # print(f"max={max_usn36} / {__max_usn10}")

        # setf 'usn' for those not found (i.e., none)
        for k, v in self.tree.items():
            # print(f"usn = {v['usn']}, k={k}")
            # if v["usn"] == FV_NOT_FOUND:
            if v["usn"] is None:
                # print(f"usn={v['usn']}, k={k}")
                usn10_4nf += 1
                __usn36 = int_to_base36(usn10_4nf).lower()
                v["usn"] = __usn36

                result["set"] += 1
                result["set_docs"][v["usn"]] = k
            else:
                __usn10 = int(v["usn"], base=36)
                # print(f"usn={v['usn']}/{__usn10}, k = {k}")
                if __usn10 > __max_usn10:
                    result["to_be_copied"] += 1
                    result["to_be_copied_docs"][v["usn"]] = k

        # setf 'parent_node.usn' for those not found (i.e., none)
        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            kpn = v["parent_node"]["oname_fr"]
            if v["parent_node"].get("usn", None) is None:
                if kpn in self.tree:
                    v["parent_node"]["usn"] = self.tree[kpn]["usn"]

        return result

    ###
    def update2db(self):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().update2db("oname_fr", tmp.tree, tree4post=None)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class OrgBRv3All(OrganizationV3):
    # for files like 批量查询数据导出（企业信息）-【爱企查】-D712003032217433342473248.xls

    ###
    def __init__(self, year=""):

        arg = {
            "coll": "org_bizreg",
            "year": year,
            #
            "map_db2t": {
                "usn": {"source": "usn"},
                "oname_fr": {"source": "oname_fr", "key": True},
                "usci_org_fr": {"source": "usci_org_fr"},
                "category": {"source": "category"},
                #
                ### bio
                "bio": {"source": "bio"},  # {}
                # bio: {oname, oname_aka, former_name_fr, category_fr,type_org_fr, industry_org_fr,location_org_fr }
                #
                "misc": {"source": "misc"},  # {}
                #
                # "aqc_download_date":{}
                "parent_node": {"source": "parent_node"},
            },
            ###
            ###
            ###
            "map_fp2t": {
                # 企业名称	经营状态	法定代表人	注册资本	实缴资本	成立日期	核准日期	营业期限	所属省份	所属城市	所属区县	电话	更多电话	邮箱	更多邮箱	统一社会信用代码	纳税人识别号	工商注册号	组织机构代码	参保人数	企业类型	所属行业	曾用名	注册地址	最新年报地址	网址	经营范围	匹配状态
                # 			电话	更多电话	邮箱	更多邮箱		最新年报地址	网址		匹配状态
                # "usn": {}
                "oname_fr": {"source": "企业名称", "key": True},
                "usci_org_fr": {"source": "统一社会信用代码"},
                # "category": {}
                #
                ### bio
                "bio.oname": {"source": "企业名称"},
                "bio.oname_aka": {"source": "企业名称"},
                "bio.former_name_fr": {"source": "曾用名"},
                # "bio.category_fr": {}
                "bio.type_org_fr": {"source": "企业类型"},
                "bio.industry_org_fr": {"source": "所属行业"},
                "bio.location_org_fr": {"source": "行政区划分", "defalut": ""},  # ?
                "bio.province": {"source": "所属省份", "defalut": ""},
                "bio.city": {"source": "所属城市", "defalut": ""},
                "bio.district": {"source": "所属区县", "defalut": ""},
                #
                ### misc
                "misc.legal_person": {"source": "法定代表人"},
                "misc.est_date": {"source": "成立日期"},
                "misc.operation_status": {"source": "经营状态"},
                "misc.registered_capital": {"source": "注册资本"},
                "misc.paidin_capital": {"source": "实缴资本"},
                "misc.rn4b": {"source": "工商注册号"},
                "misc.tin": {"source": "纳税人识别号"},
                "misc.code_org": {"source": "组织机构代码"},
                "misc.taxpayer_qualification": {
                    "source": "纳税人资质",
                    "defalut": "",
                },  # ?
                "misc.operational_period": {"source": "营业期限"},
                "misc.approval_date": {"source": "核准日期"},
                "misc.reg_authority": {"source": "登记机关", "defalut": ""},  # ?
                "misc.insured_persons": {"source": "参保人数"},
                "misc.address_org": {"source": "注册地址"},
                "misc.business_scope": {"source": "经营范围"},
                "business_scope": {"source": "匹配状态", "check": True},
                #
                # "aqc_download_date":{}
                # "parent_node":{} # {}
            },
        }

        super().__init__(year, xargs=arg)
        return

    ###
    def get_flist(self, folder):
        file_key = "批量查询数据导出（企业信息）"
        super().get_flist(folder, file_key)
        return

    ###
    def insert2db(self, tree4post=None):
        orgbr4chk = self.__class__()
        orgbr4chk.load_db()
        return super().insert2db("oname_fr", orgbr4chk.tree, tree4post)

    ###
    def load_db2db(self):
        return super().load_db(xmap=self.map_db2db)

    ###
    def load_fp(self):

        result = {"docs": 0, "files": 0, "fname": {}}
        self.tree = {}

        #
        if len(self.flist) == 0:
            return result

        #
        for fp in self.flist:

            # fill na, read as string
            df_temp = pd.read_excel(fp, nrows=0)
            converters_dict = {col: str for col in df_temp.columns}
            df = pd.read_excel(fp, converters=converters_dict)
            df = df.fillna("")
            rows = df.to_dict(orient="records")

            for row in rows:
                __doc = {}
                for kmap, vmap in self.map_fp2t.items():
                    __v_dst = row.get(vmap["source"], vmap.get("default", None))

                    __check = ""
                    if vmap.get("check", False) == True:
                        __check = __v_dst
                        continue

                    if vmap.get("key", False) == True:
                        __k_dst = __v_dst
                    else:
                        self.set_nested_node(__doc, kmap, __v_dst)

                # doc done
                if __check != "失败":
                    self.tree.update({__k_dst: __doc})
                    result["docs"] += 1

            # file done
            result["files"] += 1
            result["fname"][fp] = {}

        return result

    ### set 'category' after load file
    def setf_category(self):

        usci_category = {
            # from GB32100-2015
            # 1: 登记管理部门是 '机构编制'
            "11": "机关[11]",
            "12": "事业单位[12]",
            "13": "中央编办直接管理机构编制的群众团体[13]",
            "19": "其他[19]",
            # 5: 登记管理部门是 '民政'
            "51": "社会团体[21]",
            "52": "民办非企业单位[22]",
            "53": "基金会[23]",
            "59": "其他[29]",
            # 9: 登记管理部门是 '工商'
            "91": "企业[91]",
            "92": "个体工商户[92]",
            "93": "农民专业合作社[93]",
            # Y: 登记管理部门是 '其他'
            "Y1": "其他[Y1]",
        }

        category_unknown = {}
        for k, v in self.tree.items():
            # print(
            #     f"k={k}, v={v}, bio is none={v['bio'] is None}, bio={self.tree[k]['bio']}"
            # )
            usci_org_fr = self.getf_usci_org_fr(k)
            if usci_org_fr[:2] in usci_category:
                self.putf_category_fr(k, usci_category[usci_org_fr[:2]])
            else:
                category_unknown[k] = {}

            match self.getf_category_fr(k):
                case "机关[11]":
                    self.putf_category(k, "gpo")  # Party and government organs
                case "事业单位[12]":
                    self.putf_category(k, "pin")  # public institution
                case "民办非企业单位[22]":
                    self.putf_category(k, "pne")  # private non-enterprise unit
                case "企业[91]":
                    self.putf_category(k, "egc")  # enterprise, group, company
                case _:
                    self.putf_category(k, "FV_NOT_FOUND")
                    category_unknown[k] = {}

        # for k, v in self.tree.items():
        #     if v["type1_org"] == "-":
        #         if "事业单位" in v["reg_authority"]:
        #             v["category"] = "pin"  # set as 事业单位
        #         elif "未公示" in v["reg_authority"]:
        #             v["category"] = "gpo"  # set as 党政机关
        #         else:
        #             v["category"] = ""
        #             category_unknown[k] = {}
        #     elif "公司" in v["type1_org"]:
        #         v["category"] = "ent"  # set as 企业
        #     else:
        #         v["category"] = "org"
        #         category_unknown[k] = {}

        return category_unknown

    ###

    def setf_parent_node(self, treex):
        # 2 treex, one from file, one from 'organization'
        # treex = {child:parent}

        result = {"set": 0, "skip": 0}

        # set "" / {} if null
        for k, v in treex.items():
            # print(f"k={k},v={v}")
            if v["pnode_name"] is None:
                v["pnode_name"] = ""
        for k, v in self.tree.items():
            if v["parent_node"] is None:
                v["parent_node"] = {}

        for k, v in self.tree.items():
            if k in treex:
                if v["parent_node"].get("oname_fr", "") == treex[k]["pnode_name"]:
                    result["skip"] += 1
                    continue  # both empty or both have something
                if treex[k]["pnode_name"] != "":  # tree has something but self is empty
                    result["set"] += 1
                    # print(
                    #     f"k=#{k}#,self=#{v['parent_node'].get('oname_fr','')}#, tree={treex[k]['pnode_name']}"
                    # )
                    v["parent_node"]["oname_fr"] = treex[k]["pnode_name"]

        return result

    ###
    def setf_parent_node1(self, treex):  # 废弃

        # treex = org_pchild.tree
        # treex = {
        #     "oname_fr": {"usn": "xx", "parent_node": {"usn": "xx", "name_fr": "xx"}}
        # }

        # copy 'parent_node' from 'org_pchild'
        for k, v in self.tree.items():  # k = oname_fr
            v["parent_node"] = {}
            if k in treex:
                v["parent_node"] = treex[k]["parent_node"]

        # set 'parent_node.usn'
        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            if v["parent_node"]["usn"] == FV_NOT_FOUND:
                if v["parent_node"]["oname_fr"] in self.tree:
                    v["parent_node"]["usn"] = self.tree[v["parent_node"]["oname_fr"]][
                        "usn"
                    ]

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
                    "parent_node.oname_fr": v["parent_node"]["oname_fr"],
                }

        return __pnode_usn_not_found

    ###
    def setf_usn(self, treex):
        # treex = organization.tree = {oname_fr:usn}

        # set  {} if null
        for k, v in self.tree.items():
            if v["parent_node"] is None:
                v["parent_node"] = {}

        __not_found = {}

        for k, v in self.tree.items():
            if k in treex:
                v["usn"] = treex[k]["usn"]
            else:
                # v["usn"] = FV_NOT_FOUND
                __not_found[k] = {}

        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            kpn = v["parent_node"]["oname_fr"]
            if kpn in treex:
                v["parent_node"]["usn"] = treex[kpn]["usn"]
            else:
                # v["parent_node"]["usn"] = FV_NOT_FOUND
                if kpn in __not_found:
                    continue  # should in
                __not_found[kpn] = {}
                print(f"ERROR :: k={k}, kpn={kpn}, 'usn' not found for kpn.\n")

            # print(f"k={k}, parent_node=#{v['parent_node']}#")

            # pnode = v["parent_node"]
            # if pnode["oname_fr"] in treex:
            #     pnode["usn"] = treex[pnode["oname_fr"]]["usn"]
            # else:
            #     pnode["usn"] = FV_NOT_FOUND
            #     __not_found[pnode["oname_fr"]] = {}

        return __not_found

    ###
    def setf_usn4nf(self, max_usn36):

        __max_usn10 = int(max_usn36, base=36)
        usn10_4nf = __max_usn10
        result = {"set": 0, "set_docs": {}, "to_be_copied": 0, "to_be_copied_docs": {}}
        # print(f"max={max_usn36} / {__max_usn10}")

        # setf 'usn' for those not found (i.e., none)
        for k, v in self.tree.items():
            # print(f"usn = {v['usn']}, k={k}")
            # if v["usn"] == FV_NOT_FOUND:
            if v["usn"] is None:
                # print(f"usn={v['usn']}, k={k}")
                usn10_4nf += 1
                __usn36 = int_to_base36(usn10_4nf).lower()
                v["usn"] = __usn36

                result["set"] += 1
                result["set_docs"][v["usn"]] = k
            else:
                __usn10 = int(v["usn"], base=36)
                # print(f"usn={v['usn']}/{__usn10}, k = {k}")
                if __usn10 > __max_usn10:
                    result["to_be_copied"] += 1
                    result["to_be_copied_docs"][v["usn"]] = k

        # setf 'parent_node.usn' for those not found (i.e., none)
        for k, v in self.tree.items():
            if v["parent_node"] == {}:
                continue
            kpn = v["parent_node"]["oname_fr"]
            if v["parent_node"].get("usn", None) is None:
                if kpn in self.tree:
                    v["parent_node"]["usn"] = self.tree[kpn]["usn"]

        return result

    ###
    def update2db(self):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().update2db("oname_fr", tmp.tree, tree4post=None)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class OrgNoUSCI(RemAi):
    ###
    def __init__(self, year=""):

        arg = {
            "coll": "fOrgNoUSCI",
            "year": year,
            #
            "map_fp2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "oname": {"source": "oname"},
                "usci_org_fr": {"source": "usci_org_fr", "format": "str"},
                "location_org_fr": {"source": "location_org_fr", "format": "str"},
            },
        }

        super().__init__(arg)
        return

    ###
    def get_flist(self, folder):
        file_key = "remai-org-no-usci.xlsx"
        super().get_flist(folder, file_key)
        return


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class OrgPCAqc(RemAiv3):

    ###
    def __init__(self, year=""):

        arg = {
            "coll": "",
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
            # __v_dst["parent_node"] = {}
            # __v_dst["parent_node"]["usn"] = ""
            fn = fp.name
            __org = fn[len("【爱企查】") : -len("股东信息.xls")]

            df_temp = pd.read_excel(fp, nrows=0)
            converters_dict = {col: str for col in df_temp.columns}

            df = pd.read_excel(fp, converters=converters_dict)
            df = df.fillna("")  # to be amended for different value for diff column.
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
                            __v_dst["pnode_name"] = row[f"Unnamed: {__col_index + 1}"]

            count_fp += 1
            self.tree.update({__org: __v_dst})

        return count_fp


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class OrgPChild(RemAiv3):
    ###
    def __init__(self, year=""):

        arg = {
            "coll": "",
            "year": year,
            #
            "map_fp2t": {
                "child": {"source": "child", "key": True},
                "pnode_name": {"source": "parent"},
            },
        }

        super().__init__(arg)
        return

    ###
    def get_flist(self, folder):
        file_key = "remai-org-parent-child.xlsx"
        super().get_flist(folder, file_key)
        return

    ###
    def setf_usn(self, treex):

        __not_found = 0
        for k, v in self.tree.items():

            if k in treex:
                v["usn"] = treex[k]["usn"]
            else:
                v["usn"] = FV_NOT_FOUND
                __not_found += 1

            __name_pnode = v["parent_node"]["oname_fr"]
            if __name_pnode in treex:
                v["parent_node"]["usn"] = treex[__name_pnode]["usn"]
            else:
                v["parent_node"]["usn"] = FV_NOT_FOUND
                __not_found += 1

        return __not_found


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class OrgAsToBe(RemAi):
    ###
    def __init__(self, year=""):

        arg = {
            "coll": "org_astobe",
            "year": year,
            #
            "map_db2t": {
                "oname_fr": {"source": "oname_fr", "key": True},
                "usn": {"source": "usn"},
                "category": {"source": "category"},
            },
        }

        super().__init__(arg)
        return


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class StudentYear(RemAi):

    ###
    def __init__(self, year):

        arg = {
            "coll": "student_" + year,
            "year": year,
            #
            "to_fpath": "exp-student-"
            + year
            + "-"
            + datetime.now().strftime("%m%d")
            + ".xlsx",
            #
            "map_db2t": {
                # bio
                "sid": {"source": "sid", "key": True},  # key # f01
                "bio": {"source": "bio"},
                # "sname": {"source": "sname"},  # f02
                # "gender": {"source": "gender"},  # f03
                # "student_from_code": {"source": "student_from_code"},  # f04
                # "student_from": {"source": "student_from"},  # f05
                # major
                "major": {"source": "major"},
                "specialization": {"source": "specialization"},
                "major_code": {"source": "major_code"},
                "degree_year": {"source": "degree_year"},  # f06
                # path-after-graduate
                "path_after_graduate_code": {
                    "source": "path_after_graduate_code"
                },  # f07
                "path_after_graduate": {"source": "path_after_graduate"},  # f08
                "path_fr": {"source": "path_fr"},  # f09
                "position_type_code": {"source": "position_type_code"},  # f10
                "position_type": {"source": "position_type"},  # f11
                # organization
                "oname": {"source": "oname"},  # f12
                "oname_fr": {"source": "oname_fr"},  # f13
                "category": {"source": "category"},  # ent, gpo, etc. # f14
                "category_fr": {"source": "category_fr"},  # f15
                "usci_org": {"source": "usci_org"},  # f16
                "usci_org_fr": {
                    "source": "usci_org_fr"
                },  # usci from 'organization' # f17
                "type_org_code": {"source": "type_org_code"},
                "type_org": {"source": "type_org"},  # f18
                "type_org_fr": {"source": "type_org_fr"},  # f19
                "industry_org_code": {"source": "industry_org_code"},
                "industry_org": {"source": "industry_org"},  # f20
                "industry_org_fr": {"source": "industry_org_fr"},  # f21
                "location_org_code": {"source": "location_org_code"},  # f22
                "location_org": {"source": "location_org"},  # f23
                "location_org_fr": {"source": "location_org_fr"},  # f24
                "address_org": {"source": "address_org"},  # f25
                "usn": {"source": "usn"},  # f26
                "egc_tagx": {"source": "egc_tagx"},  # glb, chn, etc. #f27-f35
                "root_node": {"source": "root_node"},  # f36-f37
            },
            #
            "map_t2fp": {
                # source: tree in memory; destination: file
                # bio # f01-f05
                "sid": {"source": "sid", "key": True},
                "sname": {"source": "bio.sname"},
                "gender": {"source": "bio.gender"},
                "student_from_code": {"source": "bio.student_from_code"},
                "student_from": {"source": "bio.student_from"},
                # major # f06
                "degree_year": {"source": "degree_year"},
                # path-after-graduate # f07-f11
                "path_after_graduate_code": {"source": "path_after_graduate_code"},
                "path_after_graduate": {"source": "path_after_graduate"},
                "path_fr": {"source": "path_fr"},
                "position_type_code": {"source": "position_type_code"},
                "position_type": {"source": "position_type"},
                # organization # f12-f26
                "oname": {"source": "oname"},
                "oname_fr": {"source": "oname_fr"},
                "category": {"source": "category"},  # ent, gpo, etc.
                "category_fr": {"source": "category_fr"},
                "usci_org": {"source": "usci_org"},
                "usci_org_fr": {"source": "usci_org_fr"},
                "type_org": {"source": "type_org"},
                "type_org_fr": {"source": "type_org_fr"},
                "industry_org": {"source": "industry_org"},
                "industry_org_fr": {"source": "industry_org_fr"},
                "location_org_code": {"source": "location_org_code"},
                "location_org": {"source": "location_org"},
                "location_org_fr": {"source": "location_org_fr"},
                "address_org": {"source": "address_org"},
                "usn": {"source": "usn"},
                # orgnaization.egc_tagx # f27-f35
                "glb": {"source": "egc_tagx.glb500"},
                "chn": {"source": "egc_tagx.chn500"},
                "soe": {"source": "egc_tagx.soe"},
                "plc": {"source": "egc_tagx.plc"},
                "nht": {"source": "egc_tagx.nht"},
                "isc": {"source": "egc_tagx.isc100"},
                "fic": {"source": "egc_tagx.fic500_all"},
                "mfg": {"source": "egc_tagx.fic500_mfg"},
                "svc": {"source": "egc_tagx.fic100_svc"},
                # organization.root_node # f36-f37
                "root_usn": {"source": "root_node.usn"},
                "root_oname_fr": {"source": "root_node.oname_fr"},
            },
        }

        super().__init__(arg)
        return

    ###
    def copyf_h_tagx(self, tree4copy):

        __tag_copied = {
            "got-job": 0,
            "match": 0,
            "match-fn": {},
            "not-found": {},
            "egc_tagx": {},
        }
        for k, v in self.tree.items():

            # only focus 'got-job'
            if v["path_fr"] == "got-job":
                __tag_copied["got-job"] += 1
            else:
                continue

            __match = False
            for korg, vorg in tree4copy.items():

                if v["oname_fr"] == vorg["oname_fr"]:
                    __tag_copied["match"] += 1
                    __match = True

                if v["oname_fr"] in vorg["former_name_fr"]:
                    __tag_copied["match"] += 1
                    __match = True
                    __tag_copied["match-fn"][k] = v["oname_fr"]

                if __match == True:
                    v["category"] = vorg["category"]  # f14
                    v["category_fr"] = vorg["category_fr"]  # f15
                    v["usci_org_fr"] = vorg["usci_org_fr"]  # f17
                    v["type_org_fr"] = vorg["type_org_fr"]  # f19
                    v["industry_org_fr"] = vorg["industry_org_fr"]  # f21
                    v["location_org_fr"] = vorg["location_org_fr"]  # f21
                    v["usn"] = korg  # f26
                    v["root_node"] = vorg["root_node"]  # f36-f37

                    for tag, mark in vorg["egc_tagx"].items():  # f27-f35
                        # print(
                        #     f"year = {self.year}, vorg['egc_tagx'] = {vorg['egc_tagx']}"
                        # )
                        if v["egc_tagx"] is None:
                            v["egc_tagx"] = {}
                        v["egc_tagx"][tag] = mark[self.year]

                        # count mark + [mark] for each tag
                        if v["egc_tagx"][tag] != FV_NOT_FOUND:
                            if __tag_copied["egc_tagx"].get(tag, 0) == 0:  # not exist
                                __tag_copied["egc_tagx"][tag] = 0
                            __tag_copied["egc_tagx"][tag] += 1

                    # if match then break
                    break

            if __match == False:
                v["category"] = FV_NOT_FOUND  # f14
                v["category_fr"] = FV_NOT_FOUND  # f15
                v["usci_org_fr"] = FV_NOT_FOUND  # f17
                v["type_org_fr"] = FV_NOT_FOUND  # f19
                v["industry_org_fr"] = FV_NOT_FOUND  # f21
                v["location_org_fr"] = FV_NOT_FOUND  # f21
                v["usn"] = FV_NOT_FOUND  # f26
                v["egc_tagx"] = {}  # f27-f35
                v["root_node"] = {}  # f36-f37

                __tag_copied["not-found"][k] = v["oname_fr"]

        return __tag_copied

    ### set some additional fields (to post to db)
    def setf_more2db(self):

        # set field 'organization_fr'
        for k, v in self.tree.items():  # k = sid
            match v["path_after_graduate_code"]:
                case "271":  # 271	科研助理、管理助理—科研助理、管理助理
                    v["oname_fr"] = "科研助理管理助理[271]"
                case "46":  # 46	应征义务兵
                    v["oname_fr"] = "应征义务兵[46]"
                case "502":  # 502	国家基层项目—三支一扶
                    v["oname_fr"] = "国家基层项目[502]"
                case "503":  # 503	国家基层项目—西部计划
                    v["oname_fr"] = "国家基层项目[503]"
                case "512":  # 512	地方基层项目—地方选调生
                    v["oname_fr"] = "地方基层项目[512]"
                case "519":  # 519	地方基层项目—其他地方基层项目
                    v["oname_fr"] = "地方基层项目[519]"
                case "76":  # 76	自由职业
                    v["oname_fr"] = "自由职业[76]"
                case "_":  # others
                    v["oname_fr"] = v["organization"]

        return

    ###
    def update2db(self, tree4post=None):
        tmp = StudentYear(self.year)
        tmp.load_db()
        return super().update2db("sid", tmp.tree, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class Student(RemAiv3):
    # for toxls too.

    ###
    def __init__(self, year, xargs=None):

        args = {
            "coll_base": "student_" + year,
            "coll": "student_" + year,
            "year": year,
            #
            "to_fpath": "exp-student-"
            + year
            + "-"
            + datetime.now().strftime("%m%d")
            + ".xlsx",
            #
            "map_db2t": {
                "sid": {"source": "sid", "key": True},  # key
                "bio": {"source": "bio"},  # {}
                # bio: sname, gender, student_from_code, student_from
                "campus": {"source": "campus"},  # {}
                # campus: major, specialization, major_code, degree_year, graduate_year, degree
                "path": {"source": "path"},  # {}
                # path: path_after_graduate_code, path_after_graduate, path_fr, position_type_code, position_type
                "org": {"source": "org"},  # {}
                "usn": {"source": "usn"},
                "egc_tagx": {"source": "egc_tagx"},  # {}
                "root_node": {"source": "root_node"},  # {}
            },
            #
            #
            "map_t2fp": {
                # for toxls to export student to excel file.
                "sid": {"source": "sid", "key": True},
                ### bio - 8
                # "exam_number": {"source": "bio.exam_number"},
                "sname": {"source": "bio.sname"},
                "gender": {"source": "bio.gender"},
                # "birth_date": {"source": "bio.birth_date"},
                "student_from_code": {"source": "bio.student_from_code"},
                "student_from": {"source": "bio.student_from"},
                # "nationality": {"source": "bio.nationality"},
                # "political_status": {"source": "bio.political_status"},
                #
                ### campus - 6
                # "major": {"source": "campus.major"},
                # "specialization": {"source": "campus.specialization"},
                # "major_code": {"source": "campus.major_code"},
                # "graduate_year": {"source": "campus.graduate_year"},
                # "degree": {"source": "campus.degree"},
                "degree_year": {"source": "campus.degree_year"},
                #
                ### path - 6
                "path_after_graduate_code": {"source": "path.path_after_graduate_code"},
                "path_after_graduate": {"source": "path.path_after_graduate"},
                "path_fr": {"source": "path.path_fr"},
                "contract_date": {"source": "path.contract_date"},
                "position_type_code": {"source": "path.position_type_code"},
                "position_type": {"source": "path.position_type"},
                ### org - 16
                "oname": {"source": "org.oname"},
                "oname_fr": {"source": "org.oname_fr"},
                "category": {"source": "org.category"},  # ent, gpo, etc.
                "category_fr": {"source": "org.category_fr"},
                "usci_org": {"source": "org.usci_org"},
                "usci_org_fr": {"source": "org.usci_org_fr"},
                # "type_org_code": {"source": "org.type_org_code"},
                "type_org": {"source": "org.type_org"},
                "type_org_fr": {"source": "org.type_org_fr"},
                # "industry_org_code": {"source": "org.industry_org_code"},
                "industry_org": {"source": "org.industry_org"},
                "industry_org_fr": {"source": "org.industry_org_fr"},
                "location_org_code": {"source": "org.location_org_code"},
                "location_org": {"source": "org.location_org"},
                "location_org_fr": {"source": "org.location_org_fr"},
                "province": {"source": "org.province"},
                "city": {"source": "org.city"},
                "district": {"source": "org.district"},
                "address_org": {"source": "org.address_org"},
                #
                "usn": {"source": "usn"},
                # egc_tagx
                "glb": {"source": "egc_tagx.glb500"},
                "chn": {"source": "egc_tagx.chn500"},
                "soe": {"source": "egc_tagx.soe"},
                "plc": {"source": "egc_tagx.plc"},
                "nht": {"source": "egc_tagx.nht"},
                "isc": {"source": "egc_tagx.isc100"},
                "fic": {"source": "egc_tagx.fic500_all"},
                "mfg": {"source": "egc_tagx.fic500_mfg"},
                "svc": {"source": "egc_tagx.fic100_svc"},
                # root_node
                "root_usn": {"source": "root_node.usn"},
                "root_oname_fr": {"source": "root_node.oname_fr"},
            },
        }

        #
        if xargs is not None:
            args.update(xargs)

        super().__init__(args)
        return

    ###
    def copyf_h_tagx(self, tree4copy):

        __tag_copied = {
            "got-job": 0,
            "match": 0,
            "match-fn": {},
            "not-found": {},
            "egc_tagx": {},
        }
        for k, v in self.tree.items():

            # only focus 'got-job'
            # print(f"k={k}, v={v}")
            if v["path"]["path_fr"] == "got-job":
                __tag_copied["got-job"] += 1
            else:
                continue

            __match = False
            for korg, vorg in tree4copy.items():

                oname_fr = self.getf_oname_fr(k)

                if oname_fr == vorg["oname_fr"]:
                    __tag_copied["match"] += 1
                    __match = True

                if oname_fr in vorg["bio"]["former_name_fr"]:
                    __tag_copied["match"] += 1
                    __match = True
                    __tag_copied["match-fn"][k] = oname_fr

                if __match == True:
                    self.putf_category(k, vorg["category"])
                    self.putf_category_fr(k, vorg["bio"]["category_fr"])
                    self.putf_usci_org_fr(k, vorg["usci_org_fr"])
                    self.putf_type_org_fr(k, vorg["bio"]["type_org_fr"])
                    self.putf_industry_org_fr(k, vorg["bio"]["industry_org_fr"])
                    self.putf_location_org_fr(k, vorg["bio"]["location_org_fr"])
                    self.putf_province(k, vorg["bio"].get("province", ""))
                    self.putf_city(k, vorg["bio"].get("city", ""))
                    self.putf_district(k, vorg["bio"].get("district", ""))
                    v["usn"] = korg  # f26
                    v["root_node"] = vorg["root_node"]  # f36-f37

                    for tag, mark in vorg["egc_tagx"].items():  # f27-f35
                        # print(
                        #     f"year = {self.year}, vorg['egc_tagx'] = {vorg['egc_tagx']}"
                        # )
                        if v["egc_tagx"] is None:
                            v["egc_tagx"] = {}
                        v["egc_tagx"][tag] = mark[self.year]

                        # count mark + [mark] for each tag
                        if v["egc_tagx"][tag] != FV_NOT_FOUND:
                            if __tag_copied["egc_tagx"].get(tag, 0) == 0:  # not exist
                                __tag_copied["egc_tagx"][tag] = 0
                            __tag_copied["egc_tagx"][tag] += 1

                    # if match then break
                    break

            if __match == False:
                self.putf_category(k, FV_NOT_FOUND)
                self.putf_category_fr(k, FV_NOT_FOUND)
                self.putf_usci_org_fr(k, FV_NOT_FOUND)
                self.putf_type_org_fr(k, FV_NOT_FOUND)
                self.putf_industry_org_fr(k, FV_NOT_FOUND)
                self.putf_location_org_fr(k, FV_NOT_FOUND)
                self.putf_province(k, FV_NOT_FOUND)
                self.putf_city(k, FV_NOT_FOUND)
                self.putf_district(k, FV_NOT_FOUND)
                v["usn"] = FV_NOT_FOUND  # f26
                v["egc_tagx"] = {}  # f27-f35
                v["root_node"] = {}  # f36-f37

                # print(f"v={v}")
                __tag_copied["not-found"][k] = v["org"]["oname_fr"]

        return __tag_copied

    ### set some additional fields (to post to db)
    def setf_more2db(self):

        # set field 'organization_fr'
        for k, v in self.tree.items():  # k = sid
            match v["path_after_graduate_code"]:
                case "271":  # 271	科研助理、管理助理—科研助理、管理助理
                    v["oname_fr"] = "科研助理管理助理[271]"
                case "46":  # 46	应征义务兵
                    v["oname_fr"] = "应征义务兵[46]"
                case "502":  # 502	国家基层项目—三支一扶
                    v["oname_fr"] = "国家基层项目[502]"
                case "503":  # 503	国家基层项目—西部计划
                    v["oname_fr"] = "国家基层项目[503]"
                case "512":  # 512	地方基层项目—地方选调生
                    v["oname_fr"] = "地方基层项目[512]"
                case "519":  # 519	地方基层项目—其他地方基层项目
                    v["oname_fr"] = "地方基层项目[519]"
                case "76":  # 76	自由职业
                    v["oname_fr"] = "自由职业[76]"
                case "_":  # others
                    v["oname_fr"] = v["organization"]

        return

    ###
    def update2db(self, key_field=None, tree4chk=None, tree4post=None):

        if key_field is None:
            key_field = "sid"

        if tree4chk is None:
            tmp = self.__class__(self.year)
            tmp.load_db()
            tree4chk = tmp.tree

        return super().update2db(key_field, tree4chk, tree4post)

    # --------------------------------------------------------------------------
    ### campus
    # degree
    def getf_degree(self, node):
        return self.tree[node]["campus"]["degree"]

    # degree_year
    def getf_degree_year(self, node):
        return self.tree[node]["campus"]["degree_year"]

    def putf_degree_year(self, node, v):
        self.tree[node]["campus"]["degree_year"] = v

    ### path
    # path_after_graduate_code
    def getf_path_after_graduate_code(self, node):
        return self.tree[node]["path"]["path_after_graduate_code"]

    # path_fr
    def putf_path_fr(self, node, v):
        self.tree[node]["path"]["path_fr"] = v

    ### org
    # category
    def putf_category(self, node, v):
        self.tree[node]["org"]["category"] = v

    # category_fr
    def putf_category_fr(self, node, v):
        self.tree[node]["org"]["category_fr"] = v

    # industry_org_fr
    def putf_industry_org_fr(self, node, v):
        self.tree[node]["org"]["industry_org_fr"] = v

    # location_org_fr
    def putf_location_org_fr(self, node, v):
        self.tree[node]["org"]["location_org_fr"] = v

    # province
    def putf_province(self, node, v):
        self.tree[node]["org"]["province"] = v

    # city
    def putf_city(self, node, v):
        self.tree[node]["org"]["city"] = v

    # district
    def putf_district(self, node, v):
        self.tree[node]["org"]["district"] = v

    # oname_fr
    def getf_oname_fr(self, node):
        return self.tree[node]["org"]["oname_fr"]

    def putf_oname_fr(self, node, v):
        self.tree[node]["org"]["oname_fr"] = v

    # type_org_fr
    def putf_type_org_fr(self, node, v):
        self.tree[node]["org"]["type_org_fr"] = v

    # usci_org_fr
    def putf_usci_org_fr(self, node, v):
        self.tree[node]["org"]["usci_org_fr"] = v


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# for loading excel file to db
class StudentWashd(RemAi):

    ###
    def __init__(self, year, coll=None):

        if coll != None:
            __coll = coll
        else:
            __coll = "student_" + year + "_" + datetime.now().strftime("%y%m")

        arg = {
            "coll": __coll,
            "year": year,
            #
            "map_db2t": {
                ### bio #d01-d05
                "sid": {"source": "sid", "key": True},  # key
                "bio": {"source": "bio"},
                # "sname": {"source": "sname"},
                # "gender": {"source": "gender"},
                # "student_from_code": {"source": "student_from_code"},
                # "student_from": {"source": "student_from"},
                ### major # d06-d11
                "major": {"source": "major"},
                "specialization": {"source": "specialization"},
                "major_code": {"source": "major_code"},
                # to set degree_year with 'degree'
                "graduate_year": {"source": "graduate_year"},
                # to set degree_year with 'graduate_year'
                "degree": {"source": "degree"},  ##
                "degree_year": {"source": "degree_year"},
                ### path-after-graduate # d12-d16
                "path_after_graduate_code": {"source": "path_after_graduate_code"},
                "path_after_graduate": {"source": "path_after_graduate"},
                "path_fr": {"source": "path_fr"},
                "position_type_code": {"source": "position_type_code"},
                "position_type": {"source": "position_type"},
                ### organization #d17-d
                "oname": {"source": "oname"},
                "oname_fr": {"source": "oname_fr"},
                "category": {"source": "category"},
                "category_fr": {"source": "category_fr"},
                "usci_org": {"source": "usci_org"},
                "usci_org_fr": {"source": "usci_org_fr"},  # usci from 'organization'
                "type_org_code": {"source": "type_org_code"},
                "type_org": {"source": "type_org"},
                "type_org_fr": {"source": "type_org_fr"},
                "industry_org_code": {"source": "industry_org_code"},
                "industry_org": {"source": "industry_org"},
                "industry_org_fr": {"source": "industry_org_fr"},
                "location_org_code": {"source": "location_org_code"},
                "location_org": {"source": "location_org"},
                "location_org_fr": {"source": "location_org_fr"},
                "address_org": {"source": "address_org"},
                "usn": {"source": "usn"},
                "egc_tagx": {"source": "egc_tagx"},  # glb, chn, etc.
                "root_node": {"source": "root_node"},
            },
            "map_fp2t": {
                ### bio
                "sid": {"source": "学号", "format": "str", "key": True},  # key #d01
                "bio.exam_number": {"source": "考生号", "format": "str"},
                "bio.sname": {"source": "姓名", "format": "str"},  # d02
                "bio.gender": {"source": "性别", "format": "str"},  # d03
                "bio.birth_date": {"source": "出生日期", "format": "str"},
                "bio.student_from_code": {
                    "source": "生源地代码",
                    "format": "str",
                },  # d04
                "bio.student_from": {"source": "生源地名称", "format": "str"},  # d05
                "bio.nationality": {"source": "民族名称", "format": "str"},
                "bio.political_status": {"source": "政治面貌", "format": "str"},
                # "tmp000": {"source": "城乡生源", "format": "str"},
                ### major
                "major": {"source": "专业名称", "format": "str"},  # d06
                "specialization": {"source": "专业方向", "format": "str"},  # d07
                "major_code": {"source": "专业代码", "format": "str"},  # d08
                "graduate_year": {"source": "毕业年份", "format": "str"},  # d09
                "degree": {"source": "学历层次", "format": "str"},  # d10
                # 'degree_year' set based on 'degree' + 'graduate_year'#d011
                # "tmp000": {"source": "入学年月", "format": "str"},
                # "tmp000": {"source": "毕业日期", "format": "str"},
                # "tmp000": {"source": "培养方式", "format": "str"},
                # "tmp000": {"source": "委培单位", "format": "str"},
                # "tmp000": {"source": "学习形式", "format": "str"},
                ### path-after-graduate
                "path_after_graduate_code": {
                    "source": "毕业去向类别代码",
                    "format": "str",
                },  # d12
                "path_after_graduate": {"source": "毕业去向", "format": "str"},  # d13
                # path_fr set based on 'path_after_graduate_code' #d14
                # "tmp000": {"source": "签约日期", "format": "str"},
                "position_type_code": {
                    "source": "工作职位类别代码",
                    "format": "str",
                },  # d15
                "position_type": {"source": "工作职位类别", "format": "str"},  # d16
                ### organization
                "oname": {"source": "单位名称", "format": "str"},
                # oname_fr set based on 'oname' + 'path_after_graduate_code'
                "usci_org": {"source": "统一社会信用代码", "format": "str"},
                # 'usci_org_fr' copy from 'organization'
                # "tmp000": {"source": "留学国家地区", "format": "str"},
                # "tmp000": {"source": "留学院校中文名称", "format": "str"},
                # "tmp000": {"source": "留学院校外文名称", "format": "str"},
                # "tmp000": {"source": "留学专业中文名称", "format": "str"},
                # "tmp000": {"source": "留学专业外文名称", "format": "str"},
                "type_org_code": {"source": "单位性质代码", "format": "str"},
                "type_org": {"source": "单位性质", "format": "str"},
                "industry_org_code": {"source": "单位行业代码", "format": "str"},
                "industry_org": {"source": "单位行业", "format": "str"},
                "location_org_code": {"source": "单位所在地代码", "format": "str"},
                "location_org": {"source": "单位所在地", "format": "str"},
                "address_org": {"source": "单位地址", "format": "str"},
            },
        }

        super().__init__(arg)
        return

    ###
    def get_flist(self, folder):
        file_key = "student_" + self.year + ".xlsx"
        super().get_flist(folder, file_key)
        return

    ###
    def get_year_fp(self, xls):

        for row in xls:
            year_fp = "y" + f"{row['毕业年份']}"
            # print(f"year_fp = #{year_fp}#")
            break
        return year_fp

    ###
    def insert2db(self):
        stu4chk = StudentWashd(self.year)
        stu4chk.load_db()
        return super().insert2db("sid", stu4chk.tree, tree4post=None)

    ### set some additional fields (to post to db)
    def setf_xfields(self):

        result = {"degree_year": {}, "path_fr": {}}

        # set field 'oname_fr'
        for k, v in self.tree.items():  # k = sid

            match v["path_after_graduate_code"]:
                case "271":  # 271	科研助理、管理助理—科研助理、管理助理
                    v["oname_fr"] = "科研助理管理助理[271]"
                case "46":  # 46	应征义务兵
                    v["oname_fr"] = "应征义务兵[46]"
                case "502":  # 502	国家基层项目—三支一扶
                    v["oname_fr"] = "国家基层项目[502]"
                case "503":  # 503	国家基层项目—西部计划
                    v["oname_fr"] = "国家基层项目[503]"
                case "512":  # 512	地方基层项目—地方选调生
                    v["oname_fr"] = "地方基层项目[512]"
                case "519":  # 519	地方基层项目—其他地方基层项目
                    v["oname_fr"] = "地方基层项目[519]"
                case "76":  # 76	自由职业
                    v["oname_fr"] = "自由职业[76]"
                case _:  # others
                    v["oname_fr"] = v["oname"]

            # set degree_year
            v["degree_year"] = ""
            match v["degree"]:
                case "二学位毕业" | "本科生毕业":
                    v["degree_year"] = "bachelor"
                case "本科生结业":
                    v["degree_year"] = "bachelor"
                    result["degree_year"][k] = v["degree"]
                case "硕士生毕业":
                    v["degree_year"] = "master"
                case "博士生毕业":
                    v["degree_year"] = "doctor"

            if v["degree_year"] == "":
                raise ValueError(
                    f"学号={k}的学历层次取值有误，期望是'二学位毕业,'本科生毕业','本科生结业', '硕士生毕业','博士生毕业'。"
                )
            else:
                v["degree_year"] += "-" + self.year  # -， not _

            # set path_fr
            match v["path_after_graduate_code"]:

                case (
                    "10"
                    | "11"
                    | "12"
                    | "46"
                    | "76"
                    | "271"
                    | "502"
                    | "503"
                    | "512"
                    | "519"
                ):
                    v["path_fr"] = "got-job"
                case "85" | "801" | "802" | "80":
                    v["path_fr"] = "further-study"
                case _:
                    result["path_fr"][k] = v["path_after_graduate_code"]
                    v["path_fr"] = FV_NOT_FOUND
                    # loggi(
                    #     f"学号={k}的'毕业去向类别代码'({v['path_after_graduate_code']})取值有误，期望是'10,11,12,271,46,502,503,512,519,76,801,802,85'。"
                    # )

        return result

    ###

    ###
    def update2db(self):
        stu4chk = StudentWashd(self.year)
        stu4chk.load_db()
        return super().update2db("sid", stu4chk.tree, tree4post=None)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class StuImport(Student):
    # for loading excel file to db

    ###
    def __init__(self, year, xargs=None):

        args = {
            "coll": "student_" + year + "_" + datetime.now().strftime("%y%m"),
            "year": year,
            #
            "map_fp2t": {
                "sid": {"source": "学号", "key": True},  # key
                ### bio - 8
                "bio.exam_number": {"source": "考生号"},
                "bio.sname": {"source": "姓名"},
                "bio.gender": {"source": "性别"},
                "bio.birth_date": {"source": "出生日期"},
                "bio.student_from_code": {"source": "生源地代码"},
                "bio.student_from": {"source": "生源地名称"},
                "bio.nationality": {"source": "民族名称"},
                "bio.political_status": {"source": "政治面貌"},
                # "tmp000": {"source": "城乡生源", "format": "str"},
                #
                ### campus - 6
                "campus.major": {"source": "专业名称", "format": "str"},
                "campus.specialization": {"source": "专业方向", "format": "str"},
                "campus.major_code": {"source": "专业代码", "format": "str"},
                "campus.graduate_year": {"source": "毕业年份", "format": "str"},
                "campus.degree": {"source": "学历层次", "format": "str"},
                # "campus.degree_year": {}, set based on 'degree' + 'graduate_year'
                #
                # "tmp000": {"source": "入学年月", "format": "str"},
                # "tmp000": {"source": "毕业日期", "format": "str"},
                # "tmp000": {"source": "培养方式", "format": "str"},
                # "tmp000": {"source": "委培单位", "format": "str"},
                # "tmp000": {"source": "学习形式", "format": "str"},
                #
                ### path - 6
                "path.path_after_graduate_code": {"source": "毕业去向类别代码"},
                "path.path_after_graduate": {"source": "毕业去向"},
                # "path.path_fr": {} set based on 'path_after_graduate_code'
                "path.contract_date": {"source": "签约日期"},
                "path.position_type_code": {"source": "工作职位类别代码"},
                "path.position_type": {"source": "工作职位类别"},
                #
                ### org - 16
                "org.oname": {"source": "单位名称"},
                "org.oname_fr": {"source": "oname_fr"},
                # "category": {},
                # "category_fr": {}, # copy from 'organization'
                "org.usci_org": {"source": "统一社会信用代码"},
                # "usci_org_fr": {},
                # "tmp000": {"source": "留学国家地区", "format": "str"},
                # "tmp000": {"source": "留学院校中文名称", "format": "str"},
                # "tmp000": {"source": "留学院校外文名称", "format": "str"},
                # "tmp000": {"source": "留学专业中文名称", "format": "str"},
                # "tmp000": {"source": "留学专业外文名称", "format": "str"},
                "org.type_org_code": {"source": "单位性质代码", "format": "str"},
                "org.type_org": {"source": "单位性质", "format": "str"},
                # "type_org_fr": {},
                "org.industry_org_code": {"source": "单位行业代码", "format": "str"},
                "org.industry_org": {"source": "单位行业", "format": "str"},
                # "industry_org_fr": {},
                "org.location_org_code": {"source": "单位所在地代码", "format": "str"},
                "org.location_org": {"source": "单位所在地", "format": "str"},
                # "location_org_fr": {},
                "org.address_org": {"source": "单位地址", "format": "str"},
            },
        }

        if xargs is not None:
            args.update(xargs)

        super().__init__(year, xargs=args)
        return

    ###
    def get_flist(self, folder, file_key=None):
        if file_key is None:
            file_key = "student_" + self.year + ".xlsx"
        super().get_flist(folder, file_key)
        return

    ###
    def get_year_fp(self, xls):

        for row in xls:
            year_fp = "y" + f"{row['毕业年份']}"
            # print(f"year_fp = #{year_fp}#")
            break
        return year_fp

    ###
    def insert2db(self, key_field=None, tree4chk=None, tree4post=None):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().insert2db("sid", tmp.tree, tree4post=None)

    ### set some additional fields (to post to db)
    def setf_xfields(self):

        result = {"degree_year": {}, "path_fr": {}}

        # set field 'oname_fr'
        for k, v in self.tree.items():  # k = sid

            match self.getf_path_after_graduate_code(k):
                case "271":  # 271	科研助理、管理助理—科研助理、管理助理
                    self.putf_oname_fr(k, "科研助理管理助理[271]")
                case "46":  # 46	应征义务兵
                    self.putf_oname_fr(k, "应征义务兵[46]")
                case "502":  # 502	国家基层项目—三支一扶
                    self.putf_oname_fr(k, "国家基层项目[502]")
                case "503":  # 503	国家基层项目—西部计划
                    self.putf_oname_fr(k, "国家基层项目[503]")
                case "512":  # 512	地方基层项目—地方选调生
                    self.putf_oname_fr(k, "地方基层项目[512]")
                case "519":  # 519	地方基层项目—其他地方基层项目
                    self.putf_oname_fr(k, "地方基层项目[519]")
                case "76":  # 76	自由职业
                    self.putf_oname_fr(k, "自由职业[76]")
                case _:  # others
                    # v["oname_fr"] = v["oname"]
                    pass

            # set degree_year
            self.putf_degree_year(k, "")
            degree = self.getf_degree(k)
            match degree:
                case "二学位毕业" | "本科生毕业":
                    self.putf_degree_year(k, "bachelor")
                case "本科生结业":
                    self.putf_degree_year(k, "bachelor")
                    result["degree_year"][k] = degree
                case "硕士生毕业":
                    self.putf_degree_year(k, "master")
                case "硕士生结业":
                    self.putf_degree_year(k, "master")
                    result["degree_year"][k] = degree
                case "博士生毕业":
                    self.putf_degree_year(k, "doctor")
                case "博士生结业":
                    self.putf_degree_year(k, "doctor")
                    result["degree_year"][k] = degree

            if self.getf_degree_year(k) == "":
                print(f"k={k}, v={v}")
                raise ValueError(
                    f"学号={k}的学历层次取值有误，期望是'二学位毕业,'本科生毕业','本科生结业', '硕士生毕业','硕士生结业','博士生毕业'，'博士生结业'。"
                )
            else:
                self.putf_degree_year(
                    k, self.getf_degree_year(k) + "-" + self.year
                )  # -， not _

            # set path_fr
            match self.getf_path_after_graduate_code(k):

                case (
                    "10"
                    | "101"
                    | "102"
                    | "11"
                    | "12"
                    | "46"
                    | "76"
                    | "271"
                    | "502"
                    | "503"
                    | "512"
                    | "519"
                ):
                    self.putf_path_fr(k, "got-job")
                case "85" | "801" | "802" | "80":
                    self.putf_path_fr(k, "further-study")
                case _:
                    result["path_fr"][k] = self.getf_path_after_graduate_code(k)
                    self.putf_path_fr(k, "FV_NOT_FOUND")
                    # loggi(
                    #     f"学号={k}的'毕业去向类别代码'({v['path_after_graduate_code']})取值有误，期望是'10,11,12,271,46,502,503,512,519,76,801,802,85'。"
                    # )

        return result

    ###

    ###
    def update2db(self, key_field=None, tree4chk=None, tree4post=None):
        if key_field is None:
            key_field = "sid"
        if tree4chk is None:
            tmp = self.__class__(self.year)
            tmp.load_db()
            tree4chk = tmp.tree
        return super().update2db(key_field, tree4chk, tree4post)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class StuImpMes(StuImport):
    # for loading excel file to db, for MES (Mechanical and Electrical (M&E) Sector)

    ###
    def __init__(self, year, xargs=None):

        args = {
            "coll": "mes_student_" + year + "_" + datetime.now().strftime("%y%m"),
            "year": year,
            #
            "map_fp2t": {
                "sid": {"source": "学号", "key": True},  # key
                ### bio - 1
                "bio.gender": {"source": "性别"},
                #
                ### campus - 4
                "campus.major": {"source": "专业名称", "format": "str"},
                "campus.graduate_year": {"source": "毕业年份", "format": "str"},
                "campus.degree": {"source": "学历层次", "format": "str"},
                # "campus.degree_year": {}, set based on 'degree' + 'graduate_year'
                ### path - 1
                "path.position_type": {"source": "工作职位类别"},
                #
                ### org - 16
                "org.oname": {"source": "单位名称"},
                "org.oname_fr": {"source": "oname_fr"},
                # "org.category": {},
                # "org.category_fr": {}, # copy from 'organization'
                # "org.usci_org_fr": {},
                "org.type_org": {"source": "单位性质", "format": "str"},
                # "org.type_org_fr": {},
                # "org.industry_org_fr": {},
                # "org.location_org_fr": {},
                "org.address_org": {"source": "单位地址", "format": "str"},
            },
        }

        if xargs is not None:
            args.update(xargs)

        super().__init__(year, xargs=args)
        return

    ###
    def get_flist(self, folder):
        file_key = "mes_student_" + self.year + ".xlsx"
        super().get_flist(folder, file_key)
        return

    ###
    def get_year_fp(self, xls):

        for row in xls:
            year_fp = "y" + f"{row['毕业年份']}"
            # print(f"year_fp = #{year_fp}#")
            break
        return year_fp

    ###
    def insert2db(self):
        tmp = self.__class__(self.year)
        tmp.load_db()
        return super().insert2db("sid", tmp.tree, tree4post=None)

    ### set some additional fields (to post to db)
    def setf_xfields(self):

        result = {"degree_year": {}, "path_fr": {}}

        # set field 'oname_fr'
        for k, v in self.tree.items():  # k = sid

            """
            match v["path_after_graduate_code"]:
                case "271":  # 271	科研助理、管理助理—科研助理、管理助理
                    v["oname_fr"] = "科研助理管理助理[271]"
                case "46":  # 46	应征义务兵
                    v["oname_fr"] = "应征义务兵[46]"
                case "502":  # 502	国家基层项目—三支一扶
                    v["oname_fr"] = "国家基层项目[502]"
                case "503":  # 503	国家基层项目—西部计划
                    v["oname_fr"] = "国家基层项目[503]"
                case "512":  # 512	地方基层项目—地方选调生
                    v["oname_fr"] = "地方基层项目[512]"
                case "519":  # 519	地方基层项目—其他地方基层项目
                    v["oname_fr"] = "地方基层项目[519]"
                case "76":  # 76	自由职业
                    v["oname_fr"] = "自由职业[76]"
                case _:  # others
                    v["oname_fr"] = v["oname"]
            """

            # set degree_year
            self.putf_degree_year(k, "")
            degree = self.getf_degree(k)
            match degree:
                case "二学位毕业" | "本科生毕业":
                    self.putf_degree_year(k, "bachelor")
                case "本科生结业":
                    self.putf_degree_year(k, "bachelor")
                    result["degree_year"][k] = degree
                case "硕士生毕业":
                    self.putf_degree_year(k, "master")
                case "硕士生结业":
                    self.putf_degree_year(k, "master")
                    result["degree_year"][k] = degree
                case "博士生毕业":
                    self.putf_degree_year(k, "doctor")
                case "博士生结业":
                    self.putf_degree_year(k, "doctor")
                    result["degree_year"][k] = degree

            if self.getf_degree_year(k) == "":
                print(f"k={k}, v={v}")
                raise ValueError(
                    f"学号={k}的学历层次取值有误，期望是'二学位毕业,'本科生毕业','本科生结业', '硕士生毕业','硕士生结业','博士生毕业'，'博士生结业'。"
                )
            else:
                self.putf_degree_year(
                    k, self.getf_degree_year(k) + "-" + self.year
                )  # -， not _

            """
            # set path_fr
            match v["path_after_graduate_code"]:

                case (
                    "10"
                    | "11"
                    | "12"
                    | "46"
                    | "76"
                    | "271"
                    | "502"
                    | "503"
                    | "512"
                    | "519"
                ):
                    v["path_fr"] = "got-job"
                case "85" | "801" | "802" | "80":
                    v["path_fr"] = "further-study"
                case _:
                    result["path_fr"][k] = v["path_after_graduate_code"]
                    v["path_fr"] = FV_NOT_FOUND
                    # loggi(
                    #     f"学号={k}的'毕业去向类别代码'({v['path_after_graduate_code']})取值有误，期望是'10,11,12,271,46,502,503,512,519,76,801,802,85'。"
                    # )
            """

        return result

    ###

    ###
    def update2db(self):
        stu4chk = self.__class__(self.year)
        stu4chk.load_db()
        return super().update2db("sid", stu4chk.tree, tree4post=None)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class StuMes(Student):
    # for loading excel file to db, for MES (Mechanical and Electrical (M&E) Sector)

    ###
    def __init__(self, year, xargs=None):

        args = {
            "coll": "mes_student_" + year,
            "year": year,
            "to_fpath": "exp-mes_student-"
            + year
            + "-"
            + datetime.now().strftime("%m%d")
            + ".xlsx",
            #
            "map_t2fp": {
                "学号": {"source": "sid", "key": True},  # key
                ### bio - 1
                "性别": {"source": "bio.gender"},
                #
                ### campus - 4
                "专业名称": {"source": "campus.major"},
                "毕业年份": {"source": "campus.graduate_year"},
                "学历层次": {"source": "campus.degree"},
                # "campus.degree_year": {}, set based on 'degree' + 'graduate_year'
                ### path - 1
                "工作职位类别": {"source": "path.position_type"},
                #
                ### org - 16
                "单位名称": {"source": "org.oname"},
                "oname_fr": {"source": "org.oname_fr"},
                # "org.category": {},
                # "org.category_fr": {}, # copy from 'organization'
                # "org.usci_org_fr": {},
                "单位性质": {"source": "org.type_org", "format": "str"},
                # "org.type_org_fr": {},
                # "org.industry_org_fr": {},
                # "org.location_org_fr": {},
                "单位地址": {"source": "org.address_org", "format": "str"},
                #
                "usci_org_fr": {"source": "org.usci_org_fr"},
                "category": {"source": "org.category"},  # ent, gpo, etc.
                "category_fr": {"source": "org.category_fr"},
                "type_org": {"source": "org.type_org"},
                "type_org_fr": {"source": "org.type_org_fr"},
                "industry_org_fr": {"source": "org.industry_org_fr"},
                "location_org_fr": {"source": "org.location_org_fr"},
                "province": {"source": "org.province"},
                "city": {"source": "org.city"},
                "district": {"source": "org.district"},
                "usn": {"source": "usn"},
                # root_node
                "root_usn": {"source": "root_node.usn"},
                "root_oname_fr": {"source": "root_node.oname_fr"},
            },
        }
        super().__init__(year, xargs=args)
        return

    ###
    def copyf_h_tagx(self, tree4copy):

        __tag_copied = {
            "got-job": 0,
            "match": 0,
            "match-fn": {},
            "not-found": {},
            "egc_tagx": {},
        }
        for k, v in self.tree.items():

            # only focus 'got-job'
            # if v["path_fr"] == "got-job":
            __tag_copied["got-job"] += 1
            # else:
            #     continue

            __match = False
            for korg, vorg in tree4copy.items():
                oname_fr = self.getf_oname_fr(k)

                if oname_fr == vorg["oname_fr"]:
                    __tag_copied["match"] += 1
                    __match = True

                if oname_fr in vorg["bio"]["former_name_fr"]:
                    __tag_copied["match"] += 1
                    __match = True
                    __tag_copied["match-fn"][k] = oname_fr

                if __match == True:
                    # print(f"korg={korg}, vorg={vorg}")
                    self.putf_category(k, vorg["category"])
                    self.putf_category_fr(k, vorg["bio"]["category_fr"])
                    self.putf_usci_org_fr(k, vorg["usci_org_fr"])
                    self.putf_type_org_fr(k, vorg["bio"]["type_org_fr"])
                    self.putf_industry_org_fr(k, vorg["bio"]["industry_org_fr"])
                    self.putf_location_org_fr(k, vorg["bio"]["location_org_fr"])
                    self.putf_province(k, vorg["bio"].get("province", ""))
                    self.putf_city(k, vorg["bio"].get("city", ""))
                    self.putf_district(k, vorg["bio"].get("district", ""))
                    v["usn"] = korg  # f26
                    v["root_node"] = vorg["root_node"]  # f36-f37

                    for tag, mark in vorg["egc_tagx"].items():  # f27-f35
                        # print(
                        #     f"year = {self.year}, vorg['egc_tagx'] = {vorg['egc_tagx']}"
                        # )
                        if v["egc_tagx"] is None:
                            v["egc_tagx"] = {}
                        v["egc_tagx"][tag] = mark[self.year]

                        # count mark + [mark] for each tag
                        if v["egc_tagx"][tag] != FV_NOT_FOUND:
                            if __tag_copied["egc_tagx"].get(tag, 0) == 0:  # not exist
                                __tag_copied["egc_tagx"][tag] = 0
                            __tag_copied["egc_tagx"][tag] += 1

                    # if match then break
                    break

            if __match == False:
                self.putf_category(k, FV_NOT_FOUND)
                self.putf_category_fr(k, FV_NOT_FOUND)
                self.putf_usci_org_fr(k, FV_NOT_FOUND)
                self.putf_type_org_fr(k, FV_NOT_FOUND)
                self.putf_industry_org_fr(k, FV_NOT_FOUND)
                self.putf_location_org_fr(k, FV_NOT_FOUND)
                self.putf_province(k, FV_NOT_FOUND)
                self.putf_city(k, FV_NOT_FOUND)
                self.putf_district(k, FV_NOT_FOUND)
                v["usn"] = FV_NOT_FOUND  # f26
                v["egc_tagx"] = {}  # f27-f35
                v["root_node"] = {}  # f36-f37

                __tag_copied["not-found"][k] = oname_fr

        return __tag_copied


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# student for pick, e.g., export 'organization' for student, y2024, bachelor.
class Stu4Pick(RemAiv3):
    ###
    def __init__(self, year):

        coll = "student_" + year
        arg = {
            "coll": coll,
            "year": year,
            "map_db2t": {
                "usn": {"source": "usn", "key": True},
                "root_node": {"source": "root_node"},
            },
        }

        super().__init__(arg)
        return


# ------------------------------------------------------------------------------


def add_org():
    ###
    org_bizreg = OrgBizReg()
    org_pchild = OrgPChild()
    org_astobe = OrgAsToBe()

    ### get file list
    logging.info("-" * 72)
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
            logging.info("-" * 16)
            logging.warning(
                f"{org_bizreg.coll} :: 'category' unknown for {len(result)} doc(s) : {result} "
            )
            logging.info("-" * 16)

        # insert into 'org_bizreg'
        result = org_bizreg.insert2db("oname_fr", tmp.tree)
        if result["ops4db"] == True:
            logging.info(
                f"{org_bizreg.coll} :: insert done, [{result['result'].inserted_count}] inserted, [{len(tmp.tree)}] before operation."
            )
        else:
            logging.info(f"{org_bizreg.coll} :: insert done, no doc posted to db.")

        # insert into 'org_pchild'
        tmp = OrgPChild()
        tmp.load_db()
        # for k, v in tmp.tree.items():
        #     print(f"{tmp.coll} :: k={k}, v={v}")
        result = org_pchild.insert2db("oname_fr", tmp.tree)
        if result["ops4db"] == True:
            logging.info(
                f"{org_pchild.coll} :: insert done, [{result['result'].inserted_count}] inserted, [{len(tmp.tree)}] before operation."
            )
        else:
            logging.info(f"{org_pchild.coll} :: insert done, no doc posted to db.")

    # return

    ### step #2 ： set 'usn' for 'org_bizreg' & 'org_pchild'
    logging.info("-" * 72)
    org4usn = Org4Usn()
    org4usn.load_db()

    # for 'org_bizreg'
    org_bizreg.load_db()
    # logging.info(f"{org_bizreg.coll} :: set field 'usn' based on '{org4usn.coll}'")
    result = org_bizreg.setf_usn(org4usn.tree)
    if result > 0:
        logging.info("-" * 16)
        logging.warning(f"{org_bizreg.coll} :: 'usn' of {result} doc(s) not found.")
        logging.info("-" * 16)

    tmp = OrgBizReg()
    tmp.load_db()
    result = org_bizreg.update2db("oname_fr", tmp.tree)
    if result["ops4db"] == True:
        logging.info(
            f"{org_bizreg.coll} :: 'usn' update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified."
        )
    else:
        logging.info(f"{org_bizreg.coll} :: 'usn' update done, no doc changed in db.")

    # for 'org_pchild'
    org_pchild.load_db()
    # logging.info(f"{org_pchild.coll} :: set field 'usn' based on '{orgv2.coll}'")
    result = org_pchild.setf_usn(org4usn.tree)
    if result > 0:
        logging.warning(f"{org_pchild.coll} :: 'usn' of {result} doc(s) not found.")

    tmp = OrgPChild()
    tmp.load_db()
    result = org_pchild.update2db("oname_fr", tmp.tree)
    if result["ops4db"] == True:
        logging.info(
            f"{org_pchild.coll} :: 'usn' update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified."
        )
    else:
        logging.info(f"{org_pchild.coll} :: 'usn' update done, no doc changed in db.")

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
            if v["parent_node"]["oname_fr"] in __usn_not_found:
                continue
            __usn_nf_404_pc[v["parent_node"]["oname_fr"]] = {}

    if len(__usn_nf_404_pc) > 0:
        logging.info("-" * 16)
        logging.warning(
            f"  [{len(__usn_nf_404_pc)}] more 'usn' not found in '{org_pchild.coll}', not-found : {__usn_nf_404_pc}"
        )
        logging.info("-" * 16)

    # org_bizreg = {}
    # org_pchild = {}
    # org_astobe = {}

    ###
    logging.info("-" * 72)

    choice = input(
        "[Step #2] Load docs ('usn' not found) from 'org_bizreg' to 'org_astobe', go ？(Y/n) : "
    )
    logging.info("-" * 72)
    if choice in ["Y", "y"]:

        print(f"size = {org_bizreg.size} (before)")
        org_bizreg.load_db(queryx={"usn": "nf-404"})
        print(f"size = {org_bizreg.size} (after)")
        # org_bizreg.show_tree()

        tmp = Org4Usn()
        max_usn = tmp.get_max_usn()
        # print(f"{tmp.coll} :: max_usn = {max_usn}")
        tmp = ""

        # set usn for usn not found
        org_bizreg.setf_usn4nf(max_usn)
        # org_bizreg.show_tree()

        # set parent_node
        tmp = OrgPChild()
        tmp.load_db()
        result = org_bizreg.setf_parent_node(tmp.tree)
        if len(result) > 0:
            logging.info("-" * 16)
            logging.warning(
                f"{org_bizreg.coll} :: [{len(result)}] parent_node's 'usn' not found : {result}"
            )
            logging.info("-" * 16)
        # org_bizreg.show_tree()

        # insert into 'org_astobe'
        tmp = OrgAsToBe()
        tmp.load_db()
        # print(f"{tmp.coll} :: size = {tmp.size}")
        # print(f"{org_bizreg.coll} :: size = {org_bizreg.size}")
        # if tmp.size > 0:
        #     logging.warning(
        #         f"{tmp.coll} :: [{tmp.size}] doc(s) in db. empty it and try again."
        #     )
        #     return
        result = tmp.insert2db("oname_fr", tree4chk=tmp.tree, tree4post=org_bizreg.tree)
        if result["ops4db"] == True:
            logging.info(
                f"{tmp.coll} :: insert done, [{result['result'].inserted_count}] inserted."
            )
        else:
            logging.info(
                f"{tmp.coll} :: no doc inserted into db (source = {org_bizreg.tree})."
            )

        # update 'org_asis2b' after insertion
        tmp = OrgAsToBe()
        tmp.load_db()
        result = tmp.update2db("name_fr", tree4chk=tmp.tree, tree4post=org_bizreg.tree)
        if result["ops4db"] == True:
            logging.info(
                f"{tmp.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified."
            )
        else:
            logging.info(
                f"{tmp.coll} :: no doc updated to db (source = {org_bizreg.tree})."
            )

    ### load from 'org_asis2b' and insert into 'organization'
    logging.info("-" * 72)
    choice = input(
        "[Step #3] Insert into 'organization' from 'org_astobe', go ？(Y/n) : "
    )
    logging.info("-" * 72)
    if choice not in ["Y", "y"]:
        return

    org_astobe = OrgAsToBe()
    org_astobe.load_db()

    # insert into 'organization' from 'org_asis2b'
    tmp = Org4Usn()
    tmp.load_db()
    result = tmp.insert2db("oname_fr", tree4chk=tmp.tree, tree4post=org_astobe.tree)
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


# ------------------------------------------------------------------------------
def exp_org_stu():

    def __exp_org(pick4org, *xyears):
        # print(f"__exp_org :: xyears = #{xyears}#")
        org4exp = Organization(*xyears)  # !!! xyears is list/tuple, call it with *
        logging.info(f"{org4exp.coll} :: exporting docs to excel ...'")
        org4exp.load_db()
        # print(f"{org4exp.coll} :: size = {org4exp.size}")

        xtrees4pick = {}
        for year in xyears:
            match pick4org:
                case "1":
                    query = {"degree_year": "bachelor-" + year}
                case "2":
                    query = {
                        "$or": [
                            {"degree_year": "master-" + year},
                            {"degree_year": "doctor-" + year},
                        ]
                    }
                    query = {"degree_year": "bachelor-" + year}
                case "3":
                    query = {}

            stu4pick = Stu4Pick(year)
            stu4pick.load_db(query)
            xtrees4pick[year] = {}
            for k, v in stu4pick.tree.items():
                xtrees4pick[year][k] = v

        result = org4exp.post2fp(xtrees4pick)
        logging.info(
            f"{org4exp.coll} :: {result} doc(s) export to '{org4exp.to_fpath}.'"
        )

        return

    ###
    def __exp_stu(year):

        stu4exp = StudentYear(year)
        logging.info("-" * 72)
        logging.info(f"{stu4exp.coll} :: exporting docs to excel ...'")
        stu4exp.load_db()

        result = stu4exp.post2fp()
        logging.info("-" * 72)
        logging.info(
            f"{stu4exp.coll} :: {result} doc(s) export to '{stu4exp.to_fpath}.'"
        )

        return

    ###
    choice = input("Your choice (1-organizaton, 2-student, other = quit : ")

    ### export 'organization', a or several years
    if choice == "1":
        while True:
            __is_valid, xyears = input_xyears()
            if __is_valid:
                break

        choice = input(
            f"Export 'organization' with years {xyears}, go ? (q=quit, other is go) : "
        )
        if choice in ["q", "Q"]:
            return

        while True:
            pick4org = input(
                "Organization for : 1-bachelor, 2-master&doctor, 3-all (1/2/3) : "
            )
            if pick4org in ["1", "2", "3"]:
                break
        #
        # print(f"xyears = #{xyears}#")
        __exp_org(pick4org, *xyears)

    ### export student_yxxx
    if choice == "2":
        __is_valid, year = input_year()
        if __is_valid:
            __exp_stu(year)
    return


# ------------------------------------------------------------------------------
def xtgme(remaicfg):

    # copy tags and some field to 'mes_student' from 'organization'
    stumes = StuMes(remaicfg.year)
    stumes.load_db()

    org = OrganizationV3(remaicfg.year)
    org.load_db()
    rlog.info("-" * 72)
    rlog.info(f"{stumes.coll} :: copy tags and some fields from '{org.coll}'")

    result = stumes.copyf_h_tagx(org.tree)
    if result["got-job"] == result["match"]:
        rlog.info(f"{result['got-job']} student(s) got job, {result['match']} matched.")
    else:
        rlog.warning(
            f"{result['got-job']} student(s) got job, {result['match']} matched, {result['got-job'] - result['match']} not found."
        )
        rlog.warning(f"not-found = {result['not-found']}\n")
    if len(result["match-fn"]) > 0:
        rlog.info(
            f"{len(result['match-fn'])} student(s) matched former name : {result['match-fn']}"
        )
    rlog.info(f"tags copied (excl. 'nf-404') : {result['egc_tagx']}")

    # post to 'mes_student'
    result = stumes.update2db()
    rlog.info(result["message"])

    return


# ------------------------------------------------------------------------------
def xtags(remaicfg, args=None):

    organization = OrganizationV3(remaicfg.year)  # like 'y2024'
    # print(f"{org.coll} :: {org.year}")
    rlog.info("-" * 72)
    rlog.info(f"{remaicfg.year} :: tag '{organization.coll}'.")
    rlog.info("-" * 72 + "\n")
    organization.load_db()

    # 'organization' : set more fields.
    rlog.info("-" * 72)
    rlog.info(f"{organization.coll} :: set more fields like 'child_node' etc.")
    result = organization.setf_more2db()
    if len(result["psn_nf_404"]) > 0:
        rlog.warning(
            f"{organization.coll}::{len(result['psn_nf_404'])} 'h_psn' not found : {result['psn_nf_404']}\n"
        )

    #
    list4by = [
        ListGlb500(remaicfg.year),
        ListChn500(remaicfg.year),
        ListSoe(remaicfg.year),
        ListPlc(remaicfg.year),
        NationalHte(remaicfg.year),
        ListIsc100(remaicfg.year),
        ListFic500All(remaicfg.year),
        ListFic500Mfg(remaicfg.year),
        ListFic100Svc(remaicfg.year),
    ]

    #
    for listx in list4by:

        rlog.info("-" * 72)
        # print(f"listx :: {__listx.coll}, {__listx.year}, {__listx.tag}, {__listx.886}")

        # load into listx from db
        # listx.load_db()
        listx.load_db(tree4load=organization.tree)  # for nht only
        if listx.size == 0:
            rlog.warning(
                f"{organization.coll} :: empty '{listx.coll}', then skip tag '{listx.tag}'.\n"
            )

        # 0/3 : clear mark
        for k, v in organization.tree.items():
            organization.setf_egc_tagx(
                k,
                listx.tag,
                "",
            )

        # 1/3 : set mark
        c4mark = {"node": {}, "mark": {}}
        for k, v in organization.tree.items():
            result = listx.get_mark(k, v)
            if len(result) > 0:
                organization.setf_egc_tagx(
                    k,
                    listx.tag,
                    result,
                )
                c4mark["node"][k] = v["oname_fr"]
                c4mark["mark"][result] = {}
        # if listx.tag == "nht":
        #     print(f"c4mark = {c4mark}")
        rlog.info(
            f"{organization.coll} :: tag '{listx.tag}', {len(c4mark['node'])} doc(s) got mark  '{list(c4mark['mark'].keys())}'."
        )

        # 2/3 : set mark for root_node, if appplicable
        # print(f"tag={listx.tag}, tag_root={listx.tag_root}")
        if listx.tag_root == True:
            result = organization.setf_egc_tagx_root(listx.tag, listx.mark)
            if len(result) > 0:
                rlog.info(
                    f"{organization.coll} :: tag '{listx.tag}', {len(result)} root-node(s) marked : {result}."
                )
        else:
            rlog.info(
                f"{organization.coll} :: tag '{listx.tag}', tag-root not applicable."
            )

        # 3/3 : set field 'egc_tagx' for the rest (still blank, not set yet)
        organization.tag_count["hit"] = {}
        organization.tag_count["h_set"] = 0
        organization.tag_count["nf_404"] = 0
        for k, v in organization.tree.items():
            if v["parent_node"] == {}:
                organization.setf_egc_tagx_rest(
                    k, listx.tag, organization.get_egc_tagx(k, listx.tag), listx.mark
                )
        rlog.info(
            f"{organization.coll} :: tag '{listx.tag}'\n hit : {len(organization.tag_count['hit'])}, h_set : {organization.tag_count['h_set']}, nf_404 : {organization.tag_count['nf_404']}"
        )

        # x/3 : post to 'organization'
        result = organization.update2db()
        if result["ops4db"] == True:
            rlog.info(
                f"{organization.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified."
            )
        else:
            rlog.info(f"{organization.coll} :: update done, no doc changed in db.")

        ### end of each tag

    # copy tags and some field to 'student' from 'organization'
    rlog.info("-" * 72)
    student = Student(remaicfg.year)
    student.load_db()
    organization.load_db()
    rlog.info(f"{student.coll} :: copy tags and some fields from '{organization.coll}'")
    result = student.copyf_h_tagx(organization.tree)
    if result["got-job"] == result["match"]:
        rlog.info(f"{result['got-job']} student(s) got job, {result['match']} matched.")
    else:
        rlog.warning(
            f"{result['got-job']} student(s) got job, {result['match']} matched, {result['got-job'] - result['match']} not found."
        )
        rlog.warning(f"not-found = {result['not-found']}\n")
    if len(result["match-fn"]) > 0:
        rlog.info(
            f"{len(result['match-fn'])} student(s) matched former name : {result['match-fn']}"
        )
    rlog.info(f"tags copied (excl. 'nf-404') : {result['egc_tagx']}")

    # post to 'student'
    result = student.update2db()
    rlog.info(result["message"])

    # print(f"student.tree={student.show_t()}")
    return


# ------------------------------------------------------------------------------
def toxls(remaicfg, args=None):

    # export 'student' to file
    for year in remaicfg.toxls_xyears:

        stu2xls = Student(year)
        rlog.info("-" * 72)
        rlog.info(f"{stu2xls.coll} :: exporting docs to excel ...'")
        rlog.info("-" * 72 + "\n")

        stu2xls.load_db()
        result = stu2xls.post2fp()
        rlog.info(
            f"{stu2xls.coll} :: {result} doc(s) export to '{stu2xls.to_fpath}.'\n"
        )

    # export 'org' to file
    xyears = remaicfg.toxls_xyears
    # print(f"xyears={xyears}")
    xyears.sort()
    # print(f"xyears={xyears}")
    org2xls = OrganizationV3(xyears)
    rlog.info("-" * 72)
    rlog.info(f"{org2xls.coll} :: exporting docs to excel ...'")
    rlog.info("-" * 72 + "\n")

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

    yearx_list = list(xyears)
    # print(f"xyears='{xyears}'")
    # print(f"yearx_list='{yearx_list}'")
    yearx_list.sort()  # e.g., yearx_list = ["y2021", "y2022"]
    # print(f"yearx_list='{yearx_list}'")

    for __to_file, __in_tree in __map_egc_tagx.items():
        for yearx in yearx_list:
            # print(f"yearx='{yearx}'")
            org2xls.map_t2fp[__to_file + yearx[-2:]] = {
                "source": "egc_tagx." + __in_tree + "." + yearx
            }

    xtree = {}
    for year in xyears:
        print(f"year = {year}, xyears={xyears}")
        match remaicfg.toxls_pick:
            case -1:
                query = {
                    "$or": [
                        {"campus.degree_year": "master-" + year},
                        {"campus.degree_year": "doctor-" + year},
                    ]
                }
            case 0:
                query = {}
            case 1:
                query = {"campus.degree_year": "bachelor-" + year}
            case _:
                raise ValueError("-1 / 0 / 1 expected for pick.")

        stu4pick = Stu4Pick(year)
        # print(f"query={query}\n")
        stu4pick.load_db(query)
        xtree[year] = {}
        for k, v in stu4pick.tree.items():  # k = usn, v = root_node
            xtree[year][k] = v
        # print(f"xtree={xtree}\n")
    org2xls.load_db()
    result = org2xls.post2fp(tree4pick=xtree)
    rlog.info(f"{org2xls.coll} :: {result} doc(s) export to '{org2xls.to_fpath}.'\n")

    return


# ------------------------------------------------------------------------------
def txlme(remaicfg):

    # export 'mes_student' to file
    xyears = remaicfg.toxls_xyears
    xyears.sort()
    for year in xyears:

        stu2xls = StuMes(year)
        rlog.info("-" * 72)
        rlog.info(f"{stu2xls.coll} :: exporting docs to excel ...'")
        rlog.info("-" * 72 + "\n")

        stu2xls.load_db()
        result = stu2xls.post2fp()
        rlog.info(
            f"{stu2xls.coll} :: {result} doc(s) export to '{stu2xls.to_fpath}.'\n"
        )


# ------------------------------------------------------------------------------
def wash_org_stu(year):
    # 'student_yxxxx' : set field 'oname_fr' (organization name, formal & registered)

    # check if value of year is valid
    if isinstance(year, str) is False:
        raise TypeError("string expected. e.g., 'y2024'.")
    elif (len(year) != 5) or (not year[1:].isdigit()):
        raise ValueError("y followed by 4-digit year expected. e.g., 'y2024'.")

    # 'student_yxxxx' : set more additional field, such as 'oname_fr'
    student = StudentYear(year)
    student.load_db()

    logging.info(f"{student.coll} :: set more fields like 'oname_fr' etc.")
    student.setf_more2db()

    tmp = StudentYear(year)
    tmp.load_db()
    # result = student.update2db("sid")
    result = student.update2db("sid", tree4chk=tmp.tree)
    if result["ops4db"] == True:
        logging.info(
            f"{student.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified."
        )
    else:
        logging.info(f"{student.coll} :: update done, no doc changed in db.")

    # ### organization::wash 'usn' + 'name_fr' to confirm whether all are unique.
    # gdtree = OrganizationDB(yearx)
    # # organization.wash()
    # key2wash = ["usn", "name_fr"]
    # logging.info(f"coll={gdtree.coll}::washing data, key-to-wash={key2wash}")
    # result_tree = gdtree.wash(key2wash)
    # for key in key2wash:
    #     if len(result_tree[key]) > 0:
    #         logging.warning(
    #             f"coll={gdtree.coll}::field={key}, value is NOT unique. Fix it then try again. {len(result_tree[key])} value(s) are duplicated: {result_tree[key]}"
    #         )
    #     else:
    #         logging.info(f"coll={gdtree.coll}::field={key}, all value are unique.")

    # ### student::wash 'sid'  to confirm whether all are unique.
    # gdtree = StudentYearDB(yearx)
    # key2wash = ["sid"]
    # # key2wash = ["sid", "name", "organization_fr"]
    # logging.info(f"coll={gdtree.coll}::washing data, key-to-wash={key2wash}")
    # result_tree = gdtree.wash(key2wash)
    # for key in key2wash:
    #     if len(result_tree[key]) > 0:
    #         logging.warning(
    #             f"coll={gdtree.coll}::field={key}, value is NOT unique. Fix it then try again. {len(result_tree[key])} value(s) are duplicated: {result_tree[key]}"
    #         )
    #     else:
    #         logging.info(f"coll={gdtree.coll}::field={key}, all value are unique.")

    # return

    return


###
def washd(remaicfg, args=None):

    # # 连接到 MongoDB
    # client = MongoClient()
    # db = client.get_database(HOME_DB)

    # print(f"{db.list_collection_names()}")

    # client.close()
    # return

    # migration2()
    # return

    # org4usn = Org4Usn()
    # org4usn.load_db()
    # print(f"{org4usn.show_t(count=100)}")
    # return

    # client = MongoClient()

    # try:
    #     # 替换为你的数据库名称
    #     db = client.get_database(HOME_DB)

    #     # 获取集合
    #     org2503_collection = db["bk_organization_250331"]
    #     org_collection = db["organization"]

    #     # 从 org2503 集合中获取所有文档
    #     org2503_docs = org2503_collection.find()

    #     # 遍历每个文档，根据 usn 更新 org 集合
    #     for doc in org2503_docs:
    #         # print(f"doc={doc}")
    #         usn = doc["usn"]
    #         nht_period = doc["misc"].get("nht_period", {})

    #         # 更新 org 集合中的文档
    #         org_collection.update_one(
    #             {"usn": usn},  # 匹配条件
    #             {"$set": {"nht_period": nht_period}},  # 更新操作
    #         )

    #     print("Update completed.")
    # finally:
    #     client.close()

    # return

    # get list of collection names
    client = MongoClient()
    db = client.get_database(HOME_DB)
    list_collection_names = db.list_collection_names().copy()
    print(f"list_collection_names={list_collection_names}")
    client.close()

    #
    rlog.info("-" * 72)
    rlog.info("add student file to db.")
    rlog.info("-" * 72 + "\n")

    stuimp = StuImport(remaicfg.year)

    load_fp = False
    if stuimp.coll_base in list_collection_names:
        if args.load_fp is True:
            load_fp = True
    else:
        load_fp = True

    if load_fp is False:
        rlog.info(f"{stuimp.coll_base} already exists, skip.")
    if load_fp is True:
        stuimp.get_flist(remaicfg.stu_fpath)
        rlog.info(
            f"{stuimp.coll} :: {len(stuimp.flist)} file(s)(就业方案导出) found.\n"
        )

        # read excel file
        count, fp = stuimp.load_fp()
        rlog.info(f"{stuimp.coll} :: {count} doc(s) read from file(s).")
        # student.show_tree()
        # print(f"size={student.size},show_t = {student.show_t()}")

        # insert into 'student_yxxx_ddhhmm'
        result = stuimp.insert2db()
        if result["ops4db"] == True:
            rlog.info(
                f"{stuimp.coll} :: insert done, [{result['result'].inserted_count}] inserted."
            )
        else:
            rlog.info(f"{stuimp.coll} :: insert done, no doc posted to db.")

        # set more fields
        # print(f"coll = {stuimp.coll}, map_db2t = {stuimp.map_db2t}\n")
        stuimp.load_db()  # load db, then setf_xfields()
        rlog.info(
            f"{stuimp.coll} :: set more fields, 'oname_fr' / 'degree_year' / 'path_fr' ..."
        )
        result = stuimp.setf_xfields()
        if len(result["degree_year"]) > 0:
            rlog.warning(
                f"{len(result['degree_year'])} student(s) have bad value of 'degree' : {result['degree_year']}.\n"
            )
        if len(result["path_fr"]) > 0:
            rlog.warning(
                f"{len(result['path_fr'])} student(s) have bad value of 'path_after_graduate' : {result['path_fr']}.\n"
            )
        # student.show_tree()

        result = stuimp.update2db()
        if result["ops4db"] == True:
            rlog.info(
                f"{stuimp.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified.\n"
            )
        else:
            rlog.info(f"{stuimp.coll} ::  update done, no doc changed in db.\n")

    ###
    # student :: wash empty field 'oname_fr'.
    """
    student = StudentYear(remaicfg.year)
    student.load_db()
    result = {}
    rlog.info("-" * 144)
    rlog.info(f"{student.coll} :: wash field 'oname_fr'.")
    for k, v in student.tree.items():
        match v["oname_fr"]:
            case "":
                # print(f"k={k}, v={v}")
                if v["path_fr"] == "got-job":
                    v["oname_fr"] = FV_NOT_FOUND
                    result[k] = {}
            case _:
                pass
    if len(result) > 0:
        rlog.info(
            f"{len(result)} doc(s) 's 'oname_fr' are set as '{FV_NOT_FOUND}' : {show_t(result,show='k',cr='no')} \n"
        )
    result = student.update2db()
    if result["ops4db"] == True:
        rlog.info(
            f"{student.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified.\n"
        )
    else:
        rlog.info(f"{student.coll} ::  update done, no doc changed in db.\n")
    """

    ###
    ###
    ###
    rlog.info("-" * 72)
    rlog.info("add organization files to db.")
    rlog.info("-" * 72 + "\n")

    #
    # print(f"remai_config:{remai_config}")
    # for k, v in remai_config.items():
    #     print(f"k={k}, v={v}")
    #     if type(v) == dict:
    #         for k1, v1 in v.items():
    #             print(f"k1={k1}, v1={v1}")
    org4br = OrgBizRegV3(remaicfg.year)
    org4br.get_flist(remaicfg.usci_fpath)
    rlog.info(f"{len(org4br.flist)} file(s)(工商注册信息) found.")
    result = org4br.load_fp()
    rlog.info(f"read {result} file(s)(工商注册信息) done.")

    # insert into 'org_bizreg'
    result = org4br.insert2db()
    rlog.info(result["message"])

    #
    org4brall = OrgBRv3All(remaicfg.year)
    org4brall.get_flist(remaicfg.usci_fpath)
    rlog.info(f"{len(org4brall.flist)} file(s)(工商注册信息) found.")
    result = org4brall.load_fp()
    # print(f"{org4brall.show_t()}")
    # print(f"result={result}")
    rlog.info(
        f"read {result['files']} file(s)(工商注册信息) done, {result['docs']} doc(s) read."
    )
    result = org4brall.insert2db()
    rlog.info(result["message"])

    #
    org4nu = OrgNoUSCI()
    org4nu.get_flist(remaicfg.usci_fpath)
    rlog.info(f" {len(org4nu.flist)} file(s)(无社会信用代码) found.")
    c4fp, fp = org4nu.load_fp()
    rlog.info(f" read {c4fp} doc(s) (无社会信用代码) done, from file '{fp.name}'.")
    result = org4br.insert2db(org4nu.tree)
    rlog.info(result["message"])

    #
    org4br = OrgBizRegV3(remaicfg.year)
    org4br.load_db()  # load db to set something

    # set 'category' for 'org_bizreg'
    rlog.info(f"{org4br.coll} :: set field 'category_fr' & 'category'")
    result = org4br.setf_category()
    if len(result) > 0:
        rlog.error(
            f"{org4br.coll} :: 'category' unknown for {len(result)} doc(s) : {result} \n"
        )

    # setf 'parent_node', based on file
    org4pc = OrgPChild()
    org4pc.get_flist(remaicfg.usci_fpath)
    rlog.info(f" {len(org4pc.flist)} file(s)(层级关系) found.")
    # print(f"fp={org4pchild.flist}")
    c4fp, fp = org4pc.load_fp()
    rlog.info(f" read {c4fp} doc(s) (层级关系) done, from file '{fp.name}'.")
    # org4pchild.show_tree(20)
    result = org4br.setf_parent_node(org4pc.tree)
    rlog.info(
        f"{org4br.coll} :: set field 'parent_node' based on file, skip: {result['skip']}, set: {result['set']}, pc: {show_t(result['pc'],cr='no')}, diff: {show_t(result['diff'],cr='no')}."
    )
    # print("=" * 144)
    # org4bizreg.show_tree(60)

    # setf 'parent_node', based on file from aqc
    org4pchild = OrgPCAqc()
    org4pchild.get_flist(remaicfg.usci_fpath)
    rlog.info(f" {len(org4pchild.flist)} file(s)(层级关系) found.")
    # print(f"fp={org4pchild.flist}")
    c4fp = org4pchild.load_fp()
    rlog.info(f" read {c4fp} doc(s) (层级关系) done.")
    # org4pchild.show_tree(20)
    result = org4br.setf_parent_node(org4pchild.tree)
    # print(f"org4pchild={org4pchild.show_t(count=30)}")
    rlog.info(
        f"{org4br.coll} :: set field 'parent_node' based on file-aqc, skip: {result['skip']}, set: {result['set']}, pc: {show_t(result['pc'],cr='no')}, diff: {show_t(result['diff'],cr='no')}."
    )

    # setf "parent_node", based on 'organization
    org4usn = Org4Usn()
    org4usn.load_db()  # load from 'organization', to setf 'parent_node', and 'usn'
    # print(f"{org4usn.show_t()}")
    result = org4br.setf_parent_node(org4usn.tree)
    rlog.info(
        f"{org4br.coll} :: set field 'parent_node' based on '{org4usn.coll}', skip: {result['skip']}, set: {result['set']}, pc: {show_t(result['pc'],cr='no')}, diff: {show_t(result['diff'],cr='no')}."
    )
    # print("=" * 144)
    # org4bizreg.show_tree(60)

    rlog.info(f"{org4br.coll} :: set field 'usn' based on '{org4usn.coll}'.")
    result = org4br.setf_usn(org4usn.tree)
    if len(result) > 0:
        rlog.info(
            f"{org4br.coll} :: 'usn' of {len(result)} doc(s) not found : {result}.\n"
        )
    # print("=" * 144)
    # org4bizreg.show_tree(60)

    # setf 'usn' for those not found
    max_usn = org4usn.get_max_usn()
    # print("=" * 144)
    # org4bizreg.show_tree(60)
    result = org4br.setf_usn4nf(max_usn)
    rlog.info(
        f"{org4br.coll} :: set 'usn' for nf-404 done. \n[{result['set']}] doc(s) set : {result['set_docs']}\n[{result['to_be_copied']}] doc(s) not copied to 'organization' yet : {result['to_be_copied_docs']}\n"
    )
    # print("=" * 144)
    # org4bizreg.show_tree(60)

    #
    rlog.info("-" * 72)
    rlog.info(f"{org4br.coll} :: wash field 'former_name_fr'.")
    for k, v in org4br.tree.items():
        # print(f"k={k}, v={v}")
        fnames = org4br.getf_nformer(k)
        match fnames:
            case None | "-":
                org4br.putf_nformer(k, {})
            case {}:
                pass
            case _:
                # several former names are seperated by '\n', from aiqicha.com
                fnames = fnames.split("\n")
                fname = {fn: {} for fn in fnames}
                # print(f"fname_node = {fname_node}")
                org4br.putf_nformer(k, fname)

    # org_bizreg :: wash field 'misc'
    rlog.info("-" * 72)
    rlog.info(f"{org4br.coll} :: wash field 'misc'.")
    field_none = {}
    for k, v in org4br.tree.items():
        # print()
        if v.get("misc", None) is None:
            v["misc"] = {}
            field_none[k] = {}
    if len(field_none) > 0:
        rlog.info(
            f"{len(field_none)} doc(s) 's have null 'misc', set empty value instead : {show_t(field_none, cr='no')}"
        )

    result = org4br.update2db()
    rlog.info(result["message"])

    # check if 'usn' all set
    org4br.load_db()
    usn_nf = {}
    pnode_usn_nf = {}
    for k, v in org4br.tree.items():
        # print(f"k={k}, v={v}")
        if v["usn"] is None:
            usn_nf[k] = {}
        if len(v["usn"]) == 0:
            usn_nf[k] = {}

        if v["parent_node"] == {}:
            continue
        if v["parent_node"].get("usn", None) is None:
            v["parent_node"]["usn"] = ""
            pnode_usn_nf[k] = {}
        if len(v["parent_node"]["usn"]) == 0:
            pnode_usn_nf[k] = {}

    if len(usn_nf) > 0:
        rlog.error(
            f"{org4br.coll} :: {len(usn_nf)} doc(s)'s 'usn' not found : {usn_nf}\n"
        )
    if len(pnode_usn_nf) > 0:
        rlog.error(
            f"{org4br.coll} :: {len(pnode_usn_nf)} doc(s)'s 'parent_node.usn' not found : {pnode_usn_nf}\n"
        )

    ###
    ###
    ###
    org = OrganizationV3(remaicfg.year)
    org.load_db()

    ### organization :: split former_name if there are some.
    rlog.info("-" * 72)
    rlog.info(f"{org.coll} :: wash field 'former_name_fr'.")
    for k, v in org.tree.items():
        # print(f"k={k}, v={v}")
        fnames = org.getf_nformer(k)
        match fnames:
            case None | "-":
                org.putf_nformer(k, {})
            case {}:
                pass
            case _:
                # several former names are seperated by '\n', from aiqicha.com
                fnames = fnames.split("\n")
                fname = {fn: {} for fn in fnames}
                # print(f"fname_node = {fname_node}")
                org.putf_nformer(k, fname)

    # organization :: wash field 'misc'
    rlog.info("-" * 72)
    rlog.info(f"{org.coll} :: wash field 'misc'.")
    field_none = {}
    for k, v in org.tree.items():
        if v.get("misc", None) is None:
            v["misc"] = {}
            field_none[k] = v["oname_fr"]
    if len(field_none) > 0:
        rlog.info(
            f"{len(field_none)} doc(s) 's have null 'misc', set empty value instead : {show_t(field_none, cr='no')}"
        )

    # update organization
    result = org.update2db()
    rlog.info(result["message"])

    # 'organization': verify if 'usn' and 'oname_fr' is unique
    organization = OrganizationV3(remaicfg.year)
    rlog.info(f"{organization.coll} :: check if 'usn' and 'oname_fr' is unique.")
    result = organization.check_usn_oname_fr()
    if len(result["usn_dup"]) > 0:
        rlog.warning(
            f"{len(result['usn_dup'])} usn(s) are duplicated : {result['usn_dup']}\n"
        )
    else:
        rlog.info("all usn are unique.\n")
    if len(result["oname_fr_dup"]) > 0:
        rlog.info(
            f"{len(result['oname_fr_dup'])} usn(s) are duplicated : {result['oname_fr_dup']}\n"
        )
    else:
        rlog.info("all oname_fr are unique.\n")

    ###
    ###
    ### add listx files to db, such as list_glb500_y2024.xlsx.
    rlog.info("-" * 72)
    rlog.info("add listx files to db.")
    rlog.info("-" * 72 + "\n")

    list2wd = [
        # ListGlb500Imp(remaicfg.year),
        # ListChn500Imp(remaicfg.year),
        ListIsc100(remaicfg.year),
        ListFic500All(remaicfg.year),
        ListFic500Mfg(remaicfg.year),
        ListFic100Svc(remaicfg.year),
    ]

    for listx in list2wd:

        rlog.info("-" * 72)
        # rlog.info(f"{listx.coll} :: add listx to db ...")

        #
        load_fp = False
        if listx.coll in list_collection_names:
            if args.load_fp is True:
                load_fp = True
        else:
            load_fp = True

        if load_fp is False:
            rlog.info(f"{listx.coll} already exists, skip.")
            continue

        #
        listx.coll += "_" + datetime.now().strftime("%y%m")
        listx.fpkey = "list" + "_" + listx.tag + ".xlsx"
        listx.sheet = listx.tag + "_" + remaicfg.year
        print(f"coll={listx.coll}\nfpkey={listx.fpkey}\nsheet={listx.sheet}")

        #
        listx.get_flist(remaicfg.listx_fpath)
        # if len(listx.flist) > 0:
        rlog.info(f"{listx.coll} :: {len(listx.flist)} file(s) found.")

        #
        count, fp = listx.load_fp()
        if count > 0:
            rlog.info(f"{listx.coll} :: {count} doc(s) read from file(s).")

        # insert into 'list_xxxx_yyyy'
        tmp = listx.__class__(listx.year)
        tmp.coll += "_" + datetime.now().strftime("%y%m")
        tmp.load_db()
        result = listx.insert2db(tree4chk=tmp.tree)
        rlog.info(result["message"])

    ###
    ###
    ###
    ### set field 'usn' for listx
    rlog.info("-" * 72)
    rlog.info("set field 'usn' for listx (glb500, etc.) ...")
    rlog.info("-" * 72 + "\n")

    #
    list2wd = [
        ListGlb500(remaicfg.year),
        ListChn500(remaicfg.year),
        ListSoe(remaicfg.year),
        # ListPlc(remai_config["washd"]["year"]),
        ListIsc100(remaicfg.year),
        ListFic500All(remaicfg.year),
        ListFic500Mfg(remaicfg.year),
        ListFic100Svc(remaicfg.year),
    ]
    org4usn = Org4Usn()
    org4usn.load_db()

    #
    for listx in list2wd:

        rlog.info("-" * 72)
        rlog.info(f"{listx.coll} :: set field 'usn' ...")
        listx.load_db()
        if listx.size == 0:
            rlog.warning(f"{listx.coll} :: empty db, then skipped.\n")
            continue

        result = listx.setf_usn(org4usn.tree)
        if len(result) > 0:
            rlog.warning(
                f"{listx.coll} :: [{len(result)}] doc(s)'s 'usn' are not found : {show_t(result, show='k', cr='no')}\n"
            )
        else:
            rlog.info(f"{listx.coll} :: set field 'usn' done.")

        ###
        # if ("chn500" in listx.coll) or ("isc100" in listx.coll):
        # if "isc100" in listx.coll:
        #     print(f"{show_t(result, count=100, show='k', cr='no')}\n")

        # update to db
        result = listx.update2db()
        if result["ops4db"] == True:
            rlog.info(
                f"{listx.coll} :: update done, [{result['result'].matched_count}] matched, [{result['result'].modified_count}] modified.\n"
            )
        else:
            rlog.info(f"{listx.coll} :: update done, no doc changed in db.\n")

    return


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
def wshme(remaicfg):

    rlog.info("-" * 72)
    rlog.info("add mes_student file to db.")
    rlog.info("-" * 72 + "\n")

    stumes = StuImpMes(remaicfg.year)
    stumes.get_flist(remaicfg.stu_fpath)
    rlog.info(f"{stumes.coll} :: {len(stumes.flist)} file(s)(就业方案导出) found.\n")

    # read excel file
    count, fp = stumes.load_fp()
    rlog.info(f"{stumes.coll} :: {count} doc(s) read from file(s).")

    # insert into 'mes_student_yxxx_yymm'
    result = stumes.insert2db()
    rlog.info(result["message"])

    # set more fields
    stumes.load_db()  # load db, then setf_xfields()
    rlog.info(f"{stumes.coll} :: set more fields, 'degree_year' ...")
    result = stumes.setf_xfields()
    if len(result["degree_year"]) > 0:
        rlog.warning(
            f"{len(result['degree_year'])} student(s) have bad value of 'degree' : {result['degree_year']}.\n"
        )
    if len(result["path_fr"]) > 0:
        rlog.warning(
            f"{len(result['path_fr'])} student(s) have bad value of 'path_after_graduate' : {result['path_fr']}.\n"
        )

    #
    result = stumes.update2db()
    rlog.info(result["message"])

    return


# ------------------------------------------------------------------------------
def washdcp(remaicfg):

    org4bizreg = OrgBizRegV3()
    org4bizreg.load_db2db()

    organization = OrganizationV3(remaicfg.year)
    rlog.info("-" * 72)
    rlog.info(f"{organization.coll} :: update from '{org4bizreg.coll}' ...")
    rlog.info("-" * 72 + "\n")

    #
    result = organization.insert2db(tree4post=org4bizreg.tree)
    rlog.info(result["message"])

    #
    result = organization.update2db(tree4post=org4bizreg.tree)
    rlog.info(result["message"])

    return


# ------------------------------------------------------------------------------
def migration():

    client = MongoClient()

    # 替换为你的数据库名称
    db = client.get_database(HOME_DB)

    # 获取集合
    coll_src = db["organization_250326"]
    coll_dst = db["organization"]

    # load source
    tree_src = {}
    docs_src = coll_src.find()
    for doc in docs_src:
        # print(f"doc={doc}")
        tree_src[doc["usn"]] = {
            "oname_fr": doc.get("oname_fr", {}),
            "nht_period": doc.get("nht_period", {}),
            "parent_node": doc.get("parent_node", {}),
        }

    # load destination
    tree_dst = {}
    docs_dst = coll_dst.find()
    for doc in docs_dst:
        tree_dst[doc["usn"]] = {
            "oname_fr": doc.get("oname_fr", {}),
            "nht_period": doc.get("nht_period", {}),
            "parent_node": doc.get("parent_node", {}),
        }
    # compare - source as base
    diff_nht = {}
    diff_prt = {}
    nf_404 = {}
    for k, v in tree_src.items():

        if k not in tree_dst:
            nf_404[k] = v["oname_fr"]
            continue

        src = v["nht_period"]
        dst = tree_dst[k]["nht_period"]
        if src != dst:
            if ((src is not None) and (src != {})) or (
                (dst is not None) and (dst != {})
            ):

                diff_nht[k] = {
                    "oname_fr": v["oname_fr"],
                    "src": src,
                    "dst": dst,
                }

        src = v["parent_node"]
        dst = tree_dst[k]["parent_node"]
        if src != dst:
            if ((src is not None) and (src != {})) or (
                (dst is not None) and (dst != {})
            ):
                diff_prt[k] = {
                    "oname_fr": v["oname_fr"],
                    "src": src,
                    "dst": dst,
                }

    # show result
    # nf_404
    if len(nf_404) > 0:
        rlog.warning(f"{len(nf_404)} usn(s) not found in 'organization'.")
        rlog.warning(f"{show_t(nf_404, count=1000)}")
    else:
        rlog.info("all usns in source are in 'organization'.")

    # diff_nht
    if len(diff_nht) > 0:
        rlog.warning(
            f"{len(diff_nht)} nht(s) are different from that in 'organization'."
        )
        rlog.warning(f"{show_t(diff_nht, count=1000)}")
    else:
        rlog.info("all nhts in source are same as that in 'organization'.")

    # diff_prt
    if len(diff_prt) > 0:
        rlog.warning(
            f"{len(diff_prt)} parent_node(s) are different from that in 'organization'."
        )
        rlog.warning(f"{show_t(diff_prt, count=1000)}")
    else:
        rlog.info("all parent_nodes in source are same as that in 'organization'.")

    client.close()
    return


# ------------------------------------------------------------------------------
def migration2():

    client = MongoClient()

    # load source
    db = client.get_database("remai")
    coll_src = db["bk_organization_250214"]
    tree_src = {}
    docs_src = coll_src.find()
    for doc in docs_src:
        # print(f"doc={doc}")

        if doc["h_psn"] == "i-root":
            parent_node = {}
        else:
            parent_node = {
                "oname_fr": "",
                "usn": doc["h_psn"],
            }

        tree_src[doc["usn"]] = {
            "oname_fr": doc["name_fr"],
            # "nht_period": doc.get("nht_period", {}),
            "parent_node": parent_node,
        }

    for k, v in tree_src.items():
        if v["parent_node"] == {}:
            continue
        psn = v["parent_node"]["usn"]
        v["parent_node"]["oname_fr"] = tree_src[psn]["oname_fr"]

    # load destination
    db = client.get_database(HOME_DB)
    coll_dst = db["organization"]
    tree_dst = {}
    docs_dst = coll_dst.find()
    for doc in docs_dst:
        tree_dst[doc["usn"]] = {
            "oname_fr": doc.get("oname_fr", {}),
            # "nht_period": doc.get("nht_period", {}),
            "parent_node": doc.get("parent_node", {}),
        }

    # compare - source as base
    # diff_nht = {}
    diff_prt = {}
    nf_404 = {}
    for k, v in tree_src.items():

        if k not in tree_dst:
            nf_404[k] = v["oname_fr"]
            continue

        # src = v["nht_period"]
        # dst = tree_dst[k]["nht_period"]
        # if src != dst:
        #     if ((src is not None) and (src != {})) or (
        #         (dst is not None) and (dst != {})
        #     ):

        #         diff_nht[k] = {
        #             "oname_fr": v["oname_fr"],
        #             "src": src,
        #             "dst": dst,
        #         }

        src = v["parent_node"]
        dst = tree_dst[k]["parent_node"]
        if src != dst:
            if ((src is not None) and (src != {})) or (
                (dst is not None) and (dst != {})
            ):
                diff_prt[k] = {
                    "oname_fr": v["oname_fr"],
                    "src": src,
                    "dst": dst,
                }

    # show result
    # nf_404
    if len(nf_404) > 0:
        rlog.warning(f"{len(nf_404)} usn(s) not found in 'organization'.")
        rlog.warning(f"{show_t(nf_404, count=1000)}")
    else:
        rlog.info("all usns in source are in 'organization'.")

    # diff_nht
    # if len(diff_nht) > 0:
    #     rlog.warning(
    #         f"{len(diff_nht)} nht(s) are different from that in 'organization'."
    #     )
    #     rlog.warning(f"{show_t(diff_nht, count=1000)}")
    # else:
    #     rlog.info("all nhts in source are same as that in 'organization'.")

    # diff_prt
    if len(diff_prt) > 0:
        rlog.warning(
            f"{len(diff_prt)} parent_node(s) are different from that in 'organization'."
        )
        rlog.warning(f"{show_t(diff_prt, count=1000)}")
    else:
        rlog.info("all parent_nodes in source are same as that in 'organization'.")

    client.close()
    return


# ------------------------------------------------------------------------------
def main():

    parser = argparse.ArgumentParser(
        description=" Welcome to REMAI, REcruit & EMployment of AI school.",
        add_help=True,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # command 'washd'
    washd_parser = subparsers.add_parser("washd", help="wash table & add data into db.")
    washd_parser.add_argument(
        "--load_fp", action="store_true", help="add files into db."
    )
    washd_parser.set_defaults(func=washd)

    # command 'xtags'
    xtags_parser = subparsers.add_parser(
        "xtags", help="tag 'organization' and copy tags to 'student'."
    )
    xtags_parser.set_defaults(func=xtags)

    # command 'toxls'
    xtags_parser = subparsers.add_parser(
        "toxls", help="export 'organization' and 'student' to xlsx files."
    )
    xtags_parser.set_defaults(func=toxls)

    #
    try:
        remaicfg.load()
    except Exception as e:
        rlog.exception(f"read {remaicfg.fn} error: {e}\nfix it, then try again.\n")

    # 解析命令行参数
    args = parser.parse_args()

    if hasattr(args, "func"):
        args.func(remaicfg, args)
    else:
        parser.print_help()

    # # 使用解析的参数
    # print(f"文件名: {args.filename}")
    # if args.verbose:
    #     print("详细输出已启用")
    # print(f"处理文件的次数: {args.count}")

    return

    # arguments = sys.argv

    # organization = Organization("y2024")
    # organization.load_db()
    # tree_info = organization.show_h("b6b")
    # print(f"{tree_info}")
    # return

    #
    arguments = sys.argv
    if len(arguments) != 2:
        rlog.info(
            f"usage: {arguments[0]} [xtags][washd][washdcp][toxls][xtgme][wshme][txlme]",
            end="\n\n",
        )
        return

    # read config file
    # print(f"argv[0] = #{arguments[0]}#")
    # config_file = arguments[0].split(".")
    # config_file = config_file[0]
    # config_file = config_file + ".json"
    # remaicfg = RemaiConfig()

    try:
        remaicfg.load()
    except Exception as e:
        rlog.exception(f"read {remaicfg.fn} error: {e}\nfix it, then try again.\n")
    # finally:
    #     logging.info("fix it, then try again.\n")

    #
    match arguments[1]:
        case "xtags":  # tag 'organization' & 'student_yxxxx'
            xtags(remaicfg)
        case "xtgme":
            xtgme(remaicfg)
        case "washd":
            # print("it is '--wash'.")
            washd(remaicfg)
        case "washdcp":
            # print("it is '--wash'.")
            washdcp(remaicfg)
        case "toxls":
            # print("it is 'toxls'.")
            toxls(remaicfg)
        case "txlme":
            # print("it is 'toxls'.")
            txlme(remaicfg)
        case "wshme":
            wshme(remaicfg)
        case _:  # _, not '_' or '-'
            print(
                f"usage: {arguments[0]} [xtags][washd][washdcp][toxls][xtgme][wshme][txlme]",
                end="\n\n",
            )

    return

    ###
    while True:
        print(" " * 36)
        print("Welcome to Remai. Make a choice:")
        print("=" * 36)
        print("1. tag organization & student")
        print("2. upload excel to db")
        print("3. copy field 'usn' from 'organization')")
        print("4. wash organization & student")
        print("5. add more organization")
        print("6. export organization & student")
        print("=" * 36)

        choice = input("Your choice(1/2/3/4/5/6, q=quit): ")
        if choice not in ["1", "2", "3", "4", "5", "6", "q", "Q"]:
            continue

        match choice:
            case "1":
                __is_valid, year = input_year()
                if __is_valid:
                    tag_org_stu(year)
            case "2":
                __is_valid, year = input_year()
                if __is_valid:
                    upload_frxx(year)
            case "3":
                __is_valid, year = input_year()
                if __is_valid:
                    set_usn(year)
            case "4":
                __is_valid, year = input_year()
                if __is_valid:
                    wash_org_stu(year)
            case "5":
                add_org()
            case "6":
                exp_org_stu()
            case "q" | "Q":
                logging.info(">>> The End <<<")
                return


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main()


"""
db.list_glb500_y2024.aggregate([
    {
        $project: {
            "rank":1,
            "enterprise":1,
            "revenue":1,
            "profit":1,
            "country":1,
            "doc_ops":1,
            "usn":1,

            "oname_fr": "$organization_fr",

            "_id": 0
        }
    },
    {
        $out: "list_glb500_y2024_250308" 
    }
]);
"""

"""
db.list_isc100_y2024.aggregate([
    {
        $project: {
            "no":1,
            "oname":"$name",
            "business_brand":1,
            "hq_location":1,
            "oname_fr":"$name_fr",
            "doc_ops":1,
            "usn":1,
            "_id": 0
        }
    },
    {
        $out: "list_isc100_y2024_250308" 
    }
]);
"""
"""
db.list_fic500_mfg_y2024.aggregate([
    {
        $project: {
            "rank":1,
            "oname":"$name",
            "hq_location":1,
            "revenue":1,
            "oname_fr":"$name_fr",
            "doc_ops":1,
            "usn":1,
            "_id": 0
        }
    },
    {
        $out: "list_fic500_mfg_y2024_250308" 
    }
]);
"""


# mongoexport --db=remai --collection=list_fic100_svc_y2023 --out=remai-list_fic100_svc_y2023.json
# mongoexport --db=remaiv3 --collection=organization --out=remai-organization.json
# mongoimport --db=remaiv3 --collection=bk_organization_250326 --file=remai-organization.json

# db.list_chn500_y2023.updateMany({},{$rename:{"list_chn500_y2023":"oname_fr"}})
# db.list_glb500_y2023.updateMany({},{$rename:{"enterprise":"oname"}})

"""
【Python日志功能】一.日志基础与基本配置
https://blog.csdn.net/qq_52466006/article/details/142289322
"""

"""
organization = {
    "usn": "",  # 唯一编号,
    "oname_fr": "",  # 正规/工商注册的名字
    "usci_org_fr": "",  # 统一社会信用代码
    "category": "",
    "bio": {
        "oname": "",  # 名称。来自jnu导出
        "oname_aka": "",  # 俗称/简称。
        "former_name_fr": {},  # 曾用名，多个
        "category_fr": "",  # 企业/事业
        "type_org_fr": "",  #
        "industry_org_fr": "",
        "location_org_fr": "",  # 到省市区
    "misc":{
        "nht_period": {},
        }
    },
    "egc_tgax": {},
    "parent_node": {},
    "root_node": {},
    "child_node": {},
}

db.organization_250326.aggregate([
  {
    $project: {
      usn: "$usn",
      oname_fr: "$oname_fr",
      usci_org_fr: "$usci_org_fr",
      category: "$category",
      "bio.oname": "$oname",
      "bio.oname_aka": "$oname_fr",
      "bio.former_name_fr": "$former_name_fr",
      "bio.category_fr": "$category_fr",
      "bio.type_org_fr": "$type_org_fr",
      "bio.industry_org_fr": "$industry_org_fr",
      "bio.location_org_fr": "$location_org_fr",
      "misc.nht_period": "$nht_period",
      egc_tagx:"$egc_tagx",
      parent_node:"$parent_node",
      root_node:"$root_node",
      child_node:"$child_node",
      h_comments:"$h_comments",
      doc_ops:"$doc_ops",
      
    }
  },
  {
    $out: "250326_organization"
  }
]);

//250329
db.organization.aggregate([{ $match: {} }, { $out: "bk_organization_250331_2149" }])
db.bk_organization_250331.aggregate([{ $match: {} }, { $out: "organization" }])
db.organization.updateMany({},{$rename:{"etg_tagx":"egc_tagx"}})
mongoexport --db=remaiv3 --collection=mes_student_y2024 --type=csv --fields=org.oname_fr --query='{ "usn": "nf-404" }' --out=mes_student_y2024-nf404.csv

mongoexport --db=remaiv3 --collection=list_fic100_svc_y2023 --type=csv --fields=oname_fr --query='{ "usn": "nf-404" }' --out=list_fic100_svc_y2023-nf404.csv

mongoexport --db=remaiv3 --collection=organization --type=csv --fields=oname_fr  --out=organization-250330.csv

db.student_y2022.aggregate([
  {
    $match: {
      $expr: {
        $and: [
          { $ne: ["$oname", "$oname_fr"] },  // oname 和 oname_fr 不相等
          { $ne: ["$oname_fr", "自由职业[76]"] } // oname_fr 不等于 "自由职业"
        ]
      }
    }
  },
  {
    $project: {
      oname: 1,
      oname_fr: 1
    }
  },
  {
    $out: "tmp_student"
  }
]);
"""
