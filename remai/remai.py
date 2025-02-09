#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from datetime import date, datetime, timedelta, timezone
import sys
from pymongo import MongoClient, UpdateOne, InsertOne
import logging

logging.basicConfig(level=logging.INFO)

### --------------------------------------------------------------------------------------------------------------------
### class definition
### --------------------------------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class ListChn500Year:

    #
    def __init__(self, year):
        self.__tag = "chn500"
        self.__tag_mark = "chn"  # mark as 'chn' if it is in the list of chn.
        self.__year = year
        self.__tree = {}

    #
    def load(self, tree):
        for node in tree:
            tmp_node = {
                node["organization_fr"]: {
                    "rank": node["rank"],
                    "enterprise": node["enterprise"],
                    "revenue": node["revenue"],
                    "profit": node["profit"],
                }
            }
            self.__tree.update(tmp_node)

    @property
    def mark(self):
        return self.__tag_mark

    @property
    def size(self):
        return len(self.__tree)

    @property
    def tag(self):
        return self.__tag

    @property
    def tree(self):
        return self.__tree

    @property
    def year(self):
        return self.__year

    @year.setter
    def year(self, year):
        if type(year) == str:
            self.__year = year
        else:
            raise TypeError("string expected such as 'y2024'.")


# ------------------------------------------------------------------------------
class ListGlb500Year:

    #
    def __init__(self, year):
        self.__tag = "glb500"
        self.__tag_mark = "glb"  # mark as 'glb' if it is in the list of glb.
        self.__year = year
        self.__tree = {}

    #
    def load(self, tree):
        for node in tree:
            tmp_node = {
                node["organization_fr"]: {
                    "rank": node["rank"],
                    "enterprise": node["enterprise"],
                    "revenue": node["revenue"],
                    "profit": node["profit"],
                    "country": node["country"],
                }
            }
            self.__tree.update(tmp_node)

    @property
    def mark(self):
        return self.__tag_mark

    @property
    def size(self):
        return len(self.__tree)

    @property
    def tag(self):
        return self.__tag

    @property
    def tree(self):
        return self.__tree

    @property
    def year(self):
        return self.__year

    @year.setter
    def year(self, year):
        if type(year) == str:
            self.__year = year
        else:
            raise TypeError("string expected such as 'y2024'.")


# ------------------------------------------------------------------------------
class ListIsc100Year:

    ###
    def __init__(self, year):
        self.__tag = "isc100"
        self.__tag_mark = "isc"
        self.__year = year
        self.__tree = {}

    def load(self, tree):
        for node in tree:
            tmp = {
                node["name_fr"]: {
                    "usn": node["usn"],
                }
            }
            self.__tree.update(tmp)

        return

    @property
    def mark(self):
        return self.__tag_mark

    @property
    def size(self):
        return len(self.__tree)

    @property
    def tag(self):
        return self.__tag

    @property
    def tree(self):
        return self.__tree

    @property
    def year(self):
        return self.__year

    @year.setter
    def year(self, year):
        if type(year) == str:
            self.__year = year
        else:
            raise TypeError("string expected such as 'y2024'.")
        return


# ------------------------------------------------------------------------------
class ListPLC:

    ###
    def __init__(self, year):
        self.__tag = "plc"
        self.__tag_mark = "plc"
        # self.__tag_mark = {"plc":"chn-mainland", "lcx":{} }
        self.__year = year
        self.__tree = {}

    def load(self, tree):
        for node in tree:
            tmp_node = {
                node["name_fr"]: {
                    "eff_date": node["eff_date"],
                    "exp_date": node["exp_date"],
                    "region": node["region"],
                }
            }
            self.__tree.update(tmp_node)

        return

    @property
    def mark(self):
        return self.__tag_mark

    @property
    def size(self):
        return len(self.__tree)

    @property
    def tag(self):
        return self.__tag

    @property
    def tree(self):
        return self.__tree

    @property
    def year(self):
        return self.__year

    @year.setter
    def year(self, year):
        if type(year) == str:
            self.__year = year
        else:
            raise TypeError("string expected such as 'y2024'.")


# ------------------------------------------------------------------------------
class ListSOE:

    ###
    def __init__(self, year):
        self.__tag = "soe"
        self.__tag_mark = "soe"  # mark as 'soe' if it is in the list of soe.
        self.__year = year  # although soe is not relevant to year
        self.__tree = {}

    def load(self, tree):
        for node in tree:
            tmp_node = {node["organization_fr"]: {}}
            self.__tree.update(tmp_node)

        return

    @property
    def mark(self):
        return self.__tag_mark

    @property
    def size(self):
        return len(self.__tree)

    @property
    def tag(self):
        return self.__tag

    @property
    def tree(self):
        return self.__tree

    @property
    def year(self):
        return self.__year

    @year.setter
    def year(self, year):
        if type(year) == str:
            self.__year = year
        else:
            raise TypeError("string expected such as 'y2024'.")


# ------------------------------------------------------------------------------
class Organization:

    ###
    def __init__(self):
        self.__tree = {}
        self.__post2db_year = ""
        self.__post2db_tags = {}

        # doc["sn"]: {
        #     "name_fr": doc["name_fr"],
        #     "category": doc["category"],
        #     "hierarchy_psn": doc["hierarchy_psn"],

    ###
    def load(self, tree):
        for node in tree:
            tmp_node = {
                node["sn"]: {
                    "name_fr": node["name_fr"],
                    "hierarchy_psn": node["hierarchy_psn"],
                    "category": node["category"],
                    "root_node": {},  # {"sn": "xxx", "name_fr": "xxx"}
                    "nht_period": node["nht_period"],
                    "child": {},
                    "glb500": {},
                    "chn500": {},
                    "soe": {},
                    "plc": {},
                    "nht": {},
                    "isc100": {},
                }
            }
            self.__tree.update(tmp_node)

    ###
    def set_h(self):

        tree_psn_nf04 = {}

        for key, value in self.__tree.items():
            psn = str(value.get("hierarchy_psn"))
            if (psn != "0") and (psn != ""):
                if psn in self.__tree:
                    self.__tree.get(psn)["child"][key] = {}
                else:
                    node = {psn: {}}
                    tree_psn_nf04.update(node)

        return tree_psn_nf04

    ###
    def set_root_node(self, node, root_node):

        self.__tree[node]["root_node"] = {
            "sn": root_node,
            "name_fr": self.__tree[root_node]["name_fr"],
        }

        for child in self.__tree[node]["child"]:
            self.set_root_node(child, root_node)

        return

    ### set tag nht (national high-tech)
    def set_tag_nht(self, year):

        if int(year[1:]) <= 2000:
            logging.warning(
                "'y' followed by 4-digit year is expected. e.g., 'y2024'. set tag nht skipped."
            )
            return

        ydate = date.fromisoformat(year[1:] + "0101")
        for node, value in self.__tree.items():

            tmp_node = {year: "nil"}
            for eff_date, exp_date in value["nht_period"].items():
                if eff_date != "":
                    if (date.fromisoformat(eff_date) <= ydate) and (
                        ydate <= date.fromisoformat(exp_date)
                    ):
                        tmp_node = {year: "nht"}
            value["nht"].update(tmp_node)

        #
        for node, value in self.__tree.items():
            psn = str(value["hierarchy_psn"])
            if (psn == "") or (
                psn == "0"
            ):  # set as "nf04" if it is not in the list of xx.
                self.__set_tag_xx_child(
                    node,
                    "nht",
                    "nht",
                    year,
                    value["nht"].get(year, ""),
                )

        return

    ###
    def set_tag_xx(self, list_xx, is_set_tag_xx_root=1):
        # list_xx could be list_soe, list_chn500_y2023, ..., etc.

        # mark as 'xx' if organization is in the list of xx.
        for organization_fr in list_xx.tree:
            for node, value in self.__tree.items():
                if organization_fr == value["name_fr"]:
                    # e.g., value["chn500"]["y2023"] = "chn"
                    value[list_xx.tag][list_xx.year] = list_xx.mark

        #
        # will not set root tag for some tags like tag 'nht (national high tech)' and tag 'plc (public list company)'.
        if is_set_tag_xx_root == 1:
            tmp_tree = self.__set_tag_xx_root(list_xx.tag, list_xx.mark, list_xx.year)
            if len(tmp_tree) > 0:
                logging.info(
                    f"{len(tmp_tree)} root node(s) are set as tag [{list_xx.mark}] : {tmp_tree}."
                )

        for node, value in self.__tree.items():
            psn = str(value["hierarchy_psn"])
            if (psn == "") or (
                psn == "0"
            ):  # set as "nf04" if it is not in the list of xx.
                self.__set_tag_xx_child(
                    node,
                    list_xx.tag,
                    list_xx.mark,
                    list_xx.year,
                    value[list_xx.tag].get(list_xx.year, ""),
                )

        return

    ###
    def __get_tag_xx_child(self, node, tag, mark, year):
        if self.__tree[node][tag].get(year, "") == mark:
            return 1

        for child in self.__tree[node]["child"]:
            if self.__get_tag_xx_child(child, tag, mark, year) == 1:
                return 1

        return 0

    ###
    def __set_tag_xx_root(self, tag, mark, year):

        tmp_tree = {}
        for node, value in self.__tree.items():
            psn = str(value["hierarchy_psn"])
            if (psn == "") or (psn == "0"):
                if self.__get_tag_xx_child(node, tag, mark, year) == 1:
                    if self.__tree[node][tag].get(year, "") != mark:
                        self.__tree[node][tag][year] = "[" + mark + "]"
                        tmp_node = {node: value["name_fr"]}
                        tmp_tree.update(tmp_node)

        return tmp_tree

    ###
    def __set_tag_xx_child(self, node, tag_xx, mark_xx, year, mark):

        # if (self.__tree[node]["root_node"]["sn"] == "bcz") and (
        #     (tag_xx == "plc") or (tag_xx == "soe")
        # ):
        #     print(
        #         f">>>tag={tag_xx}. node={node}, value={self.__tree[node]}, mark={mark}"
        #     )

        value = self.__tree[node][tag_xx]
        if value.get(year, "") != mark_xx:

            if (mark != mark_xx) and (mark != ("[" + mark_xx + "]")):
                tmp_node = {year: "nf04"}
            else:
                tmp_node = {year: "[" + mark_xx + "]"}

            value.update(tmp_node)

        mark_for_child = value[year]
        if mark_for_child == mark_xx:
            mark_for_child = "[" + mark_xx + "]"
        for child in self.__tree[node]["child"]:
            self.__set_tag_xx_child(child, tag_xx, mark_xx, year, mark_for_child)

        return

    ###
    def show_h(self, node, tag, year, loh=1):

        basic_info = (
            "["
            + node
            + "]"
            + self.__tree[node]["name_fr"]
            + "("
            + self.__tree[node][tag].get(year, "")
            + ")"
            + "("
            + self.__tree[node]["root_node"]["sn"]
            + ")"
        )

        if loh == 1:
            more_info = "*"
        elif loh >= 2:
            more_info = "|" * (loh - 1)
            if self.__tree[node]["child"] == {}:
                more_info = more_info + "+"
            else:
                more_info = more_info + str(loh)

        basic_info = more_info + basic_info
        print(basic_info)

        for child in self.__tree[node]["child"]:
            self.show_h(child, tag, year, loh + 1)

        return

    # --------------------------------------------------------------------------
    @property
    def post2db_tags(self):
        return self.__post2db_tags

    @post2db_tags.setter
    def post2db_tags(self, tags):
        if type(tags) == dict:
            self.__post2db_tags = tags
        else:
            raise TypeError("dict expected such as {'glb500':{}, 'soe':{}}.")

    @property
    def post2db_year(self):
        return self.__post2db_year

    @post2db_year.setter
    def post2db_year(self, year):
        if type(year) == str:
            self.__post2db_year = year
        else:
            raise TypeError("string expected such as 'y2024'.")

    @property
    def size(self):
        return len(self.__tree)

    @property
    def tree(self):
        return self.__tree


# ------------------------------------------------------------------------------
class Student:

    def __init__(self, year):
        self.__year = year
        self.__tree = {}
        self.__post2db_tags = {}

    ###
    def load(self, tree):
        for node in tree:
            tmp_node = {
                str(node["sid"]): {
                    "name": node["name"],
                    "degree_year": node["degree_year"],
                    "path_fr": node["path_fr"],
                    "organization_fr": node["organization_fr"],
                    "hierarchy": {"sn": "", "root_node": {}},
                    self.__year: {},
                }
            }
            self.__tree.update(tmp_node)

    ###
    def copy_tag_xx(self, organization):

        for node in organization.tree:

            for tag in self.__post2db_tags:
                if (tag in organization.tree[node]) == False:
                    logging.warning(
                        f"tag ({tag}) is not found in node ({node}) of organization. copy skipped."
                    )
                    break
                if (self.__year in organization.tree[node][tag]) == False:
                    logging.warning(
                        f"year ({self.__year}) is not found in tag ({tag}) of node ({node}) of organization. copy skipped."
                    )
                    break

                for key in self.__tree:
                    if (
                        self.__tree[key]["organization_fr"]
                        == organization.tree[node]["name_fr"]
                    ):  # org name matched
                        self.__tree[key][self.__year][tag] = organization.tree[node][
                            tag
                        ][self.__year]

        return

    ###
    def copy_hierarchy(self, organization):

        for node, value_org in organization.tree.items():
            for key, value_std in self.__tree.items():
                if value_std["organization_fr"] == value_org["name_fr"]:
                    value_std["hierarchy"]["sn"] = node
                    value_std["hierarchy"]["root_node"] = value_org["root_node"]

        return

    @property
    def post2db_tags(self):
        return self.__post2db_tags

    @post2db_tags.setter
    def post2db_tags(self, tags):
        if type(tags) == dict:
            self.__post2db_tags = tags
        else:
            raise TypeError(" dict expected such as {'glb500':{}, 'chn500':{}}.")

    @property
    def size(self):
        return len(self.__tree)

    @property
    def tree(self):
        return self.__tree

    @property
    def year(self):
        return self.__year

    @year.setter
    def year(self, year):
        if type(year) == str:
            self.__year = year
        else:
            raise TypeError("string expected.")


# ------------------------------------------------------------------------------
class TagOrgChn500:

    def __init__(self, year):
        self.__tag = "chn500"
        self.__year = year
        self.__tree = {}

    ###
    def load(self, tree):
        for node in tree:
            tmp_node = {
                node["sn"]: {
                    "name_fr": node["name_fr"],
                    "hierarchy_psn": node["hierarchy_psn"],
                    "root_node": {},  # {"sn": "xxx", "name_fr": xxx}
                    "chn500": {self.__year: "nil"},
                    "child": {},
                }
            }
            self.__tree.update(tmp_node)

    ###
    def set_child(self):

        tree_psn_nf04 = {}

        for key, value in self.__tree.items():
            psn = str(value.get("hierarchy_psn"))
            if (psn != "0") and (psn != ""):
                if psn in self.__tree:
                    self.__tree.get(psn)["child"][key] = {}
                else:
                    node = {psn: {}}
                    tree_psn_nf04.update(node)

        return tree_psn_nf04

    ###
    def set_root_node(self, node, root_node):

        self.__tree[node]["root_node"] = {
            "sn": root_node,
            "name_fr": self.__tree[root_node]["name_fr"],
        }

        for child in self.__tree[node]["child"]:
            self.set_root_node(child, root_node)

        return

    ###

    def __get_tag_child(self, node):

        if self.__tree[node]["chn500"].get(self.__year) == "chn":
            return 1

        for child in self.__tree[node]["child"]:
            if self.__get_tag_child(child) == 1:
                return 1

        return 0

    def __set_tag_root(self):

        tmp_tree = {}
        for node, value in self.__tree.items():
            psn = str(value["hierarchy_psn"])
            if ((psn == "") or (psn == "0")) and (
                value["chn500"].get(self.__year, "") == "nil"
            ):
                if self.__get_tag_child(node) == 1:
                    self.__tree[node]["chn500"][self.__year] = "[chn]"
                    tmp_node = {node: value["name_fr"]}
                    tmp_tree.update(tmp_node)

        return tmp_tree

    def __set_tag_child(self, node, tag):

        if (tag != "chn") and (tag != "[chn]"):
            self.__tree[node]["chn500"][self.__year] = "nf04"
        elif self.__tree[node]["chn500"][self.__year] != "chn":
            self.__tree[node]["chn500"][
                self.__year
            ] = tag  # i.e., [chn]. can't be "chn".

        tag_for_child = self.__tree[node]["chn500"][self.__year]
        if tag_for_child == "chn":
            tag_for_child = "[chn]"

        for child in self.__tree[node]["child"]:
            self.__set_tag_child(child, tag_for_child)

        return

    def set_tag_chn500(self, tree_chn500):

        # set 'chn' flag according to list_chn500.
        for organization_fr in tree_chn500:
            for node, value in self.__tree.items():
                if organization_fr == value["name_fr"]:
                    value["chn500"][self.__year] = "chn"

        tmp_tree = self.__set_tag_root()
        if len(tmp_tree) > 0:
            logging.info(
                f"{len(tmp_tree)} root node(s) are set as tag [chn] : {tmp_tree}."
            )

        for node, value in self.__tree.items():
            psn = str(value["hierarchy_psn"])
            if (psn == "") or (psn == "0"):  # set as "nf04" if it is not chn_500
                self.__set_tag_child(node, value["chn500"][self.__year])

        return

    ###
    def show_h(self, node, loh=1):

        basic_info = (
            "["
            + node
            + "]"
            + self.__tree[node]["name_fr"]
            + "("
            + self.__tree[node]["chn500"].get(self.year, "")
            + ")"
            + "("
            + self.__tree[node]["root_node"]["sn"]
            + ")"
        )

        if loh == 1:
            more_info = "*"
        elif loh >= 2:
            more_info = "|" * (loh - 1)
            if self.__tree[node]["child"] == {}:
                more_info = more_info + "+"
            else:
                more_info = more_info + str(loh)

        basic_info = more_info + basic_info
        print(basic_info)

        for child in self.__tree[node]["child"]:
            self.show_h(child, loh + 1)

        return

    @property
    def size(self):
        return len(self.__tree)

    @property
    def tree(self):
        return self.__tree

    @property
    def tag(self):
        return self.__tag

    @property
    def year(self):
        return self.__year

    @year.setter
    def year(self, year):
        if type(year) == str:
            self.__year = year
        else:
            raise TypeError("string expected.")


### --------------------------------------------------------------------------------------------------------------------
###
### --------------------------------------------------------------------------------------------------------------------
def collection_copy(collection_to, collection_from, field_mapping_to_from):

    client = MongoClient()
    db = client["remai"]

    collection_a = db.get_collection(collection_from)
    collection_b = db.get_collection(collection_to)

    cursor_a = collection_a.find()
    doc_list = []
    # doc_b = {field_b: "" for field_b in field_mapping.values()}

    for doc_a in cursor_a:
        # print("-" * 36)
        # print(">>>doc_a=%s" % doc_a)
        doc_b = {}  # set to {}, otherwise doc_list will be changed.
        for field_to, field_from in field_mapping_to_from.items():
            if field_to == "sid":
                doc_b[field_to] = str(doc_a[field_from])
            else:
                doc_b[field_to] = doc_a[field_from]
            # print("///doc_b=%s" % doc_b)

        # print(">>>doc_b=%s" % doc_b)
        doc_list.append(doc_b)

        # print("len=%d, doc_list=%s" % (len(doc_list), doc_list))

    # print(">>>doc_list=%s" % doc_list)
    result = collection_b.insert_many(doc_list)

    client.close()

    return result


### --------------------------------------------------------------------------------------------------------------------
###
### --------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    gd_client = MongoClient()
    gd_db = gd_client["remai"]

    # load from db.
    logging.info("loading from db. {'collection': 'list_chn500_y2023', 'year': 'y2023}")
    tmp_list = gd_db.get_collection("list_chn500_y2023").find()
    list_chn500_y2023 = ListChn500Year("y2023")
    list_chn500_y2023.load(tmp_list)
    logging.info("loaded.")

    sn_to_check = {
        "asx": {},
        "aue": {},
        "aso": {},
        "bcz": {},
        "a5g": {},
        "aa4": {},
        "asn": {},
        "avp": {},
        "aw1": {},
    }

    # for node in sn_to_check:
    #     print(f"sn={node}, value={tag_org_chn500.tree[node]}")

    ###
    """
    logging.info("update to db. {'collection': 'tag_org_chn500'}")
    bulk_operations = []
    for node, value in tag_org_chn500.tree.items():
        # print(f"{node}, {value['chn500'][tag_org_chn500.year]}")
        bulk_operations.append(
            UpdateOne(
                {"sn": node},
                {
                    "$set": {
                        "chn500": {
                            tag_org_chn500.year: value["chn500"][tag_org_chn500.year]
                        },
                        # "root_node": {},
                        # "root_node": {
                        #     "sn": value["root_node"]["sn"],
                        #     "name_fr": value["root_node"]["name_fr"],
                        # },
                    }
                },
            )
        )

    result = gd_db.get_collection("tag_org_chn500").bulk_write(bulk_operations)
    logging.info(
        f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
    )
    logging.info("updated.")
    """

    ###
    organization = Organization()
    logging.info("loading from db. {'collection': 'organization'}")
    tmp_list = gd_db.get_collection("organization").find()
    organization.load(tmp_list)
    logging.info("loaded.")

    tree_nf04 = organization.set_h()
    if len(tree_nf04) > 0:
        logging.warning(f"parent sn not found : {tree_nf04}")

    for node, value in organization.tree.items():
        psn = str(value["hierarchy_psn"])
        if (psn == "0") or (psn == ""):
            organization.set_root_node(node, node)

    logging.info("set tag nht to organization.")
    # for node, value in organization.tree.items():
    #     print(f">>>organization::node={node}, value={value}")
    organization.set_tag_nht("y2023")
    logging.info("set done.")

    print("-" * 36)
    print("check value of organization")
    for node in sn_to_check:
        print("-" * 36)
        print(f"sn={node}, value={organization.tree[node]}")

    print("-" * 36)
    print(f"check value of organization. tag='nht'")
    for node in sn_to_check:
        print("-" * 36)
        organization.show_h(node, "nht", "y2023")
    ###
    # sys.exit()

    ###
    list_soe_y2023 = ListSOE("y2023")
    logging.info("loading from db. {'collection': 'list_soe'}")
    tmp_list = gd_db.get_collection("list_soe").find()
    list_soe_y2023.load(tmp_list)
    logging.info("loaded.")

    ###
    list_glb500_y2023 = ListGlb500Year("y2023")
    logging.info("loading from db. {'collection': 'list_glb500_y2023'}")
    tmp_list = gd_db.get_collection("list_glb500_y2023").find()
    list_glb500_y2023.load(tmp_list)
    logging.info("loaded.")

    ###
    list_plc = ListPLC("y2023")
    logging.info("loading from db. {'collection': 'list_plc'}")
    tmp_list = gd_db.get_collection("list_plc").find()
    list_plc.load(tmp_list)
    logging.info("loaded.")

    # for key, value in list_plc.tree.items():
    #     print(f"name={key}, value={value}.")

    ###
    logging.info(
        f"setting tag({list_soe_y2023.tag}) of year({list_soe_y2023.year}) to organization..."
    )
    organization.set_tag_xx(list_soe_y2023)
    logging.info("set done.")

    ###
    logging.info(
        f"setting tag({list_chn500_y2023.tag}) of year({list_chn500_y2023.year}) to organization..."
    )
    organization.set_tag_xx(list_chn500_y2023)
    logging.info("set done.")

    ###
    logging.info(
        f"setting tag({list_glb500_y2023.tag}) of year({list_glb500_y2023.year}) to organization..."
    )
    organization.set_tag_xx(list_glb500_y2023)
    logging.info("set done.")

    ###
    logging.info(
        f"setting tag({list_plc.tag}) of year({list_plc.year}) to organization..."
    )
    organization.set_tag_xx(list_plc, 0)

    # organization.set_tag_xx(list_plc)
    logging.info("set done.")

    ###
    logging.info(f"loading from db. collection = list_isc100_y2023.")

    gr_year = "y2023"
    tmp_list = ListIsc100Year(gr_year)
    tmp = gd_db.get_collection("list_isc100_" + gr_year).find()
    tmp_list.load(tmp)

    logging.info("loaded.")

    logging.info(
        f"setting tag({tmp_list.tag}) of year({tmp_list.year}) to organization..."
    )
    organization.set_tag_xx(tmp_list)
    logging.info("set done.")

    print(f">>> check organization. tag={tmp_list.tag}, year={tmp_list.year}")
    for node, value in organization.tree.items():
        if value[tmp_list.tag][tmp_list.year] != "nf04":
            print(f"node={node}, value={value}")

    # logging.info(
    #     f"copy tag ({tag_org_chn500.tag}, {tag_org_chn500.year}) + root_node from 'tag_org_chn500' to 'organization'"
    # )
    # organization.copy_tag(tag_org_chn500)
    # organization.copy_root_node(tag_org_chn500)
    # logging.info("copied.")

    print("-" * 36)
    print("check value of organization")
    for node in sn_to_check:
        print("-" * 36)
        print(f"sn={node}, value={organization.tree[node]}")

    print("-" * 36)
    print(f"check value of organization. tag={list_glb500_y2023.tag}")
    # for node in sn_to_check:
    #     print("-" * 36)
    #     organization.show_h(node, list_glb500_y2023.tag, "y2023")

    print("-" * 36)
    print(f"check value of organization. tag={list_chn500_y2023.tag}")
    # for node in sn_to_check:
    #     print("-" * 36)
    #     organization.show_h(node, list_chn500_y2023.tag, "y2023")

    print("-" * 36)
    print(f"check value of organization. tag={list_soe_y2023.tag}")
    # for node in sn_to_check:
    #     print("-" * 36)
    #     organization.show_h(node, list_soe_y2023.tag, "y2023")

    print("-" * 36)
    print(f"check value of organization. tag={list_plc.tag}")
    for node in sn_to_check:
        print("-" * 36)
        organization.show_h(node, list_plc.tag, "y2023")
    ###

    """
    organization.post2db_year = "y2023"
    organization.post2db_tags = {
        list_glb500_y2023.tag: {},
        list_chn500_y2023.tag: {},
        list_soe_y2023.tag: {},
    }
    logging.info(
        "update to db. {'collection': 'organization', 'tag': %s, 'year': %s}"
        % (organization.post2db_tags, organization.post2db_year)
    )
    bulk_operations = []
    for node, value in organization.tree.items():
        bulk_operations.append(
            UpdateOne(
                {"sn": node},
                {
                    "$set": {
                        "chn500": {
                            tag_org_chn500.year: value["chn500"][tag_org_chn500.year]
                        },
                        "root_node": value["root_node"],
                    }
                },
            )
        )

    result = gd_db.get_collection("organization").bulk_write(bulk_operations)
    logging.info(
        f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
    )
    logging.info("updated.")
    """

    ###
    # """
    student_y2023 = Student("y2023")
    logging.info("loading from db. {'collection': 'student_y2023'}")
    tmp_list = gd_db.get_collection("student_y2023").find()
    student_y2023.load(tmp_list)
    logging.info("loaded.")

    # for sid in student_y2023.tree:
    #     print("-" * 36)
    #     print(f"sid={sid}, value={student_y2023.tree[sid]}")

    sid_to_check = {
        "1020316135": {},
        "1191190115": {},
        "6201910022": {},
        "1033190634": {},
    }
    for sid in sid_to_check:
        print("-" * 36)
        print(f"sid={sid}, value={student_y2023.tree[sid]}")

    #
    logging.info("copying tags ...")
    student_y2023.post2db_tags = {
        "glb500": {},
        "chn500": {},
        "soe": {},
        "plc": {},
        "nht": {},
        "isc100": {},
    }
    student_y2023.copy_tag_xx(organization)
    logging.info("copied.")

    ###
    logging.info("copying hierarchy ...")
    student_y2023.copy_hierarchy(organization)
    logging.info("copied.")

    for sid in sid_to_check:
        print("-" * 36)
        print(f"sid={sid}, value={student_y2023.tree[sid]}")
    # """

    """
    logging.info(f"updating to db. collection = student_y2023.")
    bulk_operations = []
    for node, value in student_y2023.tree.items():
        bulk_operations.append(
            UpdateOne(
                {"sid": node},
                {
                    "$set": {
                        "hierarchy": value["hierarchy"],
                        student_y2023.year: value[student_y2023.year],
                    }
                },
            )
        )

    result = gd_db.get_collection("student_y2023").bulk_write(bulk_operations)
    logging.info(
        f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
    )
    logging.info("updated.")
    """

    gd_client.close()

    logging.info(">>> END <<<")

    ### collection copy
    """
    logging.info(
        "copy collection. {'collection_to': 'tag_org_chn500', 'collection_from': 'organization}"
    )

    field_mapping = {
        "sn": "sn",
        "name_fr": "name_fr",
        "hierarchy_psn": "hierarchy_psn",
    }
    collection_copy("tag_org_chn500", "organization", field_mapping)
    logging.info("copied.")
    """

    ### import nht
    """
    logging.info("loading from db. collection = nht_eff_exp.")
    tmp_list = gd_db.get_collection("nht_eff_exp").find()
    logging.info("loaded.")
    for node in tmp_list:
        tmp_date = node["eff_date1"]
        eff_date1 = ""
        if tmp_date != "":
            eff_date1 = date.fromisoformat(tmp_date)
        print(
            f"node={node},eff_date1={eff_date1}, tmp_date={tmp_date}, exp_date={node['exp_date1']}"
        )
    """

    """
    logging.info("loading from db. collection = nht_eff_exp.")
    tmp_list = gd_db.get_collection("nht_eff_exp").find()
    bulk_operations = []
    for node in tmp_list:
        tmp_node = {"sn": node["sn"], "name": node["name"], "nht_period": {}}
        if node["eff_date1"] != "":
            tmp_period = {node["eff_date1"]: node["exp_date1"]}
            # tmp_period = {
            #     date.fromisoformat(node["eff_date1"]): date.fromisoformat(
            #         node["exp_date1"]
            #     )
            # }
            tmp_node["nht_period"].update(tmp_period)
        if node["eff_date2"] != "":
            # tmp_period = {
            #     date.fromisoformat(node["eff_date2"]): date.fromisoformat(
            #         node["exp_date2"]
            #     )
            # }
            tmp_period = {node["eff_date2"]: node["exp_date2"]}
            tmp_node["nht_period"].update(tmp_period)

        bulk_operations.append(InsertOne(tmp_node))

    result = gd_db.get_collection("tmp_nht").bulk_write(bulk_operations)
    logging.info(f"{result.inserted_count} documents inserted.")
    logging.info("inserted.")
    """

    """
    logging.info("loading from db. collection = tmp_nht.")
    tmp_list = gd_db.get_collection("tmp_nht").find()
    logging.info("update collection. collection = organization.")
    bulk_operations = []
    for node in tmp_list:
        bulk_operations.append(
            UpdateOne(
                {"sn": node["sn"]},
                {"$set": {"nht_period": node["nht_period"]}},
            )
        )

    result = gd_db.get_collection("organization").bulk_write(bulk_operations)
    logging.info(
        f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
    )
    logging.info("updated.")
    """

### --------------------------------------------------------------------------------------------------------------------
### import data
### --------------------------------------------------------------------------------------------------------------------

"""
mongoimport -d remai -c list_chn500_y2023 --type=csv --headerline ~/mdb8/inf-data/list_chn500_y2023.csv
mongoimport -d remai -c list_soe --type=csv --headerline ~/mdb8/inf-data/list_soe.csv
mongoimport -d remai -c list_plc --type=csv --headerline ~/mdb8/inf-data/list_plc.csv
mongoimport -d remai -c nht_eff_exp --type=csv --headerline ~/mdb8/inf-data/nht_eff_exp.csv
mongoimport -d remai -c list_isc100_y2023 --type=csv --headerline ~/mdb8/inf-data/list_isc100_y2023.csv
mongoimport -d remai -c list_fic500_all_y2023 --type=csv --headerline ~/mdb8/inf-data/list_fic500_all_y2023.csv
mongoimport -d remai -c list_fic500_mfg_y2023 --type=csv --headerline ~/mdb8/inf-data/list_fic500_mfg_y2023.csv
mongoimport -d remai -c list_fic100_svc_y2023 --type=csv --headerline ~/mdb8/inf-data/list_fic100_svc_y2023.csv
"""
