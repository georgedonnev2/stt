#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, datetime, timedelta, timezone
import random
import sys
from pymongo import MongoClient, UpdateOne, InsertOne
import logging

logging.basicConfig(level=logging.INFO)

### --------------------------------------------------------------------------------------------------------------------
### other definition
### --------------------------------------------------------------------------------------------------------------------

remai_tagx = (
    "glb500",
    "chn500",
    "soe",
    "plc",
    "nht",
    "isc100",
    "fic500_all",
    "fic500_mfg",
    "fic100_svc",
    # "test_only",
)
### --------------------------------------------------------------------------------------------------------------------
### class definition
### --------------------------------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# RemaiTree: base class
# ------------------------------------------------------------------------------
class RemaiTree:

    ###
    def __init__(self, tag="", mark=("",), year=""):
        self.__tag = tag
        self.__tag_mark = mark
        self.__year = year
        self.__tree = {}

    ### show value of tree
    # size_to_show: 0 < < 1, percent; =1, all(100%), >1, numbers
    def show(self, size_to_show=1):

        show_keys = {}
        show_size = 0
        if size_to_show == 1:
            show_keys = list(self.tree.keys())
        if (size_to_show > 0) and (size_to_show < 1):
            show_size = int(self.size * size_to_show)
        if size_to_show > 1:
            # print(f"size_to_show is greater than 1.")
            show_size = min(size_to_show, self.size)

        if (size_to_show > 0) and (size_to_show != 1):
            int_list = [random.randint(1, self.size) for _ in range(int(show_size))]
            key_list = list(self.tree.keys())
            for index in int_list:
                node = {key_list[index]: {}}
                show_keys.update(node)

        count = 1
        for node in show_keys:
            print(f"#{count}, node={node}, value={self.tree[node]}")
            count = count + 1

        return

    ###
    def set_mark(self, org):
        # org = {node: value}
        for node, value in org.items():
            for kx, vx in self.tree.items():
                if value["name_fr"] == kx:
                    # print(f"organization::name_fr={value['name_fr']}, key={key}")
                    return self.mark[0]
        return ""

    ### property

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

    @tree.setter
    def tree(self, tree):
        if type(tree) == dict:
            self.__tree = tree
        else:
            raise TypeError("dict expected such as {}.")

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
class CertNationalHte(RemaiTree):

    def __init__(self, year):
        super().__init__("nht", ("nht",), year)

    ### load from organization
    def load(self, org):
        self.tree = {}

        for node, value in org.tree.items():
            tmp = {
                node: {"name_fr": value["name_fr"], "nht_period": value["nht_period"]}
            }
            self.tree.update(tmp)
        return

    ###
    def set_mark(self, org):

        ydate = date.fromisoformat(self.year[1:] + "0101")

        for node, value in org.items():
            for kx, vx in self.tree.items():
                if node == kx:
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
class ListChn500Year(RemaiTree):

    def __init__(self, year):
        super().__init__("chn500", ("chn",), year)

    ### load into tree
    def load(self, db_cursor):
        self.tree = {}

        for node in db_cursor:
            tmp = {
                node["organization_fr"]: {
                    "rank": int(node["rank"]),
                    # "country": node["country"],
                    "usn": node.get("usn", ""),
                }
            }
            self.tree.update(tmp)
        return

    ###
    # def set_mark(self, listx, org):
    #     # org = {node: value}
    #     # listx = {kx: vx}
    #     for kx, vx in listx.items():
    #         for node, value in org.items():
    #             if value["name_fr"] == kx:
    #                 # print(f"organization::name_fr={value['name_fr']}, key={key}")
    #                 return self.mark[0]
    #     return ""


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListFic100SvcYear(RemaiTree):

    def __init__(self, year):
        super().__init__("fic100_svc", ("fic",), year)

    ### load into tree
    def load(self, db_cursor):
        self.tree = {}

        for node in db_cursor:
            tmp = {
                node["name_fr"]: {
                    "usn": node.get("usn", ""),
                }
            }
            self.tree.update(tmp)
        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListFic500AllYear(RemaiTree):

    def __init__(self, year):
        super().__init__("fic500_all", ("fic",), year)

    ### load into tree
    def load(self, db_cursor):
        self.tree = {}

        for node in db_cursor:
            tmp = {
                node["name_fr"]: {
                    "usn": node.get("usn", ""),
                }
            }
            self.tree.update(tmp)


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListFic500MfgYear(RemaiTree):

    def __init__(self, year):
        super().__init__("fic500_mfg", ("fic",), year)

    ### load into tree
    def load(self, db_cursor):
        self.tree = {}

        for node in db_cursor:
            tmp = {
                node["name_fr"]: {
                    "usn": node.get("usn", ""),
                }
            }
            self.tree.update(tmp)
        return


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListGlb500Year(RemaiTree):

    def __init__(self, year):
        super().__init__("glb500", ("glb",), year)

    ### load into tree
    def load(self, db_cursor):
        self.tree = {}

        for node in db_cursor:
            tmp = {
                node["organization_fr"]: {
                    "rank": int(node["rank"]),
                    "country": node["country"],
                    "usn": node.get("usn", ""),
                }
            }
            self.tree.update(tmp)


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListIsc100Year(RemaiTree):

    def __init__(self, year):
        super().__init__("isc100", ("isc",), year)

    ### load into tree
    def load(self, db_cursor):
        self.tree = {}

        for node in db_cursor:
            tmp = {
                node["name_fr"]: {
                    "usn": node.get("usn", ""),
                }
            }
            self.tree.update(tmp)


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListPlc(RemaiTree):

    def __init__(self, year):
        super().__init__("plc", ("plc", "lcx"), year)

    ### load into tree
    def load(self, db_cursor):
        self.tree = {}

        for node in db_cursor:
            if node["name_fr"] in self.tree:
                tmp = {
                    str(node["code"]): {
                        "region": node["region"],
                        "section": node["section"],
                        "eff_date": node["eff_date"],
                        "exp_date": node["exp_date"],
                    },
                }
                self.tree[node["name_fr"]]["code"].update(tmp)

            else:
                tmp = {
                    node["name_fr"]: {
                        "usn": node.get("usn", ""),
                        "code": {
                            str(node["code"]): {
                                "region": node["region"],
                                "section": node["section"],
                                "eff_date": node["eff_date"],
                                "exp_date": node["exp_date"],
                            },
                        },
                    }
                }
                self.tree.update(tmp)
        return

    ###
    def set_mark(self, org):
        # org = {node: value}
        for node, value in org.items():
            for kx, vx in self.tree.items():
                if value["name_fr"] == kx:
                    mk = self.mark[1]
                    for k1, v1 in vx["code"].items():
                        if v1["region"] == "chn-mainland":
                            mk = self.mark[0]
                    return mk
        return ""


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class ListSoe(RemaiTree):

    def __init__(self, year):
        super().__init__("soe", ("soe", "prov", "city"), year)

    ### load into tree
    def load(self, db_cursor):
        self.tree = {}

        for node in db_cursor:
            tmp = {
                node["organization_fr"]: {
                    "category": node["category"],
                    "usn": node.get("usn", ""),
                }
            }
            self.tree.update(tmp)

    ###
    # def set_mark(self, listx, org):
    #     # org = {node: value}
    #     # listx = {kx: vx}
    #     for kx, vx in listx.items():
    #         for node, value in org.items():
    #             if value["name_fr"] == kx:
    #                 # print(f"organization::name_fr={value['name_fr']}, key={key}")
    #                 return self.mark[0]
    #     return ""


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class Organization(RemaiTree):

    ### load into tree
    def load(self, db_cursor):

        self.tree = {}
        for node in db_cursor:
            tmp = {
                node["sn"]: {
                    "name_fr": node["name_fr"],
                    "category": node["category"],
                    "hierarchy_psn": str(node["hierarchy_psn"]),  # will be str.
                    "root_node": node.get("root_node", {}),
                    "nht_period": node.get("nht_period", {}),
                    "child": {},
                    # # "glb500": node.get("glb500", {}),  # 《财富》世界500强
                    # # "chn500": node.get("chn500", {}),  # 《财富》中国500强
                    # # "soe": node.get("soe", {}),  # 央企。待增加省企、市企。
                    # "glb500": {},  # 《财富》世界500强
                    # "chn500": {},  # 《财富》中国500强
                    # "soe": {},  # 央企。待增加省企、市企。
                    # "plc": node.get("plc", {}),  # 上市公司。
                    # "nht": node.get("nht", {}),  # 国家高新技术企业
                    # # 互联网100强（互联网协会isc发布）
                    # "isc100": node.get("isc100", {}),
                    # # 民营企业500强（全国工商联acfic发布）
                    # "fic500_all": node.get("fic500_all", {}),
                    # # 民营企业500强-制造业（全国工商联acfic发布）
                    # "fic500_mfg": node.get("fic500_mfg", {}),
                    # # 民营企业100强-服务业（全国工商联acfic发布）
                    # "fic100_svc": node.get("fic100_svc", {}),
                },
            }
            self.tree.update(tmp)

            for tag in remai_tagx:
                tmp = {tag: node.get(tag, {})}
                self.tree[node["sn"]].update(tmp)

        return

    ### set hierarchy (i.e., field 'child')
    def set_h(self):

        tree_psn_nf04 = {}

        for key, value in self.tree.items():
            psn = str(value.get("hierarchy_psn"))
            if (psn != "0") and (psn != ""):
                if psn in self.tree:
                    self.tree.get(psn)["child"][key] = {}
                else:
                    node = {psn: {}}
                    tree_psn_nf04.update(node)

        return tree_psn_nf04

    ### set field 'root_node' for further statiscs, such as how many students employed by China Mobile.
    def set_root_node(self, node, root_node):

        self.tree[node]["root_node"] = {
            "sn": root_node,
            "name_fr": self.tree[root_node]["name_fr"],
        }

        for child in self.tree[node]["child"]:
            self.set_root_node(child, root_node)

        return

    ### set tagx
    def set_tagx(self, listx, set_tagx_root=True):

        # set mark. e.g. if organization is in the list of global 500, it is marked as 'glb'.
        for node, value in self.tree.items():
            tmp = listx.set_mark({node: value})
            if len(tmp) > 0:
                # print(f"tmp={tmp}, node={node},value={value}")
                value[listx.tag][listx.year] = tmp

        # will not set root tag for some tags like tag 'nht (national high tech)' and tag 'plc (public list company)'.
        if set_tagx_root == True:
            tmp = self.__set_tagx_root(listx.tag, listx.mark, listx.year)
            if len(tmp) > 0:
                logging.info(
                    f"{len(tmp)} root node(s) are set as tag [{listx.mark}] : {tmp}."
                )

        # set mark for child node
        for node, value in self.tree.items():
            psn = str(value["hierarchy_psn"])
            if (psn == "") or (psn == "0"):
                # set as "nf04" if it is not in the list of xx.
                self.__set_tagx_child(
                    node,
                    listx.tag,
                    listx.mark,
                    listx.year,
                    value[listx.tag].get(listx.year, ""),
                )

        return

    def __set_tagx_child(self, node, tagx, markx, year, mark):

        value = self.tree[node][tagx]
        tmp = {year: "nf04"}

        if value.get(year, "") in markx:
            tmp = {year: value.get(year, "")}
        if (mark[:1] == "[") and (mark[-1:] == "]"):
            if mark[1:-1] in markx:
                tmp = {year: mark}
        # print(f"tmp={tmp}")
        value.update(tmp)

        mark_for_child = value[year]
        if mark_for_child in markx:
            mark_for_child = "[" + mark_for_child + "]"
        for child in self.tree[node]["child"]:
            self.__set_tagx_child(child, tagx, markx, year, mark_for_child)

        return

    def __set_tagx_root(self, tag, mark, year):

        tmp = {}
        # for m in mark:
        #     mk = m
        # print(f"tmp={tmp}, mk={mk}")
        for node, value in self.tree.items():
            psn = str(value["hierarchy_psn"])
            if (psn == "") or (psn == "0"):
                if self.__get_tagx_child(node, tag, mark, year) == True:
                    if self.tree[node][tag].get(year, "") != mark[0]:
                        self.tree[node][tag][year] = "[" + mark[0] + "]"
                        tmp.update({node: value["name_fr"]})
        return tmp

    def __get_tagx_child(self, node, tag, mark, year):
        if self.tree[node][tag].get(year, "") == mark[0]:
            return True

        for child in self.tree[node]["child"]:
            if self.__get_tagx_child(child, tag, mark, year) == True:
                return True

        return False


# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
class StudentYear(RemaiTree):

    def __init__(self, year):
        super().__init__("", (","), year)

    ### load into tree
    def load(self, db_cursor):
        self.tree = {}

        for node in db_cursor:
            tmp = {
                str(node["sid"]): {
                    "name": node["name"],
                    "degree_year": node["degree_year"],
                    "path_fr": node["path_fr"],
                    "organization_fr": node["organization_fr"],
                    "hierarchy": node.get("hierarchy", {}),
                    self.year: {},
                }
            }

            self.tree.update(tmp)
            # print(f"tree={self.tree}")

            for tag in remai_tagx:
                tmp = node.get(self.year).get(tag, {})
                if tmp != {}:
                    tmp = {tag: tmp}
                    # print(f"tmp={tmp}")
                self.tree[str(node["sid"])][self.year].update(tmp)
                # self.tree[node[str(node["sid"])]][self.year].update(tmp)

            # print(f"student::size={self.size}")
            # count = 1
            # for node, value in self.tree.items():
            #     print(f"#{count}, node={node}, value={value}")
            #     count = count + 1
            #     if count >= 10:
            #         break
        return

    ### copy organization information
    def copy_h(self, list):

        for kx, vx in self.tree.items():
            for node, value in list.tree.items():
                if vx["organization_fr"] == value["name_fr"]:
                    vx["hierarchy"]["sn"] = node
                    vx["hierarchy"]["root_node"] = value["root_node"]
                    vx["hierarchy"]["category"] = value["category"]

                    break

        return

    ### copy tag
    def copy_tagx(self, list):

        for kx, vx in self.tree.items():
            for node, value in list.tree.items():
                if vx["organization_fr"] == value["name_fr"]:
                    for tag in remai_tagx:
                        vx[self.year][tag] = value.get(tag).get(self.year)

                    break
        return


### --------------------------------------------------------------------------------------------------------------------
### flow
### --------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    gd_year = ("y2023",)
    # gd_year = ("y2020", "y2021", "y2022", "y2023", "y2024")
    organization_schema = {}

    gd_client = MongoClient()
    gd_db = gd_client["remai"]

    organization = Organization()

    ### organization::load from db
    logging.info(f"loading from db. collection = organization.")
    db_cursor = gd_db.get_collection("organization").find()
    organization.load(db_cursor)
    logging.info("loaded.")

    ### organization::set child
    logging.info(f"setting child/hierarchy for organization.")
    tmp = organization.set_h()
    if len(tmp) > 0:
        logging.warning(
            f"sn which are parent sn of someone are not found. they are : {tmp}."
        )
    logging.info(f"set done.")

    ### organization::set root_node
    logging.info(f"setting field 'root_node' for organization.")
    for node, value in organization.tree.items():
        psn = str(value.get("hierarchy_psn"))
        if (psn == "0") or (psn == ""):
            organization.set_root_node(node, node)
    logging.info(f"set done.")

    ### organization::set tag glb500
    for year in gd_year:
        # list_glb500::load
        gd_tree = ListGlb500Year(year)
        print(
            f"list_glb500::size={gd_tree.size}, tag={gd_tree.tag}, mark={gd_tree.mark}, year={gd_tree.year}"
        )
        logging.info(f"list_glb500::loading from db, year={year}.")
        db_cursor = gd_db.get_collection("list_glb500_" + year).find()
        gd_tree.load(db_cursor)
        if gd_tree.size == 0:
            logging.warning(
                f"list_glb500:: nothing loaded from colletion {'list_glb500_' + year}, might be not exist or empty."
            )
        else:
            logging.info("loaded.")

        # list_glb500::check value
        # print(f"list_glb500::check value, year={year}")
        # gd_tree.show(10)

        # print(f"check value::organization")
        # organization.show(10)

        # print(f"tag={gd_tree.tag}, mark={gd_tree.mark}, year={gd_tree.year}")
        if gd_tree.size > 0:
            logging.info(f"organization::set tag={gd_tree.tag}, year={gd_tree.year}.")
            organization.set_tagx(gd_tree)
            logging.info("set done.")
        else:
            logging.warning(
                f"organization::set tag={gd_tree.tag}, year={gd_tree.year} skipped as {'list_glb500_' + gd_tree.year} is empty."
            )

    ### organization::set tag chn500
    for year in gd_year:

        gd_tree = ListChn500Year(year)

        # list_chn500::load
        logging.info(f"list_chn500::loading from db, year={year}.")
        db_cursor = gd_db.get_collection("list_chn500_" + year).find()
        gd_tree.load(db_cursor)
        if gd_tree.size == 0:
            logging.warning(
                f"list_chn500:: nothing loaded from colletion {'list_chn500_' + year}, might be not exist or empty."
            )
        else:
            logging.info("loaded.")

        #
        if gd_tree.size > 0:
            logging.info(f"organization::set tag={gd_tree.tag}, year={gd_tree.year}.")
            organization.set_tagx(gd_tree)
            logging.info("set done.")
        else:
            logging.warning(
                f"organization::set tag={gd_tree.tag}, year={gd_tree.year} skipped as {'list_chn500_' + gd_tree.year} is empty."
            )

    ### organization::set tag soe
    for year in gd_year:

        gd_tree = ListSoe(year)

        # list_soe::load
        logging.info(f"list_soe::loading from db.")
        db_cursor = gd_db.get_collection("list_soe").find()
        gd_tree.load(db_cursor)
        if gd_tree.size == 0:
            logging.warning(
                f"list_soe:: nothing loaded from colletion {'list_soe'}, might be not exist or empty."
            )
        else:
            logging.info("loaded.")

        #
        if gd_tree.size > 0:
            logging.info(f"organization::set tag={gd_tree.tag}, year={gd_tree.year}.")
            organization.set_tagx(gd_tree)
            logging.info("set done.")
        else:
            logging.warning(
                f"organization::set tag={gd_tree.tag}, year={gd_tree.year} skipped as list_soe is empty."
            )

    ### organization::set tag plc
    for year in gd_year:

        gd_tree = ListPlc(year)

        # list_plc::load
        logging.info(f"list_plc::loading from db.")
        db_cursor = gd_db.get_collection("list_plc").find()
        gd_tree.load(db_cursor)
        if gd_tree.size == 0:
            logging.warning(
                f"list_plc:: nothing loaded from colletion {'list_plc'}, might be not exist or empty."
            )
        else:
            logging.info("loaded.")

        #
        if gd_tree.size > 0:
            logging.info(f"organization::set tag={gd_tree.tag}, year={gd_tree.year}.")
            organization.set_tagx(gd_tree, False)
            logging.info("set done.")
        else:
            logging.warning(
                f"organization::set tag={gd_tree.tag}, year={gd_tree.year} skipped as list_plc is empty."
            )

    ### organization::set tag nht
    for year in gd_year:

        gd_tree = CertNationalHte(year)

        # cert_national_hte::load
        logging.info(f"cert_national_hte::loading from organization.")
        # db_cursor = gd_db.get_collection("list_plc").find()
        gd_tree.load(organization)
        if gd_tree.size == 0:
            logging.warning(f"cert_national_hte:: nothing loaded from organization.")
        else:
            logging.info("loaded.")

        # print(f"cert_national_hte::check value")
        # gd_tree.show(50)
        #
        if gd_tree.size > 0:
            logging.info(f"organization::set tag={gd_tree.tag}, year={gd_tree.year}.")
            organization.set_tagx(gd_tree, False)
            logging.info("set done.")
        else:
            logging.warning(
                f"organization::set tag={gd_tree.tag}, year={gd_tree.year} skipped as cert_national_hte is empty."
            )

    ### organization::set tag isc100
    for year in gd_year:

        gd_tree = ListIsc100Year(year)

        # list_isc100::load
        logging.info(f"list_isc100::loading from db, year={year}.")
        db_cursor = gd_db.get_collection("list_isc100_" + year).find()
        gd_tree.load(db_cursor)
        if gd_tree.size == 0:
            logging.warning(
                f"list_isc100:: nothing loaded from colletion {'list_isc100_' + year}, might be not exist or empty."
            )
        else:
            logging.info("loaded.")

        #
        if gd_tree.size > 0:
            logging.info(f"organization::set tag={gd_tree.tag}, year={gd_tree.year}.")
            organization.set_tagx(gd_tree)
            logging.info("set done.")
        else:
            logging.warning(
                f"organization::set tag={gd_tree.tag}, year={gd_tree.year} skipped as {'list_isc100_' + gd_tree.year} is empty."
            )

    ### organization::set tag fic500_all
    for year in gd_year:

        gd_tree = ListFic500AllYear(year)

        # list_fic500_all::load
        logging.info(f"list_fic500_all::loading from db, year={year}.")
        db_cursor = gd_db.get_collection("list_fic500_all_" + year).find()
        gd_tree.load(db_cursor)
        if gd_tree.size == 0:
            logging.warning(
                f"list_fic500_all:: nothing loaded from colletion {'list_fic500_all_' + year}, might be not exist or empty."
            )
        else:
            logging.info("loaded.")

        #
        if gd_tree.size > 0:
            logging.info(f"organization::set tag={gd_tree.tag}, year={gd_tree.year}.")
            organization.set_tagx(gd_tree)
            logging.info("set done.")
        else:
            logging.warning(
                f"organization::set tag={gd_tree.tag}, year={gd_tree.year} skipped as {'list_fic500_all_' + gd_tree.year} is empty."
            )

    ### organization::set tag fic500_mfg
    for year in gd_year:

        gd_tree = ListFic500MfgYear(year)

        # list_fic500_mfg::load
        logging.info(f"list_fic500_mfg::loading from db, year={year}.")
        db_cursor = gd_db.get_collection("list_fic500_mfg_" + year).find()
        gd_tree.load(db_cursor)
        if gd_tree.size == 0:
            logging.warning(
                f"list_fic500_mfg:: nothing loaded from colletion {'list_fic500_mfg_' + year}, might be not exist or empty."
            )
        else:
            logging.info("loaded.")

        #
        if gd_tree.size > 0:
            logging.info(f"organization::set tag={gd_tree.tag}, year={gd_tree.year}.")
            organization.set_tagx(gd_tree)
            logging.info("set done.")
        else:
            logging.warning(
                f"organization::set tag={gd_tree.tag}, year={gd_tree.year} skipped as {'list_fic500_mfg_' + gd_tree.year} is empty."
            )

    ### organization::set tag fic100_svc
    for year in gd_year:

        gd_tree = ListFic100SvcYear(year)

        # list_fic100_svc::load
        logging.info(f"list_fic100_svc::loading from db, year={year}.")
        db_cursor = gd_db.get_collection("list_fic100_svc_" + year).find()
        gd_tree.load(db_cursor)
        if gd_tree.size == 0:
            logging.warning(
                f"list_fic100_svc:: nothing loaded from colletion {'list_fic100_svc_' + year}, might be not exist or empty."
            )
        else:
            logging.info("loaded.")

        #
        if gd_tree.size > 0:
            logging.info(f"organization::set tag={gd_tree.tag}, year={gd_tree.year}.")
            organization.set_tagx(gd_tree)
            logging.info("set done.")
        else:
            logging.warning(
                f"organization::set tag={gd_tree.tag}, year={gd_tree.year} skipped as {'list_fic100_svc_' + gd_tree.year} is empty."
            )

    ### student::copy tag from organization
    for year in gd_year:

        student = StudentYear(year)

        # student::load
        logging.info(f"student::loading from db, year={student.year}.")
        db_cursor = gd_db.get_collection("student" + "_" + student.year).find()
        student.load(db_cursor)
        if student.size == 0:
            logging.warning(
                f"student:: nothing loaded from colletion {'student' +'_'+ student.year}, might be not exist or empty."
            )
        else:
            logging.info("loaded.")

        #
        if student.size > 0:

            logging.info(
                f"student::copy hierarchy from organization, year={student.year}."
            )
            student.copy_h(organization)
            logging.info("copied.")

            logging.info(f"student::copy tags from organization, year={student.year}.")
            student.copy_tagx(organization)
            logging.info("copied.")

        else:

            logging.warning(
                f"student::copy hierarchy and tags skipped as {'student' + '_' + student.year} is empty."
            )

        ### student::check value
        print(f"student::check value")
        student.show(20)

        # """
        logging.info(f"student::updating to db, year={student.year}.")
        bulk_operations = []
        for kx, vx in student.tree.items():
            bulk_operations.append(
                UpdateOne(
                    {"sid": kx},
                    {
                        "$set": {
                            "hierarchy": vx["hierarchy"],
                            student.year: vx[student.year],
                        }
                    },
                )
            )

        result = gd_db.get_collection("student" + "_" + student.year).bulk_write(
            bulk_operations
        )
        logging.info(
            f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
        )
        logging.info("updated.")
        # """

    ### organization::check value
    print(f"check value::organization")
    organization.show(20)

    ### organization::update to db

    # organization::update hierarchy
    logging.info(f"organization::updating hierarchy to db.")
    bulk_operations = []
    for kx, vx in organization.tree.items():
        bulk_operations.append(
            UpdateOne(
                {"sn": kx},
                {
                    "$set": {
                        "root_node": vx["root_node"],
                    }
                },
            )
        )

    result = gd_db.get_collection("organization").bulk_write(bulk_operations)
    logging.info(
        f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
    )
    logging.info("updated.")

    for year in gd_year:

        # """
        # organization::update tags
        logging.info(f"organization::updating tags to db, year={year}.")
        bulk_operations = []
        for kx, vx in organization.tree.items():
            tmp_update = {"$set": {}}
            for tag in remai_tagx:
                if vx.get(tag, {}) == {}:
                    continue
                if vx.get(tag).get(year, "") == "":
                    continue

                tmp = {tag: {year: vx[tag][year]}}
                tmp_update["$set"].update(tmp)
                # tmp_update = {
                #     "$set": {
                #         "glb500": {"y2023": "nf04"},
                #         "chn500": {"y2023": "nf04"},
                #         "soe": {"y2023": "nf04"},
                #         "plc": {"y2023": "nf04"},
                #         "nht": {"y2023": "nf04"},
                #         "isc100": {"y2023": "nf04"},
                #         "fic500_all": {"y2023": "nf04"},
                #         "fic500_mfg": {"y2023": "nf04"},
                #         "fic100_svc": {"y2023": "nf04"},
                #     }
                # }

            if tmp_update["$set"] == {}:
                continue

            print(f"tmp_update={tmp_update}")
            bulk_operations.append(
                UpdateOne(
                    {"sn": kx},
                    tmp_update,
                )
            )

        result = gd_db.get_collection("organization").bulk_write(bulk_operations)
        logging.info(
            f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
        )
        logging.info("updated.")

    gd_client.close()

    logging.info(">>> The End <<<")
