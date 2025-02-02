from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)


# gdclient = MongoClient()
# client = MongoClient("mongodb://localhost:27017/")
# gdb = gdclient["remai"]
# gdcoll = gdb["glb500_yr23"]


### ----------------------------------------------------------------------------
# set field glb_y2023 of collection [org_glb500]
def set_glb500(db="remai", year="y2023"):

    gdclient = MongoClient()

    if db == None:
        gdb = gdclient["remai"]
    else:
        gdb = gdclient[db]

    gdoc_list = gdb.get_collection("org_glb500").aggregate(
        [
            {
                "$lookup": {
                    "from": "glb500_yr23",
                    "localField": "name_fr",
                    "foreignField": "organizaition_frn",
                    "as": "glb500",
                }
            }
        ]
    )

    for gdoc in gdoc_list:
        if len(gdoc.get("glb500")) > 0:
            # logging.info(">>>glb500, frn: %s" % gdoc["glb500"][0]["organizaition_frn"])
            # logging.info("_id: %s" % gdoc["_id"])

            gdb.get_collection("org_glb500").update_one(
                {"_id": gdoc["_id"]}, {"$set": {"glb_y2023": "glb"}}
            )
        else:
            gdb.get_collection("org_glb500").update_one(
                {"_id": gdoc["_id"]}, {"$set": {"glb_y2023": "nil"}}
            )

    gdclient.close()

    return


### ----------------------------------------------------------------------------
def post_to_db(collection, tree, field):
    for key, value in tree.items():
        collection.update_one({"sn": key}, {"$set": {field: value[field]}})

    return


### ----------------------------------------------------------------------------
def get_hierarchy():
    _client = MongoClient()
    _db = _client["remai"]

    # _doc_list = _db.get_collection("org_glb500").find()
    _doc_list = _db.get_collection("org_glb500").find().limit(5000)
    _org_list = dict()

    # for key in _doc_list:
    #     logging.info("key: %s" % (key))
    # return

    for _doc in _doc_list:
        if _doc["sn"] in _org_list:
            logging.info(
                "organization (sn: %s, name_fr: %s) is already in the list."
                % _doc["sn"]
                % _doc["name_fr"]
            )
        else:
            _org = {
                _doc["sn"]: {
                    "name_fr": _doc["name_fr"],
                    "hierarchy_psn": _doc["hierarchy_psn"],
                    "glb_y2023": _doc["glb_y2023"],
                    "child": {},
                }
            }
            _org_list.update(_org)
            # logging.info("_org_list: %s" % _org_list)

    # logging.info(">>> org_list: %s" % _org_list)

    _client.close()

    return _org_list


# set_glb500()

org_list = get_hierarchy()

for key, value in org_list.items():
    # logging.info("key: %s, value: %s" % (key, value))

    psn = str(value.get("hierarchy_psn"))

    if psn != "0":
        # logging.info("parent of %s is %s" % (key, psn))

        if psn in org_list:
            # logging.info("parent=%s" % org_list.get(psn, "0"))
            org_list.get(psn)["child"][key] = {}


logging.info("hierarchy is done!")
# logging.info(">>> org_list = %s" % org_list)


def show_hierarchy(tree, indent=0):
    for key, value in tree.items():
        print("key=%s, value=%s" % (key, value))
        print(" " * indent + key)

        print("child: %s" % value["child"])
        for child in value["child"]:
            print(">>>%s" % child)
            show_hierarchy(tree[child], indent + 2)

    return


dict = {
    "aa": {
        "name_fr": "aa",
        "hierarchy_psn": 0,
        "glb_y2023": "nil",
        "child": {"a1": {}, "a2": {}},
    },
    "a1": {
        "name_fr": "a1",
        "hierarchy_psn": "aa",
        "glb_y2023": "nil",
        "child": {
            "b11": {},
        },
    },
    "a2": {
        "name_fr": "a2",
        "hierarchy_psn": "aa",
        "glb_y2023": "nil",
        "child": {},
    },
    "b11": {
        "name_fr": "a2",
        "hierarchy_psn": "aa",
        "glb_y2023": "nil",
        "child": {},
    },
}


# def show_h(tree, tree_c={}, indent=0):

#     for key in tree:
#         print("tree=%s, key=%s, indent=%s" % (tree, key, indent))
#         print(" " * indent + key)

#         for key_c in tree[key]["child"]:
#             tree_c = {}
#             tree_c[key_c] = tree.get(key_c)
#             print("tree_c=%s" % tree_c)
#             show_h(tree, tree_c, indent + 2)

#     return

# 1️⃣2️⃣3️⃣4️⃣5️⃣6️⃣7️⃣8️⃣9️⃣


def show_h(node, tree, loh=1, is_flag="glb_y2023"):
    if loh == 1:
        print(
            "*"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    elif (loh == 2) and (tree[node]["child"] == {}):
        print(
            "|+"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    elif (loh == 2) and (tree[node]["child"] != {}):
        print(
            "\033[31m|2"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
            + "\033[37m"
        )
    elif (loh == 3) and (tree[node]["child"] == {}):
        print(
            "||+"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    elif (loh == 3) and (tree[node]["child"] != {}):
        print(
            "\033[32m||3\033[37m"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    elif (loh == 4) and (tree[node]["child"] == {}):
        print(
            "|||+"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    elif (loh == 4) and (tree[node]["child"] != {}):
        print(
            "\033[33m|||4"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
            + "\033[37m"
        )
    elif (loh == 5) and (tree[node]["child"] == {}):
        print(
            "||||+"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    elif (loh == 5) and (tree[node]["child"] != {}):
        print(
            "\033[34m||||5\033[37m"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    elif (loh == 6) and (tree[node]["child"] == {}):
        print(
            "|||||+"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    elif (loh == 6) and (tree[node]["child"] != {}):
        print(
            "\033[35m|||||6"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
            + "\033[37m"
        )
    elif (loh == 7) and (tree[node]["child"] == {}):
        print(
            "||||||+"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    elif (loh == 7) and (tree[node]["child"] != {}):
        print(
            "\033[36m||||||7\033[37m"
            + "["
            + node
            + "]"
            + tree[node]["name_fr"]
            + "("
            + tree[node][is_flag]
            + ")"
        )
    # if loh == 1:
    #     print("✳️" + " [" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 2) and (tree[node]["child"] == {}):
    #     print("┆⚪️" + "[" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 2) and (tree[node]["child"] != {}):
    #     print("┆2️⃣" + " [" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 3) and (tree[node]["child"] == {}):
    #     print("┆┆⚪️" + "[" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 3) and (tree[node]["child"] != {}):
    #     print("┆┆3️⃣" + " [" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 4) and (tree[node]["child"] == {}):
    #     print("┆┆┆⚪️" + "[" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 4) and (tree[node]["child"] != {}):
    #     print("┆┆┆4️⃣" + " [" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 5) and (tree[node]["child"] == {}):
    #     print("┆┆┆┆⚪️" + "[" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 5) and (tree[node]["child"] != {}):
    #     print("┆┆┆┆5️⃣" + " [" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 6) and (tree[node]["child"] == {}):
    #     print("┆┆┆┆⚪️" + "[" + node + "]" + tree[node]["name_fr"])
    # elif (loh == 6) and (tree[node]["child"] != {}):
    #     print("┆┆┆┆5️⃣" + " [" + node + "]" + tree[node]["name_fr"])
    else:
        print(" " * loh + node + tree[node]["name_fr"])
    # print(">>>tree[node]=%s" % tree[node])

    for child in tree[node]["child"]:
        # print(">>>child=%s" % child)
        # print(">>>tree[node][child]=%s" % tree[node]["child"])
        # print(" " * (indent + 2) + child)
        show_h(child, tree, loh + 1)

    return


# show_h("aa", dict)
# for node in org_list:
#     psn = str(org_list[node]["hierarchy_psn"])
#     if (psn == "") or (psn == "0"):
#         print("-" * 20)
#         show_h(node, org_list)


###
def set_glb500_h(node, tree):

    if tree[node]["glb_y2023"] == "glb":
        logging.info(">>>glb found, k=%s, v=%s" % (node, tree[node]))
        return 1

    for child in tree[node]["child"]:
        if set_glb500_h(child, tree) == 1:
            return 1

    return 0


###
def set_glb500_h1(node, tree, is_glb500):
    if (is_glb500 != "glb") and (is_glb500 != "[glb]"):
        tree[node]["glb_y2023"] = "nf04"
    elif tree[node]["glb_y2023"] != "glb":
        tree[node]["glb_y2023"] = is_glb500

    _is_glb500 = tree[node]["glb_y2023"]
    if _is_glb500 == "glb":
        _is_glb500 = "[glb]"

    for child in tree[node]["child"]:
        set_glb500_h1(child, tree, _is_glb500)

    return


###

logging.info("-" * 20)
show_h("aue", org_list)
logging.info("-" * 20)
show_h("ave", org_list)
logging.info("-" * 20)
show_h("b15", org_list)

for node, value in org_list.items():
    psn = str(value["hierarchy_psn"])
    if ((psn == "") or (psn == "0")) and (value["glb_y2023"] == "nil"):
        retcode = set_glb500_h(node, org_list)
        if retcode == 1:
            org_list[node]["glb_y2023"] = "[glb]"
            logging.info(">>> set parent as [glb]")

### ----
logging.info("-" * 20)
show_h("aue", org_list)
logging.info("-" * 20)
show_h("ave", org_list)
logging.info("-" * 20)
show_h("b15", org_list)

for node, value in org_list.items():
    psn = str(value["hierarchy_psn"])
    if (psn == "") or (psn == "0"):
        set_glb500_h1(node, org_list, value["glb_y2023"])

### ----
logging.info("-" * 20)
show_h("aue", org_list)
logging.info("-" * 20)
show_h("ave", org_list)
logging.info("-" * 20)
show_h("b15", org_list)

"""
for key, value in dict.items():
    print("k=%s, v=%s" % (key, value))

    for key_c, value_c in value["child"].items():

        # print("kc=%s, vc=%s" % (key_c, value_c))
        # print("dict.key_c=%s" % dict.get(key_c))
        dict_c = {}
        dict_c[key_c] = dict.get(key_c)
        print("dict_c=%s" % dict_c)
"""
# show_hierarchy(org_list)


# gdclient = MongoClient()
# gdb = gdclient["remai"]
# gdcollection = gdb.get_collection("org_glb500")
# logging.info("update glb_y2023 to db...")
# post_to_db(gdcollection, org_list, "glb_y2023")
# logging.info("update done!")


if __name__ == "__main__":

    org_glb500_list = get_hierarchy()
    for node in org_glb500_list:
        psn = str(org_glb500_list[node]["hierarchy_psn"])
        if (psn == "") or (psn == "0"):
            print("-" * 20)
            show_h(node, org_glb500_list, 1, "glb_y2023")
