from pymongo import MongoClient, UpdateOne
import logging

logging.basicConfig(level=logging.INFO)


### ----------------------------------------------------------------------------


def copy_collection(collection_to, collection_from, field_mapping_to_from):

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
            print("///doc_b=%s" % doc_b)

        # print(">>>doc_b=%s" % doc_b)
        doc_list.append(doc_b)

        # print("len=%d, doc_list=%s" % (len(doc_list), doc_list))

    # print(">>>doc_list=%s" % doc_list)
    result = collection_b.insert_many(doc_list)

    client.close()

    return result


### ----------------------------------------------------------------------------
def load4db_list_glb500_yxxxx(year="y2023"):

    client = MongoClient()
    db = client["remai"]
    collection = db.get_collection("list_glb500_" + year)

    # cursor = collection.find().limit(10)
    cursor = collection.find()
    list = dict()

    # {
    #   "_id": {
    #     "$oid": "679ac14c4b9e0eb5a52fff27"
    #   },
    #   "rank": 5,
    #   "enterprise": "中国石油天然气集团有限公司（CHINA NATIONAL PETROLEUM)",
    #   "revenue": 483019.2,
    #   "profit": 21079.7,
    #   "country": "中国",
    #   "organization_fr": "中国石油天然气集团有限公司"
    # }

    for doc in cursor:
        # print("doc=%s" % doc)
        item = {
            doc["organization_fr"]: {
                "rank": doc["rank"],
                "enterprise": doc["enterprise"],
                "revenue": doc["revenue"],
                "profit": doc["profit"],
                "country": doc["country"],
            }
        }
        list.update(item)

    logging.debug("list=%s" % list)

    client.close()

    return list


### ----------------------------------------------------------------------------
def load4db_organization():

    client = MongoClient()
    db = client["remai"]
    collection = db.get_collection("organization")

    cursor = collection.find()
    list = {}

    # doc = {
    #     "_id": ObjectId("678f421c1be6fa013737e356"),
    #     "sn": "bs7",
    #     "name": "上海感图网络科技有限公司",
    #     "name_fr": "上海感图网络科技有限公司",
    #     "name_aka": "",
    #     "category": "ent",
    #     "hierarchy_psn": "",
    #     "h_comments": "",
    # }

    for doc in cursor:
        # print("doc=%s" % doc)
        item = {
            doc["sn"]: {
                "name_fr": doc["name_fr"],
                "category": doc["category"],
                "hierarchy_psn": doc["hierarchy_psn"],
                "glb500": {},
            }
        }
        list.update(item)

    client.close()

    return list


### ----------------------------------------------------------------------------
def load4db_student_yyyy(year):

    client = MongoClient()
    db = client["remai"]
    collection = db.get_collection("student_" + year)

    # cursor = collection.find().limit(10)
    cursor = collection.find()
    list = dict()

    # {
    #     "_id": {"$oid": "679e0ff2245c3fc7bf35f19f"},
    #     "sid": 1191190324,
    #     "name": "汪磊",
    #     "major": "数字媒体技术(080906)",
    #     "path_after_graduate": "其他录用形式就业",
    #     "organization": "三七互娱网络科技集团股份有限公司",
    #     "type_org": "其他企业",
    #     "industry_org": "信息传输、软件和信息技术服务业",
    #     "location_org": "广东省广州市市辖区",
    #     "position_type": "其他专业技术人员",
    #     "degree_year": "bachelor-y2023",
    #     "path_fr": "got-job",
    #     "organization_fr": "三七互娱网络科技集团股份有限公司",
    # }

    for doc in cursor:
        # print("doc=%s" % doc)
        item = {
            str(doc["sid"]): {
                "name": doc["name"],
                "degree_year": doc["degree_year"],
                "path_fr": doc["path_fr"],
                "organization_fr": doc["organization_fr"],
                "sn": "",
                year: {},
            }
        }

        list.update(item)

    client.close()

    return list


### ----------------------------------------------------------------------------
def post2db_org_glb500(tree, field, year):

    client = MongoClient()
    db = client["remai"]
    collection = db.get_collection("org_glb500")

    bulk_operations = []
    for node in tree:
        # print("sn=%s, value=%s" % (node, tree[node][field][year]))
        bulk_operations.append(
            UpdateOne({"sn": node}, {"$set": {field: {year: tree[node][field][year]}}})
        )

    result = collection.bulk_write(bulk_operations)
    print(
        f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
    )

    client.close()

    return


### ----------------------------------------------------------------------------
def post2db_organization(tree, field, year):

    client = MongoClient()
    db = client["remai"]
    collection = db.get_collection("organization")

    bulk_operations = []
    for node in tree:
        # print("sn=%s, value=%s" % (node, tree[node][field][year]))
        bulk_operations.append(
            UpdateOne({"sn": node}, {"$set": {field: {year: tree[node][field][year]}}})
        )

    result = collection.bulk_write(bulk_operations)
    print(
        f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
    )

    client.close()

    return


### ----------------------------------------------------------------------------
def post2db_student_yyyy(tree, field, year):

    client = MongoClient()
    db = client["remai"]
    # collection = db.get_collection("student_gy2023")
    collection = db.get_collection("student_" + year)

    bulk_operations = []
    for node in tree:
        # print("sn=%s, value=%s" % (node, tree[node][field][year]))
        bulk_operations.append(
            UpdateOne(
                {"sid": node},
                {
                    "$set": {
                        year: {field: tree[node][year][field]},
                        "sn": tree[node]["sn"],
                    }
                },
            )
        )

    result = collection.bulk_write(bulk_operations)
    print(
        f"Matched {result.matched_count} documents and modified {result.modified_count} documents."
    )

    client.close()

    return


### ----------------------------------------------------------------------------
def load4db_org_glb500():

    client = MongoClient()
    db = client["remai"]
    collection = db.get_collection("org_glb500")

    # cursor = collection.find().limit(10)
    cursor = collection.find()
    org_list = dict()

    for doc in cursor:
        # print("doc=%s" % doc)
        org = {
            doc["sn"]: {
                "name_fr": doc["name_fr"],
                "hierarchy_psn": doc["hierarchy_psn"],
                "glb500": {},
                "child": {},
            }
        }
        org_list.update(org)

    logging.debug("org_list=%s" % org_list)

    client.close()

    return org_list


###-----------------------------------------------------------------------------
def set_tag_xx_yyyy(list_to, list_ref, field, year):

    for node in list_to:
        list_to[node][year][field] = "nil"
        list_to[node]["sn"] = "nil"

    for node_ref in list_ref:
        for node_to in list_to:
            if list_ref[node_ref]["name_fr"] == list_to[node_to]["organization_fr"]:
                list_to[node_to]["sn"] = node_ref
                list_to[node_to][year][field] = list_ref[node_ref][field][year]

    return


###-----------------------------------------------------------------------------
def copy_flag_xx_yyyy(list_to, list_from, field, year):

    for node in list_to:
        list_to[node][field][year] = "nil"

    for node_from in list_from:
        for node_to in list_to:

            # if sn == sn
            if node_to == node_from:
                list_to[node_to][field][year] = list_from[node_from][field][year]

    return


###-----------------------------------------------------------------------------
def set_flag_glb500(org_list, list_glb500, year):

    ll_org_glb500 = {}
    tree = org_list.copy()
    for sn in tree:
        tree[sn]["glb500"][year] = "nil"

    for key_glb in list_glb500:
        for sn in tree:
            if tree[sn]["name_fr"] == key_glb:
                tree[sn]["glb500"][year] = "glb"
                node = {sn: tree[sn]["name_fr"]}
                ll_org_glb500.update(node)
            # else:
            #     tree[sn]["glb500"][year] = "nil"

    return tree, ll_org_glb500


###-----------------------------------------------------------------------------
def set_flag_glb500_child(node, tree, field, year, flag):

    if (flag != "[glb]") and (flag != "glb"):
        tree[node][field][year] = "nf04"
    elif tree[node][field][year] != "glb":
        tree[node][field][year] = flag  # i.e., [glb]. can't be "glb".

    flag_for_child = tree[node][field][year]
    if flag_for_child == "glb":
        flag_for_child = "[glb]"

    for child in tree[node]["child"]:
        set_flag_glb500_child(child, tree, "glb500", "y2023", flag_for_child)

    return


###-----------------------------------------------------------------------------
def get_child_flag(node, tree, field, year):

    if tree[node][field][year] == "glb":
        return 1

    for child in tree[node]["child"]:
        if get_child_flag(child, tree, field, year) == 1:
            return 1

    return 0


###-----------------------------------------------------------------------------
def set_h(tree):

    tree_h = tree.copy()
    print("tree_h=%s" % tree_h)
    tree_error = {}

    for key, value in tree_h.items():
        psn = str(value.get("hierarchy_psn"))
        if (psn != "0") and (psn != ""):
            if psn in tree_h:
                tree_h.get(psn)["child"][key] = {}
            else:
                node = {psn: {}}
                print("node=%s" % node)
                tree_error.update(node)

    return tree_h, tree_error


###-----------------------------------------------------------------------------
def show_h(node, tree, loh=1, name="name_fr", field="", year=""):

    # print("loh=%d, name=%s, field=%s, year=%s" % (loh, name, field, year))

    basic_info = "[" + node + "]" + tree[node][name]

    if field != "":
        if year != "":
            more_info = "(" + tree[node].get(field, "").get(year, "") + ")"
        else:
            # print("tree[node][field]=%s" % tree[node][field])
            more_info = "(" + str(tree[node].get(field, "")) + ")"
    else:
        if year != "":
            more_info = "(" + tree[node].get(year, "") + ")"
        else:
            more_info = ""
    basic_info = basic_info + more_info

    if loh == 1:
        more_info = "*"
    elif loh >= 2:
        more_info = "|" * (loh - 1)
        if tree[node]["child"] == {}:
            more_info = more_info + "+"
        else:
            more_info = more_info + str(loh)
    basic_info = more_info + basic_info

    print(basic_info)

    for child in tree[node]["child"]:
        show_h(child, tree, loh + 1, name, field, year)

    return


###-----------------------------------------------------------------------------
# """
org_list = load4db_org_glb500()
ll_glb500_y2023 = load4db_list_glb500_yxxxx("y2023")
# print("list=%s" % ll_glb500_y2023)

# set global_500_yyyy flag if the enterprise is in the list of global_500_yyyy.
org_list, tmp_tree = set_flag_glb500(org_list, ll_glb500_y2023, "y2023")
if len(tmp_tree) > 0:
    print("-" * 36)
    print("len=%d" % len(tmp_tree))
    # print("tree_glb500=%s" % tmp_tree)

# set hierarchy of organization according to parent sn.
org_list, tmp_tree = set_h(org_list)
print("org_list=%s" % org_list)
if len(tmp_tree) > 0:
    print("tree_error=%s" % tmp_tree)

# set parent as [glb] if there is a child glb500.
tmp_tree = {}
for node, value in org_list.items():
    psn = str(value["hierarchy_psn"])
    if ((psn == "") or (psn == "0")) and (value["glb500"]["y2023"] == "nil"):
        if get_child_flag(node, org_list, "glb500", "y2023") == 1:
            org_list[node]["glb500"]["y2023"] = "[glb]"
            tmp_node = {node: value["name_fr"]}
            tmp_tree.update(tmp_node)
if len(tmp_tree) > 0:
    print("-" * 36)
    print("len=%d, parent_tree_glb500=%s" % (len(tmp_tree), tmp_tree))

# set child as [glb] if parent is global_500/
tmp_tree = {}
for key, value in org_list.items():
    psn = str(value["hierarchy_psn"])
    if (psn == "") or (psn == "0"):  # set as "nf04" if it is not global_500.
        set_flag_glb500_child(
            key, org_list, "glb500", "y2023", value["glb500"]["y2023"]
        )


# for node, value in org_list.items():
#     psn = str(value.get("hierarchy_psn"))
#     if (psn == "0") or (psn == ""):
#         print("-" * 36)
#         show_h(node, org_list, 1, "name_fr", "glb500", "y2023")

# update glb flag to db
# post2db_org_glb500(org_list, "glb500", "y2023")


# copy tags to collection organization.
logging.info("loading doc from db, collection = organization")
organization_list = load4db_organization()

logging.info("setting flag: collection = organization, field = glb500, year = y2023")
copy_flag_xx_yyyy(organization_list, org_list, "glb500", "y2023")

# logging.info(
#     "updating doc to db. {'collection': 'organizaion', 'field': 'glb500', 'year': 'y2023'}"
# )
# post2db_organization(organization_list, "glb500", "y2023")

# for node in organization_list:
#     print("key=%s, value=%s" % (node, organization_list[node]))
# """

# """
### copy tags to collection student_yyyy.
logging.info("loading doc from db. {'collection': 'student_y2023'}")
student_y2023_list = load4db_student_yyyy("y2023")
# print("student_y2023_list=%s" % student_y2023_list)

set_tag_xx_yyyy(student_y2023_list, organization_list, "glb500", "y2023")
# for key, value in student_y2023_list.items():
#     print("key=%s, value=%s" % (key, value))

logging.info(
    "updating doc to db. {'collection': 'student_y2023', 'field': 'glb500', 'year': 'y2023'}"
)
post2db_student_yyyy(student_y2023_list, "glb500", "y2023")
logging.info("update done.")
# """


### --------------------------------------------------------------------------------------------------------------------
"""
field_mapping = {
    "rank": "rank",
    "enterprise": "enterprise",
    "revenue": "revenue",
    "profit": "profit",
    "country": "country",
    "organizaition_frn": "organization_fr",
}
copy_collection("glb500_yr23", "list_glb500_y2023", field_mapping)
"""

"""
field_mapping_to_from = {
    # !!! to : from
    "sid": "sid",
    "name": "name",
    "major": "major",
    "path_after_graduate": "path_after_graduate",
    "organization": "organization",
    "type_org": "type_org",
    "industry_org": "industry_org",
    "location_org": "location_org",
    "position_type": "position_type",
    "degree_year": "degree",  # change field name
    "path_fr": "path_type",  # change field name
    "organization_fr": "organization",  # copy organization to organization_fr
}
field_mapping_to_from = {
    # !!! to : from
    "sid": "sid",
    "name": "name",
    "major": "major",
    "path_after_graduate": "path_after_graduate",
    "organization": "organization",
    "type_org": "type_org",
    "industry_org": "industry_org",
    "location_org": "location_org",
    "position_type": "position_type",
    "degree_year": "degree_year",
    "path_fr": "path_fr",
    "organization_fr": "organization_fr",
}
logging.info("copy collection. {'from': 'student_gy23', 'to': 'student_y2023'}")

copy_collection("student_y2023", "student_gy2023", field_mapping_to_from)
# copy_collection("student_y2023", "student_gy23", field_mapping_to_from)
logging.info("copy done.")
"""

# doc_list = []
# doc_b = {
#     "rank": 2,
#     "enterprise": "沙特阿美公司（SAUDI ARAMCO)",
#     "revenue": 603651.4,
#     "profit": 159069.0,
#     "country": "沙特阿拉伯",
#     "organization_fr": "沙特阿美公司",
# }
# doc_list.append(doc_b)
# print("doc_list=%s" % doc_list)
# doc_b = {
#     "rank": 5,
#     "enterprise": "中国石油天然气集团有限公司（CHINA NATIONAL PETROLEUM)",
#     "revenue": 483019.2,
#     "profit": 21079.7,
#     "country": "中国",
#     "organization_fr": "中国石油天然气集团有限公司",
# }
# doc_list.append(doc_b)
# print("doc_list=%s" % doc_list)
# doc_b = {
#     "rank": 6,
#     "enterprise": "中国石油化工集团有限公司（SINOPEC GROUP)",
#     "revenue": 471154.2,
#     "profit": 9656.9,
#     "country": "中国",
#     "organization_fr": "中国石油化工集团有限公司",
# }
# doc_list.append(doc_b)
# print("doc_list=%s" % doc_list)

# org_list = OrganizationListH()
# print("org_list=%s" % org_list)

# org_list = {
#     "abc": {"name_fr": "name-abc", "hierarchy_psn": "", "glb500": {"y2023": "glb"}}
# }

# print(">>>org_list:%s" % org_list)
# tree = org_list
# field = "glb500"
# year = "y2023"
# for node in tree:

#     tmp = "[" + node + "]" + tree[node]["name_fr"] + "(" + tree[node][field][year] + ")"
#     print("tmp=%s" % tmp)
#     tmp = "*" + tmp
#     print("tmp=%s" % tmp)


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
# db.student_gy23.aggregate([{$group:{_id: "$path_type", count:{$sum:1}}}])

"""
use sample_airbnb
db.listingsAndReviews.updateMany(
  { security_deposit: { $lt: 100 } },
  {
    $set: { security_deposit: 100, minimum_nights: 1 }
  }
)
"""
# db.student_y2023.updateMany(
#     { degree_year: "bachelor-23"},
#     {
#         $set: { degree_year: "bachelor-y2023"}
#     }
# )
# db.student_y2023.updateMany(
#     { degree_year: "master-23"},
#     {
#         $set: { degree_year: "master-y2023"}
#     }
# )
# db.student_y2023.updateMany(
#     { degree_year: "doctor-23"},
#     {
#         $set: { degree_year: "doctor-y2023"}
#     }
# )

# db.student_y2023.aggregate([{$group:{_id: "$degree_year", count:{$sum:1}}}])
