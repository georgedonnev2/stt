from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)

gdclient = MongoClient()
# client = MongoClient("mongodb://localhost:27017/")

gdb = gdclient["remai"]
gdcoll = gdb["glb500_yr23"]
# print("collection: %s" % collection)
# document = collection.find_one()
# for cursor in collection.find():
#     print(cursor)

# update name_fr of [organization]
# 构建更新操作
# pipeline = [
#     {
#         '$set': {
#             'name_fr': '$name'
#         }
#     }
# ]
# result = gdb.get_collection("organization").update_many({}, pipeline)

# print(f"Matched {result.matched_count} documents and modified {result.modified_count} documents.")

# gdb.get_collection("organization").update_many(
#     {},

#     { "$set" : {"name_fr": "$name"} }


# )

# exit(0)


# collection: org_glb500
# gdcollist = gdb.list_collection_names()
# if "org_glb500" in gdcollist:
#     logging.info("collection [org_glb500] exists. try to drop it...")
#     gdb.drop_collection("org_glb500")
#     logging.info("collection [org_glb500] is dropped.")
# else:
#     logging.info("collection [org_glb500] does not exist.")

# gdoc_list = gdb["organization"].find()
# for gdoc in gdoc_list:
#     # logging.info(
#     #     gdoc["sn"]
#     # )

#     gdb["org_glb500"].insert_one(
#         {
#             "sn": gdoc["sn"],
#             "name_fr": gdoc["name_fr"],
#             "hierarchy_psn": gdoc["hierarchy_psn"]
#         }
#     )

# def lookup(self,destination,localField,foreignField,as_field):
#         return self.colleceion.aggregate([{"$lookup":{"from":destination,"localField":localField,"foreignField":foreignField,"as":as_field}}])

# gdcoll = gdb.get_collection("org_glb500")
# pipeline = [
#     {
#         "$lookup": {
#             "from": "glb500_y23",
#             "localField": "name_fr",
#             "foreignField": "organizaition_frn",
#             "as": "glb500xxx"
#         }
#     }
# ]
# logging.info("pipeline: %s" % pipeline)

# server = MongoDBServer('remai','org_glb500')

# for item in server.lookup("glb500_yr23","name_fr","organizaition_frn","glb500"):

# gdoc_list = gdcoll.aggregate(pipeline)
# gdoc_list = gdb.get_collection("org_glb500").aggregate(pipeline)


def set_glb500():
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
            logging.info(">>>glb500, frn: %s" % gdoc["glb500"][0]["organizaition_frn"])
            logging.info("_id: %s" % gdoc["_id"])

            gdb.get_collection("org_glb500").update_one(
                {"_id": gdoc["_id"]}, {"$set": {"glb_y2023": "glb"}}
            )
        else:
            gdb.get_collection("org_glb500").update_one(
                {"_id": gdoc["_id"]}, {"$set": {"glb_y2023": "nil"}}
            )


def get_hierarchy():
    _client = MongoClient()
    _db = _client["remai"]

    # _doc_list = _db.get_collection("org_glb500").find()
    _doc_list = _db.get_collection("org_glb500").find().limit(10)
    _org_list = dict()

    for _doc in _doc_list:
        # logging.info("_doc: %s" % _doc)

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

    logging.info(">>> org_list: %s" % _org_list)

    _client.close()

    return _org_list


####
class TreeNode:

    def __init__(self, value):
        self.value = value
        self.children = []

    def add_child(self, child_node):
        self.children.append(child_node)

    def __repr__(self):
        return f"tn({self.value})"


root = TreeNode("CEO")
vp_engineering = TreeNode("VP of Engineering")
vp_sales = TreeNode("VP of Sales")

root.add_child(vp_engineering)
root.add_child(vp_sales)

engineering_manager = TreeNode("Engineering Manager")
vp_engineering.add_child(engineering_manager)

sales_manager = TreeNode("Sales Manager")
vp_sales.add_child(sales_manager)

print("root and children...")
print(root)  # 输出: TreeNode(CEO)
print(root.children)  # 输出: [TreeNode(VP of Engineering), TreeNode(VP of Sales)]


def preorder_traversal(node, loh=0):

    if node is None:
        return
    else:
        if loh <= 1:
            print(node.value)
        else:
            print("|" + "+" * (2 * loh - 1) + node.value)
            # print(node)

    for child in node.children:
        # print("child of %s >>>" % node)
        preorder_traversal(child, loh + 1)

    return


preorder_traversal(root)

print(root)  # 输出: TreeNode(CEO)
print(root.children)  # 输出: [TreeNode(VP of Engineering), TreeNode(VP of Sales)]


org_list = get_hierarchy()
for sn, value in org_list.items():
    # logging.info(
    #     "sn: %s, org: %s, psn: %s"
    #     % (sn, org_list[sn].get("name_fr"), org_list[sn].get("hierarchy_psn"))
    # )
    logging.info(
        "%s, %s, %s, %s, %s"
        % (
            sn,
            value.get("name_fr"),
            value.get("hierarchy_psn"),
            value.get("glb_y2023"),
            value.get("child"),
        )
    )

"""

root = TreeNode("organization")
for sn in org_list:
    org_node = TreeNode(sn)

    if org_list[sn].get("hierarchy_psn") == 0:
        print("psn is zero.")
        root.add_child(org_node)
    else:
        print("psn: %s" % org_list[sn].get("hierarchy_psn"))

preorder_traversal(root)

"""

"""
def build_tree(node_parent_map):
    # 初始化树结构，每个节点都设置为一个空字典（用于存储子节点）
    tree = {node: {} for node in node_parent_map}
    print("tree: %s" % tree)

    # 遍历每个节点，将其添加到其父节点的子节点列表中
    print(" map.items: %s" % node_parent_map.items())
    for node, parent in node_parent_map.items():
        print("node: %s, parent: %s" % (node, parent))
        if parent is None:
            # 根节点，特殊处理（例如，可以将根节点存储在另一个变量中）
            root = node
        else:
            print("<<<tree: %s" % tree)

            # print("tree[parent][node]: %s" % (tree[parent][node]))
            tree[parent][node] = {}  # 将当前节点作为子节点添加到其父节点下
            # print("tree[parent][node]: %s" % (tree[parent][node]))
            print(">>>tree: %s" % tree)

    return tree, root
"""

# 使用示例数据构建树
node_parent_map = {
    "CEO": None,
    "CTO": "CEO",
    "CFO": "CEO",
    "HR_Director": "CEO",
    "Engineering_VP": "CTO",
    "Product_VP": "CTO",
    "Finance_Director": "CFO",
    "Recruitment_Manager": "HR_Director",
    "Benefits_Manager": "HR_Director",
}

# tree, root = build_tree(node_parent_map)


# 打印树结构（为了可读性，这里使用递归函数打印）
def print_tree(node, tree, indent=0):
    print(" " * indent + node)
    for child in tree[node]:
        print_tree(child, tree, indent + 2)


# 打印根节点及其子树
# print_tree(root, tree)


dict = {"A": 11, "B": 22, "C": 33, "D": 44}
logging.info("dict: %s" % dict)
logging.info("dict.items: %s" % dict.items())
logging.info("dict.keys: %s" % dict.keys())
logging.info("dict.values: %s" % dict.values())

dict1 = {"F": 55, "G": 66}
dict.update(dict1)
logging.info("dict: %s" % dict)
dict2 = {"A": 10, "B": 11}
dict.update(dict2)
logging.info("dict: %s" % dict)

gdclient.close()
