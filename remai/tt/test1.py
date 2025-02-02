import pymongo
import random
import string
from pymongo.collation import Collation
random.seed(10)
letters = string.ascii_lowercase
upper = string.ascii_uppercase


class MongoDBServer():
    def __init__(self,database,collation) -> None:
        # self.client = pymongo.MongoClient('mongodb://ellis:ellischen@192.168.214.133:32000/')
        self.client = pymongo.MongoClient()
        self.database = self.client[database]
        
        self.colleceion = self.database[collation]
        
    def insert_many(self,documents):
        self.colleceion.insert_many(documents)

    def lookup(self,destination,localField,foreignField,as_field):
        return self.colleceion.aggregate([{"$lookup": 
            {"from":destination,"localField":localField,"foreignField":foreignField,"as":as_field}}])


# insert docs into [orders]
# server = MongoDBServer('gdtestdb','orders')

# server.insert_many([
#    { "_id" : 1, "item" : "almonds", "price" : 12, "quantity" : 2 },
#    { "_id" : 2, "item" : "pecans", "price" : 20, "quantity" : 1 },
#    { "_id" : 3  }
# ] )

# insert docs into [inventory]
# print("insert docs into [inventory]...")
# server = MongoDBServer('gdtestdb','inventory')

# server.insert_many([
#    { "_id" : 1, "sku" : "almonds", "description": "product 1", "instock" : 120 },
#    { "_id" : 2, "sku" : "bread", "description": "product 2", "instock" : 80 },
#    { "_id" : 3, "sku" : "cashews", "description": "product 3", "instock" : 60 },
#    { "_id" : 4, "sku" : "pecans", "description": "product 4", "instock" : 70 },
#    { "_id" : 5, "sku": None, "description": "Incomplete" },
#    { "_id" : 6 }
# ] )

# query
# print("query...")
# server = MongoDBServer('gdtestdb','orders')

# for item in server.lookup("inventory","item","sku","inventory_docs"):
#     print(item)

# print("done!")



# {'_id': 1, 'item': 'almonds', 'price': 12, 'quantity': 2, 'inventory_docs': [
#     {'_id': 1, 'sku': 'almonds', 'description': 'product 1', 'instock': 120}]
# }
# {'_id': 2, 'item': 'pecans', 'price': 20, 'quantity': 1, 'inventory_docs': [{'_id': 4, 'sku': 'pecans', 'description': 'product 4', 'instock': 70}]}
# {'_id': 3, 'inventory_docs': [{'_id': 5, 'sku': None, 'description': 'Incomplete'}, {'_id': 6}]}



# print("query...")
# server = MongoDBServer('remai','org_glb500')

# for item in server.lookup("glb500_yr23","name_fr","organizaition_frn","glb500"):
#     print(item)


# insert docs into [glb500_test]
print("insert docs into [tt_glb500_yxxxx]...")
server = MongoDBServer('remai','tt_glb500_yxxxx')

server.insert_many([
    {
        "rank": 28,
        "enterprise": "中国工商银行股份有限公司（INDUSTRIAL & COMMERCIAL BANK OF CHINA)",
        "revenue": 214766.3,
        "profit": 53589.3,
        "country": "中国",
        "organization_fr": "中国工商银行股份有限公司"
    },
    {
        "rank": 3,
        "enterprise": "国家电网有限公司（STATE GRID)",
        "revenue": 530008.8,
        "profit": 8191.9,
        "country": "中国",
        "organization_fr": "国家电网有限公司"
    }
]
)
print("done!")

# insert docs into [tt_org_glb500]
print("insert docs into [tt_org_glb500]...")
server = MongoDBServer('remai','tt_org_glb500')

server.insert_many([
    {
          "sn": "a03",
          "name_fr": "中国工商银行股份有限公司",
          "hierarchy_psn": 0
    },
    {
          "sn": "a0f",
          "name_fr": "安克创新科技股份有限公司深圳分公司",
          "hierarchy_psn": "aus"

    }
]
)
print("done!")

# query
print("query...")
server = MongoDBServer('remai','tt_org_glb500')

for item in server.lookup("tt_glb500_yxxxx","name_fr","organization_fr","glb500_docs"):
    if item["glb500_docs"] == []:
        print("null")
    else:
        print("is not null")
        
    print(item)




print("done!")