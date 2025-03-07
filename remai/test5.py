if __name__ == "__main__":

    name = "SKhailishigongsi"
    ll = name.lower()
    print(f"name = {name}, ll={ll}")
    exit

    listx = ["y2023", "y2021", "y2024", "y2020"]
    listx.sort()
    print(f"listx={listx}")

    exit


map_t2fp = {
    "name": {"source": "name", "key": True},
    "score": {"source": "score"},
    "gender": {"source": "gender"},
    "root_node.name": {"source": "root_node.name"},
}

listx = [
    {
        "name": "zhangsan",
        "score": 99,
        "gender": "male",
        "root_node": {"usn": "abc", "name": "lisi"},
    },
    {
        "name": "wangwu",
        "score": 88,
        "gender": "female",
        "root_node": {"usn": "def", "name": "zhaoliu"},
    },
    {
        "name": "name3",
        "score": 77,
        "gender": "male",
        "root_node": {},
    },
]


def nested_node(nodex, docx):
    # e.g., nodex = "root_node.name"
    __listx = nodex.split(".")
    print(f"nested_node = {__listx}")
    __value = docx
    for __itemx in __listx:
        __value = __value.get(__itemx, None)
        if __value == None:
            break

    print(f"value={__value}")
    return __value


explist = []
for itemx in listx:
    docx = {}
    print("*" * 72)
    for kmap, vmap in map_t2fp.items():
        print("-" * 36)
        nested_node(vmap["source"], itemx)
