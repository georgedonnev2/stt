tree = {"bio": {"political_status": "中共预备党员"}}
tp = {"bio": {"name": "george"}}
print(f"tree={tree}")
tree.update(tp)
print(f"tree={tree}")


# def change_tree(tree):
#     print(f"sub::tree={tree}")
#     tree["sname"] = "wangwu"
#     print(f"sub::tree={tree}")

#     pt = tree
#     pt["sname"] = "dongliu"
#     print(f"sub::pt={pt}")
#     print(f"sub::tree={tree}")
#     return


# tree = {
#     "sname": "zhangsan",
#     "gender": "female",
# }

# pt = tree
# pt["sname"] = "lisi"
# print(f"pt={pt}")
# print(f"tree={tree}")

# change_tree(tree)
# print(f"tree={tree}")


# def get_nested_value(nested_key, tree):

#     keys = nested_key.split(".")
#     value = tree
#     for key in keys:
#         # print(f"k=")
#         value = value.get(key, None)
#         if value is None:
#             break
#     return value


# treex = {
#     "1001": {
#         "sname": "zhangsan",
#         "gender": "female",
#         "organization": {
#             "oname_fr": "company_a",
#             "location": "wuxi",
#         },
#     },
#     "2002": {
#         "sname": "lisi",
#         "gender": "male",
#         "organization": {
#             "oname_fr": "company_b",
#             "location": "shanghai",
#         },
#     },
# }

# map = {
#     "sname": "sname",
#     "gender": "gender",
#     "oname_fr": "organization.oname_fr",
#     "location": "organization.location",
# }

# fptree = {}

# for kx, vx in treex.items():
#     kdst = kx
#     vdst = {}
#     for kmap, vmap in map.items():
#         vdst[kmap] = get_nested_value(vmap, vx)

#     fptree.update({kdst: vdst})

# print(f"fptree = {fptree}\n")
# print(f"treex = {treex}\n")


# treex = {}
# tree = treex
# tree["name"] = {}
# print(f"tree=#{tree}#, treex=#{treex}#")

# tree = tree["name"]
# tree["first_name"] = "george"
# print(f"tree=#{tree}#, treex=#{treex}#")

# tree = treex
# tree = tree["name"]
# tree["last_name"] = "donne"
# print(f"tree=#{tree}#, treex=#{treex}#")

# print("-" * 144)

# treex = {}
# tree = treex.copy()
# tree["name"] = {}
# print(f"tree=#{tree}#, treex=#{treex}#")

# tree = tree["name"]
# tree["first_name"] = "george"
# print(f"tree=#{tree}#, treex=#{treex}#")

# tree = treex.copy()
# tree = tree["name"]
# tree["last_name"] = "donne"
# print(f"tree=#{tree}#, treex=#{treex}#")


# def construct_nested_dict(flat_data, field_map):
#     nested_dict = {}

#     for full_path, key in field_map.items():
#         # Split the full path into components
#         keys = full_path.split(".")
#         print("-" * 144)
#         print(f"full_path=#{full_path}#, keys=#{keys}#")

#         # Start from the root of the nested dictionary
#         current_level = nested_dict
#         print(current_level)
#         print(nested_dict)
#         print(f"#1::current_level=#{current_level}#")

#         # Traverse through all keys except the last one to create nested structure
#         for k in keys[:-1]:
#             if k not in current_level:
#                 current_level[k] = {}
#                 print(
#                     f"#2::current_level=#{current_level}#, nested_dict=#{nested_dict}#"
#                 )
#             else:
#                 print(f"k is in current_level")
#             current_level = current_level[k]
#         print(f"#3::current_level=#{current_level}#, nested_dict=#{nested_dict}#")
#         # Assign the value from flat_data using the last key in the path
#         # Assume flat_data is a dictionary with keys corresponding to the 'key' in field_map
#         last_key = keys[-1]
#         print(f"last_key=#{last_key}#")
#         if key in flat_data:  # Ensure the key exists in flat_data
#             print(f"current_level[last_key]=#{current_level}#")
#             current_level[last_key] = flat_data[key]
#             print(
#                 f"current_level[last_key]=#{current_level}#, nested_dict=#{nested_dict}#"
#             )
#         else:
#             # Handle missing data if necessary, e.g., set to None or raise an error
#             current_level[last_key] = (
#                 None  # or raise KeyError(f"Missing data for key: {key}")
#             )

#     return nested_dict


# # Example usage
# flat_data = {"sname": "John Doe", "gender": "Male", "birth_date": "2000-01-01"}

# field_map = {
#     "student.major.class.sname": "sname",
#     "student.major.class.gender": "gender",
#     "student.major.class.birth_date": "birth_date",
#     "student.path": "path",
# }

# nested_structure = construct_nested_dict(flat_data, field_map)
# print(nested_structure)

# {
#     "student": {
#         "major": {
#             "class": {"sname": "John Doe", "gender": "Male", "birth_date": "2000-01-01"}
#         }
#     }
# }
# map_fp2t = {
#     "sid": {"source": "sid", "format": "str", "key": True},
#     "student.major.class.sname": {"source": "sname", "format": "str"},  # d02
#     "student.major.class.gender": {"source": "gender", "format": "str"},  # d03
#     "student..major.class.birth_date": {"source": "birth_date", "format": "str"},
# }

# map = {
#     "student.major.class.sname": "sname",
#     "student.major.class.gender": "gender",
#     "student..major.class.birth_date": "birth_date",
# }

# listfp = [
#     {"sid": 111, "sname": "zhangsan", "gender": "male", "birth_date": "20001111"},
#     {"sid": 222, "sname": "lisi", "gender": "male", "birth_date": "20001122"},
# ]

# treex = {}

# for listx in listfp:
#     __v_dst = {}
#     for kmap, vmap in map_fp2t.items():
#         if vmap.get("key", False) == True:
#             __k_dst = listx[vmap["source"]]
#         else:
#             __kmap = kmap.split(".")
#             print(f"__kmap=#{__kmap}#")
#             print(f"__kmap[:-1]={__kmap[:-1]}")
#             tmpv = __v_dst
#             for k in __kmap[:-1]:
#                 print(f"k={k},tmpv={tmpv}")
#                 if k not in tmpv:
#                     tmpv[k] = {}
#                 # tmpv = tmpv[k]
#                 print(f">>>tmpv=#{tmpv}#")
#                 # __v_dst = __v_dst[k]
#             print(f"tmpv=#{tmpv}#")
#             tmpv[kmap[-1]] = listx[vmap["source"]]
#             print(f"<<<tmpv=#{tmpv}#")

#     treex.update({__k_dst: __v_dst})

# print(f"treex={treex}")


# # 定义嵌套字段的映射
# field_map = {
#     "student.name": "name",
#     "student.age": "age",
# }


# # 构造嵌套数据结构
# def construct_nested_data(data, field_map):
#     nested_data = {}
#     for key, value in field_map.items():
#         # 分割 key 以处理嵌套字段
#         parts = key.split(".")
#         current_level = nested_data

#         # 遍历分割后的字段，逐级构建嵌套结构
#         for part in parts[:-1]:
#             if part not in current_level:
#                 current_level[part] = {}
#             current_level = current_level[part]

#         # 设置最终字段值
#         current_level[parts[-1]] = data.get(key, None)

#     return nested_data


# # 示例数据
# data = {"student.name": "Alice", "student.age": 20}

# # 使用函数构造嵌套字段
# student_data = construct_nested_data(data, field_map)

# print(student_data)


# map = {
#     "student.name": "name",
#     "student.age": "age",
# }

# current_6dof = {"x": 3, "y": 12, "z": 14, "rx": 30, "ry": 40, "rz": 50}
# x = current_6dof["x"]
# y = current_6dof["y"]
# z = current_6dof["z"]
# rx = current_6dof["rx"]
# ry = current_6dof["ry"]
# rz = current_6dof["rz"]
# # command = f"MovL(pose=\\{{x+10},{y},{z},{rx},{ry},{rz}\\})"
# command = f"{x+10},{y},{z},{rx},{ry},{rz}"
# command = "MovL(pose={" + command + "})"
# print(f"command={command}")

# import re

# # 输入字符串
# input_string = "-2,{},GetPose();"

# # 正则表达式模式
# # pattern = r"([^,]+),\s*\{([^}]*)\},\s*(.+);"
# pattern = r"([^,]+),\s*\{([^}]*)\},\s*(.+);"

# # 进行匹配
# match = re.match(pattern, input_string)

# if match:
#     print("Part 1:", match.group(1))
#     print("Part 2:", match.group(2))
#     print("Part 3:", match.group(3))
# else:
#     print("No match found")
