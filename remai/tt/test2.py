organization = {
    "001": {
        "name": "company-a",
        "glb500": {"y2023": "glb", "y2022": "nf04"},
        "soe": "nf04",
    },
    "002": {
        "name": "company-b",
        "glb500": {"y2023": "[glb]", "y2022": "[glb]"},
        "soe": "soe",
    },
}

print("-" * 36)
for node, value in organization.items():
    print(f"sn={node}, value={value}")

print("-" * 36)
for node, value in organization.items():
    print(
        f"sn={node}, glb500={value.get('glb500','')}, chn500={value.get('chn500','')}"
    )

# print("-" * 36)
# for node, value in organization.items():
#     print(
#         f"sn={node}, glb500-y2022={value.get('glb500','').get('y2022','')}, glb500-y2024={value.get('glb500','').get('y2024','')}, soe={value.get('soe','')},soe-y2022={value.get('soe','').get('y2022','')}"
#     )

for node, value in organization.items():
    tmp_node = {"y2024": "nf04"}
    organization[node]["glb500"].update(tmp_node)
print("-" * 36)
for node, value in organization.items():
    print(f"sn={node}, value={value}")


for node, value in organization.items():
    tmp_node = {"y2023": "nf04"}
    organization[node]["glb500"].update(tmp_node)
print("-" * 36)
for node, value in organization.items():
    print(f"sn={node}, value={value}")
