def exp_org(pick4org, *xyears):
    print(f"pick4org = {pick4org}, xyears = '{xyears}'")
    return


xyears = ["y2024", "y2023"]
print(f"xyears = #{xyears}#")
exp_org(123, *xyears)
exp_org(123, xyears)
