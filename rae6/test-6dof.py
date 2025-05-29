def mov(x: float, y: float, z: float) -> None:
    print(f"x={x}, y={y}, z={z}")
    return


target_6dof = [11, 22, 33]
mov(*target_6dof)

target_joint = [11.0, 22.3, 80.0, 77.2, -13.5, 6.0]
for index, angle in enumerate(target_joint):
    print(f"index={index+1}, angle={angle}")

print(f"target_joint={target_joint}")
print(f"target_joint={','.join([str(x) for x in target_joint])}")
