import socket
import time

client_skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# 建立连接:
client_skt.connect(("127.0.0.1", 9999))
client_skt.settimeout(5.0)  # timeout 5 seconds
# 接收欢迎消息:
# print(s.recv(1024).decode("utf-8"))
rcv = client_skt.recv(1024)
print(f"data_rcv = {rcv.decode('utf-8')}")


# for data in [b"Michael", b"Tracy", b"Sarah"]:
#     # 发送数据:
#     s.send(data)
#     print(s.recv(1024).decode("utf-8"))
# s.send(b"exit")

# current_6dof = {"x":1,"y":1,"z":1.0,"rx":3,"ry":4,"rz":5}
current_6dof = {}
KEY_6DOF = ["x", "y", "z", "rx", "ry", "rz"]

while True:

    print("-" * 72)
    print("1. Get Current Pose")
    print("2. Move to New Pose")
    choice = input("Your Choice (1 or 2, q=quit) : ")

    try:
        match choice:
            case "1":
                print("-" * 72)
                get_pose = "GetPose()"
                client_skt.send(get_pose.encode("utf-8"))
                time.sleep(1)
                current_6dof = client_skt.recv(1024)
                current_6dof = current_6dof.decode("utf-8")
                print(f"Current Pose : {current_6dof}")

            case "2":
                print("-" * 72)
                new_pose = input(
                    "New Pose (seperated with ',', like 100,200,100,30,10,40) : "
                )
                tmp = new_pose.split(",")
                target_6dof = dict(zip(KEY_6DOF, tmp))
                choice = input(f"Move to {target_6dof} (y/n) :")
                if choice in ["y", "Y"]:
                    movl = "MovL(pose={" + new_pose + "})"
                    client_skt.send(movl.encode("utf-8"))
                    time.sleep(1)
                    result = client_skt.recv(1024)
                    result = result.decode("utf-8")
                    print(f"Move to New Pose : {result}")

            case "q" | "Q":
                client_skt.send(b"exit")
                break  # quit loop

        # print(f"sending #{snd}#...")
        # s.send(snd.encode("utf-8"))
        # time.sleep(1)

        # print("receiving ...")

        # rcv = s.recv(1024)
        # print(f"data_rcv={rcv}\ndata_rcv_decode={rcv.decode('utf-8')}")

    except socket.timeout as e:
        print("socket receive data timetout")
    except Exception as e:
        print(f"Error: {e}")


client_skt.close()
