#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# import chardet
from datetime import datetime
from enum import Enum
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import re
import os
import socket
import shutil
import threading
import time
from time import sleep

# logging.basicConfig(level=logging.INFO)

KEY_6DOF = ["x", "y", "z", "rx", "ry", "rz"]

### config logging
current_directory = os.path.dirname(os.path.abspath(__file__))
current_file = Path(__file__).stem
log_dir = Path("log")
if not log_dir.exists():
    log_dir.mkdir()
log_dir = str(log_dir)
log_dir += "/" + current_file + ".log"
log_file_path = os.path.join(current_directory, log_dir)

# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
formatter = logging.Formatter(
    fmt="%(asctime)s - %(lineno)d - %(levelname)s - %(message)s",
    datefmt="%m-%d %H:%M:%S",
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# 100MB max. for each log file, 5 files at most, then 1st file is to be overrided.
rotating_file_handler = RotatingFileHandler(
    log_file_path, maxBytes=100 * 1024 * 1024, backupCount=5
)
rotating_file_handler.setLevel(logging.INFO)
rotating_file_handler.setFormatter(formatter)

rlog = logging.getLogger(__name__)
rlog.setLevel(logging.INFO)
rlog.addHandler(console_handler)
rlog.addHandler(rotating_file_handler)


class RaConfig:

    ###
    def __init__(self):

        file_path = __file__
        file_name_with_ext = os.path.basename(file_path)
        file_name_without_ext, file_extension = os.path.splitext(file_name_with_ext)

        self.__fn = file_name_without_ext + ".json"
        self.__config = {}

        return

    ###
    def load(self):
        with open(self.fn, encoding="utf-8") as file:
            self.__config = json.load(file)

        # print(f"config=#{self.__config}#")
        return

    @property
    def fn(self):
        return self.__fn

    @fn.setter
    def fn(self, v):
        self.__fn = v
        return

    @property
    def s6dof_fpath(self):
        return self.__config["6dof_txt_file_path"]

    @property
    def sleep_secs(self):
        sec = self.__config["check_file_every_seconds_when_idle"]
        if type(sec) is int:
            if (sec < 1) or (sec > 10):
                second = 1
            else:
                second = sec
        else:
            second = 1
        return second


###
rae6cfg = RaConfig()


### --------------------------------------------------------------------------------------------------------------------
###
### --------------------------------------------------------------------------------------------------------------------
class StatusCode(Enum):
    SUCCESS = 0
    FAIL = -1
    ALARM = -2
    EMERGENCY_STOP = -3
    POWEROFF = -4


class RobotMode(Enum):
    INIT = 1
    BRAKE_OPEN = 2
    POWEROFF = 3
    DISABLED = 4
    ENABLE = 5
    BACKDRIVE = 6
    RUNNING = 7
    SINGLE_MOVE = 8
    ERROR = 9
    PAUSE = 10
    COLLISION = 11


#  1 ROBOT_MODE_INIT 初始化状态
# 2 ROBOT_MODE_BRAKE_OPEN 有任意关节的抱闸松开
# 3 ROBOT_MODE_POWEROFF 机械臂下电状态
# 4 ROBOT_MODE_DISABLED 未使能(无抱闸松开)
# 35
#    5 ROBOT_MODE_ENABLE
# 使能且空闲
# 6 ROBOT_MODE_BACKDRIVE 拖拽模式(关节拖拽或力控拖拽)
# 7 ROBOT_MODE_RUNNING 运行状态(工程，TCP队列运动等)
# 8 ROBOT_MODE_SINGLE_MOVE 单次运动状态(点动、RunTo等)
# 9 ROBOT_MODE_ERROR
# 有未清除的报警。此状态优先级最高。 无论机械臂处于什么状态，有报警时都返回9
# 10 ROBOT_MODE_PAUSE 暂停状态
# 11 ROBOT_MODE_COLLISION 碰撞检测触发状态


class DobotRbt:

    ###
    def __init__(self, ip="192.168.5.1", port_req=29999, port_rsp=30004):
        self.__ip = ip
        self.__port_req = port_req
        self.__port_rsp = port_rsp

        self.__socket_req = 0
        self.__socket_rsp = 0
        self.__global_lock = threading.Lock()

        return

    ### connect to robot with ip and port_req. port_rsp reserved.
    def connect(self):

        #
        if self.port_req != 29999:
            raise ValueError(
                f"wrong port {self.port_req}, 29999 expected to send command."
            )
        if (self.port_rsp != 30004) and (self.port_rsp != 30005):
            raise ValueError(
                f"wrong port {self.port_rsp}, 30004 or 30005 expected to get response."
            )

        try:

            self.socket_req = socket.socket()
            self.socket_req.connect((self.ip, self.port_req))
            self.socket_req.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 144000)

            # self.socket_rsp = socket.socket()
            # self.socket_rsp.connect((self.ip,self.port_rsp))
            # self.socket_rsp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 144000)
            # any timeout?
        except socket.error:
            logging.error(f"socket error, ip={self.__ip}, port={self.port_req}")
            raise OSError(f"socket error, ip={self.__ip}, port={self.port_req}")

        return

    ### send command and get response with port_req
    def send(self, string):
        try:
            self.__socket_req.send(str.encode(string, "utf-8"))
        except Exception as e:
            rlog.error("socket error : {e}")
            while True:
                try:
                    self.__socket_req = self.reConnect(self.__ip, self.__port_req)
                    self.__socket_req.send(str.encode(string, "utf-8"))
                    break
                except Exception:
                    time.sleep(1)

        recvData = self.__reply()
        # rlog.info(f"rsp={self.get_rsp_id(recvData)}")
        rlog.info(f"recvData={recvData}")
        # self.get_rsp_id(recvData)
        message = {}
        if recvData.find("Not Tcp") != -1:
            message["error_id"] = -1
            message["value"] = recvData
            message["command"] = string
            return message
        else:
            return self.get_rsp(recvData)

    ###
    def __reply(self):
        """
        Read the return value
        """
        data = ""
        try:
            data = self.socket_req.recv(1024)
        except Exception as e:
            print(e)
            self.socket_req = self.reConnect(self.ip, self.port_req)

        finally:
            if len(data) == 0:
                data_str = data
            else:
                data_str = str(data, encoding="utf-8")
            # self.log(f'Receive from {self.ip}:{self.port}: {data_str}')
            return data_str

    def get_rsp_id(self, valueRecv):

        result = ""
        """
        解析Tcp返回值
        Parse the TCP return values
        """
        if (
            valueRecv.find("Not Tcp") != -1
        ):  # 通过返回值判断机器是否处于tcp模式 Judge whether the robot is in TCP mode by the return value
            rlog.warning("Control Mode Is Not Tcp")
            return valueRecv

        recvData = re.findall(r"-?\d+", valueRecv)
        recvData = [int(num) for num in recvData]
        if len(recvData) > 0:
            if recvData[0] != 0:
                # 根据返回值来判断机器处于什么状态 Judge what status the robot is in based on the return value
                if recvData[0] == -1:
                    print("Command execution failed")
                elif recvData[0] == -2:
                    print("The robot is in an error state")
                elif recvData[0] == -3:
                    print("The robot is in emergency stop state")
                elif recvData[0] == -4:
                    print("The robot is in power down state")
                else:
                    print("ErrorId is ", recvData[0])
        else:
            print("ERROR VALUE")

        return

    # def get_rsp(self, valueRecv):
    #     # 解析返回值，确保机器人在 TCP 控制模式
    #     if "Not Tcp" in valueRecv:
    #         print("Control Mode Is Not Tcp")
    #         return [1]
    #     return [int(num) for num in re.findall(r"-?\d+", valueRecv)] or [2]

    ###
    def get_rsp(self, recv_msg):
        message = {}

        # pattern = r"(-?\d+),\{([^\}]*)\},([a-zA-Z0-9()]+);"
        # pattern = pattern = r"(-?\d+)\s*,\s*\{([^\}]*)\}\s*,\s*([a-zA-Z0-9()]+);"
        # pattern = pattern = r"(-?\d+)\s*,\s*\{([^\}]*)\}\s*,\s*([a-zA-Z0-9()]+);"
        # pattern = r"([^,]+),\s*\{([^}]+)\},\s*(.+);"
        pattern = r"([^,]+),\s*\{([^}]*)\},\s*(.+);"
        match = re.search(pattern, recv_msg)
        if match:
            message["error_id"] = int(match.group(1))  # 第一个分组：0
            message["value"] = match.group(2)
            # part2_raw = match.group(2)
            message["command"] = match.group(3)  # 第三个分组：GetPose();

            if len(message["value"]) == 0:
                pass
            elif len(message["value"].split(",")) == 1:
                message["value"] = int(message["value"])
            elif len(message["value"].split(",")) > 1:
                message["value"] = [float(num) for num in message["value"].split(",")]

            # message["value"] = [num for num in part2_raw.split(",")]
            # print(f"message=#{message}#")
            # if len(message["value"]) == 1:
            #     if message["value"][0] == "":
            #         message["value"] = ""
            #     else:
            #         message["value"] = int(message["value"][0])
            # else:
            #     message["value"] = [float(num) for num in message["value"]]
            # print(
            #     f"len(value)=#{len(message['value'])}#, split=#{message['value'].split(',')}#, split_len=#{len(message['value'].split(','))}#"
            # )
            # print(f"message_return=#{message}#")

        else:
            message
            raise ValueError("cannot understand received message.")
        return message

    ###
    def get_status_message(self, status_code):
        error_messages = {
            StatusCode.FAIL.value: "命令执行失败。请重试。",
            StatusCode.ALARM.value: "机器人处于报警状态。清除报警后重试。",
            StatusCode.EMERGENCY_STOP.value: "机器人处于急停状态。松开急停并清除报警后重试。",
            StatusCode.POWEROFF.value: "机械臂处于下电状态。上电后重试。",
        }
        # return error_messages.get(status_code, "未知错误[{status_code}]。")
        return error_messages.get(status_code, f"未知错误[{status_code}]。")

    ###
    def get_robot_mode_message(self, robot_mode):
        robot_mode_messages = {
            RobotMode.INIT.value: "初始化状态",
            RobotMode.BRAKE_OPEN.value: "有任意关节的抱闸松开",
            RobotMode.POWEROFF.value: "机械臂下电状态",
            RobotMode.DISABLED.value: "未使能(无抱闸松开)",
            RobotMode.ENABLE.value: "使能且空闲",
            RobotMode.BACKDRIVE.value: "拖拽模式(关节拖拽或力控拖拽)",
            RobotMode.RUNNING.value: "运行状态(工程，TCP队列运动等)",
            RobotMode.SINGLE_MOVE.value: "单次运动状态(点动、RunTo等)",
            RobotMode.ERROR.value: "有未清除的报警",
            RobotMode.PAUSE.value: "暂停状态",
            RobotMode.COLLISION.value: "碰撞检测触发状态",
            # RobotMode.INIT: "初始化状态",
            # RobotMode.BRAKE_OPEN: "有任意关节的抱闸松开",
            # RobotMode.POWEROFF: "机械臂下电状态",
            # RobotMode.DISABLED: "未使能(无抱闸松开)",
            # RobotMode.ENABLE: "使能且空闲",
            # RobotMode.BACKDRIVE: "拖拽模式(关节拖拽或力控拖拽)",
            # RobotMode.RUNNING: "运行状态(工程，TCP队列运动等)",
            # RobotMode.SINGLE_MOVE: "单次运动状态(点动、RunTo等)",
            # RobotMode.ERROR: "有未清除的报警",
            # RobotMode.PAUSE: "暂停状态",
            # RobotMode.COLLISION: "碰撞检测触发状态",
        }

        # return error_messages.get(status_code, "未知错误[{status_code}]。")
        return robot_mode_messages.get(robot_mode, f"未知状态[{robot_mode}]")

    ###
    def log(self, text):
        if self.text_log:
            print(text)

    def send_data(self, string):
        # self.log(f"Send to {self.ip}:{self.port}: {string}")
        try:
            self.socket_req.send(str.encode(string, "utf-8"))
        except Exception as e:
            print(e)
            while True:
                try:
                    self.socket_req = self.reConnect(self.ip, self.port)
                    self.socket_req.send(str.encode(string, "utf-8"))
                    break
                except Exception:
                    time.sleep(1)

        return

    # def wait_reply(self):
    #     """
    #     Read the return value
    #     """
    #     data = ""
    #     try:
    #         # data = self.socket_dobot.recv(1024)
    #         data = self.socket_rsp.recv(1024)
    #     except Exception as e:
    #         print(e)
    #         self.socket_rsp = self.reConnect(self.ip, self.port_rsp)

    #     finally:
    #         if len(data) == 0:
    #             data_str = data
    #         else:
    #             data_str = str(data, encoding="utf-8")
    #             # data_str = data
    #         # self.log(f'Receive from {self.ip}:{self.port}: {data_str}')

    #             print(f"endoding={chardet.detect(data)['encoding']}")

    #             return data_str

    def wait_reply(self):
        """
        Read the return value
        """
        data = ""
        try:
            # data = self.socket_dobot.recv(1024)
            data = self.socket_req.recv(1024)
        except Exception as e:
            print(e)
            self.socket_req = self.reConnect(self.ip, self.port_req)

        finally:
            if len(data) == 0:
                data_str = data
            else:
                print(f"e6::ip={self.ip}, socket_req={self.socket_req}")
                data_str = str(data, encoding="utf-8")
                # data_str = data
                # self.log(f'Receive from {self.ip}:{self.port}: {data_str}')

                return data_str

    def close(self):
        """
        Close the port
        """
        for skt in [self.__socket_req, self.__socket_rsp]:
            if skt != 0:
                try:
                    skt.shutdown(socket.SHUT_RDWR)
                    skt.close()
                except socket.error as e:
                    print(f"Error while closing socket: {e}")
        return

    def sendRecvMsg(self, string):
        """
        send-recv Sync
        """
        with self.__global_lock:
            self.send_data(string)
            recvData = self.wait_reply()
            # self.ParseResultId(recvData)
            return recvData

    def __del__(self):
        self.close()

    def reConnect(self, ip, port):
        while True:
            try:
                socket_dobot = socket.socket()
                socket_dobot.connect((ip, port))
                break
            except Exception:
                time.sleep(1)
        return socket_dobot

    def get_pose(self) -> tuple[int, dict]:

        current_6dof = {}
        command = "GetPose()"
        result = self.send(command)
        rlog.info(f"result=#{result}#")
        if result["error_id"] == StatusCode.SUCCESS.value:
            k = ["x", "y", "z", "rx", "ry", "rz"]
            current_6dof = dict(zip(k, result["value"]))
        # else:
        #     rlog.warning(f"e6 :: {robot_e6.get_status_message(result['error_id'])}.\n")
        return result["error_id"], current_6dof

    #
    def movl(self, dest_6dof: dict) -> int:
        floats = list(dest_6dof.values())
        print(f"floats={floats}")
        str_floats = ",".join([str(x) for x in floats])
        command = "MovL(pose={" + str_floats + "})"
        result = self.send(command)
        return result["error_id"]

    @property
    def ip(self):
        return self.__ip

    @property
    def port_req(self):
        return self.__port_req

    @property
    def port_rsp(self):
        return self.__port_rsp

    @property
    def socket_req(self):
        return self.__socket_req

    @socket_req.setter
    def socket_req(self, skt):
        self.__socket_req = skt
        return

    @property
    def socket_rsp(self):
        return self.__socket_rsp

    @socket_rsp.setter
    def socket_rsp(self, skt):
        self.__socket_rsp = skt
        return


### --------------------------------------------------------------------------------------------------------------------
###
### --------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------


def main1():

    # messages = [
    #     "0,{500},RobotMode();",
    #     "0,{-100.6911,-145.1771,324.3626,-160.6002,33.1844,-75.2967},GetPose();",
    #     "0,{3},MovL(pose={-90.6911,-145.1771,324.3626,-160.6002,33.1844,-75.2967});",
    #     "-999,{},RobotMode();",
    # ]

    robot_e6 = DobotRbt()
    # logging.info("e6::initialized.")
    # for message in messages:
    #     result = robot_e6.get_rsp(message)
    #     print(f"result=#{result}#")
    # return

    # print("demo")
    # e6 = DobotDemo("192.168.5.1")
    # e6.start()
    # print("done.")

    # logging.info("e6::initialize.")
    # robot_e6 = DobotRbt()
    rlog.info("e6::initialized.")

    # for status_code in StatusCode:
    #     print(f"status_code = {status_code}")
    #     if status_code != StatusCode.SUCCESS:
    #         print(robot_e6.get_error_message(status_code))

    # print(robot_e6.get_error_message(10))

    # return
    # logging.info("e6::connect to robot.")
    robot_e6.connect()
    logging.info("e6 :: connected.\n")

    # rlog.info(
    #     f"e6::ip={robot_e6.ip}, socket_req={robot_e6.socket_req}, socket_rsp={robot_e6.socket_rsp}"
    # )

    ### mode

    command = "RobotMode()"
    rlog.info(f"command={command}")
    result = robot_e6.send(command)
    print(f"result=#{result}#")
    if result["value"] == RobotMode.DISABLED.value:
        rlog.warning(f"e6 :: {robot_e6.get_robot_mode_message(result['value'])}.\n")

        command = "EnableRobot()"
        result = robot_e6.send(command)
        command = "RobotMode()"
        result = robot_e6.send(command)
        if result["value"] == RobotMode.ENABLE.value:
            rlog.info(f"e6 :: {robot_e6.get_robot_mode_message(result['value'])}.\n")

    elif result["value"] == RobotMode.ENABLE.value:
        rlog.info(f"e6 :: {robot_e6.get_robot_mode_message(result['value'])}.\n")
    else:
        print(
            f"result['value']=#{result['value']}#, RobotMode.ENABLE=#{RobotMode.ENABLE.value}#"
        )

    ### pose
    rlog.info("-" * 36)
    current_6dof = {}
    command = "GetPose()"
    result = robot_e6.send(command)
    print(f"result=#{result}#")
    if result["error_id"] == StatusCode.SUCCESS.value:
        k = ["x", "y", "z", "rx", "ry", "rz"]
        current_6dof = dict(zip(k, result["value"]))
    else:
        rlog.warning(f"e6 :: {robot_e6.get_status_message(result['error_id'])}.\n")

    rlog.info("-" * 36)
    rlog.info("sleep 2 seconds")
    sleep(2)

    # move
    now_6dof = current_6dof

    x = current_6dof["x"]
    y = current_6dof["y"]
    z = current_6dof["z"]
    rx = current_6dof["rx"]
    ry = current_6dof["ry"]
    rz = current_6dof["rz"]

    ###
    for i in range(1, 3):
        rlog.info("-" * 144)
        rlog.info("e6 :: x + 10")
        to_6dof = f"{x+10},{y},{z},{rx},{ry},{rz}"
        command = "MovL(pose={" + to_6dof + "})"

        # command = "MovL(pose={-100.6911,-145.1771,324.3626,-160.6002,33.1844,-75.2967})"
        result = robot_e6.send(command)
        if result["error_id"] == StatusCode.SUCCESS.value:
            # current_6dof = dict((KEY_6DOF, result["value"]))
            rlog.info(f"e6 :: {result['command']} done.\n")
        else:
            msg = robot_e6.get_status_message(result["error_id"])
            rlog.warning(f"e6 :: {result['command']}, {msg}.\n")
        sleep(1)

        ###
        rlog.info("-" * 144)
        rlog.info("e6 :: y + 10")
        to_6dof = f"{x+10},{y+10},{z},{rx},{ry},{rz}"
        command = "MovL(pose={" + to_6dof + "})"
        result = robot_e6.send(command)
        if result["error_id"] == StatusCode.SUCCESS.value:
            # current_6dof = dict(zip(KEY_6DOF, result["value"]))
            rlog.info(f"e6 :: {result['command']} done.\n")
        else:
            msg = robot_e6.get_status_message(result["error_id"])
            rlog.warning(f"e6 :: {result['command']}, {msg}.\n")
        sleep(1)

        ###
        rlog.info("-" * 144)
        rlog.info("e6 :: z + 10")
        to_6dof = f"{x+10},{y+10},{z+10},{rx},{ry},{rz}"
        command = "MovL(pose={" + to_6dof + "})"
        result = robot_e6.send(command)
        if result["error_id"] == StatusCode.SUCCESS.value:
            # current_6dof = dict(zip(KEY_6DOF, result["value"]))
            rlog.info(f"e6 :: {result['command']} done.\n")
        else:
            msg = robot_e6.get_status_message(result["error_id"])
            rlog.warning(f"e6 :: {result['command']}, {msg}.\n")
        sleep(1)

        ###
        rlog.info("-" * 144)
        rlog.info("e6 :: go back")
        to_6dof = f"{x},{y},{z},{rx},{ry},{rz}"
        command = "MovL(pose={" + to_6dof + "})"

        result = robot_e6.send(command)
        if result["error_id"] == StatusCode.SUCCESS.value:
            # current_6dof = dict(zip(KEY_6DOF, result["value"]))
            rlog.info(f"e6 :: {result['command']} done.\n")
        else:
            msg = robot_e6.get_status_message(result["error_id"])
            rlog.warning(f"e6 :: {result['command']}, {msg}.\n")
        sleep(1)
    # move
    # for count in range(1,4):

    #     print(" " *36)
    #     logging.info(f">>> move #{count}")
    #     print(" " *36)

    #     logging.info("-" *36)
    #     command = "MovL(pose={-127.1304,-133.2410,326.4057,-167.2325,35.4448,-73.3100})"
    #     command = "MovL(pose={-87.6911,-145.1771,324.3626,-160.6002,33.1844,-75.2967})"
    #     logging.info(f"e6::{command}")
    #     result = robot_e6.send(command)
    #     logging.info(f"robot::{command}={result}")

    #     logging.info("-" *36)
    #     logging.info("sleep 2 seconds")
    #     sleep(2)

    #     logging.info("-" *36)
    #     command = "MovL(pose={-137.1304,-143.2410,326.4057,-167.2325,35.4448,-73.3100})"
    #     logging.info(f"e6::{command}")
    #     result = robot_e6.send(command)
    #     logging.info(f"robot::{command}={result}")

    #     logging.info("-" *36)
    #     logging.info("sleep 2 seconds")
    #     sleep(2)

    #     # mode
    #     logging.info("-" *36)
    #     command = "RobotMode()"
    #     logging.info(f"e6::{command}")
    #     result = robot_e6.send(command)
    #     logging.info(f"robot::{command}={result}")

    ###
    ###
    ### move to -118,-420

    ### pose
    logging.info("-" * 36)
    current_6dof = {}
    command = "GetPose()"
    result = robot_e6.send(command)
    print(f"result=#{result}#")
    if result["error_id"] == StatusCode.SUCCESS.value:
        k = ["x", "y", "z", "rx", "ry", "rz"]
        current_6dof = dict(zip(k, result["value"]))
    else:
        logging.warning(f"e6 :: {robot_e6.get_status_message(result['error_id'])}.\n")

    logging.info("-" * 36)
    logging.info("sleep 2 seconds")
    sleep(2)

    ###
    target_6dof = {
        "x": -118,
        "y": -420,
        "z": 100,
        "rx": current_6dof["rx"],
        "ry": current_6dof["ry"],
        "rz": current_6dof["rz"],
    }
    step = 10
    current_x = current_6dof["x"]
    current_y = current_6dof["y"]
    current_z = current_6dof["z"]
    step_x = (target_6dof["x"] - current_x) / step
    step_y = (target_6dof["y"] - current_y) / step
    step_z = (target_6dof["z"] - current_z) / step
    for i in range(1, step):

        logging.info("-" * 144)
        to_6dof = f"{current_x+step_x*i},{current_y+step_y*i},{current_z+step_z*i},{current_6dof['rx']},{current_6dof['ry']},{current_6dof['rz']}"
        logging.info("e6 :: move to {to_6dof}")
        command = "MovL(pose={" + to_6dof + "})"
        result = robot_e6.send(command)
        if result["error_id"] == StatusCode.SUCCESS.value:
            # current_6dof = dict(zip(KEY_6DOF, result["value"]))
            logging.info(f"e6 :: {result['command']} done.\n")
        else:
            msg = robot_e6.get_status_message(result["error_id"])
            logging.warning(f"e6 :: {result['command']}, {msg}.\n")
        sleep(1)

    # sleep(1)
    # logging.info("-" * 144)
    # # logging.info("e6 :: go back")
    # to_6dof = f"{x},{y},{z},{rx},{ry},{rz}"
    # x = -118
    # y = -200
    # to_6dof = f"{x},{y},{z},{rx},{ry},{rz}"
    # logging.info("e6 :: move to {to_6dof}")
    # # print(f"to_6dof = {to_6dof}")
    # command = "MovL(pose={" + to_6dof + "})"

    # result = robot_e6.send(command)
    # if result["error_id"] == StatusCode.SUCCESS.value:
    #     # current_6dof = dict(zip(KEY_6DOF, result["value"]))
    #     logging.info(f"e6 :: {result['command']} done.\n")
    # else:
    #     msg = robot_e6.get_status_message(result["error_id"])
    #     logging.warning(f"e6 :: {result['command']}, {msg}.\n")
    # sleep(1)

    # #
    # sleep(1)
    # logging.info("-" * 144)
    # # logging.info("e6 :: go back")
    # to_6dof = f"{x},{y},{z},{rx},{ry},{rz}"
    # x = -118
    # y = -250
    # to_6dof = f"{x},{y},{z},{rx},{ry},{rz}"
    # logging.info("e6 :: move to {to_6dof}")
    # # print(f"to_6dof = {to_6dof}")
    # command = "MovL(pose={" + to_6dof + "})"

    # result = robot_e6.send(command)
    # if result["error_id"] == StatusCode.SUCCESS.value:
    #     # current_6dof = dict(zip(KEY_6DOF, result["value"]))
    #     logging.info(f"e6 :: {result['command']} done.\n")
    # else:
    #     msg = robot_e6.get_status_message(result["error_id"])
    #     logging.warning(f"e6 :: {result['command']}, {msg}.\n")
    # sleep(1)

    # disable
    logging.info("-" * 36)
    command = "DisableRobot()"
    result = robot_e6.send(command)
    if result["error_id"] == StatusCode.SUCCESS.value:
        logging.info(f"e6 :: {result['command']} done.")
    else:
        msg = robot_e6.get_status_message(result["error_id"])
        logging.warning(f"e6 :: {result['command']}, {msg}.\n")

    logging.info("-" * 36)
    command = "RobotMode()"
    result = robot_e6.send(command)
    if result["error_id"] == StatusCode.SUCCESS.value:
        rmode = robot_e6.get_robot_mode_message(result["value"])
        logging.info(f"e6 :: {result['command']} done, {rmode}\n.")
    else:
        msg = robot_e6.get_status_message(result["error_id"])
        logging.warning(f"e6 :: {result['command']}, {msg}.\n")

    return
    # result = robot_e6.send("RobotMode()")
    # print(f"robot::RobotMode()={result}")

    # # if result == "Control Mode Is Not Tcp":
    # result = robot_e6.send("RequestControl()")
    # print(f"e6::result={result}")

    # result = robot_e6.send("RobotMode()")
    # print(f"robot::RobotMode()={result}")

    # command = "EnableRobot()"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # command = "RobotMode()"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")

    # command = "DisableRobot()"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # command = "RobotMode()"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")

    # command = "ClearError()"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # command = "RobotMode()"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")

    # get pose
    # command = "EnableRobot()"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")

    # {-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754}

    # for count in range(1, 10):
    #     print(f"move #{count}")
    #     command = "MovL(pose={-130.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    #     result = robot_e6.send(command)
    #     print(f"robot::{command}={result}")

    #     command = "MovL(pose={-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    #     result = robot_e6.send(command)
    #     print(f"robot::{command}={result}")

    # command = "MovL(pose={-130.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # sleep(5)
    # command = "MovL(pose={-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # # # sleep(5)

    # # command = "MovL(pose={-130.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # # result = robot_e6.send(command)
    # # print(f"robot::{command}={result}")
    # # # sleep(5)
    # # command = "MovL(pose={-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # # result = robot_e6.send(command)
    # # print(f"robot::{command}={result}")
    # # # sleep(5)

    # # command = "MovL(pose={-130.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # # result = robot_e6.send(command)
    # # print(f"robot::{command}={result}")
    # # # sleep(5)
    # # command = "MovL(pose={-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # # result = robot_e6.send(command)
    # # print(f"robot::{command}={result}")
    # # # sleep(5)

    # command = "RobotMode()"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")

    # sleep(5)
    # command = "RobotMode()"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # result = robot_e6.sendRecvMsg("RobotMode()")
    # print(f"robot::RobotMode()={result}")


# ------------------------------------------------------------------------------
def e6_init():

    robot_e6 = DobotRbt()
    rlog.info("e6 :: initialized.")

    # for status_code in StatusCode:
    #     print(f"status_code = {status_code}")
    #     if status_code != StatusCode.SUCCESS:
    #         print(robot_e6.get_error_message(status_code))

    # print(robot_e6.get_error_message(10))

    # return
    # rlog.info("e6::connect to robot.")
    robot_e6.connect()
    rlog.info("e6 :: connected.\n")

    # rlog.info(
    #     f"e6::ip={robot_e6.ip}, socket_req={robot_e6.socket_req}, socket_rsp={robot_e6.socket_rsp}"
    # )

    ### mode

    command = "RobotMode()"
    rlog.info(f"command={command}")
    result = robot_e6.send(command)
    rlog.info(f"result=#{result}#")
    if result["value"].find("Not Tcp") != -1:
        rlog.info("RequestControl()")
        command = "RequestControl()"
        result = robot_e6.send(command)
        rlog.info(f"result={result}")

        command = "RobotMode()"
        result = robot_e6.send(command)

    if result["value"] == RobotMode.DISABLED.value:
        rlog.warning(f"e6 :: {robot_e6.get_robot_mode_message(result['value'])}.\n")

        command = "EnableRobot()"
        result = robot_e6.send(command)
        command = "RobotMode()"
        result = robot_e6.send(command)
        if result["value"] == RobotMode.ENABLE.value:
            rlog.info(f"e6 :: {robot_e6.get_robot_mode_message(result['value'])}.\n")

    elif result["value"] == RobotMode.ENABLE.value:
        rlog.info(f"e6 :: {robot_e6.get_robot_mode_message(result['value'])}.\n")
    else:
        rlog.info(
            f"result['value']=#{result['value']}#, RobotMode.ENABLE=#{RobotMode.ENABLE.value}#"
        )

    return robot_e6


# ------------------------------------------------------------------------------
def get_pose(robot_e6):
    ### pose
    rlog.info("-" * 36)
    current_6dof = {}
    command = "GetPose()"
    result = robot_e6.send(command)
    print(f"result=#{result}#")
    if result["error_id"] == StatusCode.SUCCESS.value:
        k = ["x", "y", "z", "rx", "ry", "rz"]
        current_6dof = dict(zip(k, result["value"]))
    else:
        rlog.warning(f"e6 :: {robot_e6.get_status_message(result['error_id'])}.\n")

    rlog.info("-" * 36)

    return result


def movl(rae6: DobotRbt, dest_6dof: list[float]) -> tuple[int, str]:

    str_floats = ",".join([str(x) for x in dest_6dof])
    command = "MovL(pose={" + str_floats + "})"

    mesg = ""
    result = rae6.send(command)
    if result["error_id"] == StatusCode.SUCCESS.value:
        mesg = ""
        # current_6dof = dict(zip(KEY_6DOF, result["value"]))
        # rlog.info(f"e6 :: {result['command']} done.\n")
    else:
        mesg = rae6.get_status_message(result["error_id"])
        # rlog.warning(f"e6 :: {result['command']}, {msg}.\n")

    return result["error_id"], mesg


# ------------------------------------------------------------------------------


def tcplink(sock, addr, robot_e6):
    # print(f"addr={addr}")
    print("Accept new connection from %s:%s..." % addr)
    sock.send(b"Welcome!")

    while True:
        rcv = sock.recv(1024)
        rcv = rcv.decode("utf-8")
        # print(f"data_rcv = {data}\ndata_rcv_decode={data.decode('utf-8')}")
        time.sleep(1)
        if not rcv or rcv == "exit":
            break

        if rcv == "GetPose()":
            command = "GetPose()"
            result = robot_e6.send(command)
            print(f"result=#{result}#")
            if result["error_id"] == StatusCode.SUCCESS.value:
                k = ["x", "y", "z", "rx", "ry", "rz"]
                current_6dof = dict(zip(k, result["value"]))
                string_list = [str(num) for num in result["value"]]
                current_6dof_value = ",".join(string_list)
                sock.send(current_6dof_value.encode("utf-8"))
            else:
                rlog.warning(
                    f"e6 :: {robot_e6.get_status_message(result['error_id'])}.\n"
                )

        elif rcv[:4] == "MovL":
            command = rcv
            result = robot_e6.send(command)
            if result["error_id"] == StatusCode.SUCCESS.value:
                # current_6dof = dict(zip(KEY_6DOF, result["value"]))
                rlog.info(f"e6 :: {result['command']} done.\n")
                sock.send(b"success")
            else:
                msg = robot_e6.get_status_message(result["error_id"])
                rlog.warning(f"e6 :: {result['command']}, {msg}.\n")
                sock.send(msg.encode("utf-8"))

        else:
            print(f"rcv = {rcv}")
            sock.send(("Hello, %s!" % rcv).encode("utf-8"))
    sock.close()
    print("Connection from %s:%s closed." % addr)


#
def find_6dof_file(fpath: str) -> list[str]:
    """
    fpth: file path to check \n
    return: list of files
    """
    dir_path = Path(fpath)
    txt_files_sorted = sorted(dir_path.glob("*.txt"))  # 直接匹配并排序

    # if txt_files_sorted:
    #     print("找到的 .txt 文件（按名称排序）:")
    #     for file in txt_files_sorted:
    #         print(file.name)  # 输出文件名
    # else:
    #     print("目录中没有 .txt 文件")

    return txt_files_sorted


#
def read_6dof_file(file: str) -> list[float]:
    """
    file: txt file to read
    return: 6dof
    """
    floats = []

    with open(file, encoding="utf-8") as f:
        line = f.readline().strip()
        numbers = line.split(",")
        # print(f"numbers=#{numbers}#")

        if len(numbers) != 6:
            raise ValueError(
                f"文件 {file.name} 数据格式错误：应为 6 个数字，实际为 {len(numbers)} 个"
            )

        for num in numbers:
            try:
                floats.append(float(num))
            except ValueError:
                raise ValueError(f"文件 {file.name} 包含非浮点数: '{num}'")
    return floats


# ------------------------------------------------------------------------------
def main():

    # read configuration file
    try:
        rae6cfg.load()
    except Exception as e:
        rlog.exception(f"read {rae6cfg.fn} error: {e}\nfix it, then try again.\n")

    print(
        f"6dof_txt_file_path = {rae6cfg.s6dof_fpath}\ncheck_file_every_seconds_when_idle = {rae6cfg.sleep_secs} second(s)"
    )

    # init robot arm e6
    rae6 = e6_init()
    error_id, original_6dof = rae6.get_pose()
    if error_id == StatusCode.SUCCESS.value:
        rlog.info(f"original_6dof = {original_6dof}")
    else:
        rlog.warning(f"e6 :: {rae6.get_status_message(error_id)}.\n")

    ###
    message_interval = rae6cfg.sleep_secs
    nf_count = 0
    dest_dir = Path(rae6cfg.s6dof_fpath) / "moved"
    dest_dir.mkdir(parents=True, exist_ok=True)

    while True:
        flist = find_6dof_file(rae6cfg.s6dof_fpath)
        if flist:
            fn = ""
            for index, value in enumerate(flist):
                fn += value.name
                if index + 1 < len(flist):
                    fn += ", "
            rlog.info(f"{len(flist)} file(s) found : {fn}.")

            ###

            for file in flist:
                fn = file.name
                try:
                    floats = read_6dof_file(file)
                except Exception as e:
                    rlog.error(f"处理文件 {file.name} 时出错: {e}")
                    fn += "_" + datetime.now().strftime("%Y%m%d%H%M%S") + "_file_error"
                    shutil.move(str(file), str(dest_dir / fn))
                    rlog.info(f"已移动 {file.name} 到 moved 子目录 {fn}")
                # else:
                #     fn += "_" + datetime.now().strftime("%Y%m%d%H%M%S") + "_moved"
                # finally:
                #     shutil.move(str(file), str(dest_dir / fn))
                #     rlog.info(f"已移动 {file.name} 到 {dest_dir}")

                # str_floats = ",".join([str(x) for x in floats])
                # movl_command = "MovL(pose={" + str_floats + "})"
                # print(f"movl_command = {movl_command}")
                dest_6dof = dict(zip(KEY_6DOF, floats))
                print(f"floats={floats},dest_6dof={dest_6dof}")
                error_id = rae6.movl(dest_6dof)
                if error_id == StatusCode.SUCCESS.value:
                    __, current_6dof = rae6.get_pose()
                    rlog.info(f"move successs, current 6dof = {current_6dof}")
                    fn += "_" + datetime.now().strftime("%Y%m%d%H%M%S") + "_moved"
                else:
                    rlog.warning(f"e6 :: {rae6.get_status_message(error_id)}.\n")
                    fn += "_" + datetime.now().strftime("%Y%m%d%H%M%S") + "_robot_error"

                shutil.move(str(file), str(dest_dir / fn))
                rlog.info(f"已移动 {file.name} 到 moved 子目录 {fn}")

            time.sleep(rae6cfg.sleep_secs)

            # error_id = rae6.movl(original_6dof)
            # if error_id == StatusCode.SUCCESS.value:
            #     __, current_6dof = rae6.get_pose()
            #     rlog.info(f"move successs, current 6dof = {current_6dof}")

        else:
            nf_count += 1
            if nf_count * rae6cfg.sleep_secs > message_interval:
                rlog.info(f"no file(s) found.")
                nf_count = 0
                if message_interval < 60:
                    message_interval *= 2
                else:
                    message_interval = 60

            time.sleep(rae6cfg.sleep_secs)

    return

    # server_skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # # 监听端口:
    # server_skt.bind(("127.0.0.1", 9999))
    # server_skt.listen(5)
    # rlog.info("Waiting for client connection...")

    # while True:
    #     # 接受一个新连接:
    #     sock, addr = server_skt.accept()
    #     # 创建新线程来处理TCP连接:
    #     t = threading.Thread(target=tcplink, args=(sock, addr, robot_e6))
    #     t.start()


# ------------------------------------------------------------------------------
if __name__ == "__main__":

    main()


# 安全原点：0， -150， 615.2， -90,0,180
