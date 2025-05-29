#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from enum import IntEnum
import json
import logging
from logging.handlers import RotatingFileHandler
import numpy as np
import os
from pathlib import Path
import re
import shutil
import socket
import threading
import time
from time import sleep
from typing import Dict, Union

# Port Feedback
MyType = np.dtype(
    [
        (
            "len",
            np.uint16,
        ),
        ("reserve", np.byte, (6,)),
        (
            "DigitalInputs",
            np.uint64,
        ),
        (
            "DigitalOutputs",
            np.uint64,
        ),
        (
            "RobotMode",
            np.uint64,
        ),
        (
            "TimeStamp",
            np.uint64,
        ),
        (
            "RunTime",
            np.uint64,
        ),
        (
            "TestValue",
            np.uint64,
        ),
        ("reserve2", np.byte, (8,)),
        (
            "SpeedScaling",
            np.float64,
        ),
        ("reserve3", np.byte, (16,)),
        (
            "VRobot",
            np.float64,
        ),
        (
            "IRobot",
            np.float64,
        ),
        (
            "ProgramState",
            np.float64,
        ),
        (
            "SafetyOIn",
            np.uint16,
        ),
        (
            "SafetyOOut",
            np.uint16,
        ),
        ("reserve4", np.byte, (76,)),
        ("QTarget", np.float64, (6,)),
        ("QDTarget", np.float64, (6,)),
        ("QDDTarget", np.float64, (6,)),
        ("ITarget", np.float64, (6,)),
        ("MTarget", np.float64, (6,)),
        ("QActual", np.float64, (6,)),
        ("QDActual", np.float64, (6,)),
        ("IActual", np.float64, (6,)),
        ("ActualTCPForce", np.float64, (6,)),
        ("ToolVectorActual", np.float64, (6,)),
        ("TCPSpeedActual", np.float64, (6,)),
        ("TCPForce", np.float64, (6,)),
        ("ToolVectorTarget", np.float64, (6,)),
        ("TCPSpeedTarget", np.float64, (6,)),
        ("MotorTemperatures", np.float64, (6,)),
        ("JointModes", np.float64, (6,)),
        ("VActual", np.float64, (6,)),
        ("HandType", np.byte, (4,)),
        (
            "User",
            np.byte,
        ),
        (
            "Tool",
            np.byte,
        ),
        (
            "RunQueuedCmd",
            np.byte,
        ),
        (
            "PauseCmdFlag",
            np.byte,
        ),
        (
            "VelocityRatio",
            np.byte,
        ),
        (
            "AccelerationRatio",
            np.byte,
        ),
        (
            "reserve5",
            np.byte,
        ),
        (
            "XYZVelocityRatio",
            np.byte,
        ),
        (
            "RVelocityRatio",
            np.byte,
        ),
        (
            "XYZAccelerationRatio",
            np.byte,
        ),
        (
            "RAccelerationRatio",
            np.byte,
        ),
        ("reserve6", np.byte, (2,)),
        (
            "BrakeStatus",
            np.byte,
        ),
        (
            "EnableStatus",
            np.byte,
        ),
        (
            "DragStatus",
            np.byte,
        ),
        (
            "RunningStatus",
            np.byte,
        ),
        (
            "ErrorStatus",
            np.byte,
        ),
        (
            "JogStatusCR",
            np.byte,
        ),
        (
            "CRRobotType",
            np.byte,
        ),
        (
            "DragButtonSignal",
            np.byte,
        ),
        (
            "EnableButtonSignal",
            np.byte,
        ),
        (
            "RecordButtonSignal",
            np.byte,
        ),
        (
            "ReappearButtonSignal",
            np.byte,
        ),
        (
            "JawButtonSignal",
            np.byte,
        ),
        (
            "SixForceOnline",
            np.byte,
        ),
        (
            "CollisionState",
            np.byte,
        ),
        (
            "ArmApproachState",
            np.byte,
        ),
        (
            "J4ApproachState",
            np.byte,
        ),
        (
            "J5ApproachState",
            np.byte,
        ),
        (
            "J6ApproachState",
            np.byte,
        ),
        ("reserve7", np.byte, (61,)),
        (
            "VibrationDisZ",
            np.float64,
        ),
        (
            "CurrentCommandId",
            np.uint64,
        ),
        ("MActual", np.float64, (6,)),
        (
            "Load",
            np.float64,
        ),
        (
            "CenterX",
            np.float64,
        ),
        (
            "CenterY",
            np.float64,
        ),
        (
            "CenterZ",
            np.float64,
        ),
        ("UserValue[6]", np.float64, (6,)),
        ("ToolValue[6]", np.float64, (6,)),
        ("reserve8", np.byte, (8,)),
        ("SixForceValue", np.float64, (6,)),
        ("TargetQuaternion", np.float64, (4,)),
        ("ActualQuaternion", np.float64, (4,)),
        (
            "AutoManualMode",
            np.uint16,
        ),
        (
            "ExportStatus",
            np.uint16,
        ),
        (
            "SafetyState",
            np.byte,
        ),
        ("reserve9", np.byte, (19,)),
    ]
)

# MyType = np.dtype(
#     [
#         ("len", np.uint16, (1,)),  # 消息字节总长度
#         ("reserve", np.byte, (6,)),
#         ("DigitalInputs", np.uint64, (1,)),
#         ("DigitalOutputs", np.uint64, (1,)),
#         ("RobotMode", np.uint64, (1,)),  # 机器人模式 详见RobotMode指令说明
#         ("TimeStamp", np.uint64, (1,)),
#         ("RunTime", np.uint64, (1,)),
#         ("TestValue", np.uint64, (1,)),  # 内存结构测试标准值 0x01234567 89ABCDEF
#         ("reserve2", np.byte, (8,)),
#         ("SpeedScaling", np.float64, (1,)),
#         ("reserve3", np.byte, (16,)),
#         ("VRobot", np.float64, (1,)),
#         ("IRobot", np.float64, (1,)),
#         ("ProgramState", np.float64, (1,)),
#         ("SafetyOIn", np.uint16, (1,)),  # 文档是char
#         ("SafetyOOut", np.uint16, (1,)),  # 文档是char
#         ("reserve4", np.byte, (76,)),
#         ("QTarget", np.float64, (6,)),
#         ("QDTarget", np.float64, (6,)),
#         ("QDDTarget", np.float64, (6,)),
#         ("ITarget", np.float64, (6,)),
#         ("MTarget", np.float64, (6,)),
#         ("QActual", np.float64, (6,)),
#         ("QDActual", np.float64, (6,)),
#         ("IActual", np.float64, (6,)),
#         ("ActualTCPForce", np.float64, (6,)),
#         ("ToolVectorActual", np.float64, (6,)),  # TCP笛卡尔实际坐标值
#         ("TCPSpeedActual", np.float64, (6,)),
#         ("TCPForce", np.float64, (6,)),
#         ("ToolVectorTarget", np.float64, (6,)),  # TCP笛卡尔目标坐标值
#         ("TCPSpeedTarget", np.float64, (6,)),
#         ("MotorTemperatures", np.float64, (6,)),
#         ("JointModes", np.float64, (6,)),
#         ("VActual", np.float64, (6,)),
#         ("HandType", np.byte, (4,)),
#         (
#             "User",
#             np.byte,
#         ),
#         (
#             "Tool",
#             np.byte,
#         ),
#         (
#             "RunQueuedCmd",
#             np.byte,
#         ),
#         (
#             "PauseCmdFlag",
#             np.byte,
#         ),
#         (
#             "VelocityRatio",
#             np.byte,
#         ),
#         (
#             "AccelerationRatio",
#             np.byte,
#         ),
#         (
#             "reserve5",
#             np.byte,
#         ),
#         (
#             "XYZVelocityRatio",
#             np.byte,
#         ),
#         (
#             "RVelocityRatio",
#             np.byte,
#         ),
#         (
#             "XYZAccelerationRatio",
#             np.byte,
#         ),
#         (
#             "RAccelerationRatio",
#             np.byte,
#         ),
#         ("reserve6", np.byte, (2,)),
#         (
#             "BrakeStatus",
#             np.byte,
#         ),
#         (
#             "EnableStatus",
#             np.byte,
#         ),
#         (
#             "DragStatus",
#             np.byte,
#         ),
#         (
#             "RunningStatus",
#             np.byte,
#         ),
#         (
#             "ErrorStatus",
#             np.byte,
#         ),
#         (
#             "JogStatusCR",
#             np.byte,
#         ),
#         (
#             "CRRobotType",
#             np.byte,
#         ),
#         (
#             "DragButtonSignal",
#             np.byte,
#         ),
#         (
#             "EnableButtonSignal",
#             np.byte,
#         ),
#         (
#             "RecordButtonSignal",
#             np.byte,
#         ),
#         (
#             "ReappearButtonSignal",
#             np.byte,
#         ),
#         (
#             "JawButtonSignal",
#             np.byte,
#         ),
#         (
#             "SixForceOnline",
#             np.byte,
#         ),
#         (
#             "CollisionState",
#             np.byte,
#         ),
#         (
#             "ArmApproachState",
#             np.byte,
#         ),
#         (
#             "J4ApproachState",
#             np.byte,
#         ),
#         (
#             "J5ApproachState",
#             np.byte,
#         ),
#         (
#             "J6ApproachState",
#             np.byte,
#         ),
#         ("reserve7", np.byte, (61,)),
#         (
#             "VibrationDisZ",
#             np.float64,
#         ),
#         (
#             "CurrentCommandId",
#             np.uint64,
#         ),
#         ("MActual", np.float64, (6,)),
#         (
#             "Load",
#             np.float64,
#         ),
#         (
#             "CenterX",
#             np.float64,
#         ),
#         (
#             "CenterY",
#             np.float64,
#         ),
#         (
#             "CenterZ",
#             np.float64,
#         ),
#         ("UserValue[6]", np.float64, (6,)),
#         ("ToolValue[6]", np.float64, (6,)),
#         ("reserve8", np.byte, (8,)),
#         ("SixForceValue", np.float64, (6,)),
#         ("TargetQuaternion", np.float64, (4,)),
#         ("ActualQuaternion", np.float64, (4,)),
#         (
#             "AutoManualMode",
#             np.uint16,
#         ),
#         (
#             "ExportStatus",
#             np.uint16,
#         ),
#         (
#             "SafetyState",
#             np.byte,
#         ),
#         ("reserve9", np.byte, (19,)),
#     ]
# )


###
class RobotMode(IntEnum):
    INIT = 1
    BRAKE_OPEN = 2
    POWEROFF = 3
    DISABLED = 4  # 未使能(无抱闸松开)
    ENABLE = 5  # 使能且空闲
    BACKDRIVE = 6
    RUNNING = 7  # 运行状态(工程，TCP队列运动等)
    SINGLE_MOVE = 8
    ERROR = 9

    # 有未清除的报警。此状态优先级最高。 无论机械臂处于什么状态，有报警时都返回9

    PAUSE = 10
    COLLISION = 11


###
class ErrorID(IntEnum):
    NO_ERROR = 0
    FAIL = -1
    ALARM = -2
    EMERGENCY_STOP = -3
    POWEROFF = -4
    NOT_FULFIL = (
        -98
    )  # not done, might still running or unknown error occurred. # defined by George
    NOT_TCP = -99  # defined by George
    WRONG_COMMAND = -10000
    OUT_OF_RANGE = -40000  # value of parameter is out of range. # defined by George


# 错误码 描述 备注
# 0 无错误 命令下发成功
# -1 命令执行失败 已收到命令，但执行失败了
# -2 机器人处于报警状态
# 机器人报警状态下无法执行指令，需要清除
# 报警后重新下发指令。
# -3 机器人处于急停状态
# 机器人急停状态下无法执行指令，需要松开
# 急停并清除报警后重新下发指令。
# -4 机械臂处于下电状态
# 机械臂下电状态下无法执行指令，需要先给
# 机械臂上电。
# -5 机械臂处于脚本运行状态
# 机械臂处于脚本运行状态下拒绝执行部分指 令，需要先暂停/停止脚本。
# -6
# MoveJog指令运动轴与运动类型 修改coordtype参数值，详见MoveJog指令 不匹配 说明。
# -7 机械臂处于脚本暂停状态
# 机械臂处于脚本暂停状态下拒绝执行部分指
# 令，需要先停止脚本。
# -8 机械臂认证过期 机械臂处于不可用状态，需联系FAE处理。
# -10000 命令错误 下发的命令不存在
# -20000 参数数量错误 下发命令中的参数数量错误

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


### ----------------------------------------------------------------------------
class RaE6Config:

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

    # @fn.setter
    # def fn(self, v):
    #     self.__fn = v
    #     return

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
rae6cfg = RaE6Config()


### ----------------------------------------------------------------------------
class RobotArmApi:

    ###
    def __init__(self, ip="192.168.5.1", port=29999):
        """
        - port_cmd: send command and get response via this port.
        - port_rti: get real time information from robot arm via this port. 30004 - every 8ms, 30005 - every 200ms
        """
        self.__ip = ip
        self.__port = port
        self.__socket: socket = 0
        self.__global_lock = threading.Lock()

        return

    ###
    def connect(self):

        if self.port == 29999 or self.port == 30004 or self.port == 30005:
            try:
                self.socket = socket.socket()
                self.socket.connect((self.ip, self.port))
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 144000)
            except socket.error:
                rlog.error(f"socket error, ip={self.ip}, port={self.port}")
                raise OSError(f"socket error, ip={self.ip}, port={self.port}")
        else:
            raise ValueError(
                f"Connect to robot arm via port 29999 or 30004 or 30005. {self.port} is invalid."
            )

        return

    ###
    def __send_command(self, command: str):
        try:
            self.socket.send(str.encode(command, "utf-8"))
        except Exception as e:
            rlog.error(f"socket error : {e}")
            while True:
                try:
                    self.connect()
                    self.socket.send(str.encode(command, "utf-8"))
                    break
                except Exception:
                    sleep(1)
        return

    ###
    def __get_response(self) -> str:
        """
        Read the return value
        """
        data = ""
        try:
            data = self.socket.recv(1024)

        except Exception as e:
            rlog.error(f"socket error : {e}")
            self.connect()

        finally:
            if len(data) == 0:
                data_str = data
            else:
                data_str = str(data, encoding="utf-8")
            return data_str

    def __del__(self):
        self.close()

    def close(self):
        if self.socket != 0:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
                self.socket.close()
            except socket.error as e:
                rlog.error(f"Error while closing socket: {e}")
        return

    ###
    def send(self, command: str) -> str:
        with self.global_lock:
            self.__send_command(command)
            recv_data = self.__get_response()
            # get_rsp()
            return recv_data

    @property
    def ip(self):
        return self.__ip

    @property
    def port(self):
        return self.__port

    @property
    def socket(self):
        return self.__socket

    @socket.setter
    def socket(self, s):
        self.__socket = s
        return

    @property
    def global_lock(self):
        return self.__global_lock


###-----------------------------------------------------------------------------
class RobotArmFeedBack(RobotArmApi):

    ###
    def __init__(self, ip="192.168.5.1", port=30004):
        super().__init__(ip, port)
        self.__MyType = []
        self.last_recv_time = time.perf_counter()

        class item:
            def __init__(self):
                self.robotMode = -1  #
                self.robotCurrentCommandID = 0
                self.MessageSize = -1
                self.DigitalInputs = -1
                self.DigitalOutputs = -1

                # 自定义添加所需反馈数据
                self.ToolVectorActual = -1  # double 6 48 0624~0671 TCP笛卡尔实际坐标值
                self.ToolVectorTarget = -1  # TCP笛卡尔目标坐标值
                self.CurrentCommandId = -1  # 当前运动队列id
                self.EnableStatus = -1
                self.RunningStatus = -1
                self.ErrorStatus = -1

        self.feeds = item()

        return

    ###
    def __get_rtinfo(self):
        """
        返回机械臂状态
        Return the robot status
        """
        self.socket.setblocking(True)  # 设置为阻塞模式
        data = bytes()
        current_recv_time = time.perf_counter()  # 计时，获取当前时间
        temp = self.socket.recv(144000)  # 缓冲区
        if len(temp) > 1440:
            temp = self.socket.recv(144000)
        # print("get:",len(temp))
        i = 0
        if len(temp) < 1440:
            while i < 5:
                # print("重新接收")
                temp = self.socket.recv(144000)
                if len(temp) > 1440:
                    break
                i += 1
            if i >= 5:
                raise Exception("接收数据包缺失，请检查网络环境")

        interval = (current_recv_time - self.last_recv_time) * 1000  # 转换为毫秒
        self.last_recv_time = current_recv_time
        # print(f"Time interval since last receive: {interval:.3f} ms")

        data = temp[0:1440]  # 截取1440字节
        # print(len(data))
        # print(f"Single element size of MyType: {MyType.itemsize} bytes")
        self.__MyType = None

        if len(data) == 1440:
            self.__MyType = np.frombuffer(data, dtype=MyType)

        return self.__MyType

    ###
    def get_feeds(self):
        # 获取机器人状态
        while True:
            feeds = self.__get_rtinfo()
            with self.global_lock:
                if feeds is not None:

                    # rlog.info(f'feeds.TestValue={feeds["TestValue"]}')
                    # rlog.info(f'feeds.TestValue={feeds["TestValue"][0]}')
                    # rlog.info(f'feeds.TestValue={hex(feeds["TestValue"][0])}')

                    if hex((feeds["TestValue"][0])) == "0x123456789abcdef":
                        # 基础字段
                        self.feeds.MessageSize = feeds["len"][0]
                        self.feeds.robotMode = feeds["RobotMode"][0]
                        self.feeds.DigitalInputs = feeds["DigitalInputs"][0]
                        self.feeds.DigitalOutputs = feeds["DigitalOutputs"][0]
                        # 自定义添加所需反馈数据
                        self.feeds.ToolVectorActual = feeds["ToolVectorActual"][0]
                        self.feeds.ToolVectorTarget = feeds["ToolVectorTarget"][0]
                        self.feeds.CurrentCommandId = feeds["CurrentCommandId"][0]
                        self.feeds.EnableStatus = feeds["EnableStatus"][0]
                        self.feeds.RunningStatus = feeds["RunningStatus"][0]
                        self.feeds.ErrorStatus = feeds["ErrorStatus"][0]
                        """
                        self.feedData.DigitalOutputs = int(feedInfo['DigitalOutputs'][0])
                        self.feedData.RobotMode = int(feedInfo['RobotMode'][0])
                        self.feedData.TimeStamp = int(feedInfo['TimeStamp'][0])
        
                        """
                else:
                    rlog.info("feeds is none.")

        return


### ----------------------------------------------------------------------------
class RobotArmDashBoard(RobotArmApi):

    ###
    def __init__(self, ip="192.168.5.1", port=29999):
        super().__init__(ip, port)

    ###
    def RequestControl(self):
        """
        切换到TCP模式
        """
        string = "RequestControl()"
        return self.send(string)

    ###
    def EnableRobot(
        self,
        load=0.0,
        centerX=0.0,
        centerY=0.0,
        centerZ=0.0,
        isCheck=-1,
    ):
        """
        可选参数
        参数名 类型 说明
        load double
        设置负载重量，取值范围不能超过各个型号机器⼈的负载范围。单位：kg
        centerX double X⽅向偏⼼距离。取值范围：-999~ 999，单位：mm
        centerY double Y⽅向偏⼼距离。取值范围：-999~ 999，单位：mm
        centerZ double Z⽅向偏⼼距离。取值范围：-999~ 999，单位：mm
        isCheck int    是否检查负载。1表⽰检查，0表⽰不检查。如果设置为1，则机械臂
        使能后会检查实际负载是否和设置负载⼀致，如果不⼀致会⾃动下使
        能。默认值为0
        可携带的参数数量如下：
        0：不携带参数，表⽰使能时不设置负载重量和偏⼼参数。
        1：携带⼀个参数，该参数表⽰负载重量。
        4：携带四个参数，分别表⽰负载重量和偏⼼参数。
        5：携带五个参数，分别表⽰负载重量、偏⼼参数和是否检查负载。
        """
        """
            Optional parameter
            Parameter name     Type     Description
            load     double     Load weight. The value range should not exceed the load range of corresponding robot models. Unit: kg.
            centerX     double     X-direction eccentric distance. Range: -999 – 999, unit: mm.
            centerY     double     Y-direction eccentric distance. Range: -999 – 999, unit: mm.
            centerZ     double     Z-direction eccentric distance. Range: -999 – 999, unit: mm.
            isCheck     int     Check the load or not. 1: check, 0: not check. If set to 1, the robot arm will check whether the actual load is the same as the set load after it is enabled, and if not, it will be automatically disabled. 0 by default.
            The number of parameters that can be contained is as follows:
            0: no parameter (not set load weight and eccentric parameters when enabling the robot).
            1: one parameter (load weight).
            4: four parameters (load weight and eccentric parameters).
            5: five parameters (load weight, eccentric parameters, check the load or not).
                """
        string = "EnableRobot("
        if load != 0:
            string = string + "{:f}".format(load)
            if centerX != 0 or centerY != 0 or centerZ != 0:
                string = string + ",{:f},{:f},{:f}".format(centerX, centerY, centerZ)
                if isCheck != -1:
                    string = string + ",{:d}".format(isCheck)
        string = string + ")"
        return self.send(string)

    ###
    def DisableRobot(self):
        """
        Disabled the robot
        下使能机械臂
        """
        string = "DisableRobot()"
        return self.send(string)

    ###
    def ClearError(self):
        """
        Clear controller alarm information
        Clear the alarms of the robot. After clearing the alarm, you can judge whether the robot is still in the alarm status according to RobotMode.
        Some alarms cannot be cleared unless you resolve the alarm cause or restart the controller.
        清除机器⼈报警。清除报警后，⽤⼾可以根据RobotMode来判断机器⼈是否还处于报警状态。部
        分报警需要解决报警原因或者重启控制柜后才能清除。
        """
        string = "ClearError()"
        return self.send(string)

    ###
    def RobotMode(self):
        """
        获取机器⼈当前状态。
        1 ROBOT_MODE_INIT 初始化状态
        2 ROBOT_MODE_BRAKE_OPEN 有任意关节的抱闸松开
        3 ROBOT_MODE_POWEROFF 机械臂下电状态
        4 ROBOT_MODE_DISABLED 未使能（⽆抱闸松开）
        5 ROBOT_MODE_ENABLE 使能且空闲
        6 ROBOT_MODE_BACKDRIVE 拖拽模式
        7 ROBOT_MODE_RUNNING 运⾏状态(⼯程，TCP队列运动等)
        8 ROBOT_MODE_SINGLE_MOVE 单次运动状态（点动、RunTo等）
        9 ROBOT_MODE_ERROR
             有未清除的报警。此状态优先级最⾼，⽆论机械臂
             处于什么状态，有报警时都返回9
        10 ROBOT_MODE_PAUSE ⼯程状态
        11 ROBOT_MODE_COLLISION 碰撞检测触发状态
        Get the current status of the robot.
        1 ROBOT_MODE_INIT  Initialized status
        2 ROBOT_MODE_BRAKE_OPEN  Brake switched on
        3 ROBOT_MODE_POWEROFF  Power-off status
        4 ROBOT_MODE_DISABLED  Disabled (no brake switched on
        5 ROBOT_MODE_ENABLE  Enabled and idle
        6 ROBOT_MODE_BACKDRIVE  Drag mode
        7 ROBOT_MODE_RUNNING  Running status (project, TCP queue)
        8 ROBOT_MODE_SINGLE_MOVE  Single motion status (jog, RunTo)
        9 ROBOT_MODE_ERROR
             There are uncleared alarms. This status has the highest priority. It returns 9 when there is an alarm, regardless of the status of the robot arm.
        10 ROBOT_MODE_PAUSE  Pause status
        11 ROBOT_MODE_COLLISION  Collision status
        """
        string = "RobotMode()"
        return self.send(string)

    ###
    def PositiveKin(self, J1, J2, J3, J4, J5, J6, user=-1, tool=-1):
        """
        描述
        进⾏正解运算：给定机械臂各关节⻆度，计算机械臂末端在给定的笛卡尔坐标系中的坐标值。
        必选参数
        参数名 类型 说明
        J1 double J1轴位置，单位：度
        J2 double J2轴位置，单位：度
        J3 double J3轴位置，单位：度
        J4 double J4轴位置，单位：度
        J5 double J5轴位置，单位：度
        J6 double J6轴位置，单位：度
        可选参数
        参数名 类型 说明
        格式为"user=index"，index为已标定的⽤⼾坐标系索引。
        User string 不指定时使⽤全局⽤⼾坐标系。
        Tool string  格式为"tool=index"，index为已标定的⼯具坐标系索引。不指定时使⽤全局⼯具坐标系。
        Description
        Positive solution. Calculate the coordinates of the end of the robot in the specified Cartesian coordinate system, based on the given angle of each joint.
        Required parameter:
        Parameter name     Type     Description
        J1     double     J1-axis position, unit: °
        J2     double     J2-axis position, unit: °
        J3     double     J3-axis position, unit: °
        J4     double     J4-axis position, unit: °
        J5     double     J5-axis position, unit: °
        J6     double     J6-axis position, unit: °
        Optional parameter:
        Parameter name     Type     Description
        Format: "user=index", index: index of the calibrated user coordinate system.
        User     string     The global user coordinate system will be used if it is not specified.
        Tool     string     Format: "tool=index", index: index of the calibrated tool coordinate system. The global tool coordinate system will be used if it is not set.
        """
        string = "PositiveKin({:f},{:f},{:f},{:f},{:f},{:f}".format(
            J1, J2, J3, J4, J5, J6
        )
        params = []
        if user != -1:
            params.append("user={:d}".format(user))
        if tool != -1:
            params.append("tool={:d}".format(tool))
        for ii in params:
            string = string + "," + ii
        string = string + ")"
        return self.send(string)

    ###
    def InverseKin(
        self, X, Y, Z, Rx, Ry, Rz, user=-1, tool=-1, useJointNear=-1, JointNear=""
    ):
        """
        描述
        进⾏逆解运算：给定机械臂末端在给定的笛卡尔坐标系中的坐标值，计算机械臂各关节⻆度。
        由于笛卡尔坐标仅定义了TCP的空间坐标与倾斜⻆，所以机械臂可以通过多种不同的姿态到达同⼀
        个位姿，意味着⼀个位姿变量可以对应多个关节变量。为得出唯⼀的解，系统需要⼀个指定的关节
        坐标，选择最接近该关节坐标的解作为逆解结果。
        必选参数
        参数名 类型 说明
        X double X轴位置，单位：mm
        Y double Y轴位置，单位：mm
        Z double Z轴位置，单位：mm
        Rx double Rx轴位置，单位：度
        Ry double Ry轴位置，单位：度
        Rz double Rz轴位置，单位：度
        可选参数
        参数名 类型 说明
        User string  格式为"user=index"，index为已标定的⽤⼾坐标系索引。不指定时使⽤全局⽤⼾坐标系。
        Tool string  格式为"tool=index"，index为已标定的⼯具坐标系索引。不指定时使⽤全局⼯具坐标系。
        useJointNear string  ⽤于设置JointNear参数是否有效。
            "useJointNear=0"或不携带表⽰JointNear⽆效，系统根据机械臂当前关节⻆度就近选解。
            "useJointNear=1"表⽰根据JointNear就近选解。
        jointNear string 格式为"jointNear={j1,j2,j3,j4,j5,j6}"，⽤于就近选解的关节坐标。
        Description
        Inverse solution. Calculate the joint angles of the robot, based on the given coordinates in the specified Cartesian coordinate system.
        As Cartesian coordinates only define the spatial coordinates and tilt angle of the TCP, the robot arm can reach the same posture through different gestures, which means that one posture variable can correspond to multiple joint variables.
        To get a unique solution, the system requires a specified joint coordinate, and the solution closest to this joint coordinate is selected as the inverse solution。
        Required parameter:
        Parameter name     Type     Description
        X     double     X-axis position, unit: mm
        Y     double     Y-axis position, unit: mm
        Z     double     Z-axis position, unit: mm
        Rx     double     Rx-axis position, unit: °
        Ry     double     Ry-axis position, unit: °
        Rz     double     Rz-axis position, unit: °
        Optional parameter:
        Parameter name     Type     Description
        User      string     Format: "user=index", index: index of the calibrated user coordinate system. The global user coordinate system will be used if it is not set.
        Tool     string     Format: "tool=index", index: index of the calibrated tool coordinate system. The global tool coordinate system will be used if it is not set.
        useJointNear     string     used to set whether JointNear is effective.
            "useJointNear=0" or null: JointNear data is ineffective. The algorithm selects the joint angles according to the current angle.
            "useJointNear=1": the algorithm selects the joint angles according to JointNear data.
        jointNear     string     Format: "jointNear={j1,j2,j3,j4,j5,j6}", joint coordinates for selecting joint angles.
        """
        string = "InverseKin({:f},{:f},{:f},{:f},{:f},{:f}".format(X, Y, Z, Rx, Ry, Rz)
        params = []
        if user != -1:
            params.append("user={:d}".format(user))
        if tool != -1:
            params.append("tool={:d}".format(tool))
        if useJointNear != -1:
            params.append("useJointNear={:d}".format(useJointNear))
        if JointNear != "":
            params.append("JointNear={:s}".format(JointNear))
        for ii in params:
            string = string + "," + ii
        string = string + ")"
        return self.send(string)

    ###
    def GetAngle(self):
        """
        获取机械臂当前位姿的关节坐标。
        Get the joint coordinates of current posture.
        """
        string = "GetAngle()"
        return self.send(string)

    ###
    def GetPose(self, user=-1, tool=-1):
        """
        获取机械臂当前位姿在指定的坐标系下的笛卡尔坐标。
        可选参数
        参数名 类型 说明
        User string 格式为"user=index"，index为已标定的⽤⼾坐标系索引。
        Tool string 格式为"tool=index"，index为已标定的⽤⼾坐标系索引。
        必须同时传或同时不传，不传时默认为全局⽤⼾和⼯具坐标系。
        Get the Cartesian coordinates of the current posture under the specific coordinate system.
        Optional parameter:
        Parameter name     Type     Description
        User      string     Format: "user=index", index: index of the calibrated user coordinate system.
        Tool     string     Format: "tool=index", index: index of the calibrated tool coordinate system.
        They need to be set or not set at the same time. They are global user coordinate system and global tool coordinate system if not set.
        """
        string = "GetPose("
        params = []
        state = True
        if user != -1:
            params.append("user={:d}".format(user))
            state = not state
        if tool != -1:
            params.append("tool={:d}".format(tool))
            state = not state
        if not state:
            return "need to be set or not set at the same time. They are global user coordinate system and global tool coordinate system if not set"  # 必须同时传或同时不传坐标系，不传时默认为全局⽤⼾和⼯具坐标系

        for i, param in enumerate(params):
            if i == len(params) - 1:
                string = string + param
            else:
                string = string + param + ","

        string = string + ")"
        return self.send(string)

    ###
    def GetErrorID(self):
        """ """
        string = "GetErrorID()"
        return self.send(string)

    ###
    def MovJ(
        self,
        a1,
        b1,
        c1,
        d1,
        e1,
        f1,
        coordinateMode,
        user=-1,
        tool=-1,
        a=-1,
        v=-1,
        cp=-1,
    ):
        """
        描述
        从当前位置以关节运动⽅式运动⾄⽬标点。
        必选参数
        参数名 类型 说明
        P string ⽬标点，⽀持关节变量或位姿变量
        coordinateMode int  目标点的坐标值模式    0为pose方式  1为joint
        可选参数
        参数名 类型 说明
        user int ⽤⼾坐标系
        tool int ⼯具坐标系
        a int 执⾏该条指令时的机械臂运动加速度⽐例。取值范围：(0,100]
        v int 执⾏该条指令时的机械臂运动速度⽐例。取值范围：(0,100]
        cp int 平滑过渡⽐例。取值范围：[0,100]
        Description
        Move from the current position to the target position through joint motion.
        Required parameter:
        Parameter name     Type     Description
        P     string     Target point (joint variables or posture variables)
        coordinateMode     int      Coordinate mode of the target point, 0: pose, 1: joint
        Optional parameter:
        Parameter name     Type     Description
        user     int     user coordinate system
        tool     int     tool coordinate system
        a     int     acceleration rate of the robot arm when executing this command. Range: (0,100].
        v     int     velocity rate of the robot arm when executing this command. Range: (0,100].
        cp     int     continuous path rate. Range: [0,100].
        """
        string = ""
        if coordinateMode == 0:
            string = "MovJ(pose={{{:f},{:f},{:f},{:f},{:f},{:f}}}".format(
                a1, b1, c1, d1, e1, f1
            )
        elif coordinateMode == 1:
            string = "MovJ(joint={{{:f},{:f},{:f},{:f},{:f},{:f}}}".format(
                a1, b1, c1, d1, e1, f1
            )
        else:
            print("coordinateMode param is wrong")
            return ""
        params = []
        if user != -1:
            params.append("user={:d}".format(user))
        if tool != -1:
            params.append("tool={:d}".format(tool))
        if a != -1:
            params.append("a={:d}".format(a))
        if v != -1:
            params.append("v={:d}".format(v))
        if cp != -1:
            params.append("cp={:d}".format(cp))
        for ii in params:
            string = string + "," + ii
        string = string + ")"
        return self.send(string)

    ###
    def MovL(
        self,
        a1,
        b1,
        c1,
        d1,
        e1,
        f1,
        coordinateMode,
        user=-1,
        tool=-1,
        a=-1,
        v=-1,
        speed=-1,
        cp=-1,
        r=-1,
    ):
        """
        描述
        从当前位置以直线运动⽅式运动⾄⽬标点。
        必选参数
        参数名 类型 说明
        P string ⽬标点，⽀持关节变量或位姿变量
        coordinateMode int  目标点的坐标值模式    0为pose方式  1为joint
        可选参数
        参数名 类型  说明
        user int ⽤⼾坐标系
        tool int ⼯具坐标系
        a    int 执⾏该条指令时的机械臂运动加速度⽐例。取值范围：(0,100]
        v    int 执⾏该条指令时的机械臂运动速度⽐例，与speed互斥。取值范围：(0,100]
        speed int 执⾏该条指令时的机械臂运动⽬标速度，与v互斥，若同时存在以speed为
        准。取值范围：[1, 最⼤运动速度]，单位：mm/s
        cp  int 平滑过渡⽐例，与r互斥。取值范围：[0,100]
        r   int 平滑过渡半径，与cp互斥，若同时存在以r为准。单位：mm
        Description
        Move from the current position to the target position in a linear mode.
        Required parameter:
        Parameter name     Type     Description
        P     string     Target point (joint variables or posture variables)
        coordinateMode     int      Coordinate mode of the target point, 0: pose, 1: joint
        Optional parameter:
        Parameter name     Type     Description
        user     int     user coordinate system
        tool     int     tool coordinate system
        a     int     acceleration rate of the robot arm when executing this command. Range: (0,100].
        v     int     velocity rate of the robot arm when executing this command, incompatible with “speed”. Range: (0,100].
        speed     int     target speed of the robot arm when executing this command, incompatible with “v”. If both "speed" and "v” exist, speed takes precedence. Range: [0, maximum motion speed], unit: mm/s.
        cp     int     continuous path rate, incompatible with “r”. Range: [0,100].
        r     int     continuous path radius, incompatible with “cp”. If both "r" and "cp” exist, r takes precedence. Unit: mm.
        """
        string = ""
        if coordinateMode == 0:
            string = "MovL(pose={{{:f},{:f},{:f},{:f},{:f},{:f}}}".format(
                a1, b1, c1, d1, e1, f1
            )
        elif coordinateMode == 1:
            string = "MovL(joint={{{:f},{:f},{:f},{:f},{:f},{:f}}}".format(
                a1, b1, c1, d1, e1, f1
            )
        else:
            print("coordinateMode  param  is wrong")
            return ""
        params = []
        if user != -1:
            params.append("user={:d}".format(user))
        if tool != -1:
            params.append("tool={:d}".format(tool))
        if a != -1:
            params.append("a={:d}".format(a))
        if v != -1 and speed != -1:
            params.append("speed={:d}".format(speed))
        elif speed != -1:
            params.append("speed={:d}".format(speed))
        elif v != -1:
            params.append("v={:d}".format(v))
        if cp != -1 and r != -1:
            params.append("r={:d}".format(r))
        elif r != -1:
            params.append("r={:d}".format(r))
        elif cp != -1:
            params.append("cp={:d}".format(cp))
        for ii in params:
            string = string + "," + ii
        string = string + ")"
        return self.send(string)

    ###
    def GetCurrentCommandID(self):
        """
        获取当前执⾏指令的算法队列ID，可以⽤于判断当前机器⼈执⾏到了哪⼀条指令。
        Get the algorithm queue ID of the currently executed command, which can be used to judge which command is currently being executed by the robot.
        """
        string = "GetCurrentCommandID()"
        return self.send(string)

    ###
    def ParseResultId(self, valueRecv):
        """
        解析Tcp返回值
        Parse the TCP return values
        """
        if (
            valueRecv.find("Not Tcp") != -1
        ):  # 通过返回值判断机器是否处于tcp模式 Judge whether the robot is in TCP mode by the return value
            rlog.warning("Control Mode Is Not Tcp")
            return [1]

        recvData = re.findall(r"-?\d+", valueRecv)
        recvData = [int(num) for num in recvData]
        if len(recvData) > 0:
            if recvData[0] != 0:
                # 根据返回值来判断机器处于什么状态 Judge what status the robot is in based on the return value
                if recvData[0] == -1:
                    rlog.error("Command execution failed")
                elif recvData[0] == -2:
                    rlog.error("The robot is in an error state")
                elif recvData[0] == -3:
                    rlog.info("The robot is in emergency stop state")
                elif recvData[0] == -4:
                    rlog.info("The robot is in power down state")
                else:
                    rlog.info("ErrorId is ", recvData[0])
        else:
            rlog.error("ERROR VALUE")

        return recvData

    ###
    def parse_rsp(self, response: str) -> Dict[str, Union[int, str]]:
        """
        解析机器人命令响应字符串，返回结构化字典

        参数:
            response: 命令响应字符串，格式应为 "ErrorID,{ResultID},CommandString;"
            例如: "0,{1},MovL(P,user,tool,a,v|speed,cp|r);"

        返回:
            包含解析结果的字典，结构:
            {
                "error_id": int,     # 错误码 (如 0)
                "value": str,        # 结果值 (去掉大括号)
                "command": str       # 执行的原始命令 (去掉分号)
            }

        异常:
            ValueError: 当响应格式不符合预期时抛出
        """
        if not isinstance(response, str):
            raise ValueError("响应必须是字符串类型")

        if "Not Tcp" in response:
            return {
                "error_id": ErrorID.NOT_TCP,
                "value": "",  # 去掉ResultID可能的前后空格
                "command": "",  # 去掉命令可能的前后空格
            }

        # 去除字符串两端的空白字符和结尾的分号
        cleaned_response = response.strip().rstrip(";")

        # 更健壮的正则表达式，允许各部分前后有空格
        # pattern = r"^\s*(\d+)\s*,\s*{(.*?)}\s*,\s*(.*)\s*$"
        pattern = r"^\s*(-?\d+)\s*,\s*{(.*?)}\s*,\s*(.*)\s*$"
        match = re.fullmatch(pattern, cleaned_response)

        rlog.info(f"match={match}")

        if not match:
            raise ValueError(
                f"无效的响应格式: '{response}'。预期格式: 'ErrorID,{{ResultID}},Command;'"
            )

        try:
            error_id = int(match.group(1))
        except ValueError:
            raise ValueError(f"ErrorID 必须是整数，得到: '{match.group(1)}'")

        return {
            "error_id": error_id,
            "value": match.group(2).strip(),  # 去掉ResultID可能的前后空格
            "command": match.group(3).strip(),  # 去掉命令可能的前后空格
        }

    ###
    def move(self, target_6dof: list[int | float], e6feed: RobotArmFeedBack):

        is_error = True
        for i in range(0, 3):
            result = self.parse_rsp(self.RobotMode())
            if (result["error_id"] == ErrorID.NO_ERROR) and (
                result["value"] != RobotMode.ERROR
            ):
                rlog.info(f'error_id={result["error_id"]}, value={result["value"]}')
                is_error = False
                break

            if result["error_id"] == ErrorID.NOT_TCP:
                self.RequestControl()
            if result["value"] == RobotMode.ERROR:
                self.ClearError()

            rlog.info(f"e6:: Not TCP mode, or error, try#{i+1} ...")
            sleep(0.5)

        if is_error:
            return result

        # whether targe_6dof is out of range
        result = self.parse_rsp(self.InverseKin(*target_6dof))
        if result["error_id"] != ErrorID.NO_ERROR:
            return result

        joint_angles = [float(x) for x in result["value"].split(",")]

        is_out_of_range = False
        for index, angle in enumerate(joint_angles):
            match index + 1:
                case 1:
                    if (angle < -360) or (angle > 360):
                        is_out_of_range = True
                        break
                case 2:
                    if (angle < -135) or (angle > 135):
                        is_out_of_range = True
                        break
                case 3:
                    if (angle < -154) or (angle > 154):
                        is_out_of_range = True
                        break
                case 4:
                    if (angle < -160) or (angle > 160):
                        is_out_of_range = True
                        break
                case 5:
                    if (angle < -173) or (angle > 173):
                        is_out_of_range = True
                        break
                case 6:
                    if (angle < -360) or (angle > 135):
                        is_out_of_range = True
                        break

                # 2025-03-26， from 越疆伍工
                # 1)轴1: 工作范围-360°到+360°
                # 2)轴2: 工作范围-135°到+135°
                # 3)轴3: 工作范围-154°到+154
                # 4)轴4: 工作范围-160°到+160°
                # 5)轴5: 工作范围-173°到+173°
                # 6)轴6: 工作范围-360°到+360°
        if is_out_of_range:
            return {
                "error_id": ErrorID.OUT_OF_RANGE,
                "value": f"joint{index+1}, {angle} degree",
                "command": "InverseKin()",
            }
        rlog.info(f"joint_angles={joint_angles}")

        #
        # MovL = MovJ， coordinateMode=1(joint),=0(pose)
        # result = self.parse_rsp(self.MovL(*target_6dof, coordinateMode=1))
        result = self.parse_rsp(self.MovJ(*joint_angles, coordinateMode=1))
        if result["error_id"] != ErrorID.NO_ERROR:
            return result

        command_id = int(result["value"])
        for i in range(0, 5):
            if (command_id <= e6feed.feeds.CurrentCommandId) and (
                e6feed.feeds.robotMode == RobotMode.ENABLE
            ):
                rlog.info(f"e6:: move to {target_6dof}, done.")
                result["error_id"] = ErrorID.NO_ERROR
                break

            sleep(0.5)
            rlog.info(f"e6:: moving to {target_6dof}, try#{i+1}")
            result["error_id"] = ErrorID.NOT_FULFIL

        return result


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


###-----------------------------------------------------------------------------
def main():

    # read configuration file
    try:
        rae6cfg.load()
    except Exception as e:
        rlog.exception(f"read {rae6cfg.fn} error: {e}")
        # to be set default value

    rlog.info(
        f"6dof_txt_file_path = {rae6cfg.s6dof_fpath}\ncheck_file_every_seconds_when_idle = {rae6cfg.sleep_secs} second(s)"
    )

    # connect to e6
    rlog.info("connecting ...")
    e6rarm = RobotArmDashBoard(ip="192.168.5.1", port=29999)
    e6feed = RobotArmFeedBack(ip="192.168.5.1", port=30004)
    e6rarm.connect()
    e6feed.connect()
    rlog.info("e6:: initialized and connected.")

    #
    is_error = True
    for i in range(0, 3):
        result = e6rarm.parse_rsp(e6rarm.RobotMode())
        # robot_mode = int(result["value"])
        rlog.info("==========")
        rlog.info(f"result={result}")
        rlog.info(f"RobotMode.ERROR={RobotMode.ERROR}")
        rlog.info(
            f'result["error_id"] == ErrorID.NO_ERROR:{result["error_id"] == ErrorID.NO_ERROR}'
        )
        rlog.info(
            f'result["value"] != RobotMode.ERROR:{int(result["value"]) != RobotMode.ERROR}'
        )

        if (result["error_id"] == ErrorID.NO_ERROR) and (
            int(result["value"]) != RobotMode.ERROR
        ):
            rlog.info(f'error_id={result["error_id"]}, value={result["value"]}')
            is_error = False
            break

        if result["error_id"] == ErrorID.NOT_TCP:
            e6rarm.RequestControl()
        if int(result["value"]) == RobotMode.ERROR:
            e6rarm.ClearError()

        rlog.info(f"e6:: Not TCP mode, or error, try#{i+1} ...")
        sleep(0.5)

    if is_error:
        return result

    #
    result = e6rarm.parse_rsp(e6rarm.EnableRobot())
    if result["error_id"] != ErrorID.NO_ERROR:
        rlog.error(f"e6:: EnableRobot() error : {result}")
        return result
    else:
        rlog.info("e6:: enabled.")
    # result = e6rarm.EnableRobot()
    # if e6rarm.ParseResultId(result)[0] != 0:
    #     rlog.error(
    #         "e6:: EnableRobot() error. Check whether port 29999 is avaliable and try again."
    #     )
    #     return
    # else:
    #     rlog.info("e6:: enabled.")

    # e6feed.get_feeds()
    # return

    # get real-time feeds from e6, in thread
    feeds_thread = threading.Thread(target=e6feed.get_feeds)
    feeds_thread.daemon = True
    feeds_thread.start()

    #
    message_interval = rae6cfg.sleep_secs
    nf_count = 0
    dest_dir = Path(rae6cfg.s6dof_fpath) / "moved"
    dest_dir.mkdir(parents=True, exist_ok=True)

    while True:

        flist = find_6dof_file(rae6cfg.s6dof_fpath)
        if flist:
            rlog.info(
                f"{len(flist)} files(s) found: {', '.join([file.name for file in flist])}"
            )

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
                # dest_6dof = dict(zip(KEY_6DOF, floats))
                # print(f"floats={floats},dest_6dof={dest_6dof}")
                # error_id = rae6.movl(dest_6dof)
                # if error_id == StatusCode.SUCCESS.value:
                #     __, current_6dof = rae6.get_pose()
                #     rlog.info(f"move successs, current 6dof = {current_6dof}")
                #     fn += "_" + datetime.now().strftime("%Y%m%d%H%M%S") + "_moved"
                # else:
                #     rlog.warning(f"e6 :: {rae6.get_status_message(error_id)}.\n")
                #     fn += "_" + datetime.now().strftime("%Y%m%d%H%M%S") + "_robot_error"
                target_6dof = floats
                result = e6rarm.move(target_6dof, e6feed)
                if result["error_id"] != ErrorID.NO_ERROR:
                    rlog.error(
                        f"e6:: error occured while try move to {target_6dof} : {result}"
                    )
                    fn += "_" + datetime.now().strftime("%Y%m%d%H%M%S") + "_robot_error"
                else:
                    rlog.info(f"e6:: move to {target_6dof}, done!")
                    fn += "_" + datetime.now().strftime("%Y%m%d%H%M%S") + "_moved"

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

        rlog.info(
            f">>> feeds <<< mode[{e6feed.feeds.robotMode}], cmdid[{e6feed.feeds.CurrentCommandId}], enable[{e6feed.feeds.EnableStatus}], run[{e6feed.feeds.RunningStatus}], err[{e6feed.feeds.ErrorStatus}]\n6dof{e6feed.feeds.ToolVectorActual}"
        )
        # 6dof_t{e6feed.feeds.ToolVectorTarget},

        # sleep(2)

    return


###-----------------------------------------------------------------------------
if __name__ == "__main__":
    main()


"""
ClearError()
    ErrorID,{},ClearError();
    
DisableRobot()
    ErrorID,{},DisableRobot();

EnableRobot(load,centerX,centerY,centerZ,isCheck)
    ErrorID,{},EnableRobot(load,centerX,centerY,centerZ,isCheck);

RequestControl 
    ErrorID,{},RequestControl();

========

GetErrorID()
    ErrorID,{[[id,...,id], [id], [id], [id], [id], [id], [id]]},GetErrorID();
    
GetPose(user,tool)
    ErrorID,{X,Y,Z,Rx,Ry,Rz},GetPose(user,tool);
    
InverseKin(X,Y,Z,Rx,Ry,Rz,useJointNear,jointNear,user,tool)
    ErrorID,{J1,J2,J3,J4,J5,J6},InverseKin(X,Y,Z,Rx,Ry,Rz,useJointNear,jointNear,user,tool);

RobotMode()
    ErrorID,{Value},RobotMode();

========
MovL(P,user,tool,a,v|speed,cp|r)
    ErrorID,{ResultID},MovL(P,user,tool,a,v|speed,cp|r)
"""

# 安全原点：0， -150， 615.2， -90,0,180
# 0,0,0,0,0,0

# -16, -150, 614, -90, 6, 177
# J: 0, 0, 0, 7, 3, 0

# -30,-160,500,-80,10,100
