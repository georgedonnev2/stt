#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import chardet
import logging
import re
import socket
import threading
import time
from time import sleep

logging.basicConfig(level=logging.INFO)


### --------------------------------------------------------------------------------------------------------------------
###
### --------------------------------------------------------------------------------------------------------------------
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
            logging.error("socket error : {e}")
            while True:
                try:
                    self.__socket_req = self.reConnect(self.__ip, self.__port_req)
                    self.__socket_req.send(str.encode(string, "utf-8"))
                    break
                except Exception:
                    time.sleep(1)

        recvData = self.__reply()
        # logging.info(f"rsp={self.get_rsp_id(recvData)}")
        logging.info(f"recvData={recvData}")
        self.get_rsp_id(recvData)
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
        """
        解析Tcp返回值
        Parse the TCP return values
        """
        if (
            valueRecv.find("Not Tcp") != -1
        ):  # 通过返回值判断机器是否处于tcp模式 Judge whether the robot is in TCP mode by the return value
            logging.warning("Control Mode Is Not Tcp")
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

    def get_rsp(self, valueRecv):
        # 解析返回值，确保机器人在 TCP 控制模式
        if "Not Tcp" in valueRecv:
            print("Control Mode Is Not Tcp")
            return [1]
        return [int(num) for num in re.findall(r"-?\d+", valueRecv)] or [2]

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

if __name__ == "__main__":

    # print("demo")
    # e6 = DobotDemo("192.168.5.1")
    # e6.start()
    # print("done.")

    logging.info("e6::initialize.")
    robot_e6 = DobotRbt()
    logging.info("e6::initialized.")

    logging.info("e6::connect to robot.")
    robot_e6.connect()
    logging.info("e6::connected.")

    logging.info(
        f"e6::ip={robot_e6.ip}, socket_req={robot_e6.socket_req}, socket_rsp={robot_e6.socket_rsp}"
    )

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

    command = "GetPose()"
    result = robot_e6.send(command)
    print(f"robot::{command}={result}")
    # {-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754}

    command = "RobotMode()"
    result = robot_e6.send(command)
    print(f"robot::{command}={result}")

    for count in range(1, 10):
        print(f"move #{count}")
        command = "MovL(pose={-130.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
        result = robot_e6.send(command)
        print(f"robot::{command}={result}")

        command = "MovL(pose={-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
        result = robot_e6.send(command)
        print(f"robot::{command}={result}")

    command = "MovL(pose={-130.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    result = robot_e6.send(command)
    print(f"robot::{command}={result}")
    sleep(5)
    command = "MovL(pose={-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    result = robot_e6.send(command)
    print(f"robot::{command}={result}")
    # # sleep(5)

    # command = "MovL(pose={-130.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # # sleep(5)
    # command = "MovL(pose={-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # # sleep(5)

    # command = "MovL(pose={-130.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # # sleep(5)
    # command = "MovL(pose={-116.8459,-160.3124,323.4898,-178.1977,21.9300,-76.1754})"
    # result = robot_e6.send(command)
    # print(f"robot::{command}={result}")
    # # sleep(5)

    command = "RobotMode()"
    result = robot_e6.send(command)
    print(f"robot::{command}={result}")

    sleep(5)
    command = "RobotMode()"
    result = robot_e6.send(command)
    print(f"robot::{command}={result}")
    # result = robot_e6.sendRecvMsg("RobotMode()")
    # print(f"robot::RobotMode()={result}")
