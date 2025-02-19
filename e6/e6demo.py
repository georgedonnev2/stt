import logging
import re
import socket
import threading
import time


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

    ###
    def connect(self):

        #
        if self.__port_req != 29999:
            raise ValueError(
                f"wrong port {self.__port_req}, 29999 expected to send command."
            )
        if (self.__port_rsp != 30004) and (self.__port_rsp != 30005):
            raise ValueError(
                f"wrong port {self.__port_req}, 30004 or 30005 expected to get response."
            )

        try:
            for portx, sktx in {
                self.__port_req: self.__socket_req,
                self.__port_rsp: self.__socket_rsp,
            }.items():
                sktx = socket.socket()
                sktx.connect((self.__ip, portx))
                sktx.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 144000)
                # any timeout?
        except socket.error:
            logging.error(f"socket error, ip={self.__ip}, port={portx}")
            raise OSError(f"socket error, ip={self.__ip}, port={portx}")

        return

    ###
    def send(self, string):
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

        recvData = self.__reply()
        self.get_rsp_id(recvData)

        return recvData

    ###
    def __reply(self):
        """
        Read the return value
        """
        data = ""
        try:
            data = self.socket_rsp.recv(1024)
        except Exception as e:
            print(e)
            self.socket_rsp = self.reConnect(self.ip, self.port_rsp)

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
            print("Control Mode Is Not Tcp")
            return
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

    def wait_reply(self):
        """
        Read the return value
        """
        data = ""
        try:
            data = self.socket_dobot.recv(1024)
        except Exception as e:
            print(e)
            self.socket_dobot = self.reConnect(self.ip, self.port)

        finally:
            if len(data) == 0:
                data_str = data
            else:
                data_str = str(data, encoding="utf-8")
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
            self.ParseResultId(recvData)
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
def socket_req(self):
    return self.__socket_req


@property
def socket_rsp(self):
    return self.__socket_rsp


### --------------------------------------------------------------------------------------------------------------------
###
### --------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    robot_e6 = DobotRbt()

    robot_e6.connect()

    result = robot_e6.send("RobotMode()")
    print(f"robot::RobotMode()={result}")

    # result = robot_e6.sendRecvMsg("RobotMode()")
    # print(f"robot::RobotMode()={result}")
