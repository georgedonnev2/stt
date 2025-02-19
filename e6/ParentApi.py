import socket



class FeedbackInstance:
    def __init__(self):
        self.MessageSize = 0
        self.DigitalInputs = 0
        self.DigitalOutputs = 0
        self.RobotMode = 0
        self.TimeStamp = 0
        self.RunTime = 0
        self.TestValue = 0
        self.SpeedScaling = float()
        self.LinearMomentumNorm = float()
        self.VMain = float()
        self.VRobot = float()
        self.IRobot = float()
        self.ProgramState = 0
        self.SafetyStatus = 0
        self.ToolAcceleroMeter = []
        self.ElbowPosition = []
        self.ElbowVelocity = []
        self.QTarget = []
        self.QDTarget = []
        self.QDDTarget = []
        self.ITarget = []
        self.MTarget = []
        self.QActual = []
        self.QDActual = []
        self.IActual = []
        self.ActualTCPForce = []
        self.ToolVectorActual = []
        self.TCPSpeedActual = []
        self.TCPForce = []
        self.ToolVectorTarget = []
        self.TCPSpeedTarget = []
        self.MotorTemperatures = []
        self.JointModes = []
        self.VActual = []
        self.HandType = []
        self.User = ''
        self.Tool = ''
        self.RunQueuedCmd = ''
        self.PauseCmdFlag = ''
        self.VelocityRatio = ''
        self.AccelerationRatio = ''
        self.JerkRatio = ''
        self.XYZVelocityRatio = ''
        self.RVelocityRatio = ''
        self.XYZAccelerationRatio = ''
        self.RAccelerationRatio = ''
        self.XYZJerkRatio = ''
        self.RJerkRatio = ''
        self.BrakeStatus = ''
        self.EnableStatus = ''
        self.DragStatus = ''
        self.RunningStatus = ''
        self.ErrorStatus = ''
        self.JogStatusCR = ''
        self.CRRobotType = ''
        self.DragButtonSignal = ''
        self.EnableButtonSignal = ''
        self.RecordButtonSignal = ''
        self.ReappearButtonSignal = ''
        self.JawButtonSignal = ''
        self.SixForceOnline = ''
        self.CollisionState = ''
        self.ArmApproachState = ''
        self.J4ApproachState = ''
        self.J5ApproachState = ''
        self.J6ApproachState = ''
        self.MActual = []
        self.Load = float()
        self.CenterX = float()
        self.CenterY = float()
        self.CenterZ = float()
        self.Users = []
        self.Tools = []
        self.TraceIndex = float()
        self.SixForceValue = []
        self.TargetQuaternion = []
        self.ActualQuaternion = []

        

class ClassReply(object):
    def __init__(self):
        self.errorID = 0
        self.commandID = 0
        self.commandStr = ""
class DobotApi(object):
    def __init__(self):
        self.ip = "0"
        self.port = 0
        self.socket_dobot = 0
        self.index = float()

    def connectDobot(self, ip, port):
        self.ip = ip
        self.port = port
        if self.port == 29999 or self.port == 30003 or self.port == 30004 or self.port == 30005 or self.port == 30006:
            try:
                self.socket_dobot = socket.socket()
                self.socket_dobot.settimeout(30.0)
                self.socket_dobot.connect((self.ip, self.port))
            except socket.error:
                print(socket.error)
                raise Exception(
                    "Unable to set socket connection use port {} !".format(self.port), socket.error)
        else:
            raise Exception(
                "Connect to dashboard server need use port {} !".format(self.port))

    def log(self, text):
        print(text)

    def send_data(self, string):
        self.log("Send to {}:{}: {}".format(self.ip, self.port, string))
        self.socket_dobot.send(str.encode(string, 'utf-8'))

    def wait_reply(self):
        """
        Read the return value
        """
        data = self.socket_dobot.recv(1024)
        data_str = str(data, encoding="utf-8")
        print('Receive from {}:{}: {}'.format(self.ip, self.port, data_str))
        results = data_str.split(",")
        error_code = int(results[0])
        return error_code


    def wait_classRely(self):
        """
        Read the return value
        """
        classReply = ClassReply()
        data = self.socket_dobot.recv(1024)
        data_str = str(data, encoding="utf-8")
        print('Receive from {}:{}: {}'.format(self.ip, self.port, data_str))
        results = data_str.split(",")
        classReply.errorID = int(results[0])
        classReply.commandStr = results[2]
        strCommandID = results[1][1:-1]
        print(">>> strCommndID: %s" % strCommandID)
        if strCommandID != None:
            classReply.commandID = int(strCommandID)
        print(">>> classReply.commandStr: %s" % classReply.commandStr)
        return classReply

    def wait_classRely_getCommandID(self):
        """
        Read the return value
        """
        classReply = ClassReply()
        data = self.socket_dobot.recv(1024)
        data_str = str(data, encoding="utf-8")
        print('Receive from {}:{}: {}'.format(self.ip, self.port, data_str))
        results = data_str.split(",")
        classReply.errorID = int(results[0])
        classReply.commandStr = results[2]
        strCommandID = results[1][1:-1]
        # print('Receive errorID {} strCommandID '+strCommandID)
        if classReply.errorID == None or classReply.errorID != 0:
            classReply.commandID = -1
        else:
            classReply.commandID = int(strCommandID)
        # print('Receive errorID {} '.format(classReply.errorID))
        # print('Receive commandID {} '.format(classReply.commandID))
        # print(classReply.commandStr)
        return classReply

    def close(self):
        """
        Close the port
        """
        if self.socket_dobot != 0:
            self.socket_dobot.close()

    def feed_back(self):
        has_read = 0
        data = bytes()
        while True:
            while has_read < 1440:
                try:
                    temp = self.socket_dobot.recv(1440 - has_read)
                except socket.timeout as e:
                    print("time out！")
                    continue
                if len(temp) > 0:
                    has_read += len(temp)
                    data += temp

            iMsgSize = data[1]
            iMsgSize <<= 8
            iMsgSize |= data[0]
            iMsgSize &= 0x00FFFF
            if 1440 != iMsgSize:
                print(iMsgSize)
                print("1440 != iMsgSize")
            checkValue = int.from_bytes(data[48:56], 'little')
            print(checkValue)
            if 0x0123456789ABCDEF != checkValue:
                print("校验失败")
                has_read = 0
                continue
            return data


    def __del__(self):
        self.close()
