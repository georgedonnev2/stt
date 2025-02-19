import time
import threading
import numpy as np
from PythonTCPProtocol.ParentApi import *
# from ParentApi import *


# 导入枚举类
from enum import Enum
# 继承枚举类
class RobotModeEnum(Enum):
    ROBOT_MODE_INIT = 1
    ROBOT_MODE_BRAKE_OPEN = 2
    ROBOT_MODE_POWEROFF = 3
    ROBOT_MODE_DISABLED = 4
    ROBOT_MODE_ENABLE = 5
    ROBOT_MODE_BACKDRIVE = 6
    ROBOT_MODE_RUNNING = 7
    ROBOT_MODE_SINGLE_MOVE = 8
    ROBOT_MODE_ERROR = 9
    ROBOT_MODE_PAUSE = 10
    ROBOT_MODE_COLLISION = 11

# Port Feedback
MyType = np.dtype([(
    'MessageSize',
    np.uint16,
), (
    'save1',
    np.uint16,
), (
    'save2',
    np.uint16,
), (
    'save3',
    np.uint16,
), (
    'digital_input_bits',
    np.uint64,
), (
    'digital_output_bits',
    np.uint64,
), (
    'robot_mode',
    np.uint64,
), (
    'time_stamp',
    np.uint64,
), (
    'run_time',
    np.uint64,
), (
    'test_value',
    np.uint64,
), (
    'test_value_1',
    np.float64,
), (
    'speed_scaling',
    np.float64,
), (
    'linear_momentum_norm',
    np.float64,
), (
    'v_main',
    np.float64,
), (
    'v_robot',
    np.float64,
), (
    'i_robot',
    np.float64,
), (
    'ProgramState',
    np.float64,
), (
    'SafetyStatus',
    np.float64,
), ('ToolAcceleroMeter', np.float64, (3,)),
    ('elbow_position', np.float64, (3,)),
    ('elbow_velocity', np.float64, (3,)),
    ('q_target', np.float64, (6,)),
    ('qd_target', np.float64, (6,)),
    ('qdd_target', np.float64, (6,)),
    ('i_target', np.float64, (6,)),
    ('m_target', np.float64, (6,)),
    ('q_actual', np.float64, (6,)),
    ('qd_actual', np.float64, (6,)),
    ('i_actual', np.float64, (6,)),
    ('actual_TCP_force', np.float64, (6,)),
    ('tool_vector_actual', np.float64, (6,)),
    ('TCP_speed_actual', np.float64, (6,)),
    ('TCP_force', np.float64, (6,)),
    ('Tool_vector_target', np.float64, (6,)),
    ('TCP_speed_target', np.float64, (6,)),
    ('motor_temperatures', np.float64, (6,)),
    ('joint_modes', np.float64, (6,)),
    ('v_actual', np.float64, (6,)),
    # ('dummy', np.float64, (9, 6))])
    ('hand_type', np.byte, (4,)),
    ('user', np.byte,),
    ('tool', np.byte,),
    ('run_queued_cmd', np.byte,),
    ('pause_cmd_flag', np.byte,),
    ('velocity_ratio', np.byte,),
    ('acceleration_ratio', np.byte,),
    ('jerk_ratio', np.byte,),
    ('xyz_velocity_ratio', np.byte,),
    ('r_velocity_ratio', np.byte,),
    ('xyz_acceleration_ratio', np.byte,),
    ('r_acceleration_ratio', np.byte,),
    ('xyz_jerk_ratio', np.byte,),
    ('r_jerk_ratio', np.byte,),
    ('brake_status', np.byte,),
    ('enable_status', np.byte,),
    ('drag_status', np.byte,),
    ('running_status', np.byte,),
    ('error_status', np.byte,),
    ('JogStatusCR', np.byte,),
    ('CRRobotType', np.byte,),
    ('drag_button_signal', np.byte,),
    ('enable_button_signal', np.byte,),
    ('record_button_signal', np.byte,),
    ('reappear_button_signal', np.byte,),
    ('jaw_button_signal', np.byte,),
    ('six_force_online', np.byte,),
    ('CollisionState', np.byte,),
    ('ArmApproachState', np.byte,),
    ('J4ApproachState', np.byte,),
    ('J5ApproachState', np.byte,),
    ('J6ApproachState', np.byte,),
    ('reserve2', np.byte, (77,)),
    ('m_actual', np.float64, (6,)),
    ('load', np.float64,),
    ('center_x', np.float64,),
    ('center_y', np.float64,),
    ('center_z', np.float64,),
    ('user[6]', np.float64, (6,)),
    ('tool[6]', np.float64, (6,)),
    ('trace_index', np.float64,),
    ('six_force_value', np.float64, (6,)),
    ('target_quaternion', np.float64, (4,)),
    ('actual_quaternion', np.float64, (4,)),
    ('AutoManualMode', np.byte,),
    ('VibrationDisZ', np.float64,),
    ('reserve3', np.byte, (15,))])


class FeedbackV4Instance:
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
        self.ProgramState = float()
        self.SafetyStatus = float()
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
        self.AutoManualMode = ''
        self.VibrationDisZ = float()


dobotapi = DobotApi()
feedback30004 = DobotApi()
feedback30005 = DobotApi()



def ConnectDobot(ip):
    dobotapi.connectDobot(ip, 29999)
    feedback30004.connectDobot(ip, 30004)
    feedback30005.connectDobot(ip, 30005)


def waitForReplySync(isSync):
    classReply = dobotapi.wait_classRely()
    if not isSync or classReply.errorID != 0:
        return classReply.errorID
    while True:
        if GetCurrentCommandID() >= classReply.commandID and RobotModeClassReply().commandID != RobotModeEnum.ROBOT_MODE_RUNNING.value:
            break
        else:
            time.sleep(0.5)
    return classReply.errorID

def EnableRobot():
    dobotapi.send_data("EnableRobot()")
    return dobotapi.wait_reply()


def DisableRobot():
    dobotapi.send_data("DisableRobot()")
    return dobotapi.wait_reply()


def ClearError():
    dobotapi.send_data("ClearError()")
    return dobotapi.wait_reply()


def StopRobot():
    dobotapi.send_data("StopRobot()")
    return dobotapi.wait_reply()


def SpeedFactor(ratio):
    dobotapi.send_data("SpeedFactor(" + str(ratio) + ")")
    return dobotapi.wait_reply()


def User(index):
    dobotapi.send_data("User(" + str(index) + ")")
    return dobotapi.wait_reply()


def Tool(index):
    dobotapi.send_data("Tool(" + str(index) + ")")
    return dobotapi.wait_reply()


def RobotMode():
    dobotapi.send_data("RobotMode()")
    return dobotapi.wait_reply()

def RobotModeClassReply():
    dobotapi.send_data("RobotMode()")
    classReply = dobotapi.wait_classRely()
    return classReply


def SetPayload(load, inertia=None):
    data = "SetPayload(" + str(load)
    if inertia != None:
        data = data + "," + str(inertia)
    data = data + ")"
    dobotapi.send_data(data)
    return dobotapi.wait_reply()


def DO(index, status):
    dobotapi.send_data("DO(" + str(index) + "," + str(status) + ")")
    return dobotapi.wait_reply()


def DOInstant(index, status):
    dobotapi.send_data("DOInstant(" + str(index) + "," + str(status) + ")")
    return dobotapi.wait_reply()


def AO(index, status):
    dobotapi.send_data("AO(" + str(index) + "," + str(status) + ")")
    return dobotapi.wait_reply()


def AOInstant(index, status):
    dobotapi.send_data("AOInstant(" + str(index) + "," + str(status) + ")")
    return dobotapi.wait_reply()


def AccJ(R):
    dobotapi.send_data("AccJ(" + str(R) + ")")
    return dobotapi.wait_reply()


def AccL(R):
    dobotapi.send_data("AccL(" + str(R) + ")")
    return dobotapi.wait_reply()


def VelJ(R):
    dobotapi.send_data("VelJ(" + str(R) + ")")
    return dobotapi.wait_reply()


def VelL(R):
    dobotapi.send_data("VelL(" + str(R) + ")")
    return dobotapi.wait_reply()


def CP(R):
    dobotapi.send_data("CP(" + str(R) + ")")
    return dobotapi.wait_reply()


def PowerOn(R):
    dobotapi.send_data("PowerOn()")
    return dobotapi.wait_reply()


def RunScript(projectName):
    dobotapi.send_data("RunScript(" + projectName + ")")
    return dobotapi.wait_reply()


def StopScript():
    dobotapi.send_data("StopScript()")
    return dobotapi.wait_reply()


def PauseScript():
    dobotapi.send_data("PauseScript()")
    return dobotapi.wait_reply()


def ContinueScript():
    dobotapi.send_data("ContinueScript()")
    return dobotapi.wait_reply()


def PositiveKin(x, y, z, r, User=None, Tool=None):
    EnableRobot()
    string = "PositiveKin(" + str(x) + "," + str(y) + "," + str(z) + "," + str(r) + ""
    if User != None:
        string = string + ",User=" + str(User)
    if Tool != None:
        string = string + ",Tool=" + str(Tool)
    string = string + ")"
    print(string)
    dobotapi.send_data(string)
    result = dobotapi.wait_reply()
    return result


def SetCollisionLevel(level):
    dobotapi.send_data("SetCollisionLevel(" + str(level) + ")")
    return dobotapi.wait_reply()


def GetAngle():
    dobotapi.send_data("GetAngle()")
    return dobotapi.wait_reply()


def GetPose():
    dobotapi.send_data("GetPose()")
    return dobotapi.wait_reply()


def EmergencyStop():
    dobotapi.send_data("EmergencyStop()")
    return dobotapi.wait_reply()


def ModbusCreate(ip, port, slave_id, isRTU=None):
    if isRTU is None:
        dobotapi.send_data("ModbusCreate(" + ip + "," + str(port) + "," + str(slave_id) + ")")
    else:
        dobotapi.send_data("ModbusCreate(" + ip + "," + str(port) + "," + str(slave_id) + "," + str(isRTU) + ")")
    return dobotapi.wait_reply()


def ModbusClose(index):
    dobotapi.send_data("ModbusClose(" + str(index) + ")")
    return dobotapi.wait_reply()


def GetInBits(index, addr, count):
    dobotapi.send_data("GetInBits(" + str(index) + "," + str(addr) + "," + str(count) + ")")
    return dobotapi.wait_reply()


def GetInRegs(index, addr, count, valType=None):
    data = "GetInRegs(" + str(index) + "," + str(addr) + "," + str(count)
    if valType != None:
        data = data + "," + valType
    data = data + ")"
    dobotapi.send_data(data)
    return dobotapi.wait_reply()


def GetCoils(index, addr, count):
    dobotapi.send_data("GetCoils(" + str(index) + "," + str(addr) + "," + str(count) + ")")
    return dobotapi.wait_reply()


def SetCoils(index, addr, count, valTab):
    dobotapi.send_data("SetCoils(" + str(index) + "," + str(addr) + "," + str(count) + "," + str(valTab) + ")")
    return dobotapi.wait_reply()


def GetHoldRegs(index, addr, count, valType=None):
    data = "GetHoldRegs(" + str(index) + "," + str(addr) + "," + str(count)
    if valType != None:
        data = str + "," + valType
    data = data + ")"
    dobotapi.send_data(data)
    return dobotapi.wait_reply()


def SetHoldRegs(index, addr, count, valTab, valType=None):
    data = "SetHoldRegs(" + str(index) + "," + str(addr) + "," + str(count) + "," + valTab
    if valType != None:
        data = data + "," + valType
    data = data + ")"
    dobotapi.send_data(data)
    return dobotapi.wait_reply()


def GetErrorID():
    dobotapi.send_data("GetErrorID()")
    return dobotapi.wait_reply()


def DI(index):
    dobotapi.send_data("DI(" + str(index) + ")")
    return dobotapi.wait_reply()


def ToolDI(index):
    dobotapi.send_data("ToolDI(" + str(index) + ")")
    return dobotapi.wait_reply()


def AI(index):
    dobotapi.send_data("AI(" + str(index) + ")")
    return dobotapi.wait_reply()


def ToolAI(index):
    dobotapi.send_data("ToolAI(" + str(index) + ")")
    return dobotapi.wait_reply()


def BrakeControl(axisID, value):
    dobotapi.send_data("BrakeControl(" + str(axisID) + "," + str(value) + ")")
    return dobotapi.wait_reply()


def StartDrag():
    dobotapi.send_data("StartDrag()")
    return dobotapi.wait_reply()


def StopDrag():
    dobotapi.send_data("StopDrag()")
    return dobotapi.wait_reply()


def BrakeControl(index, value):
    dobotapi.send_data("BrakeControl(" + str(index) + "," + str(value) + ")")
    return dobotapi.wait_reply()


def GetDO(index):
    dobotapi.send_data("GetDO(" + str(index) + ")")
    return dobotapi.wait_reply()


def GetAO(index):
    dobotapi.send_data("GetAO(" + str(index) + ")")
    return dobotapi.wait_reply()


def GetToolDO(index):
    dobotapi.send_data("GetToolDO(" + str(index) + ")")
    return dobotapi.wait_reply()


def SetTool485(baudrate, parity=None, stop=None, identify=None):
    data = "SetTool485(" + str(baudrate)
    if parity is not None:
        data = data + "," + parity
    if stop is not None:
        data = data + "," + str(stop)
    if identify is not None:
        data = data + "," + str(identify)
    data = data + ")"
    dobotapi.send_data(data)
    return dobotapi.wait_reply()

def SetSafeWallEnable(index, value):
    dobotapi.send_data("SetSafeWallEnable(" + str(index) + "," + str(value) + ")")
    return dobotapi.wait_reply()


def MovJ(pose=[], joint=[], user=None, tool=None, a=None, v=None, cp=None, isSync=True):
    # if len(pose) != 6 and len(joint) != 6:
    #     print("Please enter the correct point value")
    #     return None
    # if len(pose) == 6 and len(joint) == 6:
    #     print("Cartesian coordinate values and joint values cannot exist at the same time")
    #     return None
    string = "MovJ("
    if len(pose) == 6:
        string = string +"pose= { "+str(pose[0])+","+str(pose[1])+","+str(pose[2])+","+str(pose[3])+","+str(pose[4])+","+str(pose[5])
    if len(joint) == 6:
        string = string +"joint= { "+str(joint[0])+","+str(joint[1])+","+str(joint[2])+","+str(joint[3])+","+str(joint[4])+","+str(joint[5])
    string = string + "}"
    if user  != None:
        string = string + ",user ="+str(user)
    if tool  != None:
        string = string + ",tool ="+str(tool)
    if a != None:
        string = string +",a="+str(a)
    if v != None:
        string = string + ",v="+str(v)
    if cp != None:
        string = string + ",cp=" + str(cp)
    string = string +")"
    dobotapi.send_data(string)
    return waitForReplySync(isSync)



def MovL(pose=[], joint=[], user=None, tool=None, a=None, v=None, cp=None, isSync=True):
    # if len(pose) != 6 and len(joint) != 6:
    #     print("Please enter the correct point value")
    #     return None
    # if len(pose) == 6 and len(joint) == 6:
    #     print("Cartesian coordinate values and joint values cannot exist at the same time")
    #     return None
    string = "MovL("
    if len(pose) == 6:
        string = string +"pose= { "+str(pose[0])+","+str(pose[1])+","+str(pose[2])+","+str(pose[3])+","+str(pose[4])+","+str(pose[5])
    if len(joint) == 6:
        string = string +"joint= { "+str(joint[0])+","+str(joint[1])+","+str(joint[2])+","+str(joint[3])+","+str(joint[4])+","+str(joint[5])
    string = string + "}"
    if user  != None:
        string = string + ",user ="+str(user)
    if tool  != None:
        string = string + ",tool ="+str(tool)
    if a != None:
        string = string +",a="+str(a)
    if v != None:
        string = string + ",v="+str(v)
    if cp != None:
        string = string + ",cp=" + str(cp)
    string = string +")"
    dobotapi.send_data(string)
    return waitForReplySync(isSync)

def Arc(pose1=[], joint1=[],pose2=[], joint2=[],user=None,tool=None, a=None, v=None, cp=None,ori_mode=None,isSync=True):
    data = "MovL("
    if len(pose1) == 6 and len(pose2) == 6:
        data = data + "pose= { " + str(pose1[0]) + "," + str(pose1[1]) + "," \
               + str(pose1[2]) + "," + str(pose1[3]) + "," + str(pose1[4]) + "," \
               + str(pose1[5])+ "},pose = {"+str(pose2[0]) + "," + str(pose2[1]) + "," \
               + str(pose2[2]) + "," + str(pose2[3]) + "," + str(pose2[4]) + "," + str(pose2[5])+ "}"
    elif len(joint1) == 6 and len(joint2) == 6:
        data = data + "joint= { " + str(joint1[0]) + "," + str(joint1[1]) + "," \
               + str(joint1[2]) + "," + str(joint1[3]) + "," + str(joint1[4]) + "," \
               + str(joint1[5])+ "},joint = {"+str(joint2[0]) + "," + str(joint2[1]) + "," \
               + str(joint2[2]) + "," + str(joint2[3]) + "," + str(joint2[4]) + "," + str(joint2[5])+ "}"
    else:
        print("Please enter the correct point")

    if user  != None:
        data = data + ",user ="+str(user)
    if tool  != None:
        data = data + ",tool ="+str(tool)
    if a != None:
        data = data +",a="+str(a)
    if v != None:
        data = data + ",v="+str(v)
    if cp != None:
        data = data + ",cp=" + str(cp)
    if ori_mode != None:
        data = data + ",ori_mode=" + str(ori_mode)
    data = data +")"

    dobotapi.send_data(data)
    return waitForReplySync(isSync)


def Circle(pose1=[], joint1=[],pose2=[], joint2=[],count=None,user=None,tool=None, a=None, v=None, cp=None,isSync=True):
    data = "Circle("
    if len(pose1) == 6 and len(pose2) == 6:
        data = data + "pose= { " + str(pose1[0]) + "," + str(pose1[1]) + "," \
               + str(pose1[2]) + "," + str(pose1[3]) + "," + str(pose1[4]) + "," \
               + str(pose1[5])+ "},pose = {"+str(pose2[0]) + "," + str(pose2[1]) + "," \
               + str(pose2[2]) + "," + str(pose2[3]) + "," + str(pose2[4]) + "," + str(pose2[5])+ "}"
    elif len(joint1) == 6 and len(joint2) == 6:
        data = data + "joint= { " + str(joint1[0]) + "," + str(joint1[1]) + "," \
               + str(joint1[2]) + "," + str(joint1[3]) + "," + str(joint1[4]) + "," \
               + str(joint1[5])+ "},joint = {"+str(joint2[0]) + "," + str(joint2[1]) + "," \
               + str(joint2[2]) + "," + str(joint2[3]) + "," + str(joint2[4]) + "," + str(joint2[5])+ "}"
    else:
        print("Please enter the correct point")

    if count != None:
        data = data + "," + str(count)
    else:
        data = data + ",1"
    if user  != None:
        data = data + ",user ="+str(user)
    if tool  != None:
        data = data + ",tool ="+str(tool)
    if a != None:
        data = data +",a="+str(a)
    if v != None:
        data = data + ",v="+str(v)
    if cp != None:
        data = data + ",cp=" + str(cp)
    data = data +")"

    dobotapi.send_data(data)
    return waitForReplySync(isSync)


def MoveJog(axisID,CoordType=None,User=None,Tool=None,isSync=True):
    string = "MoveJog( " + str(axisID)
    if CoordType != None:
        string = string + ",CoordType="+str(CoordType)
    if User != None:
        string = string + ",User="+str(User)
    if Tool != None:
        string = string + ",Tool="+str(Tool)
    string = string +")"
    dobotapi.send_data(string)
    return waitForReplySync(isSync)

def StartPath(traceName,isConst=None,multi=None,user=None,tool=None,isSync = True):
    string = "StartPath( " + str(traceName)
    if isConst != None:
        string = string + ",isConst="+str(isConst)
    if multi != None:
        string = string + ",multi="+str(multi)
    if user != None:
        string = string + ",user="+str(user)
    if tool != None:
        string = string + ",tool=" + str(tool)
    string = string +")"
    dobotapi.send_data(string)
    classReply = dobotapi.wait_classRely()
    if not isSync or classReply.errorID != 0:
        return classReply.errorID
    while True:
        if RobotModeClassReply().commandID == RobotModeEnum.ROBOT_MODE_ENABLE and RobotModeClassReply().commandID != RobotMode.ROBOT_MODE_RUNNING:
            break
        else:
            time.sleep(0.5)
    return classReply.errorID

def GetStartPose(traceName):
    string = "GetStartPose( " + str(traceName)
    string = string +")"
    dobotapi.send_data(string)
    result = dobotapi.wait_reply()
    return result


def RelMovJTool(x, y, z, rx, ry, rz, user=None, tool=None, a=None, v=None, cp=None, isSync=True):
    string = "RelMovJTool( " + str(x) + "," + str(y) + "," + str(z) + "," + str(rx) + "," + str(ry) + "," + str(rz)+""
    if user != None:
        string = string + ",user="+str(user)
    if tool != None:
        string = string + ",tool="+str(tool)
    if a != None:
        string = string +",a="+str(a)
    if v != None:
        string = string + ",v="+str(v)
    if cp != None:
        string = string + ",cp=" + str(cp)
    string = string +")"
    dobotapi.send_data(string)
    return waitForReplySync(isSync)

def RelMovLTool(x, y, z, rx, ry, rz, user=None, tool=None, a=None, v=None, cp=None, isSync=True):
    string = "RelMovLTool( " + str(x) + "," + str(y) + "," + str(z) + "," + str(rx) + "," + str(ry) + "," + str(rz)+""
    if user != None:
        string = string + ",user="+str(user)
    if tool != None:
        string = string + ",tool="+str(tool)
    if a != None:
        string = string +",a="+str(a)
    if v != None:
        string = string + ",v="+str(v)
    if cp != None:
        string = string + ",cp=" + str(cp)
    string = string +")"
    dobotapi.send_data(string)
    return waitForReplySync(isSync)

def RelMovJUser(x, y, z, rx, ry, rz, user=None, tool=None, a=None, v=None, cp=None, isSync=True):
    string = "RelMovJUser( " + str(x) + "," + str(y) + "," + str(z) + "," + str(rx) + "," + str(ry) + "," + str(rz)+""
    if user != None:
        string = string + ",user="+str(user)
    if tool != None:
        string = string + ",tool="+str(tool)
    if a != None:
        string = string +",a="+str(a)
    if v != None:
        string = string + ",v="+str(v)
    if cp != None:
        string = string + ",cp=" + str(cp)
    string = string +")"
    dobotapi.send_data(string)
    return waitForReplySync(isSync)

def RelMovLUser(x, y, z, rx, ry, rz, user=None, tool=None, a=None, v=None, cp=None, isSync=True):
    string = "RelMovLUser( " + str(x) + "," + str(y) + "," + str(z) + "," + str(rx) + "," + str(ry) + "," + str(rz)+""
    if user != None:
        string = string + ",user="+str(user)
    if tool != None:
        string = string + ",tool="+str(tool)
    if a != None:
        string = string +",a="+str(a)
    if v != None:
        string = string + ",v="+str(v)
    if cp != None:
        string = string + ",cp=" + str(cp)
    string = string +")"
    dobotapi.send_data(string)
    return waitForReplySync(isSync)


def RelJointMovJ(x, y, z, rx, ry, rz, user=None, tool=None, a=None, v=None, cp=None, isSync=True):
    string = "RelJointMovJ( " + str(x) + "," + str(y) + "," + str(z) + "," + str(rx) + "," + str(ry) + "," + str(rz)+""
    if user != None:
        string = string + ",user="+str(user)
    if tool != None:
        string = string + ",tool="+str(tool)
    if a != None:
        string = string +",a="+str(a)
    if v != None:
        string = string + ",v="+str(v)
    if cp != None:
        string = string + ",cp=" + str(cp)
    string = string +")"
    dobotapi.send_data(string)
    return waitForReplySync(isSync)

def GetCurrentCommandID():
    string = "GetCurrentCommandID()"
    dobotapi.send_data(string)
    classReply = dobotapi.wait_classRely_getCommandID()
    # print("GetCurrentCommandID {}".format(classReply.commandID))
    return classReply.commandID

def EnableSafeSkin(status):
    string = "EnableSafeSkin( " + str(status) + ")"
    dobotapi.send_data(string)
    result = dobotapi.wait_reply()
    return result

def SetSafeSkin(part, status):
    string = "SetSafeSkin( " + str(part)+","+str(status) + ")"
    dobotapi.send_data(string)
    result = dobotapi.wait_reply()
    return result





class registeringCallbacks(object):
    def __init__(self):
        self.list = []
        self.func = None
        thread = threading.Thread(target=self.thread_hanlde)
        thread.start()

    def register(self, func):
        self.func = func
        self.list.append(func)
        return func

    def thread_hanlde(self):
        while True:
            for fun in self.list:
                fun()


instance30004 = FeedbackV4Instance()
def GetFeedback30004():
    a = np.frombuffer(feedback30004.feed_back(), dtype=MyType)
    instance30004.MessageSize = a["MessageSize"][0]
    instance30004.DigitalInputs = a["digital_input_bits"][0]
    instance30004.DigitalOutputs = a["digital_output_bits"][0]
    instance30004.RobotMode = a["robot_mode"][0]
    instance30004.TimeStamp = a["time_stamp"][0]
    instance30004.RunTime = a["run_time"][0]
    instance30004.TestValue = a["test_value"][0]
    instance30004.SpeedScaling = a["speed_scaling"][0]
    instance30004.LinearMomentumNorm = a["linear_momentum_norm"][0]
    instance30004.VMain = a["v_main"][0]
    instance30004.VRobot = a["v_robot"][0]
    instance30004.IRobot = a["i_robot"][0]
    instance30004.ProgramState = a["ProgramState"][0]
    instance30004.SafetyStatus = a["SafetyStatus"][0]
    instance30004.ToolAcceleroMeter = a["ToolAcceleroMeter"][0]
    instance30004.ElbowPosition = a["elbow_position"][0]
    instance30004.ElbowVelocity = a["elbow_velocity"][0]
    instance30004.QTarget = a["q_target"][0]
    instance30004.QDTarget = a["qd_target"][0]
    instance30004.QDDTarget = a["qdd_target"][0]
    instance30004.ITarget = a["i_target"][0]
    instance30004.MTarget = a["m_target"][0]
    instance30004.QActual = a["q_actual"][0]
    instance30004.QDActual = a["qd_actual"][0]
    instance30004.IActual = a["i_actual"][0]
    instance30004.ActualTCPForce = a["actual_TCP_force"][0]
    instance30004.ToolVectorActual = a["tool_vector_actual"][0]
    instance30004.TCPSpeedActual = a["TCP_speed_actual"][0]
    instance30004.TCPForce = a["TCP_force"][0]
    instance30004.ToolVectorTarget = a["Tool_vector_target"][0]
    instance30004.TCPSpeedTarget = a["TCP_speed_target"][0]
    instance30004.MotorTemperatures = a["motor_temperatures"][0]
    instance30004.JointModes = a["joint_modes"][0]
    instance30004.VActual = a["v_actual"][0]
    instance30004.HandType = a["hand_type"][0]
    instance30004.User = a["user"][0]
    instance30004.Tool = a["tool"][0]
    instance30004.RunQueuedCmd = a["run_queued_cmd"][0]
    instance30004.PauseCmdFlag = a["pause_cmd_flag"][0]
    instance30004.VelocityRatio = a["velocity_ratio"][0]
    instance30004.AccelerationRatio = a["acceleration_ratio"][0]
    instance30004.JerkRatio = a["jerk_ratio"][0]
    instance30004.XYZVelocityRatio = a["xyz_velocity_ratio"][0]
    instance30004.RVelocityRatio = a["r_velocity_ratio"][0]
    instance30004.XYZAccelerationRatio = a["xyz_acceleration_ratio"][0]
    instance30004.RAccelerationRatio = a["r_acceleration_ratio"][0]
    instance30004.XYZJerkRatio = a["xyz_jerk_ratio"][0]
    instance30004.RJerkRatio = a["r_jerk_ratio"][0]
    instance30004.BrakeStatus = a["brake_status"][0]
    instance30004.EnableStatus = a["enable_status"][0]
    instance30004.DragStatus = a["drag_status"][0]
    instance30004.RunningStatus = a["running_status"][0]
    instance30004.ErrorStatus = a["error_status"][0]
    instance30004.JogStatusCR = a["JogStatusCR"][0]
    instance30004.CRRobotType = a["CRRobotType"][0]
    instance30004.DragButtonSignal = a["drag_button_signal"][0]
    instance30004.EnableButtonSignal = a["enable_button_signal"][0]
    instance30004.RecordButtonSignal = a["record_button_signal"][0]
    instance30004.ReappearButtonSignal = a["reappear_button_signal"][0]
    instance30004.JawButtonSignal = a["jaw_button_signal"][0]
    instance30004.SixForceOnline = a["six_force_online"][0]
    instance30004.CollisionState = a["CollisionState"][0]
    instance30004.ArmApproachState = a["ArmApproachState"][0]
    instance30004.J4ApproachState = a["J4ApproachState"][0]
    instance30004.J5ApproachState = a["J5ApproachState"][0]
    instance30004.J6ApproachState = a["J6ApproachState"][0]
    instance30004.MActual = a["m_actual"][0]
    instance30004.Load = a["load"][0]
    instance30004.CenterX = a["center_x"][0]
    instance30004.CenterY = a["center_y"][0]
    instance30004.CenterZ = a["center_z"][0]
    instance30004.Users = a["user[6]"][0]
    instance30004.Tools = a["tool[6]"][0]
    instance30004.TraceIndex = a["trace_index"][0]
    instance30004.SixForceValue = a["six_force_value"][0]
    instance30004.TargetQuaternion = a["target_quaternion"][0]
    instance30004.ActualQuaternion = a["actual_quaternion"][0]
    instance30004.AutoManualMode = a["AutoManualMode"][0]
    instance30004.VibrationDisZ = a["VibrationDisZ"][0]
    return instance30004


instance30005 = FeedbackV4Instance()
def GetFeedback30005():
    a = np.frombuffer(feedback30005.feed_back(), dtype=MyType)
    instance30005.MessageSize = a["MessageSize"][0]
    instance30005.DigitalInputs = a["digital_input_bits"][0]
    instance30005.DigitalOutputs = a["digital_output_bits"][0]
    instance30005.RobotMode = a["robot_mode"][0]
    instance30005.TimeStamp = a["time_stamp"][0]
    instance30005.RunTime = a["run_time"][0]
    instance30005.TestValue = a["test_value"][0]
    instance30005.SpeedScaling = a["speed_scaling"][0]
    instance30005.LinearMomentumNorm = a["linear_momentum_norm"][0]
    instance30005.VMain = a["v_main"][0]
    instance30005.VRobot = a["v_robot"][0]
    instance30005.IRobot = a["i_robot"][0]
    instance30005.ProgramState = a["ProgramState"][0]
    instance30005.SafetyStatus = a["SafetyStatus"][0]
    instance30005.ToolAcceleroMeter = a["ToolAcceleroMeter"][0]
    instance30005.ElbowPosition = a["elbow_position"][0]
    instance30005.ElbowVelocity = a["elbow_velocity"][0]
    instance30005.QTarget = a["q_target"][0]
    instance30005.QDTarget = a["qd_target"][0]
    instance30005.QDDTarget = a["qdd_target"][0]
    instance30005.ITarget = a["i_target"][0]
    instance30005.MTarget = a["m_target"][0]
    instance30005.QActual = a["q_actual"][0]
    instance30005.QDActual = a["qd_actual"][0]
    instance30005.IActual = a["i_actual"][0]
    instance30005.ActualTCPForce = a["actual_TCP_force"][0]
    instance30005.ToolVectorActual = a["tool_vector_actual"][0]
    instance30005.TCPSpeedActual = a["TCP_speed_actual"][0]
    instance30005.TCPForce = a["TCP_force"][0]
    instance30005.ToolVectorTarget = a["Tool_vector_target"][0]
    instance30005.TCPSpeedTarget = a["TCP_speed_target"][0]
    instance30005.MotorTemperatures = a["motor_temperatures"][0]
    instance30005.JointModes = a["joint_modes"][0]
    instance30005.VActual = a["v_actual"][0]
    instance30005.HandType = a["hand_type"][0]
    instance30005.User = a["user"][0]
    instance30005.Tool = a["tool"][0]
    instance30005.RunQueuedCmd = a["run_queued_cmd"][0]
    instance30005.PauseCmdFlag = a["pause_cmd_flag"][0]
    instance30005.VelocityRatio = a["velocity_ratio"][0]
    instance30005.AccelerationRatio = a["acceleration_ratio"][0]
    instance30005.JerkRatio = a["jerk_ratio"][0]
    instance30005.XYZVelocityRatio = a["xyz_velocity_ratio"][0]
    instance30005.RVelocityRatio = a["r_velocity_ratio"][0]
    instance30005.XYZAccelerationRatio = a["xyz_acceleration_ratio"][0]
    instance30005.RAccelerationRatio = a["r_acceleration_ratio"][0]
    instance30005.XYZJerkRatio = a["xyz_jerk_ratio"][0]
    instance30005.RJerkRatio = a["r_jerk_ratio"][0]
    instance30005.BrakeStatus = a["brake_status"][0]
    instance30005.EnableStatus = a["enable_status"][0]
    instance30005.DragStatus = a["drag_status"][0]
    instance30005.RunningStatus = a["running_status"][0]
    instance30005.ErrorStatus = a["error_status"][0]
    instance30005.JogStatusCR = a["JogStatusCR"][0]
    instance30005.CRRobotType = a["CRRobotType"][0]
    instance30005.DragButtonSignal = a["drag_button_signal"][0]
    instance30005.EnableButtonSignal = a["enable_button_signal"][0]
    instance30005.RecordButtonSignal = a["record_button_signal"][0]
    instance30005.ReappearButtonSignal = a["reappear_button_signal"][0]
    instance30005.JawButtonSignal = a["jaw_button_signal"][0]
    instance30005.SixForceOnline = a["six_force_online"][0]
    instance30005.CollisionState = a["CollisionState"][0]
    instance30005.ArmApproachState = a["ArmApproachState"][0]
    instance30005.J4ApproachState = a["J4ApproachState"][0]
    instance30005.J5ApproachState = a["J5ApproachState"][0]
    instance30005.J6ApproachState = a["J6ApproachState"][0]
    instance30005.MActual = a["m_actual"][0]
    instance30005.Load = a["load"][0]
    instance30005.CenterX = a["center_x"][0]
    instance30005.CenterY = a["center_y"][0]
    instance30005.CenterZ = a["center_z"][0]
    instance30005.Users = a["user[6]"][0]
    instance30005.Tools = a["tool[6]"][0]
    instance30005.TraceIndex = a["trace_index"][0]
    instance30005.SixForceValue = a["six_force_value"][0]
    instance30005.TargetQuaternion = a["target_quaternion"][0]
    instance30005.ActualQuaternion = a["actual_quaternion"][0]
    instance30005.AutoManualMode = a["AutoManualMode"][0]
    instance30005.VibrationDisZ = a["VibrationDisZ"][0]
    return instance30005
