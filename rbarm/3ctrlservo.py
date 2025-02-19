#!/usr/bin/env python3
#coding=utf-8

import time
from Arm_Lib import Arm_Device

# 创建机械臂对象
Arm = Arm_Device()
time.sleep(.1)

# 单独控制一个舵机运动到某个角度
id = 6
Arm.Arm_serial_servo_write(id, 90, 500)
time.sleep(1)

# 控制一个舵机循环切换角度
id = 6
 
def main():
    while True:
        Arm.Arm_serial_servo_write(id, 120, 500)
        time.sleep(1)
        Arm.Arm_serial_servo_write(id, 50, 500)
        time.sleep(1)
        Arm.Arm_serial_servo_write(id, 120, 500)
        time.sleep(1)
        Arm.Arm_serial_servo_write(id, 180, 500)
        time.sleep(1)
 
try :
    main()
except KeyboardInterrupt:
    print(" Program closed! ")
    pass

del Arm # 释放掉 Arm 对象  