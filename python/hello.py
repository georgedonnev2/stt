#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path

'Learning Python3'

# ex

# print("hello world!")
# print(100+200+300)

# ex-02

'''
name = input()
print("hello ", name)

name = input('Please enter your name: ')
print("hello, ", name)
'''

# ex
# print('1024 * 768 =', 1024*768)

# ex

'''
def _private_1(name):
    return 'Hello, %s' % name
def _private_2(name):
    return 'Hi, %s' % name
def greeting(name):
    if len(name) > 3:
        return _private_1(name)
    else:
        return _private_2(name)
    


greeting('abc')
greeting('abc')
'''

# ex
'''
FILE = Path(__file__).resolve()
lib_root = FILE.parents[0]
lib_site_pkg = os.path.dirname(lib_root)
lib_python = os.path.dirname(lib_site_pkg)
lib_path = os.path.dirname(lib_python)
shared_path = os.path.join(os.path.dirname(lib_path), "share")
share_root = os.path.join(shared_path, "dofbot_garbage_yolov5")
cfg_folder = os.path.join(share_root, "config")
dp_cfg_path = os.path.join(cfg_folder, "dp.bin")

print('__file__', __file__)
print('path', Path(__file__))
print('file: %s' % FILE)
print('file.parents: %s' % (FILE.parents))
print('lib_root(file.parents[0]): %s' % lib_root)
'''

# ex
'''
函数多返回值 代码示例
'''

def multiple_return():
    a = 1
    b = 2
    c = 3
    return a, b, c

def main(args=None):
    result = multiple_return()
    print(f"return: {result}, type of return: {type (result)}")

    x, y, z = multiple_return()
    print("x=%d, y=%d, z=%d; type of x:%s" %(x,y,z, type(x)))

    i, j, _ = multiple_return()
    print("i=%d, j=%d" %(i, j))

    m, n, o= multiple_return()
    print("m=%d, n=%d, o=%d" %(m, n, o))

if __name__ == "__main__":
    main()