#! /usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/12/30 11:32
# @Author  : wsh
# @File    : test_this_dir.py
import os

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

if __name__ == '__main__':
    print(THIS_DIR)
    print(f'当前的文件路径: {os.path.abspath(__file__)}')

