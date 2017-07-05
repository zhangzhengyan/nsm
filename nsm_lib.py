#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os

fs_root_dir = '/ifs'

#得到本地指定网卡的ip地址，
def getLocalIp(ifname = 'eth0'):
    import socket, fcntl, struct;
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]));
    ip = socket.inet_ntoa(inet[20:24]);
    return ip;



#path size
def get_path_size(strPath):
    if not os.path.exists(strPath):
        return 0;

    if os.path.isfile(strPath):
        return os.path.getsize(strPath);

    nTotalSize = 4096;  #dir's size
    dir_list = os.listdir(strPath)
    for f in dir_list:
        nTotalSize += get_path_size(os.path.join(strPath, f))

    return nTotalSize;

#dir number and file number
def get_path_capacity(strPath):
    if not os.path.exists(strPath):
        return 0;

    if os.path.isfile(strPath):
        file_num = 1;
        return file_num, 0;

    file_num = 0;
    dir_num = 1;  #current is dir
    dir_list = os.listdir(strPath)
    for f in dir_list:
        fiels , dirs = get_path_capacity(os.path.join(strPath, f))
        file_num += fiels;
        dir_num += dirs;

    return file_num , dir_num;