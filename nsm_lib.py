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