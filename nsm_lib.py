#!/usr/bin/python
# -*- coding: UTF-8 -*-

#得到本地指定网卡的ip地址，
def getLocalIp(ifname = 'ens33'):
    import socket, fcntl, struct;
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]));
    ip = socket.inet_ntoa(inet[20:24]);
    return ip;