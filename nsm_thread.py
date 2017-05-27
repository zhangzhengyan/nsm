#!/usr/bin/python
# -*- coding: UTF-8 -*-
'''
此为定义nsm的线程功能模块
包括发送线程和后续可能的接收线程
'''

import threading
import time
import nsm_lib
import nsm_agent_modules
import ConfigParser
import pika
import json

#集群几点系统信息配置文件目录
sys_conf = "/var/nsm_agent/myself"

#发送线程，发送注册、心跳等消息
class send_thread(threading.Thread):
    def __init__(self, threadID, name, channel, exchange, routing_key):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.channel = channel
        self.routing_key = routing_key
        self.exchange = exchange
    def run(self):
        send_register(self.channel, self.exchange, self.routing_key)

        """
        定时发送系统消息给server
        """


'''
这个启动注册一下，但是还需要定期发送注册信息
因为如果在agent注册时候server端还没有运行，那么将没有注册成功，所以定时发送注册以便更新
'''
def send_register(channel, exchange, routing_key):
    message = {}

    #注册消息id为0
    message["id"] = 0
    #注册消息的类型
    message["type"] = nsm_agent_modules.message_type["NSM_AGENT_REGISTER"]
    #功能id为0
    message["fun_id"] = nsm_agent_modules.fun_id["NSM_UNDEFINE"]
    message["echo"] = False
    message["sync"] = False
    message["timestamp"] = time.time()
    message["shost"] = nsm_lib.getLocalIp()

    #读取本机文件，填入body
    message["body"] = {}
    message["body"]["host_ip"] = nsm_lib.getLocalIp()

    config = ConfigParser.ConfigParser()
    config.readfp(open(sys_conf))

    message["body"]["sys_type"] = {}
    message["body"]["sys_type"]["mon"] = config.get("sys_type", "mon")
    message["body"]["sys_type"]["osd"] = config.get("sys_type", "osd")
    message["body"]["sys_type"]["mds"] = config.get("sys_type", "mds")
    message["body"]["sys_type"]["nfs"] = config.get("sys_type", "nfs")
    message["body"]["sys_type"]["cifs"] = config.get("sys_type", "cifs")
    message["body"]["sys_type"]["ftp"] = config.get("sys_type", "ftp")

    print message
    message = json.dumps(message, ensure_ascii=False)

    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)