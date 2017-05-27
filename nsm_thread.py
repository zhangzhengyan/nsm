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
import datetime

#集群几点系统信息配置文件目录
sys_conf = "/var/nsm_agent/myself"



#发送线程，发送注册、心跳等消息
class send_thread(threading.Thread):
    task_run_interval = {"10_SEC_TASK": 10, "30_SEC_TASK": 30, "60_SEC_TASK": 60, "300_SEC_TASK": 300}

    def __init__(self, threadID, name, channel, exchange, routing_key):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.channel = channel
        self.routing_key = routing_key
        self.exchange = exchange
    def run(self):
        #init register one time
        send_register(self.channel, self.exchange, self.routing_key)

        """
        定时发送系统消息给server
        """

        #time interval
        sleep_interval = 0
        now = datetime.datetime.now()

        if now.second < 30:
            sleep_interval = (30 - now.second)
        elif now.second > 30:
            sleep_interval = (90 - now.second)

        #calibrate time to 30 second
        time.sleep(sleep_interval)

        #sleep interval is 10 second.
        sleep_interval = 10

        #sleep times
        interval_count=0

        while True:
            if sleep_interval:
                time.sleep(sleep_interval)

            interval_count += 1

            # print self.task_run_interval["10_SEC_TASK"]
            # print self.task_run_interval["30_SEC_TASK"]
            # print self.task_run_interval["60_SEC_TASK"]

            #10 second task run
            if ((sleep_interval * interval_count) % self.task_run_interval["10_SEC_TASK"]) == 0:
                interval_count += self.send_task(self.task_run_interval["10_SEC_TASK"], sleep_interval)
            #30 second task run
            if ((sleep_interval * interval_count) % self.task_run_interval["30_SEC_TASK"]) == 0:
                interval_count += self.send_task(self.task_run_interval["30_SEC_TASK"], sleep_interval)
            #60 second task run
            if ((sleep_interval * interval_count) % self.task_run_interval["60_SEC_TASK"]) == 0:
                interval_count += self.send_task(self.task_run_interval["60_SEC_TASK"], sleep_interval)
            #300 second task run
            if ((sleep_interval * interval_count) % self.task_run_interval["300_SEC_TASK"]) == 0:
                interval_count += self.send_task(self.task_run_interval["300_SEC_TASK"], sleep_interval)
            '''
            annotation:
            if task runs longer and exceeds the time of other task interval, 
            the other tasks will not be executed this time
            '''
    def send_task(self, run_interval, sleep_interval):
        start_time = time.time()

        if run_interval == self.task_run_interval["10_SEC_TASK"]:
            # print "10 second run"
            #send heartbeat every 10 second
            send_heartbeat(self.channel, self.exchange, self.routing_key)
            end_time = time.time()

            return int((end_time - start_time) / sleep_interval)
        if run_interval == self.task_run_interval["30_SEC_TASK"]:
            # print "30 second run"
            end_time = time.time()

            return int((end_time - start_time) / sleep_interval)
        if run_interval == self.task_run_interval["60_SEC_TASK"]:
            # print "60 second run"
            end_time = time.time()

            return int((end_time - start_time) / sleep_interval)
        if run_interval == self.task_run_interval["300_SEC_TASK"]:
            #update register every 300 second
            send_register(self.channel, self.exchange, self.routing_key)
            end_time = time.time()

            return int((end_time - start_time) / sleep_interval)





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

def send_heartbeat(channel, exchange, routing_key):
    message = {}

    # 注册消息id为0
    message["id"] = 0
    # 注册消息的类型
    message["type"] = nsm_agent_modules.message_type["NSM_AGENT_HEARTBEAT"]
    # 功能id为0
    message["fun_id"] = nsm_agent_modules.fun_id["NSM_UNDEFINE"]
    message["echo"] = False
    message["sync"] = False
    message["timestamp"] = time.time()
    message["shost"] = nsm_lib.getLocalIp()

    #message body is NULL
    message["body"] = {}

    message = json.dumps(message, ensure_ascii=False)

    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)




