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
import os

#集群节点系统信息配置文件目录
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

        #while True:
        while False:
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
    #注册类型其实只有这一个消息，功能id应该为0，但是这里将节点的状态一并发送，功能id打上状态id
    #由于注册消息定时发送，所有这样没问题，要是注册只注册一次那么状态消息应该独立出来
    message["fun_id"] = nsm_agent_modules.check_fun_id["NSM_AGENT_CHECK_NODE_STATUS"]
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
    message["body"]["sys_type"]["mon"] = int(config.get("sys_type", "mon"))
    message["body"]["sys_type"]["osd"] = int(config.get("sys_type", "osd"))
    message["body"]["sys_type"]["mds"] = int(config.get("sys_type", "mds"))
    message["body"]["sys_type"]["nfs"] = int(config.get("sys_type", "nfs"))
    message["body"]["sys_type"]["cifs"] = int(config.get("sys_type", "cifs"))
    message["body"]["sys_type"]["ftp"] = int(config.get("sys_type", "ftp"))

    message["body"]["sys_status"] = {}
    #check service status
    if message["body"]["sys_type"]["mon"] == 1:
        cmd = 'ps aux | grep ceph-mon | grep -v grep | wc -l'
        mon_num = os.popen(cmd, 'r').read()
        if int(mon_num) >= 1:
            message["body"]["sys_status"]["mon"] = 1
        else:
            message["body"]["sys_status"]["mon"] = 0

    if message["body"]["sys_type"]["osd"] == 1:
        cmd = "lsblk | awk '{print $7}' | awk -F\"/\" '{print $6}' | awk -F\"-\" '{print $2}'"
        count = 0
        for line in os.popen(cmd, 'r').readlines():
            try:
                osd_id = int(line)
                count += 1
            except ValueError:
                pass
        cmd = "ps axu | grep -v grep | grep ceph-osd | wc -l"
        result = os.popen(cmd, 'r').read()

        # print count
        # print result

        if int(result) == count:
            message["body"]["sys_status"]["osd"] = 1
        else:
            message["body"]["sys_status"]["osd"] = 0

    if message["body"]["sys_type"]["mds"] == 1:
        cmd = 'ps aux | grep ceph-mds | grep -v grep | wc -l'
        mon_num = os.popen(cmd, 'r').read()
        if int(mon_num) >= 1:
            message["body"]["sys_status"]["mds"] = 1
        else:
            message["body"]["sys_status"]["mds"] = 0

    #------------------------------------------------------------------------------------
    message = json.dumps(message, ensure_ascii=False)
    print message

    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)

def send_heartbeat(channel, exchange, routing_key):
    message = {}

    # 注册消息id为0
    message["id"] = 0
    # 注册消息的类型
    message["type"] = nsm_agent_modules.message_type["NSM_AGENT_HEARTBEAT"]
    # 功能id为0
    message["fun_id"] = nsm_agent_modules.exe_fun_id["NSM_UNDEFINE"]
    message["echo"] = False
    message["sync"] = False
    message["timestamp"] = time.time()
    message["shost"] = nsm_lib.getLocalIp()

    #message body is NULL
    message["body"] = {}

    message = json.dumps(message, ensure_ascii=False)

    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)


#receive server message and echo result
class echo_send_thread(threading.Thread):
    def __init__(self, threadID, name, channel, exchange, routing_key, queue):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.channel = channel
        self.routing_key = routing_key
        self.exchange = exchange
        self.queue = queue
    def run(self):
        #get echo message

        while True:
            if not self.queue.empty():
                echo_mes = self.queue.get()

                echo_mes = json.dumps(echo_mes, ensure_ascii=False)

                self.channel.basic_publish(exchange=self.exchange, routing_key=self.routing_key, body=echo_mes)
            else:
                time.sleep(1)
