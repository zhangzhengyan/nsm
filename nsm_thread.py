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
import sys
from nsm_lib import get_path_capacity
from nsm_lib import fs_root_dir
import re
import subprocess
import psutil

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

        while True:
        # while False:
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
            # send_heartbeat(self.channel, self.exchange, self.routing_key)
            # send_cifs_conn(self.channel, self.exchange, self.routing_key)
            # send_nfs_conn(self.channel, self.exchange, self.routing_key)
            send_node_info(self.channel, self.exchange, self.routing_key)
            end_time = time.time()

            return int((end_time - start_time) / sleep_interval)
        if run_interval == self.task_run_interval["30_SEC_TASK"]:
            # print "30 second run"
            # send_fs_capacity(self.channel, self.exchange, self.routing_key)
            end_time = time.time()

            return int((end_time - start_time) / sleep_interval)
        if run_interval == self.task_run_interval["60_SEC_TASK"]:
            # print "60 second run"
            end_time = time.time()

            return int((end_time - start_time) / sleep_interval)
        if run_interval == self.task_run_interval["300_SEC_TASK"]:
            #update register every 300 second
            # send_register(self.channel, self.exchange, self.routing_key)
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

def send_cifs_conn(channel, exchange, routing_key):

    message = {}

    #消息id为0
    message["id"] = 0
    #消息的类型
    message["type"] = nsm_agent_modules.message_type["NSM_AGENT_CHECK"]
    message["fun_id"] = nsm_agent_modules.check_fun_id["NSM_AGENT_CHECK_CIFS_CONN"]
    message["echo"] = False
    message["sync"] = False
    message["timestamp"] = time.time()
    message["shost"] = nsm_lib.getLocalIp()

    #fill body
    message["body"] = {}

    conn_list = []

    conn_0 = {}
    conn_1 = {}
    pid_list = []

    #pid:user:group:ip
    cmd = "smbstatus -p | awk -F '[: ]+' '{if($1~/^[0-9]/)print $1\" \"$2\" \"$3\" \"$6;}'"
    for line in os.popen(cmd, 'r').readlines():
        try:
            conn_info = line.split()
            pid = str(conn_info[0])
            pid_list.append(pid)
            conn_0[pid] = {}
            conn_0[pid]["user_name"] = conn_info[1]
            conn_0[pid]["group_name"] = conn_info[2]
            conn_0[pid]["ip"] = conn_info[3]
        except:
            pass
    # pid:share_dir
    cmd = "smbstatus -S | awk '{if($2~/^[0-9]/ && $1 != \"IPC$\")print $2\" \"$1\" \"$4\" \"$5\" \"$6\" \"$7\" \"$8\" \"$9\" \"$10}'"
    #22957 IPC$ Tue Jul 4 01:29:21 PM 2017 CST
    #22991 zzy Tue Jul 4 01:32:04 PM 2017 CST
    for line in os.popen(cmd, 'r').readlines():
        try:
            conn_info = line.split()
            pid = str(conn_info[0])
            conn_1[pid] = {}
            conn_1[pid]["share_dir"] = conn_info[1]
            conn_1[pid]["timestamp"] = conn_info[2]+" "+conn_info[3]+" "\
                                       +conn_info[4]+" "+conn_info[5]+" "\
                                       +conn_info[6]+" "+conn_info[7]+" " \
                                       +conn_info[8]
            conn_1[pid]["timestamp"] = time.mktime(\
                time.strptime(conn_1[pid]["timestamp"], "%a %b %d %H:%M:%S %p %Y %Z"))
        except:
            pass

    for pid in pid_list:
        conn = {}
        conn["user_name"] = conn_0[pid]["user_name"]
        conn["group_name"] = conn_0[pid]["group_name"]
        conn["ip"] = conn_0[pid]["ip"]
        conn["share_dir"] = conn_1[pid]["share_dir"]
        conn["pid"] = pid
        conn["mount_time"] = conn_1[pid]["timestamp"]
        conn_list.append(conn)

    message["body"]["conn_list"] = conn_list

    #------------------------------------------------------------------------------------
    message = json.dumps(message, ensure_ascii=False)
    # print message

    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)

def send_nfs_conn(channel, exchange, routing_key):
    sys.path.append("/etc/ganeshactl/")
    import ganesha_mgr

    message = {}

    #消息id为0
    message["id"] = 0
    #消息的类型
    message["type"] = nsm_agent_modules.message_type["NSM_AGENT_CHECK"]
    # func id
    message["fun_id"] = nsm_agent_modules.check_fun_id["NSM_AGENT_CHECK_NFS_CONN"]
    message["echo"] = False
    message["sync"] = False
    message["timestamp"] = time.time()
    message["shost"] = nsm_lib.getLocalIp()

    #fill body
    message["body"] = {}

    conn_list = []

    client_mgr = ganesha_mgr.ManageClients()
    clientmgr = client_mgr.clientmgr
    status, errormsg, reply = clientmgr.ShowClients()
    if status == True:
        clients = reply[1]
    else:
        pass

    for client in clients:
        conn = {}
        ip_str = str(client.ClientIP)
        split_list = ip_str.split(":")
        # ::ffff:10.1.15.242, last one is ip
        ip = split_list[len(split_list) - 1]
        conn["ip"] = ip
        conn_list.append(conn)

    message["body"]["conn_list"] = conn_list

    #------------------------------------------------------------------------------------
    message = json.dumps(message, ensure_ascii=False)
    print message

    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)

def send_fs_capacity(channel, exchange, routing_key):
    message = {}

    #消息id为0
    message["id"] = 0
    #消息的类型
    message["type"] = nsm_agent_modules.message_type["NSM_AGENT_CHECK"]
    # func id
    message["fun_id"] = nsm_agent_modules.check_fun_id["NSM_AGENT_CHECK_FS_CAPACITY"]
    message["echo"] = False
    message["sync"] = False
    message["timestamp"] = time.time()
    message["shost"] = nsm_lib.getLocalIp()

    #fill body
    message["body"] = {}

    #get filesystem capacity
    files, dirs = get_path_capacity(fs_root_dir)
    message["body"]["file_num"] = files
    message["body"]["dir_num"] = dirs

    #get filesystem storage space
    cmd = "rados df | grep \"total used\" | awk '{print $3}'"
    # total used
    result  = os.popen(cmd, 'r').readline()
    try:
        used = int(result)
    except:
        pass
    else:
        message["body"]["used_space"] = used

    cmd = "rados df | grep \"total avail\" | awk '{print $3}'"
    # total avail
    result = os.popen(cmd, 'r').readline()
    try:
        avail = int(result)
    except:
        pass
    else:
        message["body"]["free_space"] = avail

# 发送节点的硬件信息，包括cpu、网卡等
def send_node_info(channel, exchange, routing_key):
    import socket
    message = {}

    # 消息id为0
    message["id"] = 0
    # 消息的类型
    message["type"] = nsm_agent_modules.message_type["NSM_AGENT_CHECK"]
    # func id
    message["fun_id"] = nsm_agent_modules.check_fun_id["NSM_AGENT_CHECK_NODE_INFO"]
    message["echo"] = False
    message["sync"] = False
    message["timestamp"] = time.time()
    message["shost"] = nsm_lib.getLocalIp()

    # fill body
    message["body"] = {}

    # get node、cpu 、mem 、disk info， use psutil module fun

    # get cpu info -----------------------------------------------------------------------------
    message["body"]["cpu"] = {}
    # try:
    #     cmd = "top -bn1 | grep \"Cpu(s)\" | sed \"s/.*, *\\([0-9.]*\\)%* id.*/\\1/\" | awk '{print 100-$1}'"
    #     result = os.popen(cmd, 'r').readline()
    #     message["body"]["cpu"]["cpu_usage"] = result
    # except:
    #     pass

    try:
        message["body"]["cpu"]["cpu_usage"] = psutil.cpu_percent(1)
    except:
        pass

    #get mem usage---------------------------------------------------------------------------
    try:
        mem = psutil.virtual_memory()
        message["body"]["cpu"]["mem_usage"] = str(mem.percent)
    except:
        pass

    #get net info-----------------------------------------------------------------------------
    message["body"]["net_card"] = []
    net_card_list = message["body"]["net_card"]
    net_card = {}

    stats = psutil.net_if_stats()
    for nic, addrs in psutil.net_if_addrs().items():
        net_card = {}
        if nic == "lo":
            continue
        else:
            net_card["name"] = nic
        if nic in stats:
            st = stats[nic]
            if st.isup:
                net_card["status"] = 1
            else:
                net_card["status"] = 0
        for addr in addrs:
            if addr.family == socket.AF_INET or addr.family == socket.AF_INET6:
                net_card["ip"] = addr.address
            elif addr.family == psutil.AF_LINK:
                net_card["mac"] = addr.address
        net_card_list.append(net_card)

    # #get net card info
    # message["body"]["net_card"] = []
    # res_list = message["body"]["net_card"]
    #
    # #get name mac ip status等
    # ifc = subprocess.Popen(["ifconfig", "-a"], stdout=subprocess.PIPE)
    # res = {}
    # ip = None
    # mac = None
    # ip_status = 0
    # mac_status = 0
    #
    # for x in ifc.stdout:
    #     if not x.strip():
    #         # 空行
    #         if not res: #如果res是空后面不用处理
    #             continue
    #         if ip:
    #             res["ip"] = ip.group(1)
    #         else:
    #             res["ip"] = ""
    #
    #         if mac:
    #             res["mac"] = mac.group(1)
    #         else:
    #             res["mac"] = ""
    #         res_list.append(res)
    #
    #         res = {}
    #         ip = None
    #         mac = None
    #         ip_status = 0
    #         mac_status = 0
    #     elif not res:
    #         # 第一行
    #         try:
    #             name = re.match(r'[A-Za-z0-9-]+', x).group()
    #             if name == "lo":
    #                 # lo 后面的行都会走这个流程
    #                 continue
    #             res["name"] = name
    #             if re.search(r'UP', x):
    #                 res["status"] = 1
    #             else:
    #                 res["status"] = 0
    #         except:
    #             #这个地方后续要处理一下
    #             pass
    #     else:
    #         # 其他行
    #         try:
    #             if ip_status == 0 and not ip:
    #                 ip = re.match(r'\s+inet\s+(\S+)', x)
    #             if ip:
    #                 ip_status = 1
    #             if mac_status == 0 and not mac:
    #                 mac = re.match(r'\s+ether\s+(\S+)', x)
    #             if mac:
    #                 mac_status = 1
    #         except:
    #             # 这个地方后续要处理一下
    #             pass

    #得到每个网卡的收发速率
    for i in range(len(net_card_list)):
        name = net_card_list[i]["name"]
        cmd = "iftop -i " + name + " -t -s 1 | grep -i \"^total\" | awk -F\":\" '{print $2}' | awk '{print $1}'"
        j = 0
        try:
            for line in os.popen(cmd).readlines():
                if j == 0:
                    net_card_list[i]["send_rate"] = str(line)
                elif j == 1:
                    net_card_list[i]["rec_rate"] = str(line)
                j += 1
        except:
            pass

    #得到磁盘信息--------------------------------------------------------------
    message["body"]["disk"] = []
    disk_table = message["body"]["disk"]
    disk = {}

    #先使用shell命令得到磁盘列表
    # cmd = "lsblk | awk '{if($1~/^.{1}d[a-z]/)print $1}'"
    cmd = "lsblk | awk '{if($1~/^[a-z].*/)print $1}'"
    disk_list = []
    for line in os.popen(cmd).readlines():
        line = line.strip('\n')
        disk_list.append(line)

    for part in psutil.disk_partitions(all=False):
        if os.name == 'nt':
            if 'cdrom' in part.opts or part.fstype == '':
                # skip cd-rom drives with no disk in it; they may raise
                # ENOENT, pop-up a Windows GUI error for a non-ready
                # partition or just hang.
                continue

        # 挂载点没有ceph盘的标识
        if not re.search("ceph", part.mountpoint):
            #已经挂载的但是非ceph使用的磁盘从磁盘列表移除
            for i in range(len(disk_list)):
                if re.search(disk_list[i], part.device):
                    disk_list.pop(i)
                    break
            continue


        '''
        disk dic example:
        disk =  {
                    "sda":{"size":314151235, "used":4534535},
                    "sdb":{"size":314151235, "used":4534535},
                    "sdc":{"size":314151235, "used":4534535}
                }
        '''
        for i in range(len(disk_list)):
            if re.search(disk_list[i], part.device):
                if not disk.has_key(disk_list[i]):
                    disk[disk_list[i]] = {}
                usage = psutil.disk_usage(part.mountpoint)

                if not disk[disk_list[i]].has_key("size"):
                    disk[disk_list[i]]["size"] = int(usage.total)
                else:
                    disk[disk_list[i]]["size"] += int(usage.total)

                if not disk[disk_list[i]].has_key("used"):
                    disk[disk_list[i]]["used"] = int(usage.used)
                else:
                    disk[disk_list[i]]["used"] += int(usage.used)

    for key in disk.keys():
        disk_info = {}
        disk_info["name"] = key
        disk_info["size"] = disk[key]["size"]
        disk_info["used"] = disk[key]["used"]
        disk_info["status"] = 1

        disk_table.append(disk_info)


    #现在cpeh使用的磁盘已经记录在disk字典里了，我们从disk_list里删除disk字典记录的磁盘
    for key in disk.keys():
        disk_list.remove(key)

    print disk_list


    #现在如果disk_list还有元素，说明是未使用的磁盘, 这个用命令取在psutil模块中不能取到没有挂载的块设备
    for disk_name in disk_list:
        cmd = "lsblk -b | grep " + disk_name + " | awk '{print $4}'"
        result = os.popen(cmd).readline()
        result.strip("\n")
        disk_info = {}
        disk_info["name"] = disk_name
        disk_info["size"] = int(result)
        disk_info["used"] = 0
        disk_info["status"] = 0
        disk_table.append(disk_info)

    #------------------------------------------------------------------------------------
    message = json.dumps(message, ensure_ascii=False)
    print message

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
