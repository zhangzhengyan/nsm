#!/usr/bin/python
# -*- coding: UTF-8 -*-

import subprocess
import commands
import os
import stat
from nsm_lib import fs_root_dir
from nsm_lib import get_path_size


'''
消息的结构体如下：
{
	id:			//消息的ID
	type:		//消息类型
	fun_id:	    //此类型消息对应的功能id
	echo:		//是否回显，是否是回显
	sync:		//是否是同步调用
	timestamp:	//时间戳
	shost:		//本机地址
	body:{}		//消息的内容
}
消息类型包括：
NSM_AGENT_EXECUTE		1   执行操作
    NSM_AGENT_EXECUTE_CREAT_DIR 1 //create dir
NSM_AGENT_HEARTBEAT		2   心跳
NSM_AGENT_CHECK			3   监控
NSM_AGENT_REGISTER		4	agent注册
'''

message_type = {"NSM_AGENT_UNDEFINE":0, "NSM_AGENT_EXECUTE":1, "NSM_AGENT_HEARTBEAT":2,"NSM_AGENT_CHECK":3, "NSM_AGENT_REGISTER":4}
exe_fun_id = {"NSM_UNDEFINE":0, "NSM_AGENT_EXECUTE_CREAT_DIR":1, "NSM_AGENT_EXECUTE_LIST_DIR":2}
check_fun_id = {"NSM_UNDEFINE":0, "NSM_AGENT_CHECK_NODE_STATUS":1}

def create_dir(body):

    '''
    body的json格式如下
    {"
        id":1,
        "type":1,
        "fun_id":1,
        "body":
        {
            "dir_name":"polly",
    	    "limit":
    	    {
    	        "user":[1, 1, 1],
    	        "user_group":[1, 0, 1],
    	        "other":[1, 0, 1]
    	    },
    	    "quota":
    	    {
    	        "max_file_num":"10000",
    	        "max_file_size":"5"
    	    }
        }
    }
    '''

    #得到目录名
    dir_name = os.path.join(fs_root_dir, body["dir_name"])
    #创建目，如果目录存在要处理一下，正常情况目录时不应该存在的
    os.makedirs(dir_name)

    #计算权限值
    limit_value = body["limit"]["user"][0] * stat.S_IRUSR \
                + body["limit"]["user"][1] * stat.S_IWUSR \
                + body["limit"]["user"][2] * stat.S_IXUSR \
                + body["limit"]["user_group"][0] * stat.S_IRGRP \
                + body["limit"]["user_group"][1] * stat.S_IWGRP \
                + body["limit"]["user_group"][2] * stat.S_IXGRP \
                + body["limit"]["other"][0] * stat.S_IROTH \
                + body["limit"]["other"][1] * stat.S_IWOTH \
                + body["limit"]["other"][2] * stat.S_IXOTH

    os.chmod(dir_name, limit_value)

    #set directory quota
    quota_cmd = 'setfattr -n ceph.quota.max_bytes -v ' + body["quota"]["max_file_size"] + ' ' + dir_name
    print quota_cmd
    os.system(quota_cmd)

    quota_cmd = 'setfattr -n ceph.quota.max_files -v ' + body["quota"]["max_file_num"] + ' ' + dir_name
    print quota_cmd
    os.system(quota_cmd)

    re_mes_body={"echo":"ok"}

    return re_mes_body

#list directories and the files under the specified directory
def list_dir(body):
    global sys_root_dir
    dir_name = body["dir_name"]
    full_dir_name = os.path.join(fs_root_dir, dir_name)

    #current dir list
    dir_list = os.listdir(full_dir_name)
    re_mes_body = {}
    file_attr = []

    for f in dir_list:
        cur_file = os.path.join(full_dir_name, f)
        attr = {}
        if os.path.isdir(cur_file):
            dir_size = get_path_size(cur_file)
            attr["name"] = f
            attr["size"] = dir_size
            attr["type"] = 1
        else:
            file_size =  os.path.getsize(cur_file)
            attr["name"] = f
            attr["size"] = file_size
            attr["type"] = 2
        file_attr.append(attr)

    re_mes_body["file_attr"] = file_attr

    return re_mes_body

# view file or directory details
def dir_details(body):
    re_mes_body = {}
    # get file or dir name
    name = body["name"]
    type = body["type"]
    export = body["export"]
    re_mes_body["name"] = name
    re_mes_body["export"] = export

    full_dir_name = os.path.join(fs_root_dir, name)
    # dir ?
    if type == 1:
        # get dir size
        dir_size = get_path_size(full_dir_name)
        re_mes_body["size"] = dir_size

        for root, dirs, files in os.walk(full_dir_name):
            # dir numbers
            re_mes_body["dirs"] = len(dirs)
            # file numbers
            re_mes_body["files"] = len(files)
        if export:
            re_mes_body["quota"] = {}
            # get directory quota
            quota_cmd = 'getfattr -n ceph.quota.max_bytes ' + full_dir_name
            quota_cmd += " | grep ceph.quota.max_bytes | awk -F\"=\" '{print $2}'"
            print quota_cmd

            # for line in os.popen(quota_cmd, 'r').readlines():
            result = os.popen(quota_cmd, 'r').read().strip('\n')
            re_mes_body["quota"]["max_file_size"] = str(result)

            # get directory quota
            quota_cmd = 'getfattr -n ceph.quota.max_files ' + full_dir_name
            quota_cmd += " | grep ceph.quota.max_files | awk -F\"=\" '{print $2}'"
            print quota_cmd
            result = os.popen(quota_cmd, 'r').read().strip('\n')
            re_mes_body["quota"]["max_file_num"] = str(result)

    else: #file ??
        file_size = os.path.getsize(full_dir_name)
        re_mes_body["size"] = file_size

    # get limit
    mode = os.stat(full_dir_name).st_mode
    limit = stat.S_IMODE(mode)

    # limit mask
    # usr_r = 0b100000000
    # usr_w = 0b010000000
    # usr_x = 0b010000000
    # grp_r = 0b000100000
    # grp_w = 0b000010000
    # grp_x = 0b000001000
    # oth_r = 0b000000100
    # oth_w = 0b000000010
    # oth_x = 0b000000001
    limit_mask = {"user": [0b100000000, 0b010000000, 0b001000000],
                  "group": [0b000100000, 0b000010000, 0b000001000],
                  "other": [0b000000100, 0b000000010, 0b000000001]}

    user = []
    for usr_limit in limit_mask["user"]:
        if (limit & usr_limit) is not 0:
            user.append(1)
        else:
            user.append(0)

    group = []
    for usr_limit in limit_mask["group"]:
        if (limit & usr_limit) is not 0:
            group.append(1)
        else:
            group.append(0)

    other = []
    for usr_limit in limit_mask["other"]:
        if (limit & usr_limit) is not 0:
            other.append(1)
        else:
            other.append(0)

    re_mes_body["limit"] = {}
    re_mes_body["limit"]["user"] = user
    re_mes_body["limit"]["group"] = group
    re_mes_body["limit"]["other"] = other


    return re_mes_body

def modify_dir(body):

    #得到目录名
    dir_name = os.path.join(fs_root_dir, body["dir_name"])
    if not os.path.exists(dir_name):
        re_mes_body = {"echo": "no"}
        return re_mes_body

    #计算权限值
    limit_value = body["limit"]["user"][0] * stat.S_IRUSR \
                + body["limit"]["user"][1] * stat.S_IWUSR \
                + body["limit"]["user"][2] * stat.S_IXUSR \
                + body["limit"]["user_group"][0] * stat.S_IRGRP \
                + body["limit"]["user_group"][1] * stat.S_IWGRP \
                + body["limit"]["user_group"][2] * stat.S_IXGRP \
                + body["limit"]["other"][0] * stat.S_IROTH \
                + body["limit"]["other"][1] * stat.S_IWOTH \
                + body["limit"]["other"][2] * stat.S_IXOTH

    os.chmod(dir_name, limit_value)

    #set directory quota
    quota_cmd = 'setfattr -n ceph.quota.max_bytes -v ' + body["quota"]["max_file_size"] + ' ' + dir_name
    os.system(quota_cmd)

    quota_cmd = 'setfattr -n ceph.quota.max_files -v ' + body["quota"]["max_file_num"] + ' ' + dir_name
    os.system(quota_cmd)

    re_mes_body={"echo":"ok"}

    return re_mes_body


#modify nfs config
def nfs_conf_modify(body):
    export = body["export"]
    fo =open("/etc/nfs.conf", "wb")

    for export_dir in export:
        #write EXPORT
        #{
        fo.write("EXPORT\n{\n")
        #export id
        export_id = "Export_ID=" + str(export_dir["export_id"]) + ";"
        fo.write("\t")
        fo.write(export_id)
        fo.write("\n")
        #path
        path = "Path=" + export_dir["path"] + ";"
        fo.write("\t")
        fo.write(path)
        fo.write("\n")
        #Pseudo
        Pseudo = "Pseudo=" + export_dir["path"] + ";"
        fo.write("\t")
        fo.write(Pseudo)
        fo.write("\n")
        #Access_Type
        access_type = "Access_Type=RO" + ";"
        fo.write("\t")
        fo.write(access_type)
        fo.write("\n")
        #Transports
        Transports = "Transports=TCP;"
        fo.write("\t")
        fo.write(Transports)
        fo.write("\n")
        #FSAL { Name = CEPH; }
        FSAL = "\tFSAL\n\t{\n\t\tName=CEPH;\n\t}\n"
        fo.write(FSAL)
        for client in export_dir["clients"]:
            #clients
            fo.write("\tCLIENT\n")
            #{
            fo.write("\t{\n")
            #ip
            client_addr = "Clients="
            i = 0
            for ip in client:
                if i == 0:
                    client_addr += ip
                else:
                    client_addr += "," + ip
                i += 1
            client_addr += ";"
            fo.write("\t\t")
            fo.write(client_addr)
            fo.write("\n")
            #Squash
            Squash = "Squash=" + client["Squash"] + ";"
            fo.write("\t\t")
            fo.write(Squash)
            fo.write("\n")
            # Access_Type
            Access_Type = "Access_Type=" + client["Access_Type"] + ";"
            fo.write("\t\t")
            fo.write(Access_Type)
            fo.write("\n")
            #}
            fo.write("\t}\n")
        #}
        fo.write("}\n")

    fo.close()

    re_mes_body = {"echo": "ok"}
    return re_mes_body

'''
fun_table is message type function function table
so fun_table[1][1]==(NSM_AGENT_EXECUTE  1 //执行操作) ->  (NSM_AGENT_EXECUTE_CREAT_DIR 1 //create dir)
'''
fun_table = [
                [''],
                ['', 'create_dir', 'list_dir', 'dir_details', 'modify_dir', 'nfs_conf_modify'],
                [''],
                [''],
                ['']
            ]