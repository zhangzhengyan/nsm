#!/usr/bin/python
# -*- coding: UTF-8 -*-

import subprocess
import commands
import os
import stat

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
fun_id = {"NSM_UNDEFINE":0, "NSM_AGENT_EXECUTE_CREAT_DIR":1}

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
    	        "max_file_size":"5G"
    	    }
        }
    }
    '''

    #得到目录名
    dir_name = body["dir_name"]
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

'''
fun_table is message type function function table
so fun_table[1][1]==(NSM_AGENT_EXECUTE  1 //执行操作) ->  (NSM_AGENT_EXECUTE_CREAT_DIR 1 //create dir)
'''
fun_table = [[''], ['', 'create_dir'], [''], [''], ['']]