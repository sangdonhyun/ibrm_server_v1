#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    Created on 2016.05.17
    @author: jhbae
'''
import time
import os
import sys
import ConfigParser

s_agent_path = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])))


# CONFIG_LOG_CODE = s_agent_path + "/config/logcode.cfg"



class LogControl(object):
    def __init__(self):
        self.logPath=self.getLogPath()
        CONFIG_LOG_CODE = os.path.join(s_agent_path ,'config','logcode.cfg')
        self.o_log_code = ConfigParser.ConfigParser()
        self.o_log_code.read(CONFIG_LOG_CODE)
    def getLogPath(self):
        
        cfg=ConfigParser.RawConfigParser()
        cfgFile=os.path.join(s_agent_path,'config','config.cfg')
        cfg.read(cfgFile)
        
        try:
            LOG_PATH= cfg.get('log','logPath').strip()
            
            if LOG_PATH == '':
                LOG_PATH = s_agent_path + "/logs/"
        except:
            LOG_PATH = s_agent_path + "/logs/"
        if not os.path.isdir(LOG_PATH):
            os.mkdir(LOG_PATH)
        return LOG_PATH
    
    def logdata(self, s_file_name, s_file_type, s_code='', s_refer_val=''):
        if os.path.isdir(self.logPath) == False:
            os.mkdir(self.logPath)
        s_refer_val = str(s_refer_val)
        s_data = "[%s] " % (s_code) + self.o_log_code.get('CODE', s_code)
        if s_refer_val != '':
            s_data = s_data + " ==> " + s_refer_val

        today = time.strftime("%Y%m%d", time.localtime(time.time()))
        # filefullname = LOG_PATH + s_file_name + "_" + str(today) + "_" + s_file_type + ".log"
        filefullname = self.logPath +'\\'+ str(today) + "_" + s_file_name + "_" + s_file_type + ".log"

        fp = file(filefullname, 'a+')
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        comment = "[" + str(now) + "] " + str(s_data) + "\n"
        print comment
        print filefullname
        fp.write(comment)
        fp.close()

if __name__=='__main__':
    job_status = {'memo': 'this job (test.sh)  is already running', 'job_id': 157, 'tg_job_dtl_id': 208452}
    LogControl().logdata('DAEMON', 'ERROR', '60005', str(job_status))