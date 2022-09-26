#-*- coding: utf-8 -*-
#!/usr/bin/python

import SocketServer
import subprocess
import sys
from threading import Thread
import time
import re
import ast
import os
import common
import shutil
import ConfigParser
#import ibrm_logger
import datetime
import logging
import glob
import ibrm_dbms
import job_state
import log_control

com = common.Common()
dbms = ibrm_dbms.fbrm_db()
log = log_control.LogControl()

############################################################################

def getInfo():
    cfg=ConfigParser.RawConfigParser()
    cfgFile=os.path.join('config','config.cfg')
    cfg.read(cfgFile)
    HOST,PORT=cfg.get('socket','HOST'),cfg.get('socket','PORT')
    return HOST,int(PORT)
    self.st = job_state.ibrm_job_stat()


def shell_path():
    return "/u01/SCRIPTS/Database/IBRM/RMAN/SCHEDULE"

############################################################################
'''  One instance per connection.
     Override handle(self) to customize action. '''

class TCPConnectionHandler(SocketServer.BaseRequestHandler):




    def getCfg(self):
        
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','config.cfg')
        cfg.read(cfgFile)
        return cfg
    def get_job_already_run(self,job_status):
        print 'job_status :',job_status
        job_id = job_status['job_id']
        memo = job_status['memo']
        tg_job_dtl_id = job_status['tg_job_dtl_id']
        try:
            log.logdata('DAEMON', 'ERROR', '60005', str(job_status))
        except Exception as e:
            print str(e)

        job_state.ibrm_job_stat().job_aleady_exist(job_id,tg_job_dtl_id,memo)

        job_state.ibrm_job_stat().evt_ins(job_status)
        # query = """UPDATE store.hs_job_dtl SET
        # job_stat ='FAIL',
        # memo = '{MEMO}
        # WHERE job_id = '{JOB_ID}'
        # """.format(JOB_ID=job_id,MEMO=memo)
        # print 'query :',query
        # dbms.queryExec(query)
        return 'OK'


    def job_complete(self,job_status):
        job_id = job_status['job_id']
        tg_job_dtl_id = job_status['tg_job_dtl_id']
        memo = job_status['memo']
        log.LogControl().logdata('DAEMON', 'INFO', '60003', str(job_status))
        job_state.ibrm_job_stat().job_submit_fila(job_id,tg_job_dtl_id,memo)


    def job_sumit_fial(self,job_status):
        job_id = job_status['job_id']
        tg_job_dtl_id = job_status['tg_job_dtl_id']
        memo = job_status['memo']
        log.LogControl().logdata('DAEMON', 'INFO', '60004', str(job_status))
        job_state.ibrm_job_stat().job_submit_fail(job_status)

    def get_list(self):
        pylist=glob.glob(os.path.join('SHELL','*.sh'))
        shell_list=[]
        for s in pylist:
            print s
            shell_dict={}
            name = os.path.basename(s)
            a_time = time.ctime(os.path.getatime(s))
            c_time = time.ctime(os.path.getatime(s))
            m_time = time.ctime(os.path.getatime(s))
            atime_obj = datetime.datetime.strptime(a_time, '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')
            ctime_obj = datetime.datetime.strptime(c_time, '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')
            mtime_obj = datetime.datetime.strptime(m_time, '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')
            shell_dict['name'] = name
            shell_dict['a_time'] = atime_obj
            shell_dict['c_time'] = ctime_obj
            shell_dict['m_time'] = mtime_obj
            shell_list.append(shell_dict)

        return {'shell_list':shell_list}

    def job_update(self,job_status):
        job_id = job_status['job_id']
        job_state.ibrm_job_stat().job_update(job_status)

    def job_submit_fail(self,log_return_data):
        print 'log update 111',log_return_data
        job_state.ibrm_job_stat().job_submit_fail(log_return_data)

    def job_log_update(self,log_return_data):
        print 'log update 111',log_return_data
        job_state.ibrm_job_stat().job_log_update(log_return_data)

    def job_status_shell_only(self,log_return_data):
        print 'job_status_shell_only :',log_return_data
        job_state.ibrm_job_stat().job_status_shell_only(log_return_data)

    def job_status_insert(self,job_status):
        print 'job_status :',job_status
        job_state.ibrm_job_stat().job_status_insert(job_status)

    def get_rman_tag(self,job_status):
        print 'job_status :',job_status
        return_data=job_state.ibrm_job_stat().get_rman_tag(job_status)
        return return_data

    def handle(self):
        
        # log = ibrm_logger.ibrm_logger().logger('ibrm_sched_log')
        # self.request is the client connection
        data = self.request.recv(1024)  # clip input at 1Kb
        print data
        cip,cport= self.client_address
#         print type(self.request),self.request,dir(self.request)
        msg='[%s] CONNECT PORT %s'%(cip,cport)
        print msg
        #f_list_str = str(self.get_list())



        try:

            info=ast.literal_eval(data)

            if info['FLETA_PASS'] != 'kes2719!':
                self.request.close()
                msg = self.client_address + ' PASSWORD INCOREECT'
                # log.info(msg)
            else:

                cmd = info['CMD']
                print 'CMD :',cmd
                # log.info(cmd)
                if cmd == 'JOB_STATUS':

                    ret_args = info['ARG']
                    print 'JOB STATUS :',ret_args
                    # log.info('job status :')
                    # log.info(str(ret_args))
                    if data is not None:
                        print 'ret_args',ret_args
                        self.job_status_insert(ret_args)
                        self.request.send(str(ret_args))
                if cmd == 'JOB_SUBMIT_FAIL':
                    ret_args = info['ARG']

                    if data is not None:
                        print 'ret_args : ',ret_args
                        self.job_sumit_fial(ret_args)
                        self.request.send(str(ret_args))
                if cmd == 'JOB_COMPLETE':
                    ret_args = info['ARG']

                    if data is not None:
                        print 'ret_args : ',ret_args
                        self.job_sumit_fial(ret_args)
                        self.request.send(str(ret_args))


                if cmd == 'JOB_UPDATE':
                    ret_args = info['ARG']
                    if data is not None:
                        print 'ret_args : ',ret_args
                        query = self.job_update(ret_args)

                        self.request.send(str(ret_args))
                if cmd == 'LOG_UPDATE':
                    ret_args = info['ARG']
                    if data is not None:
                        print 'LOG_UPDATE , log_args : ',ret_args
                        self.job_log_update(ret_args)
                        self.request.send(str(ret_args))

                if cmd == 'JOB_SUBMIT_FAIL':
                    ret_args = info['ARG']
                    if data is not None:
                        print 'LOG_UPDATE , log_args : ',ret_args
                        self.job_log_update(ret_args)
                        self.request.send(str(ret_args))

                if cmd == 'JOB_STATUS_SHELL_ONLY':
                    ret_args = info['ARG']

                    if data is not None:
                        print 'LOG_UPDATE , log_args : ',ret_args
                        self.job_status_shell_only(ret_args)
                        self.request.send(str(ret_args))

                if cmd == 'AGENT_HEALTH_CHECK':
                    ret_args = info['ARG']

                    if data is not None:
                        print 'AGENT_HEALTH_CHECK , log_args : ',ret_args
                        self.job_log_update(ret_args)
                        self.request.send(str(ret_args))

                if cmd == 'JOB_MONITOR':
                    ret_args = info['ARG']

                    if data is not None:
                        print 'JOB_MONITOR , log_args : ',ret_args
                        # self.job_log_update(ret_args)
                        yyyymmdd = datetime.datetime.now().strftime('%Y%m%d')
                        data_file = os.path.join('data','job_monitor_{YYYYMMDD}.txt'.format('YYYYMMDD=yyyymmdd'))
                        with open('data_file','a') as  f:
                            f.write(str(ret_args))

                        self.request.send(str(ret_args))

                if cmd == 'JOB_ALREADY_RUN':
                    ret_args = info['ARG']
                    if data is not None:
                        print 'JOB_ALREADY_RUN ret_args : ',ret_args
                        self.get_job_already_run(ret_args)
                        print '#'*40
                        self.request.send(str(ret_args))

                if cmd == 'GET_RMAN_TAG':
                    ret_args = info['ARG']
                    if data is not None:
                        print 'GET_RMAN_TAG ret_args : ',ret_args
                        return_data=self.get_rman_tag(ret_args)
                        print '#'*40
                        self.request.send(str(return_data))


                print datetime.datetime.now()



                self.request.close()
        except Exception as e:
            print str(e)
            self.request.close()
############################################################################

class Server(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    # Ctrl-C will cleanly kill all spawned threads
    daemon_threads = True
    # much faster rebinding
    allow_reuse_address = True

    def __init__(self, server_address, RequestHandlerClass):
        SocketServer.TCPServer.__init__(\
        self,\
        server_address,\
        RequestHandlerClass)
        sip,sport= server_address




if __name__ == "__main__":
    HOST,PORT=getInfo()
    msg= com.getHeadMsg('START iBRM Server Daemon (%s,%s)'%(HOST,PORT))
    print msg
    #log.info('ibrm_daemon start {} {}'.format(HOST,PORT))
    server = Server((HOST, PORT), TCPConnectionHandler)
    # terminate with Ctrl-C
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)


