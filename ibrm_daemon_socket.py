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
import ibrm_logger
import datetime
import glob

log = ibrm_logger.ibrm_logger().logger()
com = common.Common()

############################################################################

def getInfo():
    cfg=ConfigParser.RawConfigParser()
    cfgFile=os.path.join('config','config.cfg')
    cfg.read(cfgFile)
    HOST,PORT=cfg.get('socket','HOST'),cfg.get('socket','PORT')
    return HOST,int(PORT)


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

    def get_list(self):
        pylist=glob.glob(os.path.join('SHELL','*.sh'))
        shell_list=[]
        for s in pylist:

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

    def handle(self):
        
                
        # self.request is the client connection
        data = self.request.recv(1024)  # clip input at 1Kb
        print data
        cip,cport= self.client_address
#         print type(self.request),self.request,dir(self.request)
        msg='[%s] CONNECT PORT %s'%(cip,cport)
        print msg
        f_list_str = str(self.get_list())



        try:

            info=ast.literal_eval(data)

            if info['FLETA_PASS'] != 'kes2719!':
                self.request.close()
                msg=self.client_address + ' PASSWORD INCOREECT'
                com.logger.info(msg)
            else:

                cmd=info['CMD']

                if cmd == 'AGENT_SHELL_LIST':
                    if data is not None:
                        print f_list_str
                        self.request.send(f_list_str)

                elif cmd == 'AGENT_SHELL_DETAIL':
                    shell_name = info['NAME']
                    print 'shell_name' ,shell_name,os.path.isfile(os.path.join('SHELL',shell_name))

                    with open(os.path.join('SHELL',shell_name)) as f:
                        shell_detail = f.read()

                    send_data = "{'shell_detail':{}".format(shell_detail)
                    print send_data,type(send_data)
                    self.requst.send(send_data)
                elif cmd == 'AGENT_JOB_EXCUTE':
                    data = info['ARG']
                    print 'DATA' ,data
                    self.requst.send(data)
                elif cmd == 'AGENT_JOB_EXCUTE':
                    data = info['ARG']
                    print 'DATA' ,data
                    shell_name=data['shell_name']

                    self.requst.send(data)



                # print type(f_list_str)
                # self.reauest.sendall(f_list_str)

                print datetime.datetime.now()



                self.request.close()
        except:
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
    msg= com.getHeadMsg('START FLETA FILE RECV(%s,%s)'%(HOST,PORT))
    print msg
    log.info('ibrm_daemon start {} {}'.format(HOST,PORT))
    server = Server((HOST, PORT), TCPConnectionHandler)
    # terminate with Ctrl-C
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)

