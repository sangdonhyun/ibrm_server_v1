#-*- coding: utf-8 -*-
'''
Created on 2014. 4. 16.

@author: Administrator
'''
import threading, time
import socket
import os
import ast
import glob
import ConfigParser
import random
import ibrm_dbms

class SocketSender():
    def __init__(self,HOST,PORT):
        self.dbms = ibrm_dbms.fbrm_db()
        self.HOST = HOST
        self.PORT = int(PORT)

    def shell_detail(self,shell_name):

        # print self.HOST
        # print self.PORT

        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_STATUS'
        data['ARG'] = {}
        data['ARG']['shell_name'] = shell_name

        print data
        # Create a socket (SOCK_STREAM means a TCP socket)
        return_data=self.socket_send(data)
        print 'return data:',return_data
        return return_data


    def socket_send(self,data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sBit = False
        received = ''
        try:
            # Connect to server and send data
            sock.connect((self.HOST, self.PORT))
            sock.sendall(str(data) + "\n")

            # Receive data from the server and shut down
            received = sock.recv(1024)
            print 'recv :', received
            # if received == 'READY':
            #     cmd = 'agent_shell_list'
            #     sock.sendall(cmd)
            print 'ok'

            # recv_data=sock.recv(1024)
            # print 'recv_data :',recv_data

            sBit = True
        except socket.error as e:
            sBit = False
            print e
        finally:
            sock.close()
        return received

    def shell_list(self):

        print self.HOST
        print self.PORT

        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_SHELL_LIST'
        data['ARG'] = {}
        data['ARG']['shell_name'] = 'UPGR_Incr_Level0.sh'
        print data
        return self.socket_send(data)


    def log_update(self,job_info):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'LOG_UPDATE'
        data['ARG'] = job_info
        job_status = self.socket_send(data)


    def agent_health_check(self,job_info):
        pass

    def jos_status(self,job_info):

        print self.HOST
        print self.PORT
        data={}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_STATUS'
        data['ARG'] = job_info


        print data

        job_status =  self.socket_send(data)
        print job_status

    def send(self):

        HOST, PORT = "121.170.193.196", 53001


        data={}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_JOB_EXCUTE'
        data['ARG'] = {}
        data['ARG']['job_id'] = '00001'
        data['ARG']['db_nme'] = 'UPGR'
        data['ARG']['shell_name'] = 'UPGR_Incr_Level0.sh'
        data['ARG']['shell_type'] = 'level0'

        print data
        # Create a socket (SOCK_STREAM means a TCP socket)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sBit = False

        try:
            # Connect to server and send data
            sock.connect((HOST, PORT))
            sock.sendall(str(data) + "\n")

            # Receive data from the server and shut down
            received = sock.recv(1024)
            print 'recv :',received
            # if received == 'READY':
            #     cmd = 'agent_shell_list'
            #     sock.sendall(cmd)
            print 'ok'

            # recv_data=sock.recv(1024)
            # print 'recv_data :',recv_data



            sBit = True
        except socket.error as e:
            sBit = False
            print e
        finally:
            sock.close()

        return sBit

    def main(self):

        sBit = self.shell_detail()
        print 'sBit' , sBit



if __name__ == '__main__':

    HOST='121.170.193.207'
    PORT=53002



    job_status = {'status': 'COMPLETED', 'job_id': 65, 'tg_job_dtl_id': 258, 'pid': '16223', 'elapsed_seconds': '518', 'job_st': 'Running', 'log_file': '/u01/LOGS/IBRM/IBRM_2020_1111/IBRM_RMAN_Level1_2020_1111_17.log', 'log_contents': 'data/65_IBRM_RMAN_Level1_2020_1111_17.log'}
    log_info = {'FLETA_PASS': 'kes2719!', 'CMD': 'LOG_UPDATE',
                'ARG': job_status}
    log_info = {'FLETA_PASS': 'kes2719!', 'CMD': 'LOG_UPDATE',
                'ARG': job_status}
    SocketSender(HOST, PORT).log_update(log_info)