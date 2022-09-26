#-*- coding:utf-8 -*-
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
import datetime

class SocketSender():
    def __init__(self,HOST,PORT):
        self.dbms = ibrm_dbms.fbrm_db()
        self.HOST = HOST
        self.PORT = int(PORT)

    def shell_detail(self,db_name,shell_name):

        # print self.HOST
        # print self.PORT

        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_SHELL_DETAIL'
        data['ARG'] = {}
        data['ARG']['db_name'] = db_name
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

    def shell_list(self,db_name):

        print self.HOST
        print self.PORT

        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_SHELL_LIST'
        data['ARG'] = {}
        data['ARG']['db_name'] = db_name
        print data
        return self.socket_send(data)


    def jos_test(self,job_info):

        print self.HOST
        print self.PORT

        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_JOB_TEST'
        data['ARG'] = job_info

        return_data = self.socket_send(data)
        print 'return data:', return_data
        return return_data


    def jos_test(self):
        job_info={'job_id': 13, 'shell_name': 'IBRM_Incr_Level0.sh', 'shell_path': '/u01/SCRIPTS/Database/IBRM/RMAN/SCHEDULE', 'shell_type': 'INCR', 'db_name': 'IBRM', 'ora_home': '/u01/app/oracle/product/18.0.0/dbhome_1', 'tg_job_dtl_id': 8, 'job_exec_dt': '20201023', 'svr_ip': '121.170.193.200', 'ora_sid': 'ibrm', 'tg_job_mst_id': 13}
        print self.HOST
        print self.PORT

        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_JOB_EXCUTE'
        data['ARG'] = job_info

        return_data = self.socket_send(data)
        print 'return data:', return_data
        query = ""
        # self.dbms.dbQeuryIns()
        return return_data
    def jos_excute(self,job_info):

        print self.HOST
        print self.PORT

        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_JOB_EXCUTE'
        data['ARG'] = job_info

        return_data = self.socket_send(data)
        print 'return data:', return_data
        query = ""

        return return_data

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

    def get_job(self):
        sql="""
SELECT 
  tjm.tg_job_mst_id  -- 0[일작업ID] 일일 대상 작업 MST ID   
  ,tjm.job_exec_dt   -- 1[작업실행일자] DB 일자 or SYSTEM   
  ,tjd.tg_job_dtl_id -- 2[일작업상세ID]    
  ,tjd.run_type      -- 3[실행유형] Run, Stop, ReRun, Skip, Force Run    
  ,mj.exec_time      -- 4[실행시각] 0000~2359       
  ,mj.timeout        -- 5[제한시간] 작업 Timeout 시각(알람 설정 시 사용)    
  ,mj.alarm_yn       -- 6[알림여부]    
  ,ms.svr_nm         -- 7[서버명] 서버명                           
  ,ms.ip_addr        -- 8[IP 정보] ipv4                        
  ,ms.db_nm          -- 9[DB 명]                              
  ,ms.back_type      -- 10[백업유형] incr / arch / full / merge   
  ,ms.sh_file_nm     -- 11[쉘 파일명] Real 파일명                    
  ,ms.sh_path        -- 12[쉘 경로] 쉘스크립트 경로                     
  ,moi.ora_sid       -- 13SID     
  ,moi.ora_home      -- 14ORACLE HOME
  ,moi.db_name       -- 15DB명 
  ,tjm.tg_job_mst_id -- 16[일작업ID] 일일 대상 작업 MST ID   
  ,tjm.job_exec_dt   -- 17[작업실행일자] DB 일자 or SYSTEM   
  ,tjd.tg_job_dtl_id -- 18[일작업상세ID]    
  ,tjd.job_id        -- 19[작업ID] JOB ID Unique 
FROM 
  store.tg_job_mst tjm 
  INNER JOIN 
  store.tg_job_dtl tjd 
  ON ( 
      tjm.tg_job_mst_id = tjd.tg_job_mst_id 
      AND tjd.use_yn='Y' 
      AND tjd.run_type ='Run'
    )
  INNER JOIN 
  master.mst_job mj 
  ON ( 
    mj.use_yn ='Y'
    AND mj.job_id = tjd.job_id        
  )
  INNER JOIN 
  master.mst_shell ms 
  ON (
    ms.use_yn ='Y'
    AND ms.sh_id = mj.sh_id      
  )
  INNER JOIN 
  master.master_svr_info msi 
  ON (msi.svr_id = ms.svr_id )
  INNER JOIN 
  master.master_ora_info moi 
  ON (
    moi.svr_id = msi.svr_id 
    AND moi.ora_id = ms.db_id 
  )  
WHERE 
  tjm.use_yn ='Y'
 -- AND tjm.job_exec_dt = to_char(now(), 'YYYYMMDD') -- 실행일자로 변경하여 사용   
  AND tjm.job_exec_dt = '20201023' -- 실행일자로 변경하여 사용   

and tjd.job_id  = 19
"""
        ret= self.dbms.getRaw(sql)[0]
        # print ret
        job_id = ret[0]
        svr_ip = ret[8]

        shell_type = ret[10]
        shell_name = ret[11]
        shell_path = ret[12]
        ora_home = ret[14]
        ora_sid = ret[13]
        db_name = ret[15]
        job_info={}
        job_info['job_id'] = job_id
        job_info['svr_ip'] = svr_ip
        job_info['shell_type'] = shell_type
        job_info['shell_name'] = shell_name
        job_info['shell_path'] = shell_path
        job_info['ora_home'] = ora_home
        job_info['db_name'] = db_name
        job_info['ora_sid'] = ora_sid
        job_info['tg_job_mst_id'] =  ret[16]
        job_info['job_exec_dt'] = ret[17]
        job_info['tg_job_dtl_id'] = ret[18]
        job_info['job_id'] = ret[19]
        job_info['job_exec_dt'] = ret[1]


        return job_info

    def set_job(self,job_info):
        now = datetime.datetime.now()
        """

        hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id, tg_job_dtl_id, job_id, job_exec_dt, job_stt_dt, job_end_dt, db_type, ssn_rec_id, ssn_stmp, run_type, job_stat, rm_bk_stat, prgrs_time, upd_stat, memo, use_yn, mod_usr, mod_dt, reg_usr, reg_dt)
        1	           1	           1	         1	            3	     20201015	 20201015170000	20201015180000	Catalog	12345	null	Run	End-OK	null	0	N	테스트백업.	Y	SYS	20201015180749	test	20201015180749

hs_job_dtl_id SERIAL NOT NULL,
 hs_job_mst_id integer NOT NULL,
 tg_job_mst_id integer NOT NULL,
 tg_job_dtl_id integer NOT NULL,
 job_id integer NOT NULL,
 job_exec_dt varchar(8) NOT NULL,
 job_stt_dt varchar(14) NOT NULL,
 job_end_dt varchar(14) NOT NULL,
 db_type varchar(10) NOT NULL,
 ssn_rec_id integer NULL,
 ssn_stmp varchar(20) NULL,
 run_type varchar(20) NOT NULL,
 job_stat varchar(20) NOT NULL,
 rm_bk_stat varchar(10) NULL,
 prgrs_time integer NULL DEFAULT 0,
 upd_stat varchar(10) NOT NULL DEFAULT 'N',
 memo varchar(200) NULL,
 use_yn varchar(1) NOT NULL DEFAULT 'Y',
 mod_usr varchar(20) NULL DEFAULT 'SYS',
 mod_dt varchar(14) NULL DEFAULT TO_CHAR(current_timestamp, 'YYYYMMDDHH24MISS'),
 reg_usr varchar(20) NOT NULL,
 reg_dt varchar(14) NOT NULL DEFAULT TO_CHAR(current_timestamp, 'YYYYMMDDHH24MISS'),

        """


        """
         hs_job_mst_id SERIAL NOT NULL,
         tg_job_mst_id integer NOT NULL,
         job_exec_dt varchar(8) NOT NULL,
         job_stt_dt varchar(14) NOT NULL,
         job_end_dt varchar(14) NULL,
         use_yn varchar(1) NOT NULL DEFAULT 'Y',
         mod_usr varchar(20) NULL DEFAULT 'SYS',
         mod_dt varchar(14) NULL DEFAULT TO_CHAR(current_timestamp, 'YYYYMMDDHH24MISS'),
         reg_usr varchar(20) NOT NULL,
         reg_dt varchar(14) NOT NULL DEFAULT TO_CHAR(current_timestamp, 'YYYYMMDDHH24MISS'),
         hs_job_mst_id, tg_job_mst_id, job_exec_dt, job_stt_dt, job_end_dt, use_yn, mod_usr, mod_dt, reg_usr, reg_dt
         1	1	20201015	20201015170000	20201015180000	Y	SYS	20201015180022	test	20201015180022
        """

        set_job={}

        set_job['tg_job_mst_id'] = job_info['tg_job_mst_id']
        set_job['job_exec_dt']   = job_info['job_exec_dt']
        set_job['job_stt_dt']    = ''
        set_job['job_end_dt']    = ''
        set_job['use_yn']        = 'Y'
        set_job['reg_usr']       = 'SYS'
        set_job['reg_dt']        = now.strftime('%Y%m%d%H%M%S')
        table_name = 'store.hs_job_mst'
        job_list = [set_job]
        job_list.append(set_job)
        self.dbms.dbInsertList(job_list, table_name)


        query = "select hs_job_mst_id from store.hs_job_mst where tg_job_mst_id = '{}'".format(
            job_info['tg_job_mst_id'])
        print query
        rows = self.dbms.getRaw(query)
        hs_job_mst_id = rows[0][0]


        set_job={}
        set_job['tg_job_mst_id'] = job_info['tg_job_mst_id']
        set_job['hs_job_mst_id'] = hs_job_mst_id
        set_job['tg_job_dtl_id'] = job_info['tg_job_dtl_id']
        set_job['job_id']        = job_info['job_id']
        set_job['job_exec_dt']   = job_info['job_exec_dt']
        set_job['db_type']        = 'no catalog'

        set_job['job_stt_dt'] =''
        set_job['job_end_dt'] = ''
        set_job['run_type'] = 'Starting'
        set_job['job_stat'] = ''
        set_job['upd_stat']= ''
        set_job['use_yn'] = 'Y'
        set_job['reg_usr'] = 'SYS'
        set_job['reg_dt']  = now.strftime('%Y%m%d%H%M%S')
        table_name ='store.hs_job_dtl'
        job_list=[]
        job_list.append(set_job)
        self.dbms.dbInsertList(job_list,table_name)


if __name__ == '__main__':

    HOST='121.170.193.200'
    PORT=53001
    shell_name = 'IBRM_Archive.sh'
    shell_type = 'ARCH'
    db_name='IBRM'
    ora_sid = 'ibrm'

    ss = SocketSender(HOST, PORT)

    ss.shell_list(db_name)
    ss.shell_detail(db_name,shell_name)



