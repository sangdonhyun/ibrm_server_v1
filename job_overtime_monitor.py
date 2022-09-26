# -*- coding: utf-8 -*-
import os
import sys
import datetime
import ibrm_dbms
import time

reload(sys)
sys.setdefaultencoding('utf-8')

class ov_mon():
    def __init__(self):
        self.rdb=ibrm_dbms.fbrm_db()

    def get_set_status(self,tg_job_dtl_id):
        s, e, t = '-', '-', '-'

        query = """
        SELECT 
         alarm_tg
        FROM store.hs_job_dtl hjd 
        LEFT OUTER JOIN master.mst_job mj
        ON hjd.job_id = mj.job_id 
        LEFT OUTER JOIN master.mst_shell ms 
        ON mj.sh_id = ms.sh_id 
        WHERE tg_job_dtl_id='{}'
        """.format(tg_job_dtl_id)
        ret=self.rdb.get_row(query)
        alarm_tg=ret[0][0]
        s,e,t = alarm_tg[0],alarm_tg[1],alarm_tg[2]
        return s,e,t

    def evt_send(self,tg_job_dtl_id,tg='s'):
        query="""
        SELECT 
         hs_job_dtl_id,hs_job_mst_id, tg_job_mst_id ,tg_job_dtl_id ,hjd.job_id,job_exec_dt ,job_stat ,timeout,mj.work_div_1,ms.sh_id,ms.svr_id,ms.db_id,sh_file_nm,db_nm ,job_stt_dt,mj.alarm_tg,hjd.reg_dt,hjd.mod_dt 
        FROM store.hs_job_dtl hjd 
        LEFT OUTER JOIN master.mst_job mj
        ON hjd.job_id = mj.job_id 
        LEFT OUTER JOIN master.mst_shell ms 
        ON mj.sh_id = ms.sh_id 
        WHERE tg_job_dtl_id='{}'
        """.format(tg_job_dtl_id)
        try:
            print query
            job_info={}
            ret=self.rdb.get_row(query)[0]
            titles = "hs_job_dtl_id,hs_job_mst_id, tg_job_mst_id ,tg_job_dtl_id ,job_id,job_exec_dt ,job_stat ,timeout,work_div_1,sh_id,svr_id,db_id,sh_file_nm,db_nm ,job_stt_dt,alarm_tg,reg_dt,mod_dt".split(',')
            for i in range(len(titles)):
                job_info[titles[i].strip()] = ret[i]
            print 'job_info info :',job_info
            print 'tg :',tg

            self.set_event(job_info,tg=tg)
        except Exception as e:
            print '#'*50
            print '#' * 50
            print str(e)
            print '#' * 50
            print '#' * 50
    def get_starting_job(self):
        dt=datetime.datetime.now() - datetime.timedelta(days=25)
        dt_format=dt.strftime('%Y%m%d')
        query=""" 

            SELECT 
         hs_job_dtl_id,hs_job_mst_id, tg_job_mst_id ,tg_job_dtl_id ,hjd.job_id,job_exec_dt ,job_stat ,timeout,mj.work_div_1,ms.sh_id,ms.svr_id,ms.db_id,sh_file_nm,db_nm ,job_stt_dt,mj.alarm_tg,hjd.reg_dt,hjd.mod_dt 
        FROM store.hs_job_dtl hjd 
        LEFT OUTER JOIN master.mst_job mj
        ON hjd.job_id = mj.job_id 
        LEFT OUTER JOIN master.mst_shell ms 
        ON mj.sh_id = ms.sh_id 
        WHERE job_stat IN ('Running','Starting')   AND job_exec_dt > '{}'
        AND tg_job_dtl_id NOT IN (SELECT tg_job_dtl_id FROM event.evt_log WHERE EVT_CODE ='JOB_RUN_OVERTIME' )
        
        """.format(dt_format)
        print query
        ret_set=self.rdb.get_row(query)
        return ret_set



    def main(self):
        ret_set=self.get_starting_job()
        """
        hs_job_dtl_id,hs_job_mst_id,tg_job_mst_id ,tg_job_dtl_id ,job_id,job_exec_dt ,job_stat,use_yn ,timeout,work_div_1,work_div_1,sh_id,svr_id,db_id,sh_file_nm,db_nm        
        """
        titles = "hs_job_dtl_id,hs_job_mst_id, tg_job_mst_id ,tg_job_dtl_id ,job_id,job_exec_dt ,job_stat ,timeout,work_div_1,sh_id,svr_id,db_id,sh_file_nm,db_nm ,job_stt_dt,alarm_tg,reg_dt,mod_dt".split(',')



        print ret_set

        if not ret_set == []:
            print 'count:', len(ret_set)
            for ret in ret_set:
                job_info = {}
                for i in range(len(titles)):
                    job_info[titles[i].strip()] = ret[i]
                print job_info
                s, e, t = job_info['alarm_tg'][0], job_info['alarm_tg'][1], job_info['alarm_tg'][2]

                print s,e,t
                timeout = job_info['timeout']
                start_dt= job_info['reg_dt']

                now = datetime.datetime.now()
                ov_dt = datetime.datetime.strptime(start_dt,'%Y%m%d%H%M%S') + datetime.timedelta(minutes=timeout)
                # - datetime.timedelta(mins=int(timeout))
                if now > ov_dt:
                    print '-'*50
                    print job_info['sh_file_nm']
                    print start_dt,timeout
                    print now,ov_dt,now > ov_dt
                    print '-' * 50
                print start_dt, timeout, ov_dt
                if t == 'T':
                    if not timeout == '0':
                        if now > ov_dt:
                            self.set_event(job_info,'t')






    def set_event(self,job_info,tg='t'):
        print 'job_info :',job_info
        print 'tg :',tg
        print job_info.keys()
        print 'db_id' in job_info.keys()
        evt_info = {}
        evt_info['job_id'] = job_info['job_id']
        evt_info['tg_job_dtl_id'] = job_info['tg_job_dtl_id']
        log_dt = datetime.datetime.now().strftime('%Y%m%d')
        evt_info['log_dt'] = log_dt
        evt_info['svr_id'] = job_info['svr_id']
        evt_info['db_id'] = job_info['db_id']
        evt_info['sys_type'] = 'SCH'
        evt_info['evt_type'] = 'JOB_RUN_OVERTIME'
        evt_info['evt_code'] = 'JOB_RUN_OVERTIME'
        evt_info['evt_dtl_type'] = ''
        evt_info['evt_lvl'] = 'WARNNING'  # ERROR/CRITICAL/INFO
        if tg=='t':
            evt_info['evt_msg'] = 'DB ({}) FILE ({})  LIMIT ORVER TIME EVENT  JOB START TIME :{}  ,OVER TIME {} '.format(job_info['db_nm'],job_info['sh_file_nm'],job_info['reg_dt'],job_info['timeout'])
        elif tg=='s':
            evt_info['evt_msg'] = 'DB ({}) FILE ({})  JOB START EVENT   JOB START TIME :{}  '.format(job_info['db_nm'], job_info['sh_file_nm'], job_info['reg_dt'])
            evt_info['evt_type'] = 'JOB_START_EVENT'
            evt_info['evt_code'] = 'EVT_J_START'
        elif tg=='e':
            evt_info['evt_msg'] = 'DB ({}) FILE ({})  JOB END EVENT   JOB END TIME :{}'.format(job_info['db_nm'],job_info['sh_file_nm'],job_info['mod_dt'])
            evt_info['evt_type'] = 'JOB_START_EVENT'
            evt_info['evt_code'] = 'EVT_J_END'
        # evt_info['evt_cntn'] = '{WORK_DIV_1} {WORK_DIV_2} {JOB_NM} {SH_NM} {SVR_NM} {DB_NM}'.format(
        #     WORK_DIV_1=work_div_1, WORK_DIV_2=work_div_2, JOB_NM=job_nm, SH_NM=sh_nm, SVR_NM=svr_nm, DB_NM=db_nm)
        evt_info['dev_type'] = 'DB'
        evt_info['act_yn'] = 'Y'
        evt_info['reg_usr'] = 'SYS'
        evt_info['reg_dt'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        evt_info_list = []
        print 'evt :'
        print 'a'*40
        print 'evt_info :',evt_info
        evt_info_list.append(evt_info)
        # print 'evt_info :', evt_info
        table_name = 'event.evt_log'
        print 'table_name :',table_name
        self.rdb.dbInsertList(evt_info_list, table_name)
        print 'b'*40


if __name__=='__main__':
    cnt=1
    while True:
        ov_mon().main()
        print 'cnt :',cnt
        cnt=cnt+1
        time.sleep(60*5)