#-*- coding: utf-8 -*-
import job_scheduler
import ibrm_daemon_send
import job_state
import datetime
import ibrm_dbms


class test():
    def __init__(self):
        self.db=ibrm_dbms.fbrm_db()
        self.sc = job_scheduler.sched()
        self.to_day = datetime.datetime.now().strftime('%Y%m%d')

    def get_starting_job(self,status):

        to_day = datetime.datetime.now().strftime('%Y%m%d')
        sql = """
SELECT 
    ms.tg_job_dtl_id
    ,ms.tg_job_mst_id
    ,ms.job_stat
    ,ms.job_id
    ,ms.job_exec_dt 
    ,ms2.sh_file_nm 
    ,moi.db_name 
    ,moi.ora_sid 
    ,moi.svr_id
    ,moi.svr_ip_v4 
    ,moi.svr_hostname
    ,job_nm
    ,ms2.back_type 
    ,ms.exec_time 
    FROM(
    SELECT
      tjd.tg_job_dtl_id 
      ,tjm.tg_job_mst_id
      ,hjd.job_stat
      ,hjd.job_id
      ,tjm.job_exec_dt 
      ,tjdl.exec_time 
       FROM 
        store.tg_job_mst tjm 
        INNER JOIN 
        store.tg_job_dtl tjd 
        ON ( 
          tjd.job_exec_dt = tjd.job_exec_dt 
          AND tjd.tg_job_mst_id = tjm.tg_job_mst_id
        )
        LEFT OUTER JOIN   
        store.tg_job_dtl_log tjdl
        ON ( 
          tjdl.job_exec_dt = tjm.job_exec_dt 
          AND tjdl.tg_job_mst_id = tjd.tg_job_mst_id
          AND tjdl.tg_job_dtl_id = tjd.tg_job_dtl_id
        )
        INNER JOIN 
        store.hs_job_dtl hjd 
        ON (
           hjd.tg_job_dtl_id = tjd.tg_job_dtl_id                     
        )
        WHERE tjm.job_exec_dt ='{YYYYMMDD}'
        AND hjd.job_stat ='{STATUS}'
        )  ms
        INNER JOIN master.mst_job mj 
        ON ms.job_id=mj.job_id
        INNER JOIN master.mst_shell ms2 
        ON mj.sh_id = ms2.sh_id 
        INNER JOIN master.master_ora_info moi 
        ON ms2.svr_id = moi.svr_id 
            """.format(YYYYMMDD=to_day,STATUS=status)
        print sql
        ret = self.db.getRaw(sql)
        job_list = []
        for job in ret:
            # print job[4]
            # print job
            """
            0 ms.tg_job_dtl_id
            1,ms.tg_job_mst_id
            2,ms.job_stat
            3,ms.job_id
            4,ms.job_exec_dt 
            5,ms2.sh_file_nm 
            6,moi.db_name 
            7,moi.ora_sid 
            8,moi.svr_id
            9,moi.svr_ip_v4 
            10,moi.svr_hostname
            11,job_nm
            12,ms2.back_type
            13,job_exec_time
            """

            job_info = {}
            job_info['job_id'] = job[3]
            job_info['svr_ip'] = job[9]
            job_info['shell_type'] = job[3]
            job_info['shell_name'] = job[5]
            job_info['db_name'] = job[6]
            job_info['ora_sid'] = job[7]
            job_info['tg_job_mst_id'] = job[1]
            job_info['job_exec_dt'] = job[4]
            job_info['tg_job_dtl_id'] = job[0]
            job_info['job_exec_time'] = job[13]
            job_info['job_type'] = job[12]
            job_list.append(job_info)

        return job_list
    def get_job(self):

        to_day = datetime.datetime.now().strftime('%Y%m%d')
        sql = """
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
              ,CASE 
                WHEN exec_time::time >= (NOW() - INTERVAL '5 MINUTE')::TIME THEN 'Y'
                WHEN exec_time::time < (NOW() - INTERVAL '5 MINUTE')::TIME THEN 'N'
                ELSE 'N'      
               END AS EXEC_YN  
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
              where
               tjm.job_exec_dt = to_char(now(), '{YYYYMMDD}') order by mj.exec_time   desc -- 실행일자로 변경하여 사용   
                   
            --and tjd.tg_job_dtl_id in (select tg_job_dtl_id from store.hs_job_dtl where job_exec_dt = '{YYYYMMDD}' and job_stat = 'Starting')
              
             
            """.format(YYYYMMDD=to_day)
        print sql
        ret = self.db.getRaw(sql)
        job_list = []
        for job in ret:
            # print job[4]
            # print job
            job_id = job[0]
            svr_ip = job[8]
            ex_time = job[4]
            shell_type = job[10]
            shell_name = job[11]
            shell_path = job[12]
            ora_home = job[14]
            ora_sid = job[13]
            db_name = job[15]
            job_yn = job[20]
            job_info = {}
            job_info['job_id'] = job_id
            job_info['svr_ip'] = svr_ip
            job_info['shell_type'] = shell_type
            job_info['shell_name'] = shell_name
            job_info['shell_path'] = shell_path
            job_info['ora_home'] = ora_home
            job_info['db_name'] = db_name
            job_info['ora_sid'] = ora_sid
            job_info['tg_job_mst_id'] = job[16]
            job_info['job_exec_dt'] = job[17]
            job_info['tg_job_dtl_id'] = job[18]
            job_info['job_id'] = job[19]
            job_info['job_exec_dt'] = job[1]
            job_info['job_exec_time'] = ex_time
            job_info['job_yn'] = job_yn
            job_list.append(job_info)

        return job_list
    def job_info(self,target_job):
        tg_job_mst_id = target_job['tg_job_mst_id']
        tg_job_dtl_id = target_job['tg_job_dtl_id']
        job_id = target_job['job_id']

        sql="""select job_stt_dt,job_end_dt,ssn_rec_id,ssn_stmp,job_stat,rm_bk_stat from store.hs_job_dtl where job_exec_dt = '{YYYYMMDD}' and 
            tg_job_mst_id='{TG_JOB_MST_ID}' and tg_job_dtl_id='{TG_JOB_DTL_ID}' and job_id='{JOB_ID}'
        """.format(YYYYMMDD=self.to_day,TG_JOB_MST_ID=tg_job_mst_id,TG_JOB_DTL_ID=tg_job_dtl_id,JOB_ID=job_id)
        print sql
        job_info=self.db.getRaw(sql)[0]
        job_stt_dt = job_info[0]
        job_end_dt = job_info[1]
        ssn_rec_id = job_info[2]
        ssn_stmp = job_info[3]
        job_stat = job_info[4]
        rm_bk_stat = job_info[5]
        print job_info


    def starting_job(self):
        job_list = self.get_starting_job('Starting')
        print '='*50
        print 'START JOB ({})'.format(len(job_list))
        print '='*50
        cnt = 0
        for job in job_list:
            # print job
            print cnt, job['shell_name'], job['job_exec_dt'], job['job_exec_time'],job['job_id'],job['tg_job_dtl_id'],job['job_type']

            cnt = cnt + 1
        print '-'*50
        sel = raw_input('select one>')
        target_job = job_list[int(sel)]
        print target_job
        self.job_info(target_job)
        while True:
            print "1) end complete"
            print "2) end fail"
            print "3) rerun"
            ret=raw_input('>')
            if ret in ['1','2','3']:
                break
        if ret=='3':
            self.rerun_job(target_job)


    def running_job(self):
        job_list = self.get_starting_job('Running')
        print '=' * 50
        print 'START JOB ({})'.format(len(job_list))
        print '=' * 50
        cnt = 0
        for job in job_list:
            # print job
            print cnt, job['shell_name'], job['job_exec_dt'], job['job_exec_time'],job['job_id'],job['tg_job_dtl_id'],job['']
            cnt = cnt + 1
        print '-' * 50
        sel = raw_input('select one>')
        target_job = job_list[int(sel)]
        print target_job
        self.job_info(target_job)
        while True:
            print "1) end complete"
            print "2) end fail"
            print "3) rerun"
            ret = raw_input('>')
            if ret in ['1', '2', '3']:
                break
        if ret == '3':
            pass

    def rerun_job(self,target_job):
        print target_job
        tg_job_mst_id = target_job['tg_job_mst_id']
        tg_job_dtl_id = target_job['tg_job_dtl_id']
        job_id = target_job['job_id']
        sql="""delete from store.hs_job_dtl where job_exec_dt = '{YYYYMMDD}' and tg_job_dtl_id = '{TG_JOB_DTL_ID}' and  tg_job_dtl_id = '{TG_JOB_DTL_ID}' and  job_id='{JOB_ID}'
        """.format(YYYYMMDD=self.to_day,TG_JOB_MST_ID=tg_job_mst_id,TG_JOB_DTL_ID=tg_job_dtl_id,JOB_ID=job_id)
        self.db.queryExec(sql)
        self.job_submit(target_job)

    def main(self):
        print '-'*50
        while True:
            print '1) Starting job'
            print '2) Runing job'
            print '3) job start'
            ret=raw_input('>>')
            if ret.strip() in ['1','2','3']:
                break
        if ret.strip() == '1':
            self.starting_job()
        elif ret.strip() == '2':
            self.running_job()
        elif ret.strip()== '3':
            self.job_start()


    def job_start(self):
        sc = job_scheduler.sched()

        job_list = self.get_job()

        # print job_list
        cnt = 0
        for job in job_list:
            # print job
            print cnt, job['shell_name'],job['job_exec_dt'],job['job_exec_time']
            cnt = cnt + 1

        sel = raw_input('select one>')

        target_job = job_list[int(sel)]

        print '-'
        #target_job = {'job_id': 73, 'shell_name': 'IBRM_Archive.sh', 'shell_path': '/u01/SCRIPTS/Database/IBRM/RMAN/SCHEDULE', 'tg_job_dtl_id': 486, 'job_yn': 'Y', 'db_name': 'IBRM', 'ora_sid': 'ibrm', 'tg_job_mst_id': 33, 'job_exec_time': '0925', 'job_exec_dt': '20201117', 'svr_ip': '121.170.193.200', 'ora_home': '/u01/app/oracle/product/18.0.0/dbhome_1', 'shell_type': 'ARCH'}
        print target_job
        self.job_submit(target_job)
        # sc.submit_test1()

        #
        # job_info = {'job_id': 73, 'shell_name': 'IBRM_Archive.sh', 'shell_path': '/u01/SCRIPTS/Database/IBRM/RMAN/SCHEDULE',
        #                 'tg_job_dtl_id': 445, 'job_yn': 'Y', 'db_name': 'IBRM', 'ora_sid': 'ibrm', 'tg_job_mst_id': 32,
        #                 'job_exec_time': '0925', 'job_exec_dt': '20201116', 'svr_ip': '121.170.193.200',
        #                 'ora_home': '/u01/app/oracle/product/18.0.0/dbhome_1', 'shell_type': 'ARCH'}
        #
        #
        #

    def job_submit(self,target_job):
        HOST = '121.170.193.200'
        HOST = target_job['svr_ip']
        PORT = 53001
        job_prc = job_state.ibrm_job_stat()
        ss = ibrm_daemon_send.SocketSender(HOST, PORT)
        job_state.ibrm_job_stat().job_start_setup(target_job)
        ss.jos_excute(target_job)

if __name__ == '__main__':
    test().main()


