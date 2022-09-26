# -*- coding: utf-8 -*-
import os
import ibrm_dbms
import datetime
import job_scheduler
import ibrm_logger
import ConfigParser
import ibrm_daemon_send
import logging


class ibrm_job_stat():
    def __init__(self):
        self.dbms = ibrm_dbms.fbrm_db()
        self.cfg = self.get_cfg()
        # self.log = ibrm_logger.ibrm_logger().logger('ibrm_server_job_status')

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config', 'config.cfg')
        cfg.read(cfg_file)
        return cfg

    def set_date(self):
        self.now = datetime.datetime.now()
        self.today_str = datetime.datetime.now().strftime('%Y%m%d')
        self.now_datetime = self.now.strftime('%Y%m%d%H%M%S')
        self.odate = self.get_odate()

    def get_odate(self):
        try:
            ndate = self.cfg.get('COMMON', 'new_day_datetime')
        except:
            ndate = '0800'
        now = datetime.datetime.now()
        hm = now.strftime('%H%m')
        md = now.strftime('%d%m')
        if hm < ndate:
            odate = now - datetime.timedelta(days=1)
        else:
            odate = now
        return odate.strftime('%Y%m%d')

    def get_odate_1(self):
        try:
            ndate = self.cfg.get('COMMON', 'new_day_datetime')
        except:
            ndate = '08:00'
        now = datetime.datetime.now()
        hm = now.strftime('%H%m')
        md = now.strftime('%d%m')
        if hm < ndate:
            odate = now - datetime.timedelta(days=2)
        else:
            odate = now - datetime.timedelta(days=1)
        return odate.strftime('%Y%m%d')

    def job_stat_(self):
        """
        SELECT
        tg_job_dtl_id, tg_job_mst_id
        FROM
        store.tg_job_dtl
        where
        job_exec_dt = '20201106' and job_id = '42'

        SELECT
        hs_job_mst_id, tg_job_mst_id, job_exec_dt, job_stt_dt, job_end_dt, use_yn, mod_usr, mod_dt, reg_usr, reg_dt
        FROM
        store.hs_job_mst;


        SELECT
        hs_job_log_id, hs_job_dtl_id, hs_job_mst_id, pid, prgrs, run_spd, prgrs_time, adtnl_itm_1, adtnl_itm_2, rm_bk_stat, memo, use_yn, mod_usr, mod_dt, reg_usr, reg_dt
        FROM
        store.hs_job_log;

        insert
        into
        tg_job_mst_id, job_exec_dt, use_yn, mod_usr, mod_dt, reg_usr, ret_dt

        insert
        into
        store.hs_job_log(hs_job_dtl_id, hs_job_mst_id, rm_bk_stat, memo)


        """

    def get_hs_id(self, job_id):
        self.set_date()
        query = "SELECT  hs_job_dtl_id,hs_job_mst_id  FROM STORE.HS_JOB_DTL WHERE JOB_ID ='{JOB_ID}' and job_exec_dt='{EXEC_DT}'".format(
            JOB_ID=job_id, EXEC_DT=self.odate)
        hs_job_dtl_id, hs_job_mst_id = '', ''

        try:
            ret_set = self.dbms.getRaw(query)
            if len(ret_set) > 0:
                hs_job_dtl_id = ret_set[0][0]
                hs_job_mst_id = ret_set[0][1]
        except Exception as e:
            print str(e)

            # self.log.error('hs_job_dtl_id error')
            # self.log.error('job_id :{}'.format(job_id))

        return hs_job_dtl_id, hs_job_mst_id

    def job_complete(self, job_info):
        self.set_date()
        job_id = job_info['job_id']
        self.job_update(job_info)

    def job_start_setup(self, job_info):
        self.set_date()
        job_id = job_info['job_id']
        tg_job_dtl_id, tg_job_mst_id = self.get_tg_id(job_id)
        tg_job_mst_id = job_info['tg_job_mst_id']
        table_name = 'store.hs_job_mst'
        query = "select count(*) from {} where tg_job_mst_id = '{}' and job_exec_dt = '{}'".format(table_name,
                                                                                                   tg_job_mst_id,
                                                                                                   job_info[
                                                                                                       'job_exec_dt'])
        print query
        mst_cnt = self.dbms.getRaw(query)[0][0]
        print 'mst cnt :', mst_cnt, type(mst_cnt), mst_cnt == 0
        if mst_cnt == 0:
            set_job = {}
            set_job['tg_job_mst_id'] = job_info['tg_job_mst_id']
            set_job['job_exec_dt'] = job_info['job_exec_dt']
            set_job['job_stt_dt'] = ''
            set_job['job_end_dt'] = ''
            set_job['use_yn'] = 'Y'
            set_job['mod_usr'] = 'SYS'
            set_job['mod_dt'] = self.now_datetime
            set_job['reg_usr'] = 'SYS'
            set_job['reg_dt'] = self.now_datetime
            job_list = []
            job_list.append(set_job)
            print set_job
            print set_job.keys()

            self.dbms.dbInsertList(job_list, table_name)

        query = """
        select count(*) from store.hs_job_dtl where tg_job_dtl_id = {} and job_id={}
        """.format(tg_job_dtl_id, job_id)
        print query
        ret = self.dbms.getRaw(query)
        print ret

        if ret[0][0] == 0:
            query = "select hs_job_mst_id from store.hs_job_mst where tg_job_mst_id ='{}' and job_exec_dt = '{}'".format(
                tg_job_mst_id, self.odate)
            hs_job_mst_id = self.dbms.getRaw(query)[0][0]
            set_job = {}
            set_job['tg_job_mst_id'] = job_info['tg_job_mst_id']
            set_job['hs_job_mst_id'] = hs_job_mst_id
            set_job['tg_job_dtl_id'] = job_info['tg_job_dtl_id']
            set_job['job_id'] = job_info['job_id']
            set_job['job_exec_dt'] = job_info['job_exec_dt']
            set_job['db_type'] = 'no catalog'
            set_job['job_stt_dt'] = ''
            set_job['job_end_dt'] = ''
            set_job['run_type'] = 'Run'
            set_job['job_stat'] = 'Starting'
            set_job['upd_stat'] = ''
            set_job['use_yn'] = 'Y'
            set_job['mod_usr'] = 'SYS'
            set_job['mod_dt'] = self.now_datetime
            set_job['reg_usr'] = 'SYS'
            set_job['reg_dt'] = self.now_datetime
            table_name = 'store.hs_job_dtl'
            job_list = [set_job]
            set_job
            self.dbms.dbInsertList(job_list, table_name)

    def job_status_shell_only(self, job_status):
        """
        data['pid'] = self.pid
        data['job_id'] = self.job_id
        data['ora_sid'] = self.ora_sid
        data['job_st'] = self.job_st
        data['tg_job_dtl_id'] = self.tg_job_dtl_id
        data['elapsed_seconds'] = int(elapsed_seconds)
        data['start_time'] = start_time_str
        data['end_time'] = end_time_str
        :param job_status:
        :return:
        """

        self.set_date()
        tg_job_dtl_id = job_status['tg_job_dtl_id']
        print 'tg_job_dtl_id', tg_job_dtl_id

        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id	FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        print query
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]

        start_time = job_status['start_time']
        end_time = job_status['end_time']
        print 'start time :', start_time, end_time
        query = """UPDATE store.HS_JOB_DTL
                SET 
                job_stt_dt='{JOB_STT_DT}', 
                job_end_dt='{JOB_END_DT}', 
                prgrs_time='{PRGRS_TIME}', 
                mod_usr='SYS' ,
                mod_dt='{MOD_DT}' ,
                
                job_stat='{JOB_STAT}'
                WHERE hs_job_dtl_id = '{MST_ID}';
                            """.format(JOB_STT_DT=start_time,
                                       JOB_END_DT=end_time,
                                       PRGRS_TIME=job_status['elapsed_seconds'],
                                       MOD_DT=self.now_datetime,

                                       JOB_STAT=job_status['job_st'],
                                       MST_ID=hs_job_dtl_id)
        print '#' * 50
        print query
        print '#' * 50
        self.dbms.queryExec(query)
        print 'job_status', job_status['job_st'], job_status['job_st'] == 'Fail'

        if job_status['job_st'] == 'Fail':
            self.evt_ins(job_status)

        if job_status['job_st'] == 'End-OK':
            job_info = self.post_job(job_status['job_id'],job_status['tg_job_dtl_id'])
            print job_info
            if not job_info == None:

                if job_info['rel_exec_type'] == 'INST':
                    submit_job_info = {}
                    submit_job_info['job_id'] = job_info['job_id']
                    submit_job_info['shell_name'] = job_info['shell_name']
                    submit_job_info['shell_type'] = job_info['shell_type']
                    submit_job_info['db_name'] = job_info['db_name']
                    submit_job_info['ora_sid'] = job_info['ora_sid']
                    submit_job_info['tg_job_dtl_id'] = job_info['tg_job_dtl_id']
                    submit_job_info['svr_ip'] = job_info['svr_ip']
                    print job_info
                    print submit_job_info
                    print job_info['run_type']
                    try:
                        self.job_start_setup(job_info)
                    except Exception  as e:
                        print str(e)
                    self.job_submit(submit_job_info)


    def job_status_insert(self, job_status):
        self.set_date()

        tg_job_dtl_id = job_status['tg_job_dtl_id']
        print 'tg_job_dtl_id', tg_job_dtl_id

        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id	FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        print query
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]

        data_set = {}
        data_set['hs_job_dtl_id'] = hs_job_dtl_id
        data_set['hs_job_mst_id'] = hs_job_mst_id
        data_set['pid'] = job_status['pid']
        data_set['prgrs'] = ''
        data_set['adtnl_itm_1'] = ''
        data_set['adtnl_itm_2'] = ''
        data_set['run_spd'] = job_status['write_bps']
        data_set['prgrs_time'] = job_status['elapsed_seconds']
        data_set['rm_bk_stat'] = job_status['status']
        data_set['bk_in_size'] = job_status['input_bytes']
        data_set['memo'] = ''
        data_set['use_yn'] = 'Y'
        data_set['mod_usr'] = 'SYS'
        data_set['mod_dt'] = self.now_datetime
        data_set['reg_usr'] = 'SYS'
        data_set['reg_dt'] = self.now_datetime
        data_list = []
        data_list.append(data_set)
        db_table = 'store.hs_job_log'
        self.dbms.dbInsertList(data_list, db_table)

        print job_status['start_time']
        print job_status['end_time']
        start_time = datetime.datetime.strptime(job_status['start_time'], '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
        end_time = datetime.datetime.strptime(job_status['end_time'], '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
        print 'start time :', start_time, end_time
        query = """UPDATE store.HS_JOB_DTL
        SET 
        job_stt_dt='{JOB_STT_DT}', 
        job_end_dt='{JOB_END_DT}', 
        prgrs_time='{PRGRS_TIME}', 
        mod_usr='SYS' ,
        mod_dt='{MOD_DT}' ,
        rm_bk_stat = '{RM_BK_STAT}',
        job_stat='{JOB_STAT}'
        WHERE hs_job_dtl_id = '{MST_ID}';
                    """.format(JOB_STT_DT=start_time,
                               JOB_END_DT=end_time,
                               PRGRS_TIME=job_status['elapsed_seconds'],
                               MOD_DT=self.now_datetime,
                               RM_BK_STAT=job_status['status'],
                               JOB_STAT=job_status['job_st'],
                               MST_ID=hs_job_dtl_id)
        print '#' * 50
        print query
        print '#' * 50
        self.dbms.queryExec(query)
        print 'job_status', job_status['job_st'], job_status['job_st'] == 'Fail'

        if job_status['job_st'] == 'Fail':
            self.evt_ins(job_status)

        if job_status['job_st'] == 'End-OK':
            job_info = self.post_job(job_status['job_id'],job_status['tg_job_dtl_id'])
            print 'post job :',job_info
            print job_info['rel_exec_type'], job_info['rel_exec_type'] == 'INST'
            if job_info['rel_exec_type'] == 'INST':
                self.job_start_setup(job_info)
                self.job_submit(job_info)

    def post_job(self, job_id,tg_job_dtl_id):
        query = "SELECT pre_job_id,post_job_id,rel_exec_type	FROM master.mst_job where job_id= '{}'".format(job_id)
        ret = self.dbms.getRaw(query)[0]
        job_info = None
        print ret
        if len(ret) == 3:
            pre_job_id = ret[0]
            post_job_id = ret[1]
            rel_exec_type = ret[2]
        to_day = datetime.datetime.now().strftime('%Y%m%d')
        odate_1 = self.get_odate_1()
        print 'post_job_id :', post_job_id, not post_job_id == 0
        if not post_job_id == 0:
            query = """
            SELECT 
   tgrun.tg_job_mst_id
  ,tgrun.job_exec_dt
  ,tgrun.tg_job_dtl_id
  ,tgrun.run_type
    ,tgrun.target_yn
   ,tgrun.job_stat
   ,tgrun.job_nm
  ,tgrun.rel_exec_type 
  ,tgrun.pre_job_id 
  ,tgrun.post_job_id
  ,tgrun.exec_time
  ,tgrun.timeout
  ,tgrun.alarm_yn
  ,tgrun.svr_nm
  ,tgrun.ip_addr
  ,tgrun.db_nm
  ,tgrun.back_type
  ,tgrun.sh_file_nm
  ,tgrun.sh_path
  ,tgrun.ora_sid
  ,tgrun.ora_home
  ,tgrun.db_name
  ,tgrun.job_id 
FROM 
(
  SELECT 
     tg.tg_job_mst_id 
    ,tg.job_exec_dt  
    ,tg.tg_job_dtl_id
    ,tg.exec_time    
    ,tg.job_stat
    ,tg.run_type     
   ,CASE 
      WHEN 
        tg.job_exec_dt ='{YYYYMMDD}' AND tg.exec_time::time >= (NOW() - INTERVAL '5 MINUTE')::TIME  AND tg.exec_time::time <= NOW()::TIME 
        AND tg.run_type ='RUN' AND tg.job_stat IS NULL
        THEN 'RUN'
      WHEN tg.run_type ='RELEASE' AND tg.job_stat IS NULL THEN 'RUN'
      WHEN tg.run_type IN ('RE-RUN') THEN 'RE-RUN'
      WHEN tg.run_type IN ('PAUSE') THEN 'PAUSE'    
      ELSE 'N'      
     END AS target_yn        
    ,mj.timeout      
    ,mj.alarm_yn     
    ,ms.svr_nm       
    ,ms.ip_addr      
    ,ms.db_nm        
    ,ms.back_type    
    ,ms.sh_file_nm   
    ,ms.sh_path      
    ,moi.ora_sid      
    ,moi.ora_home     
    ,moi.db_name        
    ,tg.job_id       
    ,mj.job_nm
    ,mj.rel_exec_type
    ,mj.pre_job_id 
    ,mj.post_job_id 
  FROM 
    (
      SELECT
        ROW_NUMBER() OVER (
          PARTITION BY
            tjm.tg_job_mst_id
            , tjd.tg_job_dtl_id              
            , COALESCE(tjdl.tg_job_dtl_id)
            , COALESCE(hjd.hs_job_dtl_id)
          ORDER BY
            tjm.tg_job_mst_id
            , tjd.tg_job_dtl_id
            , tjdl.tg_job_dtl_log_id DESC      
            , hjd.hs_job_dtl_id DESC
        ) AS ord
        ,tjm.tg_job_mst_id 
        ,tjm.job_exec_dt
        ,tjd.tg_job_dtl_id 
        ,tjd.job_id 
        ,COALESCE( tjdl.run_type , tjd.run_type ) AS run_type
        ,COALESCE( tjdl.exec_time , tjd.exec_time ) AS exec_time    
        ,hjd.job_stat
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
        LEFT OUTER JOIN 
        store.hs_job_dtl hjd 
        ON (
            hjd.job_exec_dt = tjm.job_exec_dt 
            AND hjd.tg_job_mst_id = tjm.tg_job_mst_id 
            AND hjd.tg_job_dtl_id = tjd.tg_job_dtl_id                     
            AND tjdl.tg_job_dtl_id = hjd.tg_job_dtl_id 
        )
      WHERE 
      tjm.job_exec_dt >= '{YYYYMMDD}' 
    ) tg  
    INNER JOIN master.mst_job mj 
      ON mj.job_id = tg.job_id
    INNER JOIN master.mst_shell ms 
      ON ms.sh_id = mj.sh_id
    INNER JOIN 
    master.master_svr_info msi 
      ON (msi.svr_id = ms.svr_id )
    INNER JOIN 
    master.master_ora_info moi 
      ON (
        moi.svr_id = msi.svr_id 
        AND moi.ora_id = ms.db_id 
      )
  ) tgrun 
  WHERE job_id='{JOB_ID}' AND tg_job_dtl_id = '{TG_JOB_DTL_ID}'


            """.format(YYYYMMDD=odate_1, JOB_ID=post_job_id,TG_JOB_DTL_ID=tg_job_dtl_id)
            print query
            job = self.dbms.getRaw(query)[0]
            if len(job) > 0:
                job_info={}
                job_info['tg_job_mst_id'] = job[0]
                job_info['job_exec_dt'] = job[1]
                job_info['tg_job_dtl_id'] = job[2]
                job_info['run_type'] = job[3]
                job_info['target_yn'] = job[4]
                job_info['job_stat'] = job[5]
                job_info['job_nm'] = job[6]
                job_info['rel_exec_type'] = job[7]
                job_info['pre_job_id'] = job[8]
                job_info['post_job_id'] = job[9]
                job_info['exec_time'] = job[10]
                job_info['timeout'] = job[11]
                job_info['alarm_yn'] = job[12]
                job_info['svr_nm'] = job[13]
                job_info['svr_ip'] = job[14]
                job_info['db_nm'] = job[15]
                job_info['shell_type'] = job[16]
                job_info['shell_name'] = job[17]
                job_info['sh_path'] = job[18]
                job_info['ora_sid'] = job[19]
                job_info['ora_home'] = job[20]
                job_info['db_name'] = job[21]
                job_info['job_id'] = job[22]

            return job_info
        return job_info

    def set_hs_job_dtl_udt(self, job_id, tg_job_dtl_id):
        query = """
                UPDATE store.hs_job_dtl SET 
                upd_stat = 'Y', 
                use_yn= 'N', 
                mod_dt = TO_CHAR(now(), 'YYYYMMDDHH24MISS')  WHERE job_id = '{JOB_ID}'  AND tg_job_dtl_id='{TG_JOB_DTL_ID}' 
        """.format(JOB_ID=job_id, TG_JOB_DTL_ID=tg_job_dtl_id)
        print query
        self.dbms.queryExec(query)

    def set_tg_job_dtl_log(self, job_id, tg_job_dtl_id):
        # tg_job_dtl_log insert
        # hs_job_dtl update (upd_stat = 'Y', use_yn= 'N'

        run_type = 'RUN'
        odate = self.get_odate_1()
        today_str = datetime.datetime.now().strftime('%Y%m%d')
        query = """
        INSERT INTO store.tg_job_dtl_log
    (
       tg_job_dtl_id
      ,tg_job_mst_id
      ,job_id
      ,job_exec_dt
      ,job_run_dt
      ,exec_time
      ,run_type
      ,rsn_code
      ,rsn_desc
      ,memo
      ,use_yn
      ,mod_usr
      ,mod_dt
      ,reg_usr
      ,reg_dt
    )
    SELECT
      tg_job_dtl_id
      ,tg_job_mst_id
      ,job_id
      ,job_exec_dt
      ,TO_CHAR(now(), 'YYYYMMDD')
      ,exec_time
      ,'{RUN_TYPE}'
      ,'900'
      ,'RE-RUN --> RUN '
      ,''
      ,'Y'
      ,'SYS'
      ,TO_CHAR(now(), 'YYYYMMDDHH24MISS')
      ,'SYS'
      ,TO_CHAR(now(), 'YYYYMMDDHH24MISS')
    FROM
      store.tg_job_dtl
    WHERE
        job_id = '{JOB_ID}' and tg_job_dtl_id = '{TG_JOB_DTL_ID}'
        """.format(RUN_TYPE=run_type, JOB_ID=job_id, TG_JOB_DTL_ID=tg_job_dtl_id)
        print query
        self.dbms.queryExec(query)

    def set_pause(self, job_id):
        run_type = 'PAUSE'
        odate = self.get_odate()
        query = """
        INSERT INTO store.tg_job_dtl_log
    (
       tg_job_dtl_id
      ,tg_job_mst_id
      ,job_id
      ,job_exec_dt
      ,job_run_dt
      ,exec_time
      ,run_type
      ,rsn_code
      ,rsn_desc
      ,memo
      ,use_yn
      ,mod_usr
      ,mod_dt
      ,reg_usr
      ,reg_dt
    )
    SELECT
      tg_job_dtl_id
      ,tg_job_mst_id
      ,job_id
      ,job_exec_dt
      ,job_run_dt
      ,exec_time
      ,'{RUN_TYPE}'
      ,'900'
      ,'선행작업 실행중'
      ,''
      ,'Y'
      ,'SYS'
      ,TO_CHAR(now(), 'YYYYMMDDHH24MISS')
      ,'SYS'
      ,TO_CHAR(now(), 'YYYYMMDDHH24MISS')
    FROM
      store.tg_job_dtl
    WHERE
        job_id = '{JOB_ID}' and job_exec_dt = '{ODATE}'
        """.format(RUN_TYPE=run_type, JOB_ID=job_id, ODATE=odate)
        print query
        self.dbms.queryExec(query)

    def pre_job(self, job_id):
        query = "SELECT pre_job_id,post_job_id,rel_exec_type	FROM master.mst_job where job_id= '{}'".format(job_id)
        ret = self.dbms.getRaw(query)[0]
        job_info = None
        print ret
        if len(ret) == 3:
            pre_job_id = ret[0]
            post_job_id = ret[1]
            rel_exec_type = ret[2]
        to_day = datetime.datetime.now().strftime('%Y%m%d')
        print 'pre_job_id :', pre_job_id, not pre_job_id == 0
        run_bit = True
        if not pre_job_id == 0:
            yyyymmdd = datetime.datetime.now().strftime('%Y%m%d')
            query = """SELECT job_stat, rm_bk_stat   FROM store.hs_job_dtl where  job_exec_dt = '{YYYYMMDD}' and job_id = '{PRE_JOB_ID}' """.format(
                YYYYMMDD=yyyymmdd, PRE_JOB_ID=pre_job_id)
            ret_set = self.dbms.getRaw(query)[0]
            job_stat = ret_set[0]
            rm_bk_stat = ret_set[1]
            if not job_stat == 'End-OK':
                run_bit = False
                self.set_pause(job_id)

        return run_bit

    def job_log_update(self, log_return_data):
        self.set_date()
        print 'LOG_UPDATE 33333'
        print log_return_data['job_id']
        job_id = log_return_data['job_id']
        tg_job_dtl_id = log_return_data['tg_job_dtl_id']
        print 'tg_job_dtl_id :', tg_job_dtl_id
        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id	FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        print 'qeury :', query
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]
        tg_job_mst_id = ret_set[2]

        query = "SELECT COUNT(hs_job_dtl_id) FROM STORE.HS_JOB_LOGFILE WHERE hs_job_mst_id = '{}'".format(
            hs_job_mst_id)
        print query

        cnt_set = self.dbms.getRaw(query)
        cnt = cnt_set[0][0]
        if int(cnt) == 0:
            ins_bit = True
        else:
            ins_bit = False
        print 'INS BIT :', ins_bit
        print log_return_data['log_contents']
        log_file = os.path.basename(log_return_data['log_contents'])
        print log_file
        print 'sections :', self.cfg.sections()
        print 'options :', self.cfg.options('common')
        log_path = self.cfg.get('common', 'log_file_path')

        # log_path = os.path.join('E:\\','Fleta','data','ibrm_backup_log',log_file)
        log_path = os.path.join(log_path, log_file)
        print 'log path :', log_path, os.path.isfile(log_path)
        if os.path.isfile(log_path):
            with open(log_path) as f:
                log_content = f.read()
            log_content = log_content.replace("'", '`')
        else:
            log_content = 'NOT FOUND'

        if ins_bit:
            log_data = {}
            log_data['hs_job_dtl_id'] = hs_job_dtl_id
            log_data['hs_job_mst_id'] = hs_job_mst_id
            log_data['pid'] = log_return_data['pid']
            log_data['logfile_nm'] = log_return_data['log_file']
            log_data['file_cntn'] = log_content
            log_data['rm_bk_stat'] = log_return_data['status']
            log_data['prgrs_time'] = log_return_data['elapsed_seconds']
            log_data['use_yn'] = 'Y'
            log_data['memo'] = ''
            log_data['mod_usr'] = ''
            log_data['mod_dt'] = ''
            log_data['reg_usr'] = 'SYS'
            log_data['reg_dt'] = self.now_datetime
            print log_data
            log_update_list = []
            log_update_list.append(log_data)
            table_name = 'store.hs_job_logfile'
            print log_data
            self.dbms.dbInsertList(log_update_list, table_name)
        else:
            reg_date = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            query = """UPDATE store.hs_job_logfile
    	SET file_cntn='{LOG_CONTENT}', rm_bk_stat='{JOB_ST}', prgrs_time='{ELAPSED_SEC}', mod_dt='{MOD_DT}'
    	WHERE hs_job_mst_id = '{MST_ID}';
                """.format(LOG_CONTENT=log_content, JOB_ST=log_return_data['status'],
                           ELAPSED_SEC=log_return_data['elapsed_seconds'], MOD_DT=reg_date, MST_ID=hs_job_mst_id)
            print query
            self.dbms.queryExec(query)

    def get_job(self):
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
     -- AND tjm.job_exec_dt = '20201023' -- 실행일자로 변경하여 사용   

    and tjd.job_id  = 13
    """
        print sql
        ret = self.dbms.getRaw(sql)[0]

        job_id = ret[0]
        svr_ip = ret[8]

        shell_type = ret[10]
        shell_name = ret[11]
        shell_path = ret[12]
        ora_home = ret[14]
        ora_sid = ret[13]
        db_name = ret[15]
        job_info = {}
        job_info['job_id'] = job_id
        job_info['svr_ip'] = svr_ip
        job_info['shell_type'] = shell_type
        job_info['shell_name'] = shell_name
        job_info['shell_path'] = shell_path
        job_info['ora_home'] = ora_home
        job_info['db_name'] = db_name
        job_info['ora_sid'] = ora_sid
        job_info['tg_job_mst_id'] = ret[16]
        job_info['job_exec_dt'] = ret[17]
        job_info['tg_job_dtl_id'] = ret[18]
        job_info['job_id'] = ret[19]
        job_info['job_exec_dt'] = ret[1]

        return job_info

    def get_tg_id(self, job_id):
        now = datetime.datetime.now()
        today_str = datetime.datetime.now().strftime('%Y%m%d')
        now_datetime = now.strftime('%Y%m%d%H%M%S')
        tg_job_dtl_id, tg_job_mst_id = '', ''
        self.set_date()
        query = """SELECT
                tg_job_dtl_id, tg_job_mst_id
                FROM
                store.tg_job_dtl
                where
                job_exec_dt = '{EXEC_DT}' and job_id = '{JOB_ID}' order by 1 desc
                """.format(EXEC_DT=self.odate, JOB_ID=job_id)

        try:
            ret_set = self.dbms.getRaw(query)
            if len(ret_set) > 0:
                tg_job_dtl_id = ret_set[0][0]
                tg_job_mst_id = ret_set[0][1]
        except:
            pass

        return tg_job_dtl_id, tg_job_mst_id

        # job_info = {}
        # job_info['hs_job_dtl_id'] = hs_job_dtl_id
        # job_info['hs_job_mst_id'] = hs_job_mst_id
        # job_info['rm_bk_stat'] = 'Fail'
        # # job_info['memo'] = 'This job is already running'
        # job_info['use_yn'] = 'Y'
        # job_info['pid'] = ''
        # job_info['mod_usr'] = 'SYS'
        # job_info['mod_dt'] = self.now_datetime
        # job_info['reg_usr'] = 'SYS'
        # job_info['reg_dt'] = self.now_datetime
        # job_info['adtnl_itm_1'] = ''
        # job_info['adtnl_itm_2'] = ''
        # job_info['memo'] = memo
        # job_info_list = [job_info]
        # table_name = 'store.hs_job_log'
        # self.dbms.dbInsertList(job_info_list, table_name)

    def get_job_id(self, tg_job_dtl_id):
        self.set_date()

        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id	FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        print query
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]
        tg_job_mst_id = ret_set[2]
        job_id = {}
        job_id['hs_job_dtl_id'] = hs_job_dtl_id
        job_id['hs_job_mst_id'] = hs_job_mst_id
        job_id['tg_job_mst_id'] = tg_job_mst_id
        return job_id

    def job_submit_fail(self, job_id, tg_job_dtl_id, memo):
        memo = memo.replace("'", "`")
        self.set_date()

        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id	FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        print query
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]
        tg_job_mst_id = ret_set[2]

        print hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id

        query = """UPDATE store.hs_job_dtl SET

                job_stat ='Fail', 
                rm_bk_stat ='Run Fail', 
                memo = '{memo}',
                mod_dt = to_char(now(),'YYYYMMDDHH24MISS') 

                WHERE hs_job_dtl_id = '{HS_JOB_DTL_ID}'
        """.format(memo=memo, HS_JOB_DTL_ID=hs_job_dtl_id)
        print query
        self.dbms.queryExec(query)

        job_info = {}
        job_info['hs_job_dtl_id'] = hs_job_dtl_id
        job_info['hs_job_mst_id'] = hs_job_mst_id
        job_info['rm_bk_stat'] = 'Fail'
        # job_info['memo'] = 'This job is already running'
        job_info['use_yn'] = 'Y'
        job_info['pid'] = ''
        job_info['mod_usr'] = 'SYS'
        job_info['mod_dt'] = self.now_datetime
        job_info['reg_usr'] = 'SYS'
        job_info['reg_dt'] = self.now_datetime
        job_info['adtnl_itm_1'] = ''
        job_info['adtnl_itm_2'] = ''
        job_info['memo'] = memo
        job_info_list = [job_info]
        table_name = 'store.hs_job_log'
        self.dbms.dbInsertList(job_info_list, table_name)
        ret_data = {'FLETA_PASS': 'kes2719!', 'CMD': 'JOB_UPDATE_SUCC', 'ARG': {'result': 'succ'}}

        self.evt_ins(job_info)

        return ret_data

    def job_aleady_exist(self, job_id, tg_job_dtl_id, memo):
        memo = memo.replace("'", "`")

        now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        print now_str

        query = """UPDATE store.hs_job_dtl SET job_stat='Fail', memo='{}',mod_dt='{}' WHERE tg_job_dtl_id = '{}' """.format(
            memo, now_str, tg_job_dtl_id)
        print query
        self.dbms.queryExec(query)

        # job_id_info = self.get_job_id(tg_job_dtl_id)
        # hs_job_dtl_id = job_id_info['hs_job_dtl_id']
        # hs_job_mst_id = job_id_info['hs_job_mst_id']
        # tg_job_mst_id = job_id_info['tg_job_mst_id']
        # query = "select hs_job_dtl_id from store.hs_job_dtl where job_id = '{JOB_ID}' and job_exec_dt = '{TODAY}'".format(
        #     JOB_ID=job_id, TODAY=self.today_str)
        # print query
        # try:
        #     ret_set = self.dbms.getRaw(query)
        # except:
        #     pass
        # hs_job_dtl_id = ret_set[0][0]
        #
        # job_info = {}
        # job_info['hs_job_dtl_id'] = hs_job_dtl_id
        # job_info['hs_job_mst_id'] = hs_job_mst_id
        # job_info['rm_bk_stat'] = 'Fail'
        # # job_info['memo'] = 'This job is already running'
        # job_info['use_yn'] = 'Y'
        # job_info['pid'] = ''
        # job_info['mod_usr'] = 'SYS'
        # job_info['mod_dt'] = self.now_datetime
        # job_info['reg_usr'] = 'SYS'
        # job_info['reg_dt'] = self.now_datetime
        # job_info['adtnl_itm_1'] = ''
        # job_info['adtnl_itm_2'] = ''
        # job_info['memo'] = memo
        #
        # job_info_list = [job_info]
        # table_name = 'store.hs_job_log'
        #
        # self.dbms.dbInsertList(job_info_list, table_name)

    def job_fail_proc(self, job_id, memo):
        memo = memo.replace("'", "`")
        self.set_date()
        tg_job_dtl_id, tg_job_mst_id = self.get_tg_id(job_id)

        table_name = 'store.hs_job_mst'
        # memo='This job is already running'
        """
        tg_job_mst_id, job_exec_dt, use_yn, mod_usr, mod_dt, reg_usr, ret_dt
        """

        query = "select count(tg_job_mst_id) from store.tg_job_mst where tg_job_mst_id='{TG_JOB_MST_ID}' and job_exec_dt='{JOB_EXEC_DT}' ".format(
            TG_JOB_MST_ID=tg_job_mst_id, JOB_EXEC_DT=self.odate)
        print query
        cnt = self.dbms.getRaw(query)[0][0]
        print 'cnt :', cnt, cnt == 0
        if cnt == 0:
            job_info = {}
            job_info['tg_job_mst_id'] = tg_job_mst_id
            job_info['job_exec_dt'] = self.today_str
            job_info['job_stt_dt'] = ''
            job_info['job_end_dt'] = ''
            job_info['use_yn'] = 'Y'
            job_info['mod_usr'] = 'SYS'
            job_info['mod_dt'] = self.now_datetime
            job_info['reg_usr'] = 'SYS'
            job_info['reg_dt'] = self.now_datetime
            job_info_list = []
            job_info_list.append(job_info)
            table_name = 'store.hs_job_mst'
            self.dbms.dbInsertList(job_info_list, table_name)

        """
        store.hs_job_log(hs_job_dtl_id, hs_job_mst_id, rm_bk_stat, memo
        """
        query = "select hs_job_mst_id from store.hs_job_mst where tg_job_mst_id = '{TG_JOB_MST_ID}'".format(
            TG_JOB_MST_ID=tg_job_mst_id)
        print query
        try:
            ret_set = self.dbms.getRaw(query)
        except:
            pass
        hs_job_mst_id = ret_set[0][0]

        job_info = {}
        job_info['hs_job_mst_id'] = hs_job_mst_id
        job_info['tg_job_dtl_id'] = tg_job_dtl_id
        job_info['tg_job_mst_id'] = tg_job_mst_id
        job_info['job_id'] = job_id
        job_info['job_exec_dt'] = self.today_str
        job_info['job_stt_dt'] = ''
        job_info['job_end_dt'] = ''
        job_info['db_type'] = 'no catalog'
        job_info['run_type'] = ''
        job_info['job_stat'] = 'Fail'
        job_info['memo'] = memo
        job_info['mod_usr'] = 'SYS'
        job_info['mod_dt'] = self.now_datetime
        job_info['reg_usr'] = 'SYS'
        job_info['reg_dt'] = self.now_datetime
        job_info_list = [job_info]
        table_name = 'store.hs_job_dtl'
        self.dbms.dbInsertList(job_info_list, table_name)

        query = "select hs_job_dtl_id from store.hs_job_dtl where job_id = '{JOB_ID}' and job_exec_dt = '{ODATE}'".format(
            JOB_ID=job_id, ODATE=self.odate)
        print query
        try:
            ret_set = self.dbms.getRaw(query)
        except:
            pass
        hs_job_dtl_id = ret_set[0][0]

        job_info = {}
        job_info['hs_job_dtl_id'] = hs_job_dtl_id
        job_info['hs_job_mst_id'] = hs_job_mst_id
        job_info['rm_bk_stat'] = 'Fail'
        # job_info['memo'] = 'This job is already running'
        job_info['use_yn'] = 'Y'
        job_info['pid'] = ''
        job_info['mod_usr'] = 'SYS'
        job_info['mod_dt'] = self.now_datetime
        job_info['reg_usr'] = 'SYS'
        job_info['reg_dt'] = self.now_datetime
        job_info['adtnl_itm_1'] = ''
        job_info['adtnl_itm_2'] = ''
        job_info['memo'] = memo

        job_info_list = [job_info]
        table_name = 'store.hs_job_log'

        self.dbms.dbInsertList(job_info_list, table_name)

    def job_update(self, job_status):
        self.set_date()
        job_id = job_status['job_id']
        tg_job_dtl_id = job_status['tg_job_dtl_id']

        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id	FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        print query
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]
        tg_job_mst_id = ret_set[2]

        print 'tg_job_dtl_id :', tg_job_dtl_id
        print 'tg_job_mst_id :', tg_job_mst_id
        print 'hs_job_dtl_id :', hs_job_dtl_id
        print 'hs_job_mst_id :', hs_job_mst_id

        print 'job_update : '

        """
    {'FLETA_PASS': 'kes2719!', 'CMD': 'JOB_STATUS', 'ARG': {'status': 'COMPLETED', 'start_time': '2020-10-25 12:37:00', 'pid': '3335',
    'elapsed_seconds': '803', 'session_recid': '515', 'job_id': '13', 'input_type': 'DB INCR',
    'session_id': '515', 'end_time': '2020-10-25 12:50:23', 'write_bps': '117.98M',
    'session_stamp': '1054730218', 'ora_sid': 'ibrm'}}

        :param job_status:
        :return:
        """
        if 'COMPLETED' in job_status['status']:
            job_st = 'END-OK'
        else:
            job_st = 'Running'

        mod_dt = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        job_stt_dt = datetime.datetime.strptime(job_status['start_time'], '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
        job_end_dt = datetime.datetime.strptime(job_status['end_time'], '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
        ssn_rec_id = job_status['session_recid']
        ssn_stmp = job_status['session_stamp']
        rm_bk_stat = job_status['status']
        prgrs_time = job_status['elapsed_seconds']

        query = """UPDATE store.hs_job_dtl SET
                job_stt_dt ='{JOB_STT_DT}', 
                job_end_dt = '{JOB_END_DT}',
                ssn_rec_id ='{SSN_REC_ID}', 
                ssn_stmp ='{SSN_STMP}', 
                run_type ='Run', 
                job_stat ='{JOB_STATUS}', 
                rm_bk_stat ='{RM_BK_STAT}', 
                prgrs_time ='{PRGRS_TIME}',
                mod_dt ='{MOD_DT}'

                WHERE hs_job_dtl_id = '{HS_JOB_DTL_ID}'
                """.format(JOB_STT_DT=job_stt_dt, JOB_END_DT=job_end_dt, SSN_REC_ID=ssn_rec_id, SSN_STMP=ssn_stmp,
                           JOB_STATUS=job_st, RM_BK_STAT=rm_bk_stat, PRGRS_TIME=prgrs_time, MOD_DT=mod_dt,
                           HS_JOB_DTL_ID=hs_job_dtl_id)
        print query

        self.dbms.queryExec(query)

        # job_info = {}
        # job_info['hs_job_dtl_id'] = hs_job_dtl_id
        # job_info['hs_job_mst_id'] = hs_job_mst_id
        # job_info['rm_bk_stat'] = 'Fail'
        # # job_info['memo'] = 'This job is already running'
        # job_info['use_yn'] = 'Y'
        # job_info['pid'] = ''
        # job_info['mod_usr'] = 'SYS'
        # job_info['mod_dt'] = self.now_datetime
        # job_info['reg_usr'] = 'SYS'
        # job_info['reg_dt'] = self.now_datetime
        # job_info['adtnl_itm_1'] = ''
        # job_info['adtnl_itm_2'] = ''
        # job_info['memo'] = ''
        # job_info_list = [job_info]
        # table_name = 'store.hs_job_log'
        # self.dbms.dbInsertList(job_info_list, table_name)

        if job_status['job_st'] == 'End-OK':
            job_info = self.post_job(job_id,tg_job_dtl_id)
            if job_info['rel_exec_type'] == 'INST':
                job_st.job_submit(job_info)

        ret_data = {'FLETA_PASS': 'kes2719!', 'CMD': 'JOB_UPDATE_SUCC', 'ARG': {'result': 'succ'}}
        return ret_data

    def job_submit_fail(self, job_info):
        self.set_date()
        evt_info = {}
        job_id = job_info['job_id']
        evt_info['job_id'] = job_id
        odate = self.get_odate()
        query = """select hs_job_dtl_id,hs_job_mst_id,tg_job_mst_id,tg_job_dtl_id from store.hs_job_dtl where job_id='{JOB_ID}' and job_exec_dt ='{ODATE}' 
        """.format(JOB_ID=job_id, ODATE=odate)
        ret = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret[0]
        hs_job_mst_id = ret[1]
        tg_job_mst_id = ret[2]
        tg_job_dtl_id = ret[3]
        msg = "SOCKET ERROR (PLEASE CHECK IBRM AGENT)"
        query = """UPDATE store.hs_job_dtl SET
                       run_type ='RUN', 
                       job_stat ='Fail', 
                       mod_dt ='{MOD_DT}',
                       memo = '{MSG}'
                       WHERE hs_job_dtl_id = '{HS_JOB_DTL_ID}'
                       """.format(MOD_DT=self.now_datetime, HS_JOB_DTL_ID=hs_job_dtl_id, MSG=msg)
        print query
        try:
            self.dbms.queryExec(query)
        except:
            pass

        evt_info['tg_job_dtl_id'] = tg_job_dtl_id
        log_dt = datetime.datetime.now().strftime('%Y%m%d')
        evt_info['log_dt'] = log_dt

        query = "SELECT job_nm, work_div_1, work_div_2, sh_id FROM master.mst_job where job_id ='{}'".format(job_id)
        print query
        ret = self.dbms.getRaw(query)[0]

        job_nm = ret[0]
        work_div_1 = ret[1]
        work_div_2 = ret[2]
        sh_id = ret[3]

        query = """SELECT sh_nm,  svr_id, svr_nm, ip_addr, db_id, db_nm, back_type, sh_file_nm	FROM master.mst_shell where sh_id = '{}'""".format(
            sh_id)
        print query
        ret = self.dbms.getRaw(query)[0]
        sh_nm = ret[0]
        svr_id = ret[1]
        svr_nm = ret[2]
        ip_addr = ret[3]
        db_id = ret[4]
        db_nm = ret[5]
        back_type = ret[6]

        sh_file_nm = ret[7]
        evt_info['svr_id'] = svr_id
        evt_info['db_id'] = db_id
        evt_info['sys_type'] = 'SCH'
        evt_info['evt_type'] = 'SOCKET ERROR'
        evt_info['evt_code'] = 'SOCKET ERROR'
        evt_info['evt_dtl_type'] = ''
        evt_info['evt_lvl'] = 'ERROR'  # ERROR/CRITICAL/INFO
        evt_info[
            'evt_msg'] = 'SOCKET ERROR (PLEASE CHECK IBRM AGENT PROCESS) {JOB_NM}({SH_FILE_NM}) BACKUP JOB FAIL'.format(
            JOB_NM=job_nm, SH_FILE_NM=sh_file_nm)
        evt_info['evt_cntn'] = '{WORK_DIV_1} {WORK_DIV_2} {JOB_NM} {SH_NM} {SVR_NM} {DB_NM}'.format(
            WORK_DIV_1=work_div_1, WORK_DIV_2=work_div_2, JOB_NM=job_nm, SH_NM=sh_nm, SVR_NM=svr_nm, DB_NM=db_nm)
        evt_info['dev_type'] = 'DB'
        evt_info['act_yn'] = 'Y'
        evt_info['reg_usr'] = 'SYS'
        evt_info['reg_dt'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        evt_info_list = []
        evt_info_list.append(evt_info)
        print 'evt_info :', evt_info
        table_name = 'event.evt_log'
        self.dbms.dbInsertList(evt_info_list, table_name)
        # if job_st == 'End-OK':

    def evt_ins(self, return_data):
        """
        시스템유형	sys_type	varchar	20	N		TO_CHAR(current_timestamp, 'YYYYMMDD')	[시스템유형] 모니터링, 스케쥴링 ( mnt, sch )
로그일자	log_dt	varchar	8	N			[로그일자]
서버 ID	svr_id	integer		N		0	[서버 ID] 서버 ID Unique
DB ID	db_id	integer		N		0	[DB ID]
작업ID	job_id	integer		N		0	[작업ID] JOB ID Unique
일작업상세ID	tg_job_dtl_id	integer		N		0	[일작업상세ID]
이벤트유형	evt_type	varchar	20	N		0	[이벤트유형] 이벤트 분류코드 코드화 필요 ( 대 / 중 / 소 )
이벤트상세유형	evt_dtl_type	varchar	20	N		'N/A'	[이벤트상세유형]
이벤트코드	evt_code	varchar	50	N			[이벤트코드]
이벤트레벨	evt_lvl	varchar	50	N		'N/A'	[이벤트레벨]
이벤트메시지	evt_msg	varchar	200	Y			[이벤트메시지]
이벤트내용	evt_cntn	text		Y			[이벤트내용]
장비분류코드	dev_type	varchar	20	N		'N/A'	[장비분류코드] 이벤트대상장비유형
조치대상여부	act_yn	varchar	2	N		'N'	[조치대상여부] 이벤트 조치 대상여부  Y/N
조치유형	act_type	varchar	30	N		'N/A'	[조치유형] 처리유형 ( msg / tell / 등등 코드화 필요)
조치결과여부	act_rslt	varchar	10	N		'N'	[조치결과여부] 처리결과 상태 ( Y/N/C/D 등 코드처리 필요)
조치결과상세	act_desc	text		Y			[조치결과상세] 조치결과상세
조치자	act_usr	varchar	14	N		'SYS'	[조치자]
조치일시	act_dt	varchar	14	Y			[조치일시]
메모	memo	varchar	200	Y			[메모] 부가메모
사용여부	use_yn	varchar	1	N		'Y'	[사용여부] Y,N
수정자ID	mod_usr	varchar	20	Y		'SYS'	[수정자ID] 등록자의 사용자ID
수정일시	mod_dt	varchar	14	Y		TO_CHAR(current_timestamp, 'YYYYMMDDHH24MISS')	[수정일시] 수정일시.YYYYMMDDHH24MISS
등록자ID	reg_usr	varchar	20	N			[등록자ID] 등록자의 사용자ID
등록일시	reg_dt	varchar	14	N		TO_CHAR(current_timestamp, 'YYYYMMDDHH24MISS')	[등록일시] 등록일시. 백업인경우 :DB 시간
        :return:
        """
        """JOB_MONITOR
        {'status': 'COMPLETED', 'input_bytes': '63655968768', 'job_id': '37', 'input_type': 'DB INCR', 'start_time': '2020-12-04 14:38:51', 'job_st': 'End-OK', 'pid': '10377', 'session_id': '5866', 'elapsed_seconds': '524', 'end_time': '2020-12-04 14:47:35', 'tg_job_dtl_id': '198744', 'write_bps': '655.73K', 'session_stamp': '1058279929', 'ora_sid': 'ibrm', 'session_recid': '5866'}
        """
        # return_data = {'status': 'COMPLETED', 'input_bytes': '63655968768', 'job_id': '37', 'input_type': 'DB INCR', 'start_time': '2020-12-04 14:38:51', 'job_st': 'End-OK', 'pid': '10377', 'session_id': '5866', 'elapsed_seconds': '524', 'end_time': '2020-12-04 14:47:35', 'tg_job_dtl_id': '198744', 'write_bps': '655.73K', 'session_stamp': '1058279929', 'ora_sid': 'ibrm', 'session_recid': '5866'}
        evt_info = {}
        job_id = return_data['job_id']
        evt_info['job_id'] = job_id
        tg_job_dtl_id = return_data['tg_job_dtl_id']
        evt_info['tg_job_dtl_id'] = tg_job_dtl_id
        log_dt = datetime.datetime.now().strftime('%Y%m%d')
        evt_info['log_dt'] = log_dt

        query = "SELECT job_nm, work_div_1, work_div_2, sh_id FROM master.mst_job where job_id ='{}'".format(job_id)
        print query
        ret = self.dbms.getRaw(query)[0]

        job_nm = ret[0]
        work_div_1 = ret[1]
        work_div_2 = ret[2]
        sh_id = ret[3]

        query = """SELECT sh_nm,  svr_id, svr_nm, ip_addr, db_id, db_nm, back_type, sh_file_nm	FROM master.mst_shell where sh_id = '{}'""".format(
            sh_id)
        print query
        ret = self.dbms.getRaw(query)[0]
        sh_nm = ret[0]
        svr_id = ret[1]
        svr_nm = ret[2]
        ip_addr = ret[3]
        db_id = ret[4]
        db_nm = ret[5]
        back_type = ret[6]
        sh_file_nm = ret[7]
        evt_info['svr_id'] = svr_id
        evt_info['db_id'] = db_id
        evt_info['sys_type'] = 'SCH'
        evt_info['evt_type'] = 'BACKUP_JOB_FAIL'
        evt_info['evt_dtl_type'] = ''
        evt_info['evt_lvl'] = 'ERROR'  # ERROR/CRITICAL/INFO
        evt_info['evt_msg'] = '{JOB_NM}({SH_FILE_NM}) BACKUP JOB FAIL'.format(JOB_NM=job_nm, SH_FILE_NM=sh_file_nm)
        evt_info['evt_cntn'] = '{WORK_DIV_1} {WORK_DIV_2} {JOB_NM} {SH_NM} {SVR_NM} {DB_NM}'.format(
            WORK_DIV_1=work_div_1, WORK_DIV_2=work_div_2, JOB_NM=job_nm, SH_NM=sh_nm, SVR_NM=svr_nm, DB_NM=db_nm)
        evt_info['dev_type'] = 'DB'
        evt_info['act_yn'] = 'Y'
        evt_info['reg_usr'] = 'SYS'
        evt_info['evt_code'] = 'SOCKET ERROR'
        evt_info['reg_dt'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        evt_info_list = []
        evt_info_list.append(evt_info)
        print 'evt_info :', evt_info
        table_name = 'event.evt_log'
        self.dbms.dbInsertList(evt_info_list, table_name)
        # if job_st == 'End-OK':

    def job_status_fail_update(self, job_id):
        self.set_date()
        hs_job_dtl_id, hs_job_mst_id = self.get_hs_id(job_id)

        query = "SELECT  hs_job_dtl_id,hs_job_mst_id  FROM STORE.HS_JOB_DTL WHERE JOB_ID ='{JOB_ID}' and job_exec_dt='{EXEC_DT}'".format(
            JOB_ID=job_id, EXEC_DT=self.today_str)
        ret_set = self.dbms.getRaw(query)
        if len(ret_set) > 0:
            hs_job_dtl_id = ret_set[0][0]
            hs_job_mst_id = ret_set[0][1]

        query = """UPDATE store.hs_job_dtl SET
               run_type ='RUN', 
               job_stat ='Fail', 
               mod_dt ='{MOD_DT}'
               memo = ''

               WHERE hs_job_dtl_id = '{HS_JOB_DTL_ID}'
               """.format(MOD_DT=self.now_datetime, HS_JOB_DTL_ID=hs_job_dtl_id)
        print query
        try:
            self.dbms.queryExec(query)
        except:
            pass

        # query = """UPDATE store.hs_job_mst
        #             SET job_stt_dt='{JOB_STT_DT}',
        #             job_end_dt='{JOB_END_DT}'
        # 	    WHERE tg_job_mst_id= '{MST_ID}'
        # 	    """.format(JOB_STT_DT=job_stt_dt, JOB_END_DT=job_end_dt, MST_ID=mst_id)
        # print query
        # try:
        #     self.dbms.queryExec(query)
        # except:
        #     pass

    def job_submit(self, job_info):
        print 'JOB START'
        print job_info

        HOST = job_info['svr_ip']
        PORT = 53001

        ss = ibrm_daemon_send.SocketSender(HOST, PORT)
        #
        ss.jos_excute(job_info)

    def main(self):
        job_scheduler.sched().already_past_job()


if __name__ == '__main__':
    job_st = ibrm_job_stat()
    # job_info=job_st.post_job('75')
    # if job_info['rel_exec_type'] == 'INST':
    #     job_st.job_submit(job_info)
    job_st.set_pause('75')


