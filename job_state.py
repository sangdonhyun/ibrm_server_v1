# -*- coding: utf-8 -*-
import os
import ibrm_dbms
import datetime
# import job_scheduler
# import ibrm_logger
import ConfigParser
import ibrm_daemon_send
import re
import logging
import job_overtime_monitor


class StateLog(object):
    """
    ibrm_job_state class 생성시 Logger instance 가 넘어오지 않았을 때 처리를 위해 생성
    """
    def __init__(self, logger=None):
        self.log = logger

    def info(self, text):
        if self.log is not None:
            try:
                self.log.info(text)
            except Exception as e:
                self.log.error(e.message)
        else:
            try:
                print(text)
            except Exception as e:
                print(e.message)

    def error(self, text):
        if self.log is not None:
            try:
                self.log.error(text)
            except Exception as e:
                self.log.error(e.message)
        else:
            try:
                print(text)
            except Exception as e:
                print(e.message)

    def debug(self, text):
        if self.log is not None:
            try:
                self.log.debug(text)
            except Exception as e:
                self.log.error(e.message)
        else:
            try:
                print(text)
            except Exception as e:
                print(e.message)

    def warning(self, text):
        if self.log is not None:
            try:
                self.log.warning(text)
            except Exception as e:
                self.log.error(e.message)
        else:
            try:
                print(text)
            except Exception as e:
                print(e.message)

    def critical(self, text):
        if self.log is not None:
            try:
                self.log.critical(text)
            except Exception as e:
                self.log.error(e.message)
        else:
            try:
                print(text)
            except Exception as e:
                print(e.message)


class ibrm_job_stat():
    # scheduler 와 ibrm_server_daemon_socket 등 여러 모듈에서 사용하여 Logger parameter 를 받아서 처리
    def __init__(self, logger=None):
        self.dbms = ibrm_dbms.fbrm_db()
        self.cfg = self.get_cfg()
        self.ov_monitor = job_overtime_monitor.ov_mon()
        self.log = StateLog(logger)

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
            ndate = '0800'
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
        query = "SELECT  hs_job_dtl_id,hs_job_mst_id  FROM STORE.HS_JOB_DTL WHERE JOB_ID ='{JOB_ID}' and job_exec_dt='{EXEC_DT}'".format(
            JOB_ID=job_id, EXEC_DT=self.odate)
        hs_job_dtl_id, hs_job_mst_id = '', ''
        try:
            ret_set = self.dbms.getRaw(query)
            if len(ret_set) > 0:
                hs_job_dtl_id = ret_set[0][0]
                hs_job_mst_id = ret_set[0][1]
        except Exception as e:
            self.log.error(e.message)

        return hs_job_dtl_id, hs_job_mst_id

    def job_complete(self, job_info):
        self.set_date()
        job_id = job_info['job_id']
        self.job_update(job_info)

    def job_start_setup(self, job_info):
        self.set_date()
        job_id = job_info['job_id']
        tg_job_dtl_id = job_info['tg_job_dtl_id']
        # tg_job_dtl_id, tg_job_mst_id = self.get_tg_id(job_id)

        tg_job_mst_id = job_info['tg_job_mst_id']
        table_name = 'store.hs_job_mst'
        query = "select count(*) " \
                "  from {} where tg_job_mst_id = '{}' and job_exec_dt = '{}'".format(table_name,
                                                                                     tg_job_mst_id,
                                                                                     job_info['job_exec_dt'])
        self.log.info('query: {}'.format(query))
        mst_cnt = self.dbms.getRaw(query)[0][0]

        self.log.info('hs_job_mst cnt: {}'.format(mst_cnt))

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
            self.log.info('set_job: {}'.format(set_job))
            self.log.info('set_job.keys: {}'.format(set_job.keys()))
            self.dbms.dbInsertList(job_list, table_name)

        query = """select count(*) from store.hs_job_dtl where tg_job_dtl_id = {} and job_id={}"""\
            .format(tg_job_dtl_id, job_id)

        self.log.info('query: {}'.format(query))
        ret = self.dbms.getRaw(query)
        self.log.info('result: {}'.format(ret))

        if ret[0][0] == 0:
            query = "select hs_job_mst_id from store.hs_job_mst where tg_job_mst_id ='{}'".format(
                tg_job_mst_id)
            self.log.info('query: {}'.format(query))
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
        self.log.info('tg_job_dtl_id: {}'.format(tg_job_dtl_id))
        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        self.log.info('query: {}'.format(query))
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]
        start_time = job_status['start_time']
        end_time = job_status['end_time']
        self.log.info('start time: {}, end_time: {}'.format(start_time, end_time))
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

        self.log.info('#' * 50)
        self.log.info('query: {}'.format(query))
        self.log.info('#' * 50)
        self.dbms.queryExec(query)
        self.log.info('job_status: {}'.format(job_status['job_st']))

        if job_status['job_st'] == 'Fail':
            self.evt_ins(job_status)

        if job_status['job_st'] == 'End-OK':
            s, e, t = self.ov_monitor.get_set_status(tg_job_dtl_id)

            if e == 'E':
                self.ov_monitor.evt_send(tg_job_dtl_id, 'e')

            # 후행작업 존재여부 확인
            job_exist, post_tg_job_dtl_id = self.get_post_tg_job_dtl_id(tg_job_dtl_id)
            if job_exist:
                # 후행작업 수행여부 확인
                if not self.job_executed(post_tg_job_dtl_id):
                    job_info = self.post_job(job_status['job_id'], job_status['tg_job_dtl_id'])

                    self.log.info('job_info: {}'.format(job_info))
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
                            submit_job_info['shell_path'] = job_info['sh_path']  ## append line...

                            self.log.info('submit_job_info: {}'.format(submit_job_info))
                            self.log.info('run_type: {}'.format(job_info['run_type']))

                            try:
                                self.job_start_setup(job_info)
                            except Exception as e:
                                self.log.error(e.message)

                            self.job_submit(submit_job_info)

    def get_logfile(self, hs_job_dtl_id):
        query = """SELECT file_cntn FROM store.hs_job_logfile hjl  WHERE hs_job_dtl_id ='{}'
        """.format(hs_job_dtl_id)
        ret = self.dbms.getRaw(query)[0]
        log_content = ''
        if len(ret) > 0:
            log_content = ret[0]
        return log_content

    def get_progress(self):
        return '0'

    def get_rman_tag(self, job_status):
        job_id = job_status['job_id']
        query = """SELECT bk_tag FROM master.mst_shell ms WHERE sh_id IN (SELECT sh_id FROM master.mst_job mj WHERE job_id='{}') 
        """.format(job_id)
        try:
            rman_tag = self.dbms.getRaw(query)[0][0]
        except:
            rman_tag = 'unkown'
        ret_data = {'FLETA_PASS': 'kes2719!', 'CMD': 'GET_RMAN_TAG', 'ARG': {'rman_tag': rman_tag}}
        return ret_data

    def job_status_insert(self, job_status):
        self.set_date()
        tg_job_dtl_id = job_status['tg_job_dtl_id']
        self.log.info('tg_job_dtl_id: {}'.format(tg_job_dtl_id))

        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)

        ret_set = self.dbms.getRaw(query)[0]
        self.log.info('result_set: {}'.format(ret_set))
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]
        log_content = self.get_logfile(hs_job_dtl_id)
        # backup_file=self.get_backupfile(log_content,tg_job_dtl_id)

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

        try:
            data_set['bk_out_size'] = job_status['output_bytes']
        except Exception as e:
            self.log.error(e.message)
        # agent version 1.1 over
        try:
            data_set['wrt_spd'] = int(job_status['output_bytes_per_sec'])
            data_set['rd_spd'] = int(job_status['input_bytes_per_sec'])
            data_set['file_cnt'] = job_status['prg_cnt']
            data_set['tot_file_cnt'] = job_status['tot_cnt']
            data_set['prgrs'] = job_status['progress']
        except Exception as e:
            self.log.error(e.message)
            data_set['wrt_spd'] = '-1'
            data_set['rd_spd'] = '-1'
            data_set['file_cnt'] = '-1'
            data_set['tot_file_cnt'] = '-1'
            data_set['prgrs'] = '-1'

        data_set['memo'] = ''
        data_set['use_yn'] = 'Y'
        data_set['mod_usr'] = 'SYS'
        data_set['mod_dt'] = self.now_datetime
        data_set['reg_usr'] = 'SYS'
        data_set['reg_dt'] = self.now_datetime
        data_list = []
        # self.log.info(JOB_STATUS DATASET :',data_set)
        # self.log.info(ata_set['wrt_spd'],type(data_set['wrt_spd'] ))
        # self.log.info(ata_set['rd_spd'], type(data_set['rd_spd']))
        # self.log.info(ata_set['file_cnt'],type(data_set['file_cnt']))
        # self.log.info(ata_set['tot_file_cnt'],type(data_set['tot_file_cnt']))
        # self.log.info(ata_set['prgrs'],type(data_set['prgrs']))
        if 'prgrs_time' in data_set.keys():
            if data_set['prgrs_time'] == '':
                data_set['prgrs_time'] = 0
        data_list.append(data_set)

        db_table = 'store.hs_job_log'
        self.dbms.dbInsertList(data_list, db_table)
        self.log.info('start_time: {}, end_time: {}'.format(job_status['start_time'], job_status['end_time']))
        start_time = datetime.datetime.strptime(job_status['start_time'], '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
        end_time = datetime.datetime.strptime(job_status['end_time'], '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
        self.log.info('start_time: {}, end_time: {}'.format(start_time, end_time))
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
        self.log.info('#' * 50)
        self.log.info('query: {}'.format(query))
        self.log.info('#' * 50)
        self.dbms.queryExec(query)
        self.log.info('job_status: {}'.format(job_status['job_st']))

        if job_status['job_st'] == 'Fail':
            self.evt_ins(job_status)

        if job_status['job_st'] == 'End-OK':
            # 후행작업 존재여부 확인
            job_exist, post_tg_job_dtl_id = self.get_post_tg_job_dtl_id(tg_job_dtl_id)
            if job_exist:
                # 후행작업 수행여부 확인
                if not self.job_executed(post_tg_job_dtl_id):
                    job_info = self.post_job(job_status['job_id'], tg_job_dtl_id)
                    self.log.info('post job: {}'.format(job_info))
                    if not job_info == None:
                        self.log.info('rel_exec_type: {}'.format(job_info['rel_exec_type']))

                        if job_info['rel_exec_type'] == 'INST':
                            if not (job_info['run_type'] == 'HOLD'):
                                self.job_start_setup(job_info)
                                self.job_submit(job_info)

            s, e, t = self.ov_monitor.get_set_status(tg_job_dtl_id)

            self.log.info('s: {}, e: {}, t: {}'.format(s, e, t))

            if e == 'E':
                self.ov_monitor.evt_send(tg_job_dtl_id, 'e')

    def post_job(self, job_id, tg_job_dtl_id):
        query = "SELECT pre_job_id,post_job_id,rel_exec_type FROM master.mst_job where job_id= '{}'".format(job_id)
        ret = self.dbms.getRaw(query)[0]
        job_info = None
        self.log.info('result_set: {}'.format(ret))
        if len(ret) == 3:
            pre_job_id = ret[0]
            post_job_id = ret[1]
            rel_exec_type = ret[2]
        to_day = datetime.datetime.now().strftime('%Y%m%d')
        self.set_date()
        odate_1 = self.get_odate_1()

        query = "SELECT job_exec_dt FROM store.tg_job_dtl WHERE tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        target_odate = self.dbms.getRaw(query)[0][0]
        self.log.info('post_job_id: {}'.format(post_job_id))
        self.log.info('target odate: {}'.format(target_odate))
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
        tg.run_type ='RUN' 
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
 --    WHERE 
 --     tjm.job_exec_dt >= '{YYYYMMDD}' 
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
  WHERE job_id='{JOB_ID}' AND job_exec_dt='{ODATE}' order by 1 asc

            """.format(YYYYMMDD=odate_1, JOB_ID=post_job_id, TG_JOB_DTL_ID=tg_job_dtl_id, ODATE=target_odate)
            self.log.info('query: {}'.format(query))
            job = self.dbms.getRaw(query)[0]
            if len(job) > 0:
                job_info = {}
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
                job_info['shell_path'] = job[18]
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
        self.log.info('query: {}'.format(query))
        self.dbms.queryExec(query)

    def tg_job_dtl_log_run_type_insert(self, tg_job_dtl_id, run_type, module):
        """
        tg_job_dtl_log 에 지정된 RUN TYPE Insert
        rsn_desc='최초등록'이 job 생성시 insert 되어 rsn_desc에 이전 run_type 입력 가능
        """
        self.set_date()
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
                      tjdl.tg_job_dtl_id
                      ,tjdl.tg_job_mst_id
                      ,tjdl.job_id
                      ,tjdl.job_exec_dt
                      ,TO_CHAR(now(), 'YYYYMMDD')
                      ,tjdl.exec_time
                      ,'{RUN_TYPE}'
                      ,'900'
                      ,tjdl.run_type ||'-->'||'{RUN_TYPE}'
                      ,''
                      ,'Y'
                      ,'{MODULE}'
                      ,TO_CHAR(now(), 'YYYYMMDDHH24MISS')
                      ,'{MODULE}'
                      ,TO_CHAR(now(), 'YYYYMMDDHH24MISS')
                    FROM store.tg_job_dtl_log tjdl
                   WHERE 1 = 1
                     AND tg_job_dtl_id = {TG_JOB_DTL_ID}
                     AND tg_job_dtl_log_id = (SELECT MAX(tg_job_dtl_log_id)
                                                FROM store.tg_job_dtl_log tjdl2
                                               WHERE 1 = 1
                                                 AND tjdl.tg_job_dtl_id = tjdl2.tg_job_dtl_id)       
            """.format(RUN_TYPE=run_type, TG_JOB_DTL_ID=tg_job_dtl_id, MODULE=module)
        self.log.info('query: {}'.format(query))
        self.dbms.queryExec(query)

    def set_tg_job_dtl_log(self, job_id, tg_job_dtl_id):
        # tg_job_dtl_log insert
        # hs_job_dtl update (upd_stat = 'Y', use_yn= 'N'
        self.set_date()
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
        self.log.info('query: {}'.format(query))
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
      ,'선행작업 Not-OK'
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

        self.dbms.queryExec(query)

    def pre_job(self, job_id):
        query = "SELECT pre_job_id,post_job_id,rel_exec_type FROM master.mst_job where job_id= '{}'".format(job_id)
        ret = self.dbms.getRaw(query)[0]
        job_info = None
        self.log.info('result: {}'.format(ret))
        if len(ret) == 3:
            pre_job_id = ret[0]
            post_job_id = ret[1]
            rel_exec_type = ret[2]
        to_day = datetime.datetime.now().strftime('%Y%m%d')
        self.log.info('pre_job_id: {}'.format(pre_job_id))
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

    def regex_cnt(self, log_contrent, pattern):
        return len(re.findall(pattern, log_contrent))

    def job_log_update(self, log_return_data):
        """
        INPUT_BYTES_PER_SEC	NUMBER	Input read-rate-per-second
        OUTPUT_BYTES_PER_SEC	NUMBER	Output write-rate-per-second
        OUTPUT_BYTES_PER_SEC run_spd_write
        INPUT_BYTES_PER_SEC run_spd_read
        :param log_return_data:
        :return:
        """
        self.set_date()
        self.log.info('LOG_UPDATE')
        job_id = log_return_data['job_id']
        self.log.info('job_id: {}'.format(job_id))
        tg_job_dtl_id = log_return_data['tg_job_dtl_id']
        self.log.info('tg_job_dtl_id: {}'.format(tg_job_dtl_id))
        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        self.log.info('qeury: {}'.format(query))
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]
        tg_job_mst_id = ret_set[2]
        query = "SELECT COUNT(hs_job_dtl_id) FROM STORE.HS_JOB_LOGFILE WHERE hs_job_dtl_id = '{}'".format(
            hs_job_dtl_id)
        self.log.info('query: {}'.format(query))
        cnt_set = self.dbms.getRaw(query)
        cnt = cnt_set[0][0]
        if int(cnt) == 0:
            ins_bit = True
        else:
            ins_bit = False
        self.log.info('INS BIT: {}'.format(ins_bit))
        self.log.info(log_return_data['log_contents'])
        log_file = os.path.basename(log_return_data['log_contents'])
        self.log.info('log_file: {}'.format(log_file))
        self.log.info('sections: {}'.format(self.cfg.sections()))
        self.log.info('common options: {}'.format(self.cfg.options('common')))
        log_path = self.cfg.get('common', 'log_file_path')
        # log_path = os.path.join('E:\\','Fleta','data','ibrm_backup_log',log_file)
        log_path = os.path.join(log_path, log_file)
        self.log.info('log path: {}, isfile: {}'.format(log_path, os.path.isfile(log_path)))
        if os.path.isfile(log_path):
            with open(log_path) as f:
                log_content = f.read()
            log_content = log_content.replace("'", '`')
            try:
                log_content = log_content.decode('cp949').encode('utf-8')
            except:
                pass
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
            # log_data['run_spd_write'] = log_return_data['output_bytes_per_sec']
            # log_data['run_spd_read'] = log_return_data['input_bytes_per_sec']
            log_data['use_yn'] = 'Y'
            log_data['memo'] = ''
            log_data['mod_usr'] = ''
            log_data['mod_dt'] = ''
            log_data['reg_usr'] = 'SYS'
            log_data['reg_dt'] = self.now_datetime

            # self.log.info(og_data)
            log_update_list = []
            log_update_list.append(log_data)
            table_name = 'store.hs_job_logfile'
            self.log.info('log_data: {}'.format(log_data))
            self.dbms.dbInsertList(log_update_list, table_name)
        else:
            reg_date = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            if not log_content == '':
                query = """UPDATE store.hs_job_logfile
         SET file_cntn='{LOG_CONTENT}', rm_bk_stat='{JOB_ST}', prgrs_time='{ELAPSED_SEC}', mod_dt='{MOD_DT}'
         WHERE hs_job_dtl_id = '{HS_JOB_DTL_ID}';
                    """.format(LOG_CONTENT=log_content, JOB_ST=log_return_data['status'],
                               ELAPSED_SEC=log_return_data['elapsed_seconds'], MOD_DT=reg_date,
                               HS_JOB_DTL_ID=hs_job_dtl_id)

                self.dbms.queryExec(query)

    def get_backupfile(self, log_content, tg_job_dtl_id):
        self.log.info('tg_job_dtl_id: {}'.format(tg_job_dtl_id))

        query = """SELECT back_type FROM master.mst_shell ms WHERE sh_id IN (
SELECT sh_id FROM master.mst_job mj WHERE job_id IN (
SELECT job_id FROM store.hs_job_dtl hjd WHERE tg_job_dtl_id ='{}'))""".format(tg_job_dtl_id)
        self.log.info('query: {}'.format(query))
        ret = self.dbms.getRaw(query)[0]
        self.log.info('result_set: {}'.format(ret))
        backup_type = ret[0]
        """
        backup type 

        ARCH
        INCR_L0
        INCR_L1
        FULL_L0
        MRG
        INCR_MRG
        DSC
        DSD
        ASC
        ASD
        INCL_L0_DEL
        INCL_L1_DEL
        FULL_L0_DEL"""
        backup_cnt = '0'
        if backup_type == 'ARCH':
            pt = 'backup set complete'
            backup_cnt = self.regex_cnt(log_content, pt)

        if 'INCR' in backup_type or 'FULL' in backup_type:
            pt = 'backup set complete'
            backup_cnt = self.regex_cnt(log_content, pt)
        pt = 'backup set complete'
        backup_cnt = self.regex_cnt(log_content, pt)
        return backup_cnt

    def regex_cnt(self, log_content, pattern):
        return len(re.findall(pattern, log_content))

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
        self.log.info('sql: {}'.format(sql))
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
        self.set_date()
        tg_job_dtl_id, tg_job_mst_id = '', ''
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
        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        self.log.info('query: {}'.format(query))
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
        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)
        self.log.info('query: {}'.format(query))
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]
        tg_job_mst_id = ret_set[2]
        self.log.info('hs_job_dtl_id: {}, hs_job_mst_id: {}, tg_job_mst_id: {}'.
                      format(hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id))
        now_dt = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        query = """UPDATE store.hs_job_dtl SET
                job_stat ='Fail', 
                rm_bk_stat ='Run Fail', 
                memo = '{memo}',
                mod_dt ='{MOD_DT}'
                WHERE hs_job_dtl_id = '{HS_JOB_DTL_ID}'
        """.format(memo=memo, MOD_DT=now_dt, HS_JOB_DTL_ID=hs_job_dtl_id)
        self.log.info('query: {}'.format(query))
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
        if 'prgrs_time' in job_info.keys():
            if job_info['prgrs_time'] == '':
                job_info['prgrs_time'] = 0
        job_info_list = [job_info]
        table_name = 'store.hs_job_log'
        self.dbms.dbInsertList(job_info_list, table_name)
        ret_data = {'FLETA_PASS': 'kes2719!', 'CMD': 'JOB_UPDATE_SUCC', 'ARG': {'result': 'succ'}}
        self.evt_ins(job_info)
        return ret_data

    def job_aleady_exist(self, job_id, tg_job_dtl_id, memo):
        self.log.info('job_id: {}, tg_job_dtl_id: {}, memo: {}'.format(job_id, tg_job_dtl_id, memo))
        memo = memo.replace("'", "`")
        now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.log.info('format: {}'.format(now_str))
        query = """UPDATE store.hs_job_dtl SET job_stat='Fail', memo='{}',mod_dt='{}' WHERE tg_job_dtl_id = '{}' """.format(
            memo, now_str, tg_job_dtl_id)

        self.log.info('query: {}'.format(query))
        self.dbms.queryExec(query)
        # job_id_info = self.get_job_id(tg_job_dtl_id)
        # hs_job_dtl_id = job_id_info['hs_job_dtl_id']
        # hs_job_mst_id = job_id_info['hs_job_mst_id']
        # tg_job_mst_id = job_id_info['tg_job_mst_id']
        # query = "select hs_job_dtl_id from store.hs_job_dtl where job_id = '{JOB_ID}' and job_exec_dt = '{TODAY}'".format(
        #     JOB_ID=job_id, TODAY=self.today_str)
        # self.log.info(uery)
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
        self.log.info('query: {}'.format(query))
        cnt = self.dbms.getRaw(query)[0][0]
        self.log.info('cnt: {}'.foramt(cnt))

        if cnt == 0:
            job_info = {}
            job_info['tg_job_mst_id'] = tg_job_mst_id
            job_info['job_exec_dt'] = self.odate
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
        self.log.info('query: {}'.format(query))
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
        job_info['job_exec_dt'] = self.odate
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
        query = "select hs_job_dtl_id from store.hs_job_dtl where job_id = '{JOB_ID}' and job_exec_dt = '{TODAY}'".format(
            JOB_ID=job_id, TODAY=self.odate)
        self.log.info('query: {}'.format(query))
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
        if 'prgrs_time' in job_info.keys():
            if job_info['prgrs_time'] == '':
                job_info['prgrs_time'] = 0
        table_name = 'store.hs_job_log'
        self.dbms.dbInsertList(job_info_list, table_name)

    def job_update(self, job_status):
        self.set_date()
        job_id = job_status['job_id']
        tg_job_dtl_id = job_status['tg_job_dtl_id']

        query = "SELECT hs_job_dtl_id, hs_job_mst_id, tg_job_mst_id FROM store.hs_job_dtl where tg_job_dtl_id = '{TG_JOB_DTL_ID}'".format(
            TG_JOB_DTL_ID=tg_job_dtl_id)

        self.log.info('query: {}'.format(query))
        ret_set = self.dbms.getRaw(query)[0]
        hs_job_dtl_id = ret_set[0]
        hs_job_mst_id = ret_set[1]
        tg_job_mst_id = ret_set[2]
        self.log.info('tg_job_dtl_id: {}'.format(tg_job_dtl_id))
        self.log.info('tg_job_mst_id: {}'.format(tg_job_mst_id))
        self.log.info('hs_job_dtl_id: {}'.format(hs_job_dtl_id))
        self.log.info('hs_job_mst_id: {}'.format(hs_job_mst_id))
        self.log.info('job_update : ')
        """
    {'FLETA_PASS': 'kes2719!', 'CMD': 'JOB_STATUS', 'ARG': {'status': 'COMPLETED', 'start_time': '2020-10-25 12:37:00', 'pid': '3335',
    'elapsed_seconds': '803', 'session_recid': '515', 'job_id': '13', 'input_type': 'DB INCR',
    'session_id': '515', 'end_time': '2020-10-25 12:50:23', 'write_bps': '117.98M',
    'session_stamp': '1054730218', 'ora_sid': 'ibrm'}}
        :param job_status:
        :return:
        """
        if 'COMPLETED' in job_status['status']:
            job_st = 'End-OK'
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
        self.log.info('query: {}'.format(query))
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
            job_info = self.post_job(job_id, tg_job_dtl_id)
            if job_info['rel_exec_type'] == 'INST':
                self.job_submit(job_info)
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
        self.log.info('query: {}'.format(query))
        try:
            self.dbms.queryExec(query)
        except:
            pass
        evt_info['tg_job_dtl_id'] = tg_job_dtl_id
        log_dt = datetime.datetime.now().strftime('%Y%m%d')
        evt_info['log_dt'] = log_dt
        query = "SELECT job_nm, work_div_1, work_div_2, sh_id FROM master.mst_job where job_id ='{}'".format(job_id)
        self.log.info('query: {}'.format(query))
        ret = self.dbms.getRaw(query)[0]
        job_nm = ret[0]
        work_div_1 = ret[1]
        work_div_2 = ret[2]
        sh_id = ret[3]
        query = """SELECT sh_nm,  svr_id, svr_nm, ip_addr, db_id, db_nm, back_type, sh_file_nm FROM master.mst_shell where sh_id = '{}'""".format(
            sh_id)
        self.log.info('query: {}'.format(query))
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
        self.log.info('evt_info: {}'.format(evt_info))
        table_name = 'event.evt_log'
        self.dbms.dbInsertList(evt_info_list, table_name)
        # if job_st == 'End-OK':

    def evt_ins(self, return_data):
        """
        시스템유형 sys_type varchar 20 N  TO_CHAR(current_timestamp, 'YYYYMMDD') [시스템유형] 모니터링, 스케쥴링 ( mnt, sch )
로그일자 log_dt varchar 8 N   [로그일자]
서버 ID svr_id integer  N  0 [서버 ID] 서버 ID Unique
DB ID db_id integer  N  0 [DB ID]
작업ID job_id integer  N  0 [작업ID] JOB ID Unique
일작업상세ID tg_job_dtl_id integer  N  0 [일작업상세ID]
이벤트유형 evt_type varchar 20 N  0 [이벤트유형] 이벤트 분류코드 코드화 필요 ( 대 / 중 / 소 )
이벤트상세유형 evt_dtl_type varchar 20 N  'N/A' [이벤트상세유형]
이벤트코드 evt_code varchar 50 N   [이벤트코드]
이벤트레벨 evt_lvl varchar 50 N  'N/A' [이벤트레벨]
이벤트메시지 evt_msg varchar 200 Y   [이벤트메시지]
이벤트내용 evt_cntn text  Y   [이벤트내용]
장비분류코드 dev_type varchar 20 N  'N/A' [장비분류코드] 이벤트대상장비유형
조치대상여부 act_yn varchar 2 N  'N' [조치대상여부] 이벤트 조치 대상여부  Y/N
조치유형 act_type varchar 30 N  'N/A' [조치유형] 처리유형 ( msg / tell / 등등 코드화 필요)
조치결과여부 act_rslt varchar 10 N  'N' [조치결과여부] 처리결과 상태 ( Y/N/C/D 등 코드처리 필요)
조치결과상세 act_desc text  Y   [조치결과상세] 조치결과상세
조치자 act_usr varchar 14 N  'SYS' [조치자]
조치일시 act_dt varchar 14 Y   [조치일시]
메모 memo varchar 200 Y   [메모] 부가메모
사용여부 use_yn varchar 1 N  'Y' [사용여부] Y,N
수정자ID mod_usr varchar 20 Y  'SYS' [수정자ID] 등록자의 사용자ID
수정일시 mod_dt varchar 14 Y  TO_CHAR(current_timestamp, 'YYYYMMDDHH24MISS') [수정일시] 수정일시.YYYYMMDDHH24MISS
등록자ID reg_usr varchar 20 N   [등록자ID] 등록자의 사용자ID
등록일시 reg_dt varchar 14 N  TO_CHAR(current_timestamp, 'YYYYMMDDHH24MISS') [등록일시] 등록일시. 백업인경우 :DB 시간
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
        self.log.info('query: {}'.format(query))
        ret = self.dbms.getRaw(query)[0]
        job_nm = ret[0]
        work_div_1 = ret[1]
        work_div_2 = ret[2]
        sh_id = ret[3]
        query = """SELECT sh_nm,  svr_id, svr_nm, ip_addr, db_id, db_nm, back_type, sh_file_nm FROM master.mst_shell where sh_id = '{}'""".format(
            sh_id)
        self.log.info('query: {}'.format(query))
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

        self.log.info('evt_info: {}'.format(evt_info))
        table_name = 'event.evt_log'
        self.dbms.dbInsertList(evt_info_list, table_name)
        # if job_st == 'End-OK':

    def job_status_fail_update(self, job_id):
        self.set_date()
        hs_job_dtl_id, hs_job_mst_id = self.get_hs_id(job_id)
        query = "SELECT  hs_job_dtl_id,hs_job_mst_id  FROM STORE.HS_JOB_DTL WHERE JOB_ID ='{JOB_ID}' and job_exec_dt='{EXEC_DT}'".format(
            JOB_ID=job_id, EXEC_DT=self.odate)
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
        self.log.info('query: {}'.format(query))
        try:
            self.dbms.queryExec(query)
        except:
            pass
        # query = """UPDATE store.hs_job_mst
        #             SET job_stt_dt='{JOB_STT_DT}',
        #             job_end_dt='{JOB_END_DT}'
        #      WHERE tg_job_mst_id= '{MST_ID}'
        #      """.format(JOB_STT_DT=job_stt_dt, JOB_END_DT=job_end_dt, MST_ID=mst_id)
        # self.log.info(uery)
        # try:
        #     self.dbms.queryExec(query)
        # except:
        #     pass

    def get_post_tg_job_dtl_id(self, tg_job_dtl_id):
        """
        후행작업의 존재여부와 tg_job_dtl_id값을 조회
        :param tg_job_dtl_id:
        :return: 후행작업의 tg_job_dtl_id 를 반환
                 존재하지 않을 경우 0을 반환
        """
        query = "SELECT COALESCE(ptjd.tg_job_dtl_id, 0) AS post_tg_job_dtl_id " \
                "  FROM store.tg_job_dtl tjd " \
                " INNER JOIN master.mst_job mj " \
                "    ON tjd.job_id = mj.job_id " \
                "  LEFT OUTER JOIN store.tg_job_dtl ptjd " \
                "    ON tjd.job_exec_dt = ptjd.job_exec_dt    " \
                "   AND mj.post_job_id = ptjd.job_id " \
                " WHERE 1 = 1 " \
                "   AND tjd.tg_job_dtl_id = {}".format(tg_job_dtl_id)

        result = self.dbms.getRaw(query)

        if len(result) == 0:
            return False, 0
        else:
            if result[0][0] == 0:
                return False, 0
            else:
                return True, result[0][0]

    def job_executed(self, tg_job_dtl_id):
        """
        작업이 수행되면 hs_job_dtl 에 값이 존재
        :param tg_job_dtl_id:
        :return: Boolean
        """
        query = "SELECT count(*) AS cnt" \
                "  FROM store.hs_job_dtl tjd " \
                " WHERE 1 = 1 " \
                "   AND tjd.tg_job_dtl_id = {}".format(tg_job_dtl_id)

        result = self.dbms.getRaw(query)

        if result[0][0] == 0:
            return False
        else:
            return True

    def job_submit(self, job_info):
        self.log.info('JOB START')
        self.log.info('job_info: {}'.format(job_info))
        HOST = job_info['svr_ip']
        PORT = 53001
        ss = ibrm_daemon_send.SocketSender(HOST, PORT)
        #
        ss.jos_excute(job_info)


if __name__ == '__main__':
    job_st = ibrm_job_stat()
    # job_info=job_st.post_job('75')
    # if job_info['rel_exec_type'] == 'INST':
    #     job_st.job_submit(job_info)
    job_st.set_pause('75')
