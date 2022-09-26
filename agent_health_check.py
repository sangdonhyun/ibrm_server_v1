# -*- coding: utf-8 -*-
import datetime
import time
import ast
import ibrm_dbms
import ibrm_daemon_send
import socket

class ag_check():
    def __init__(self):
        self.rdb=ibrm_dbms.fbrm_db()
        self.re_cnt = 5

    def ag_list(self):
        query="""    SELECT * FROM (
    SELECT ora_id,moi.svr_id,ora_sid,db_name,svr_hostname,svr_ip_v4,ma.agt_stat,ma.agt_id,main_flag FROM master.master_ora_info moi 
    LEFT OUTER JOIN master.mst_agent ma ON moi.svr_id = ma.svr_id 
    ) tg WHERE main_flag='T'  
;
        """

        ret=self.rdb.getRaw(query)

        ag_list=[]
        if len(ret)>0:
            for db_info in ret:
                db={}
                db['ora_id'] = db_info[0]
                db['svr_id'] = db_info[1]
                db['ora_sid'] = db_info[2]
                db['db_name'] = db_info[3]
                db['svr_hostname'] = db_info[4]
                db['svr_ip_v4'] = db_info[5]
                db['agt_stat'] = db_info[6]
                db['agt_id'] = db_info[7]
                ag_list.append(db)

            return ag_list
        else:
            return []

    def evt_ins(self, evt):
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
            """
            MON
            AGT HLT
            
            ora_id,svr_id,ora_sid,db_name,svr_hostname,svr_ip_v4
            
            """
            # return_data = {'status': 'COMPLETED', 'input_bytes': '63655968768', 'job_id': '37', 'input_type': 'DB INCR', 'start_time': '2020-12-04 14:38:51', 'job_st': 'End-OK', 'pid': '10377', 'session_id': '5866', 'elapsed_seconds': '524', 'end_time': '2020-12-04 14:47:35', 'tg_job_dtl_id': '198744', 'write_bps': '655.73K', 'session_stamp': '1058279929', 'ora_sid': 'ibrm', 'session_recid': '5866'}

            evt['svr_id']
            evt['ora_id']
            log_dt=datetime.datetime.now().strftime('%Y%m%d')
            evt_info = {}

            evt_info['job_id'] = 0
            evt_info['tg_job_dtl_id'] = 0
            evt_info['log_dt'] = log_dt
            evt_info['svr_id'] =evt['svr_id']
            evt_info['db_id'] = evt['ora_id']
            evt_info['sys_type'] = 'MON'
            evt_info['evt_type'] = 'MON'
            evt_info['evt_dtl_type'] = 'AGT HLT'
            evt_info['evt_lvl'] = 'WARN'
            evt_info['evt_msg'] = 'Agent health check error {}({}) ibrm agent down'.format(evt['svr_hostname'],evt['svr_ip_v4'])
            evt_info['evt_cntn'] ='Agent health check error {}({}) ibrm agent down'.format(evt['svr_hostname'],evt['svr_ip_v4'])
            evt_info['dev_type'] = 'AGT'
            evt_info['act_yn'] = 'Y'
            evt_info['reg_usr'] = 'SYS'
            evt_info['evt_code'] = 'AGT DOWN'
            evt_info['reg_dt'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            evt_info_list = []
            query="""SELECT count(*) FROM event.evt_log el WHERE db_id={} and log_dt='{}' AND evt_dtl_type='AGT HLT' ORDER BY 1 desc""".format(evt['ora_id'],log_dt)
            ret=self.rdb.getRaw(query)

            if ret[0][0] == 0:
                evt_info_list.append(evt_info)
            evt_info_list.append(evt_info)

            table_name = 'event.evt_log'
            self.rdb.dbInsertList(evt_info_list, table_name)
            # if job_st == 'End-OK':

    def port_scan(self,ip,port):
        scan_bit=False
        TCPsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        TCPsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        TCPsock.settimeout(1)
        try:
            TCPsock.connect((ip, port))
            scan_bit = True
        except Exception as e:
            print str(e)
            scan_bit = False
        return scan_bit

    def update_agent_status(self,ag):
        """
        agt_id
        svr_id
        agt_ver
        agt_fname
        agt_path
        patch_dt
        agt_stat
        adtnl_itm_1
        adtnl_itm_2
        descr
        use_yn
        mod_usr
        mod_dt
        reg_usr
        reg_dt
        :param ag:
        :return:
        """

        cnt_query =""" select * from master.mst_agent where svr_id = '{}'
        """.format(ag['svr_id'])

        ret = self.rdb.getRaw(cnt_query)

        if len(ret) == 0:
            query="""INSERT INTO master.mst_agent
        (
        svr_id, 
        agt_ver, 
        agt_fname, 
        agt_path, 
        patch_dt, 
        agt_stat, 
        adtnl_itm_1, 
        adtnl_itm_2, 
        descr, 
        use_yn, 
        mod_usr, 
        mod_dt, 
        reg_usr, 
        reg_dt)
        VALUES(
       
        {SVR_ID}, 
        '{IBRM_AGENT_VERSION}', 
        '{IBRM_PATH}',
        '',  
        '{IBRM_AGENT_PATCH_DT}', 
        '{CHECKING}', 
        '', 
        '', 
        '', 
        'Y', 
        'SYS', 
        to_char(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'::text), 
        'SYS', 
        to_char(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'::text));
            """.format(

                SVR_ID=ag['svr_id'],
                IBRM_AGENT_VERSION=ag['IBRM_AGENT_VERSION'],
                IBRM_PATH=ag['IBRM_PATH'],
                IBRM_AGENT_PATCH_DT=ag['IBRM_AGENT_PATCH_DT'],
                CHECKING=ag['CHECKING'],
            )

        else:

            query="""
            UPDATE master.mst_agent
            SET 
                agt_ver='{IBRM_AGENT_VERSION}', 
                agt_path='{IBRM_PATH}', 
                patch_dt='{IBRM_AGENT_PATCH_DT}', 
                agt_stat='{CHECKING}', 
                reg_dt=to_char(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'::text)
            WHERE svr_id='{SVR_ID}'

            """.format(
                SVR_ID=ag['svr_id'],
                IBRM_AGENT_VERSION=ag['IBRM_AGENT_VERSION'],
                IBRM_PATH=ag['IBRM_PATH'],
                IBRM_AGENT_PATCH_DT=ag['IBRM_AGENT_PATCH_DT'],
                CHECKING=ag['CHECKING']
            )
        try:

            self.rdb.queryExec(query)
        except Exception as e:
            print str(e)



    def set_agt_hist(self,ag):
        """
        ag['CHECKING'] = info['ARG']['CHECKING']
            ag['IBRM_AGENT_VERSION'] = info['ARG']['IBRM_AGENT_VERSION']
            ag['IBRM_PATH'] = info['ARG']['IBRM_PATH']
            ag['IBRM_AGENT_PATCH_DT'] =info['ARG']['IBRM_AGENT_PATCH_DT']
            ag['AG_STATUS'] = ag_status
        :param ag:
        :return:
        """

        query="""
        INSERT INTO master.mst_agent_hist (
            agt_id,
            svr_id, 
            agt_ver, 
            agt_path,
            agt_fname, 
            patch_dt, 
            agt_pre_stat, 
            agt_stat, 
            use_yn, 
            mod_usr, 
            mod_dt, 
            reg_usr, 
            reg_dt)
        VALUES(
            '{AGT_ID}',
            '{SVR_ID}',  
            '{IBRM_AGENT_VERSION}', 
            '{IBRM_PATH}', 
            '',
            '{IBRM_AGENT_PATCH_DT}',
            '{AG_STATUS}', 
            '{CHECKING}', 
            'Y', 
            'SYS', 
            to_char(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'::text), 
            'SYS', 
            to_char(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'::text));
            """.format(
            AGT_ID=ag['agt_id'],
            SVR_ID=ag['svr_id'],
            IBRM_AGENT_VERSION=ag['IBRM_AGENT_VERSION'],
            IBRM_PATH=ag['IBRM_PATH'],
            IBRM_AGENT_PATCH_DT=ag['IBRM_AGENT_PATCH_DT'],
            AG_STATUS=ag['agt_stat'],
            CHECKING=ag['CHECKING']
            )
        try:
            self.rdb.queryExec(query)
        except Exception as e:
            print str(e)


    def check_ag_status(self,HOST,PORT,agt_stat):

        for i in range(self.re_cnt):
            port_status = self.port_scan(HOST, PORT)
            if port_status:
                break
            else:
                checking='NOK'
        print 'HOST portstat :',HOST,PORT,port_status
        if port_status:
            for i in range(self.re_cnt):
                try:
                    ss = ibrm_daemon_send.SocketSender(HOST, PORT)
                    ret = ss.ag_health_check()
                    info = ast.literal_eval(ret)
                    checking= info['ARG']['CHECKING']
                except Exception as e:
                    print str(e)
                    checking = 'NOK'


                if not checking == 'NOK':
                    break

                if agt_stat == checking:
                    break
        # checking = 'NOK'
        if checking == 'NOK':
            info = {}
            arg = {}
            arg['IBRM_AGENT_PATCH_DT'] = datetime.datetime.now().strftime('%Y%m%d')
            arg['IBRM_AGENT_VERSION'] = 'unknown'
            arg['CHECKING'] = 'NOK'
            arg['IBRM_PATH'] = 'unkown'
            info['ARG'] = arg
        return info


    def main(self):
        ag_list=self.ag_list()

        for ag in ag_list:
            print ag
            print 'agt_stat',ag['agt_stat']
            HOST= ag['svr_ip_v4']
            # HOST= '121.170.193.222'
            PORT = 53001
            # try:

            info= self.check_ag_status(HOST,PORT,ag['agt_stat'])
            print info
            # print info['AGR']['CHECKING']

            checking= info['ARG']['CHECKING']
            version = info['ARG']['IBRM_AGENT_VERSION']
            ibrm_path = info['ARG']['IBRM_PATH']
            ag['CHECKING'] = info['ARG']['CHECKING']
            ag['IBRM_AGENT_VERSION'] = info['ARG']['IBRM_AGENT_VERSION']
            ag['IBRM_PATH'] = info['ARG']['IBRM_PATH']
            ag['IBRM_AGENT_PATCH_DT'] =info['ARG']['IBRM_AGENT_PATCH_DT']

            # ag_status ='Not OK'
            print 'agt_stat :',ag['agt_stat']


            """
            agent status OK -> NOK
            agent status NOK -> OK
            """

            print checking,checking == ag['agt_stat'],ag['agt_id'] ,ag['agt_id'] > 0
            if not checking == ag['agt_stat']:

                if ag['agt_id'] > 0:
                    self.set_agt_hist(ag)
                    if checking == 'NOK':
                        self.evt_ins(ag)
            print '-' * 30
            print 'ag :',ag

            self.update_agent_status(ag)
            # except:
            #     ag['CHECKING'] = 'NOK'
            #     ag['IBRM_AGENT_VERSION'] = 'unkown'
            #     ag['IBRM_PATH'] = 'unkown'
            #     ag['IBRM_AGENT_PATCH_DT'] = 'unkown'
                # self.evt_ins(ag)
                # self.update_agent_status(ag)




if __name__=='__main__':
    cnt=1
    while True:
        ag_check().main()
        cnt=cnt+1
        print 'cnt :',cnt
        print '='*50
        time.sleep(60*5)