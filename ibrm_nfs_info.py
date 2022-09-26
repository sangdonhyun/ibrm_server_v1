import os
import ibrm_daemon_send
import ibrm_dbms
import nfs_load
import json


class nfs_info():
    def __init__(self):
        self.ss = ibrm_daemon_send
        self.rdb=ibrm_dbms.fbrm_db()
        self.nfs = nfs_load.fs_load()

    def get_cfg(self):
        pass

    def ag_list(self):
        query="""    SELECT * FROM (
    SELECT ora_id,moi.svr_id,ora_sid,db_name,svr_hostname,svr_ip_v4,ma.agt_stat,ma.agt_id,main_flag FROM master.master_ora_info moi 
    LEFT OUTER JOIN master.mst_agent ma ON moi.svr_id = ma.svr_id 
    ) tg WHERE main_flag='T'  
;
        """
        print query
        ret=self.rdb.getRaw(query)
        return ret

    def main(self):
        ag_list = self.ag_list()
        for ag in ag_list:

            print ag
            HOST = ag[5]
            PORT = 53001
            # shell_name = 'IBRM_Archive.sh'
            # shell_type = 'ARCH'
            # db_name = 'IBRM'
            # ora_sid = 'ibrm'
            #

            sender = self.ss.SocketSender(HOST, PORT)
            cmd = "AGENT_NFS_INFO"
            ret = sender.ss_socket_send(cmd)
            print type(ret)
            json_data = ret

            if not ret == '' and 'Errno 10061' not in ret:
                json_data = json.loads(ret)
                print "json_data : ",json_data
                print json_data['hostname']
                self.nfs.nfs_load(ret)



if __name__ == '__main__':
    nfs=nfs_info().main()