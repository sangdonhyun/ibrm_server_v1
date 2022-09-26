# -*- coding: utf-8 -*-
import ConfigParser
import os
import sys
import glob
import datetime
import ibrm_dbms
import json

class fs_load():
    def __init__(self):
        self.cfg = self.get_cfg()
        self.today = datetime.datetime.now()
        self.db = ibrm_dbms.fbrm_db()

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config', 'config.cfg')
        cfg.read(cfgFile)
        return cfg

    def get_data_file(self):
        path = self.cfg.get('common', 'ibrm_path')
        data_path = os.path.join(path, 'data')
        fs_data_path = os.path.join(path, 'data', 'FBRM_NFS')
        print fs_data_path
        file_list = glob.glob(os.path.join(fs_data_path, '*.json'))
        return file_list



    def get_nfs_capacity(self,fs):
        ret = '','','',''
        for arg in self.arg_list:
            if fs== arg[0]:
                ret= arg[1],arg[2],arg[3],arg[4]
        return ret
    def nfs_mounted(self,data):
        """check_date_time character varying(20) COLLATE pg_catalog."default",
        server_hostname character varying(50) COLLATE pg_catalog."default",
        server_ip character varying(16) COLLATE pg_catalog."default",
        filesystem character varying(50) COLLATE pg_catalog."default",
        mounted  character varying(50) COLLATE pg_catalog."default",
        mounted_bit  bool default False ,
        check_bit bool default False ,
        check_date_time

        fbrm_date
        server_hostname
        server_ip
        filesystem
        mounted
        zfs_hostname
        zfs_name
        zfs_ip
        mounted_bit
        ussage_blocks
        ussage_used
        ussage_available
        ussage_used_capacity
        check_bit
        zfs_cluster
        node_name
        """
        nfs_list=[]
        zfs_name = ''
        zfs_ip = ''
        blocks, Used, Available, Used_rate = '','','',''
        print '-'*40
        print self.arg_list

        for line in data.splitlines():
            if ',' in line:
                nfs={}
                lineset=line.split(',')
                # print lineset
                nfs['check_date_time'] =lineset[0]
                nfs['server_hostname'] =lineset[1]
                nfs['server_ip'] = lineset[2]
                nfs['filesystem'] = lineset[3].strip()


                blocks, Used, Available, Used_rate = self.get_nfs_capacity(nfs['filesystem'])

                # print nfs['filesystem']
                print 'available:',blocks, Used, Available, Used_rate
                # nfs['ussage_blocks'] = blocks
                nfs['ussage_used'] = Used
                nfs['ussage_available'] = Available
                nfs['ussage_used_capacity'] = Used_rate



                nfs['mounted'] = lineset[4]
                nfs['zfs_hostname'] = lineset[5]
                nfs['mounted_bit'] = lineset[6]
                nfs['check_bit'] = 'True'
                nfs['fbrm_date'] = self.today.strftime('%Y-%m-%d')


                fs= nfs['filesystem']
                if ':' in fs:
                    net_zfs=fs.split(':')[0]
                    if '.' in net_zfs:
                        zfs_name = zfs_name
                        zfs_ip = zfs_ip
                    else:
                        zfs_name  = net_zfs
                        query ="select zfs_service_ip from live.live_zfs_server_hosts where zfs_hostname = '{zfs_name}'".format(zfs_name=zfs_name)
                        print query
                        try:
                            zfs_ip =self.db.get_row(query)[0][0]
                        except:
                            zfs_ip = ''

                        query = "select zfs_name,zfs_cluster from live.live_zfs_network_interfaces where  zfs_service_ip = '{service_ip}'".format(service_ip=zfs_ip)
                        print query
                        try:
                            zfs_name = self.db.get_row(query)[0][0]
                            asn = self.db.get_row(query)[0][1]
                            nfs['zfs_name'] = zfs_name
                            nfs['node_name'] = zfs_name
                            nfs['zfs_ip'] = zfs_ip

                            nfs['zfs_cluster'] = asn

                            nfs_list.append(nfs)
                        except:
                            pass

        print nfs_list
        for arg in  self.arg_list:
            print arg


        tb_name='live.live_svr_nfs_mounted_on'
        query = "delete from {} where server_hostname = '{}'".format(tb_name, self.hostname)
        print query
        self.db.queryExec(query)
        self.db.dbInsertList(nfs_list,tb_name)
        tb_name = 'store.store_day_svr_nfs_mounted_on'

        self.db.dbInsertList(nfs_list, tb_name)

        return nfs_list


    def get_json_format(self,f):

        with open(f) as json_file:
            json_data = json.load(json_file)
        return json_data


    def get_cluster(self,service_ip):
        query ="SELECT zfs_name,zfs_default_ip,zfs_cluster,node_name FROM live.live_zfs_network_interfaces  lzni WHERE zfs_service_ip ='{}'".format(service_ip)
        rows = self.db.get_row(query)
        zfs_name, zfs_default_ip, zfs_cluster, node_name = '', '', '', ''
        if len(rows)>0:
            zfs_name, zfs_default_ip, zfs_cluster, node_name=rows[0]
        return zfs_name, zfs_default_ip, zfs_cluster, node_name

    def validIP(self,address):
        parts = address.split(".")
        if len(parts) != 4:
            return False
        for item in parts:
            if not 0 <= int(item) <= 255:
                return False
        return True
    def get_service_ip(self,hosts,hostname):
        service_ip=''
        for host in hosts:
            if 'alias' in host.keys():
                if hostname in host['alias'] :
                    service_ip = host['ip']
                    break
            if hostname == host['hostname']:
                service_ip = host['ip']
                break
        print service_ip
        return service_ip

    def nfs_load(self,json_data):
        nfs_list = []
        print type(json_data)
        json_data=json.loads(json_data)
        server_hostname = json_data['hostname']
        server_ip = json_data['ip']
        server_datetime = datetime.datetime.strptime(json_data['datetime'], '%Y-%m-%d %H:%M:%S')
        # print json_data
        # print json_data["hostname"]
        # print json_data["ip"]
        hosts = json_data["hosts"]

        fs_systems = json_data["filesystem"]

        for fs in fs_systems:
            if ':' in fs['Filesystem']:
                """
                server_hostname
                server_ip
                filesystem
                mounted
                zfs_hostname
                zfs_name
                zfs_ip
                mounted_bit
                ussage_blocks
                ussage_used
                ussage_available
                ussage_used_capacity
                check_bit
                zfs_cluster
                node_name"""
                nfs_info = dict()
                nfs_info['server_hostname'] = server_hostname
                nfs_info['server_ip'] = server_ip

                fs_set = fs['Filesystem'].split(':')
                if self.validIP(fs_set[0]):
                    service_ip = fs_set[0]
                else:
                    service_ip = self.get_service_ip(hosts, fs_set[0])

                zfs_name, zfs_default_ip, zfs_cluster, node_name = self.get_cluster(service_ip)

                nfs_info['filesystem'] = fs['Filesystem']
                nfs_info['mounted'] = fs['Mounted_on']
                nfs_info['zfs_hostname'] = fs['Mounted_on']
                nfs_info['zfs_name'] = zfs_name
                nfs_info['zfs_ip'] = zfs_default_ip
                nfs_info['mounted_bit'] = fs['mounted_bit']
                nfs_info['ussage_blocks'] = fs['1K-blocks']
                nfs_info['ussage_used'] = fs['Used']
                nfs_info['ussage_available'] = fs['Available']
                nfs_info['ussage_used_capacity'] = fs['Used_rate']
                nfs_info['check_bit'] = True
                nfs_info['zfs_cluster'] = zfs_cluster
                nfs_info['node_name'] = node_name
                nfs_info['check_date_time'] = server_datetime.strftime('%Y-%m-%d %H:%M:%S')
                nfs_info['fbrm_date'] = server_datetime.strftime('%Y-%m-%d')

                nfs_list.append(nfs_info)
        print nfs_list
        table_name = 'live.live_svr_nfs_mounted_on'
        query = "delete from {} where server_hostname = '{}' ".format(table_name, server_hostname)
        print query
        self.db.queryExec(query)
        self.db.dbInsertList(nfs_list, table_name)

    def main(self):
        flist =self.get_data_file()

        for f in flist:
            json_data=self.get_json_format(f)
            self.nfs_load(json_data)

if __name__=='__main__':
    fs_load().main()
