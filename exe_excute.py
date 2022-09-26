import os
import sys
import ConfigParser
import subprocess
import re
import glob
class exe_exc():
    def __init__(self):
        self.cfg = self.get_cfg()
        self.path=self.get_path()

    def get_path(self):
        path=self.cfg.get('common','path')
        return path

    def get_cfg(self):
        cfg=ConfigParser.RawConfigParser()
        cfg_file=os.path.join('config','ps_manager.cfg')
        cfg.read(cfg_file)
        return cfg


    def ps_search(self,ps_name):
        cmd = 'tasklist | findstr "{}'.format(ps_name)
        ret=os.popen(cmd).read()
        if not ps_name in ret:
            print '#'*50
            print '#' * 50
            print 'PROCESS START'
            print '#' * 50
            print '#' * 50


            os.chdir(self.path)
            pid = subprocess.Popen(ps_name, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE).pid
            print pid
        print os.popen(cmd).read()


    def start_ps(self,ps_name):
        pass

    def get_ps_list(self):
        opts = self.cfg.options('ps_list')
        ps_list=[]
        for opt in opts:
            ps=self.cfg.get('ps_list',opt)
            ps_list.append(ps)
            pid_tx='{}_pid.txt'.format(opt)
            if not os.path.isfile(pid_tx):
                print ps
                pid = subprocess.Popen(ps, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE).pid
                print "ps :",ps
                print 'PID :',pid
                with open(pid_tx,'w') as f:
                    f.write(str(pid))

            else:
                lask_ch= ps[-3:]
                if lask_ch =='exe':
                    ps_dir = os.path.dirname(ps)
                    f_name = os.path.basename(ps)
                    cmd='tasklist /SVC  | findstr "{}"'.format(f_name)
                    print cmd
                    ret=os.popen(cmd).read()
                    print ret
                    if re.search(ret,ps):
                        pid = subprocess.Popen(ps, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE).pid
                        print "ps :", ps
                        print 'PID :', pid
                        with open(pid_tx, 'w') as f:
                            f.write(str(pid))
                if lask_ch == '.py':
                    interp=ps.split()[0]
                    cmd='tasklist /SVC | findstr "{}"'.format(interp)
                    subp = subprocess.Popen(ps, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
                    pid=subp.pid
                    print subp.stdout
                    print "ps :", ps
                    print 'PID :', pid
                    with open(pid_tx, 'w') as f:
                        f.write(str(pid))
        return ps_list

    def get_task(self,pid):
        cmd='tasklist /SVC | findstr "{}"'.pid
        print os.popen(cmd).read()


    def list_process(self):
        opts=self.cfg.options('ps_list')
        ps_list=[]
        for opt in sorted(set(opts)):
            print opt
            ps_list.append(self.cfg.get('ps_list',opt))
        print ps_list

    def ps_check(self):
        pid_list=glob.glob('pid*.txt')
        for ps in pid_list:
            with open(ps) as f:
                pid=f.read()
            cmd='tasklist | findstr "{}"'.format(pid)
            ret=os.popen(cmd).read()
            print ret

    def get_python(self):
        cmd='tasklist | findstr pythonw'
        os.popen(cmd).read()

    def ps_start(self):
        ps_status={}
        for opt in sorted(set(self.cfg.options('ps_list'))):
            ps_cmd= self.cfg.get('ps_list',opt)
            last_ch = ps_cmd[-3:]
            print ps_cmd
            pid_tx = '{}_pid.txt'.format(opt)
            status={}
            cmd='start /B {}'.format(ps_cmd)
            print cmd
            sp=subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            pid=sp.pid
            print 'PID :',str(pid)
            with open(pid_tx,'w') as f:
                f.write(str(pid))
        print os.popen('tasklist | findstr python').read()


    def main(self):
        # print self.path
        # ps_list=self.get_ps_list()
        # for ps in ps_list:
        #     self.ps_search(ps)

        self.ps_start()








if __name__=='__main__':
    exe_exc().main()
