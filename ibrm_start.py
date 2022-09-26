import os
import subprocess
import shlex
import threading
import Queue
import psutil
import time
import ibrm_logger

pid_file = "E:\\Fleta\\JAR\\ibrm_JDM\\fleta-jdm.pid"
log = ibrm_logger.flt_logger('ibrm_starter')

class ps_start(object):

    def check_ps(self):
        cmd = """tasklist | findstr ibrm_process_manage
        """
        print (cmd)
        ret = os.popen(cmd).read()
        print ("ret :", ret)
        if ret == "":
            return None
        else:
            return ret

    def manager_start(self):
        ret=self.check_ps()
        print (ret, ret==None)
        if ret == None:
            cmd = "E:\\Fleta\\ibrm_process_manager\\ibrm_process_manage.bat"
            os.system(cmd)
            log.info('START ibrm_process_manager.bat')
            ret = self.check_ps()
            print (ret, ret == None)

        else:
            print('#'*50)
            print("ibrm_process is aleady runnning ")
            print(ret)
            print('#' * 50)

    def check_sched(self):
        pass

    def exec_scheduler_creator(self):
        os.system("E:\\Fleta\\JAR\\ibrm_JDM\\execute_JDM.bat")
        time.sleep(2)
        with open(pid_file) as f:
            pid = f.read().strip()
        log.info('NEW PID : {}'.format(pid))
        print psutil.Process(int(pid))

    def shced_start(self):
        with open(pid_file) as f:
            pid = f.read().strip()
        log.info(str(pid))
        ps = psutil.pid_exists(int(pid))
        print(type(ps),ps)
        if ps:
            ps_dict = psutil.Process(int(pid))
            log.info(str(ps_dict))
            log.info('OLD PID : %s' % pid)
            if not (ps_dict.name()) == 'java.exe':
                self.exec_scheduler_creator()
            else:
                log.info('aleady exist')
        else:
            self.exec_scheduler_creator()

    def main(self):
        self.manager_start()
        self.shced_start()


if __name__=='__main__':
    ps_start().main()