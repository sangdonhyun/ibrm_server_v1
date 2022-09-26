'''
Created on 2012. 10. 12.

@author: Administrator
'''
'''
Created on 2012. 9. 27.

@author: Administrator
'''
import sys
import os
import datetime
import time
import ConfigParser
from ftplib import FTP
import re
import logging
import glob
import logging.handlers
import base64
import hashlib
import pprint

class hangul_dict(pprint.PrettyPrinter):
    def format(self, _object, context, maxlevels, level):
        if isinstance(_object, unicode):
            return "'%s'" % _object.encode('utf8'), True, False
        elif isinstance(_object, str):
            _object = unicode(_object,'utf8')
            return "'%s'" % _object.encode('utf8'), True, False
        return pprint.PrettyPrinter.format(self, _object, context, maxlevels, level)

class Common():
    def __init__(self):





        self.server,self.user,self.passwd=None,None,None
        self.cfg = self.getCfg()
        self.env = os.environ

    def getCfg(self):
        
        cfgFile = os.path.join('config','config.cfg')

        config = ConfigParser.RawConfigParser()
        config.read(cfgFile)
#         self.fletaRecv=config.get('fletaRecv','ip')
#         self.user=config.get('fletaRecv','user')
#         self.passwd=Decode().fdec(config.get('fletaRecv','passwd'))
        return config
    

    
    


    def getNow(self,format='%Y-%m-%d %H:%M:%S'):
        return time.strftime(format)

    def getHeadMsg(self,title='FLETA BATCH LAOD'):
        now = self.getNow()
        msg = '\n'
        msg += '#'*79+'\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#### '+('TITLE     : %s'%title).ljust(71)+'###\n'
        msg += '#### '+('DATA TIME : %s'%now).ljust(71)+'###\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#'*79+'\n'
        return msg
    
    def getEndMsg(self):
        now = self.getNow()
        msg = '\n'
        msg += '#'*79+'\n'
        msg += '####  '+('END  -  DATA TIME : %s'%now).ljust(71)+'###\n'
        msg += '#'*79+'\n'
        return msg
    
    



if __name__=='__main__':
#    logTest()
#    fname=os.path.join('data','fs_ibk-test05.tmp')
#    print os.path.isfile(fname)
#    print Common().fletaPutFtp(fname,'diskinfo.SCH')
    print 'common'


    
    
    
