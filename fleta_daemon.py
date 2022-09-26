'''
Created on 2009. 12. 25.

@author: muse
'''
from socket import *
from SocketServer import ThreadingTCPServer, StreamRequestHandler

import ConfigParser
import re
import sys
import threading
import time
import os
import commands
import string
import signal
import SocketServer
import select
from threading import Thread
# import fletaAction
# import fletaStatus
#import fletaInfo

try:
    fletaHome = os.environ['FLETA_HOME']
except:
    fletaHome = ''.join(os.path.split(os.path.realpath(os.path.dirname(__file__)))[:-1])

configDir = os.path.join(fletaHome, 'config')
# fletaAct = fletaAction.Acttion()
config = ConfigParser.RawConfigParser()
# fletaLogger = fletaLogger.fletaLog().logger

config.read('%s/fleta.cfg' % configDir)

HOST = '121.170.193.196'
PORT = '53001'


# fletaLogger.info('#'*50)
# fletaLogger.info('# FLETA IP : ' + HOST)
# fletaLogger.info('# FLETA PORT : ' + PORT +'  default 50004')

PORT = int(PORT)


class RequestHandler(StreamRequestHandler):

    def handle(self):

        fletaCode = ['fletaInfo', 'schedChange', 'schedStatus', 'fletaCmd', 'FLETASTOP', 'etc', 'schedStart',
                     'schedStop', 'fletaJob', 'schedRestart', 'schedDateGet', 'schedFormGet']
        conn = self.request
        fleta_state = ''

        while 1:
            msg = conn.recv(1024)
            if not msg:
                pass
                conn.close()
                time.sleep(0.1)
                break
            if '\n' in msg:
                msg = msg.replace('\n', '')
            if ':::' in msg:
                code = str.strip(str(msg.split(':::')[0]))
                cu_msg = str.strip(str(msg.split(':::')[1]))
            else:
                re_msg = False
                code = None
                cu_msg = None
                break

            if code in fletaCode:
                re_msg = True
            else:
                re_msg = False
            #            fletaLogger.info("# %s" % msg)

            if code == 'FLETASTOP':

                try:
                    re_code = 'fleta daemon stop'
                    conn.send(re_code)
                    conn.recv(1024)
                    self.server.server_close()
                    #                    fletaLogger.info(re_code)
                    #                    fletaLogger.info('FLETASTOP True')
                    break
                except:
                    pass
            #                    fletaLogger.info('FLETASTOP False')

            if re_msg:
                if code in 'fletaJob':

                    try:
                        ret = str(fletaAct.fletaJob())
                        re_code = code + ' %s' % ret
                    except:
                        re_code = code + ' fail'

                #                    fletaLogger.info(str(re_code))

                elif code in 'fletaCmd' and len(re_msg) > 1:
                    #                    fletaLogger.info('###################\n')
                    #                    fletaLogger.info('Daemon : fletaCmd\n')
                    #                    fletaLogger.info('###################\n')

                    result_msg = commands.getoutput(re_msg)
                    if len(result_msg) > 1024:
                        result_msg = result_msg[1:1000] + '\n---MORE---\n'

                elif code in 'schedStart':
                    #                    fletaLogger.info('###################\n')
                    #                    fletaLogger.info('Daemon : schedStart\n')
                    #                    fletaLogger.info('###################\n')
                    if fletaAct.schedStart():
                        re_code = code + " success"
                    else:
                        re_code = code + " fail"

                #                    fletaLogger.info(re_code)

                elif code in 'schedStop':
                    #                    fletaLogger.info('###################\n')
                    #                    fletaLogger.info('Daemon : schecd stop\n')
                    #                    fletaLogger.info('###################\n')

                    try:
                        fletaAct.schedStop()
                        re_code = code + ' success'
                    except:
                        re_code = code + ' fail'

                #                    fletaLogger.info(re_code)

                elif code in 'schedChange':
                    #                    fletaLogger.info('###################\n')
                    #                    fletaLogger.info('Daemon : schedChange\n')
                    #                    fletaLogger.info('###################\n')
                    #                    print '#'*50
                    #                    print 'code :', code
                    #                    print 'cu_msg:', cu_msg,len(cu_msg)
                    #                    fletaLogger.info('INPUT :')
                    #                    fletaLogger.info(cu_msg)
                    try:

                        if len(cu_msg) == 0:

                            #                            fletaLogger.info('SCHEDULE DELTE')
                            f = open('%s/sched.format' % configDir, 'w')
                            f.close()
                        else:
                            fletaAct.schedChange(cu_msg)

                        #                        print 'sched stop '
                        fletaAct.schedStop()
                        #                        print 'sched Start '
                        fletaAct.schedStart()
                        re_code = code + ' success'
                    except:
                        re_code = code + ' fail'

                #                    print 're_code :', re_code

                #                    fletaLogger.info(re_code)

                elif code in 'schedDateGet':
                    #                    fletaLogger.info('###################\n')
                    #                    fletaLogger.info('Daemon : schedDateGet\n')
                    #                    fletaLogger.info('###################\n')
                    try:
                        f = open('%s/sched.date' % configDir)
                        re_code = f.read()
                        f.close()
                    except:
                        re_code = code + ' fail'

                #                    fletaLogger.info(re_code)

                elif code in 'schedFormGet':
                    #                    fletaLogger.info('###################\n')
                    #                    fletaLogger.info('Daemon : schedFormGet\n')
                    #                    fletaLogger.info('###################\n')
                    try:
                        f = open('%s/sched.format' % configDir)
                        re_code = f.read()
                        f.close()
                        if len(re_code) == 0:
                            re_code = 'schedule empty'
                    except:
                        re_code = code + ' fail'

                #                    fletaLogger.info(re_code)

                elif code in 'schedRestart':
                    #                    fletaLogger.info('###################\n')
                    #                    fletaLogger.info('Daemon : schedRestart\n')
                    #                    fletaLogger.info('###################\n')
                    try:
                        fletaAct.schedStop()
                        fletaAct.schedStart()
                    except:
                        re_code = code + ' fail'
                    else:
                        #                        fletaLogger.info(re_msg)
                        re_code = code

                elif code in 'schedStatus':
                    try:
                        re_code = commands.getoutput('ps -ef | grep schedRun.pyc | grep -v grep | wc -l')
                    except:
                        re_code = code + ' fail'
                #                    fletaLogger.info(re_code)

                elif code in 'getHostname':
                    #                    fletaLogger.info('###################\n')
                    #                    fletaLogger.info('Daemon : getHostname\n')
                    #                    fletaLogger.info('###################\n')
                    try:
                        re_code = commands.getoutput('hostname')
                    except:
                        re_code = code + ' fail'
                #                    fletaLogger.info(re_code)

                elif code in 'fletaInfo':
                    #                    fletaLogger.info('###################\n')
                    #                    fletaLogger.info('Daemon : fletaInfo\n')
                    #                    fletaLogger.info('###################\n')
                    try:
                        #                        re_code = fletaInfo.infoText()
                        import fletaCommon
                        re_code = str(fletaCommon.Common().Info())
                    except:
                        re_code = code + ' fail'
                #                    fletaLogger.info(re_code)

                if re_code == None:
                    re_code = code + ' : fail'
                #                print 'send msg:', re_code
                conn.send(re_code)


class fletaDaemonSock():
    def __init__(self, RequestHandlerClass):
        """Constructor.  May be extended, do not override."""
        self.server_address = (HOST, PORT)
        self.RequestHandlerClass = RequestHandlerClass
        self.__is_shut_down = threading.Event()
        self.__serving = True

    def serve_forever(self, poll_interval=0.5):
        """Handle one request at a time until shutdown.

        Polls for shutdown every poll_interval seconds. Ignores
        self.timeout. If you need to do periodic tasks, do them in
        another thread.
        """

        self.__is_shut_down.clear()
        while self.__serving:
            # XXX: Consider using another file descriptor or
            # connecting to the socket to wake this up instead of
            # polling. Polling reduces our responsiveness to a
            # shutdown request and wastes cpu at all other times.
            r, w, e = select.select([self], [], [], poll_interval)
            if r:
                self._handle_request_noblock()
        self.__is_shut_down.set()

    def shutdown(self):
        """Stops the serve_forever loop.

        Blocks until the loop has finished. This must be called while
        serve_forever() is running in another thread, or it will
        deadlock.
        """
        self.__serving = False
        self.__is_shut_down.wait()


class fletaTCPServer(SocketServer.ThreadingTCPServer):
    def __init__(self, (HOST, PORT), RequestHandler):
        SocketServer.ThreadingTCPServer.__init__(self, (HOST, PORT), RequestHandler)
        self.__serving = True

    def server_forever(self):
        while self.__serving:
            SocketServer.ThreadingTCPServer.handle_request(self)

    def shutdown(self):
        self.__serving = False


class fletaServer():

    def __init__(self):
        self.HOST = HOST
        self.PORT = PORT
        self.addr = (str(self.HOST), int(self.PORT))
        try:
            sys.stdout.write('IP   : %s\n' % self.HOST)
            sys.stdout.write('PORT : %s\n' % self.PORT)
            self.server = SocketServer.TCPServer(self.addr, RequestHandler)
        except:
            sys.stdout.write('\n' + '#' * 50 + '\n')
            sys.stdout.write('#' + 'DAEMON START ERROR !'.center(50) + '#\n')
            sys.stdout.write('#' + ('CHECK IP :%s !' % self.HOST).center(50) + '#\n')
            sys.stdout.write('#' + ('CHECK PORT :%s !' % self.PORT).center(50) + '#\n')
            sys.stdout.write('#' * 50 + '\n')
            raw_input()
            sys.exit(-1)

    def run(self):
        if self.HOST == None:
            #            fletaLogger.error('# FLETA IP IS NOT DEFINE CHECK fleta.cfg')
            exit

        #        fletaLogger.info('# FLETA AGENT START')
        # Create the server, binding to localhost on port 9999
        #        server = SocketServer.TCPServer((self.HOST, self.PORT), RequestHandler)

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        try:
            self.server.serve_forever()
        except:
            self.server.server_close()


def stop():
    SocketServer.TCPServer.allow_reuse_address = True


if __name__ == '__main__':
    fletaServer().run()

