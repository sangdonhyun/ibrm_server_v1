import logging.config
import os
import datetime
import logging
import logging.handlers

loggers = {}
class ibrm_logger():
    def __init__(self):
        pass


    def sched_logger(self):

        today_date = datetime.today().strftime('ibrm_sched_%Y-%m-%d')


        log = logging.getLogger('ibrm_sched_LOGGER.')
        log.setLevel(logging.DEBUG)


        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s")

        log_file=os.path.join('logs','{}_info.log'.format(today_date))
        fileHandler = logging.FileHandler(log_file)
        streamHandler = logging.StreamHandler()
        fileHandler.setFormatter(formatter)
        streamHandler.setFormatter(formatter)
        log.addHandler(fileHandler)
        log.addHandler(streamHandler)
        return log

    # def remove_handler(self):
    #     testLogger.removeHandler(h)
    def info_logger(self):

        today_date = datetime.today().strftime('ibrm_agent_%Y-%m-%d')


        log = logging.getLogger('ibrm_agent_info_LOGGER.')
        log.setLevel(logging.DEBUG)


        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s")

        log_file=os.path.join('logs','{}_info.log'.format(today_date))
        fileHandler = logging.FileHandler(log_file)
        streamHandler = logging.StreamHandler()
        fileHandler.setFormatter(formatter)
        streamHandler.setFormatter(formatter)
        log.addHandler(fileHandler)
        log.addHandler(streamHandler)
        return log

    def get_logger(self,name):

        # if a logger exists, return that logger, else create a new one
        global loggers
        if name in loggers.keys():
            return loggers[name]
        else:
            logger = logging.getLogger(name)
            logger.setLevel(logging.DEBUG)
            now = datetime.datetime.now()
            handler = logging.FileHandler(
                os.path.join('logs','ibrm_server_log_'+datetime.datetime.now().strftime("%Y-%m-%d")+ '.log'))
            formatter = logging.Formatter("%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            loggers.update(dict(name=logger))
            return logger

    def logger(self,log_name):

        today_date = datetime.today().strftime('ibrm_svr_%Y-%m-%d')


        log = logging.getLogger(log_name)
        log.setLevel(logging.DEBUG)


        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s")

        log_file=os.path.join('logs','{}.log'.format(today_date))
        fileHandler = logging.FileHandler(log_file)
        streamHandler = logging.StreamHandler()
        fileHandler.setFormatter(formatter)
        streamHandler.setFormatter(formatter)
        log.addHandler(fileHandler)
        log.addHandler(streamHandler)
        return log

    def test(self):
        log = ibrm_logger().logger('ibrm_agent_logger')
        log.info('aaa')


if __name__=='__main__':
    log = ibrm_logger().get_logger('ibrm_server_log')
    for i in range(30):

        log.info('info %s'%i)


