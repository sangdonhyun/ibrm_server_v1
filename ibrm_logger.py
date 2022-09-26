# -*- coding: utf-8 -*-
import logging
import logging.handlers
import os


def flt_logger(name):
    # if a logger exists, return that logger, else create a new one
    log = logging.getLogger(name)

    if len(log.handlers) > 0:
        return log

    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    filename = os.path.join('logs', name)

    # StreamHandler 생성
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s")
    stream_handler.setFormatter(formatter)

    # FileHandler 생성
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=filename, when='midnight', interval=1, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter("%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s")
    file_handler.setFormatter(file_formatter)
    file_handler.suffix = '%Y%m%d'

    log.addHandler(stream_handler)
    log.addHandler(file_handler)
    #test

    return log


"""
**** Fommat argument ****
 
asctime	        %(asctime)s
created	        %(created)f
exc_info	    You shouldn’t need to format this yourself.
filename	    %(filename)s
funcName	    %(funcName)s
levelname	    %(levelname)s
levelno	        %(levelno)s
lineno	        %(lineno)d
message	        %(message)s
module	        %(module)s
msecs	        %(msecs)d
msg	            You shouldn’t need to format this yourself.
name	        %(name)s
pathname	    %(pathname)s
process	        %(process)d
processName	    %(processName)s
relativeCreated	%(relativeCreated)d
stack_info	    You shouldn’t need to format this yourself.
thread	        %(thread)d
threadName	    %(threadName)s
"""
