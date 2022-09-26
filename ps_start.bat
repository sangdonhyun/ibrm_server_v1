cd E:\PycharmProjects\ibrm_daemon

taskkill /f /IM python.exe
taskkill /f /IM pythonw.exe

start cmd /c python ibrm_daemon_restapi.py
start cmd /c python ibrm_server_daemon_socket.py
start cmd /c python job_scheduler.py



taskkill /f /IM fletaRecv.exe
c:
cd C:\Fleta\fleta_recv
start cmd /c FletaRecv.exe
