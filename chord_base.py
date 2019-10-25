#!/usr/bin/python3

import threading
import json
import socket

BUFFER_SIZE = 2048
PORT_BASE=10003

def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper

@threaded
def receive():
    s      = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(('', PORT_BASE))
    except socket.error as e:
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', PORT_BASE))
        except socket.error as e:
            s.close()
            log.error("Node %d - bind: %s\n" %(key, e))
            return
    s.listen(2)
    while True:
        try:
            client, addr = s.accept()
        except socket.error:
            break
        rec = ''
        allReceived = False
        try:
            while(not allReceived):
                incomingData = client.recv(BUFFER_SIZE).decode()
                if(incomingData == ''):
                    allReceived = True
                else:
                    rec += incomingData
        except socket.error as e:
            log.error("Node %d - recv: %s\n" % (key, e))
            s.close()
            return
        print(rec)
        msg = json.loads(rec)
        try:
            msg = json.loads(rec)
        except Exception as e:
            log.error("Node %d - json.loads: %s\n" % (key, e))
            return
        try:
            cmd = msg['CMD']
            print(cmd)
        except Exception as e:
            log.error("Node %d - Error while executing received command: %s" % (key, e))
            log.error(rec + '\n')


receive()
