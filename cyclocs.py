#!/usr/bin/env python3

import threading
import sys
import socket

class readThread(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.conn = sock
    def run(self):
        while True:
            data = self.conn.recv(1024)
            print(data.decode())

class writeThread(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.conn = sock
    def run(self):
        while True:
            data = input()
            self.conn.send(data.encode())

def main():
    if not len(sys.argv) == 3:
        print("Insufficient parameters")
        return

    port = int(sys.argv[2])
    ipaddr = sys.argv[1]

    s = socket.socket()
    s.connect((ipaddr, port))
    wT = writeThread(s)
    rT = readThread(s)
    wT.start()
    rT.start()

if __name__ == '__main__':
    main()