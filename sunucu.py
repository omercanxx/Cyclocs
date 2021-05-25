import sqlite3
import socket
import sys
import threading
import queue as Q
import time
from datetime import datetime


class rThread(threading.Thread):
    def __init__(self, conn, c_addr, qThread):
        threading.Thread.__init__(self)
        self.conn = conn
        self.c_addr = c_addr
        self.qThread = qThread

    def run(self):
        dbConnection = sqlite3.connect('db\cyclocs.db')
        cursor = dbConnection.cursor()
        while True:
            data = self.conn.recv(1024)
            data_str = data.decode().strip()
            print("%s : %s" % (self.c_addr, data_str))
            self.incoming_parser(data_str, cursor)
            dbConnection.commit()
        self.conn.close()
        print("Thread %s kapanıyor" % self.threadID)

    def incoming_parser(self, data, cur):
        # Kullanıcı ismi kontrolu için
        msg = data.strip().split(" ")
        

        #region Veri girişi
        if msg[0] == "CRT":
            print("1")
            #Eğer kullanıcı tabloda var ise kullanıcının şifre kontrolü yapılıyor
            if msg[1] == "LOC":
                print("2")
                cur.execute('''INSERT INTO locations (height, camera) VALUES(?,?) ''', (msg[2], msg[3],))
        #endregion

        #region Veri silme
        elif msg[0] == "DLT":
            #Location tablomuzdan veri silinecekse
            if msg[1] == "LOC":
                #Location tablosunun hepsi temizlenecekse
                if msg[2] == "ALL":
                    cur.execute('''DELETE FROM locations''')
                #Location tablosunda yüksekliği gönderilen veri silinecekse
                else :
                    cur.execute('''DELETE FROM locations where height = ?''', (msg[2],))
            #Location tablosundan kamera silinecekse
            elif msg[1] == "CAM":
                cur.execute('''DELETE FROM locations where camera = "true"''')
            #Images tablosundan veri silinecekse
            elif msg[1] == "IMG":
                #Images tablosunun hepsi temizlenecekse
                if msg[2] == "ALL":
                    cur.execute('''DELETE FROM images''')
                #Images tablosunda pathi gönderilen veri silinecekse
                else :
                    cur.execute('''DELETE FROM images where path = ?''', (msg[2],))
        #endregion

        #region Resim Çekme
        elif msg[0] == "IMG":
            self.qThread.put("IMG")
            cur.execute('''SELECT height FROM locations WHERE camera = "true"''')
            locations = cur.fetchall()
            camera_location = locations[0]
            height = camera_location[0]
            cur.execute('''INSERT INTO images (height, path) VALUES(?, 'C:\') ''', height)
        #endregion

        #region Resim Çekme
        elif msg[0] == "MAN":
            self.qThread.put("MAN")
        #endregion
        
        #region Resim Çekme
        elif msg[0] == "OTO":
            self.qThread.put("OTO")
        #endregion
        
        
        #region Lokasyon bilgisi çekme
        elif msg[0] == "LOC":
            #Kamera başta gelsin diye sıralandı
            cur.execute('''SELECT height, camera FROM locations ORDER BY camera DESC''')
            locations = cur.fetchall()
            camera_location = locations[0]
            #height = camera_location[0]
            print(camera_location)
        #endregion
        
        #region Kameranın yukarıya hareketi
        elif msg[0] == "MVU":
            #Diğer istemcilere iletmenin yolu
            self.qThread.put("MVU")
            cur.execute('''SELECT height FROM locations WHERE camera = "true" ''')
            locations = cur.fetchall()
            camera_location = locations[0]
            height = camera_location[0]
            height = height + 1
            cur.execute('''UPDATE locations SET height = ? WHERE camera = "true"''', (height, ))
        #endregion
        
        #region Kameranın aşağı hareketi
        elif msg[0] == "MVD":
            self.qThread.put("MVD")
            cur.execute('''SELECT height FROM locations WHERE camera = "true" ''')
            locations = cur.fetchall()
            camera_location = locations[0]
            height = camera_location[0]
            height = height - 1
            cur.execute('''UPDATE locations SET height = ? WHERE camera = "true"''', (height, ))
        #endregion

        else:
            self.qThread.put("ERR")

class wThread(threading.Thread):
    def __init__(self, conn, qThread):
        threading.Thread.__init__(self)
        self.conn = conn
        self.qThread = qThread

    def run(self):

        while True:
            data = self.qThread.get()
            self.conn.send(data.encode())

def main():
    server_socket = socket.socket()
    ip = "0.0.0.0"
    port = int(sys.argv[1])
    addr_server = (ip, port)

    server_socket.bind(addr_server)
    server_socket.listen(5)

    while True:

        queueThread = Q.Queue()
        conn, addr = server_socket.accept()
        readThread = rThread(conn, addr, queueThread)
        writeThread = wThread(conn, queueThread)

        readThread.start()
        writeThread.start()
    server_socket.close()


if __name__ == "__main__":
    main()