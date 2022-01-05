#!/usr/bin/env python3

from queue import Queue
from threading import Thread
import socket

HOST = '192.168.0.7'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on (non-privileged ports are > 1023)

HOST_BROKER = '192.168.0.12'  # Standard loopback interface address (localhost)
PORT_BROKER = 1883        # Port to listen on (non-privileged ports are > 1023)

# A thread that produces data
def forwarder_server_socket_receive(c2s_q, s2c_q):
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, PORT))
            s.listen()
            conn, addr = s.accept()
            with conn:
                print('Connected by', addr)
                while True:
                    print(s2c_q.empty())
                    try:
                        if (not s2c_q.empty()):
                            data_s = s2c_q.get()
                            print('Replying: ' + data_s.decode("utf-8"))
                            s.sendall(data_s)
                        data = conn.recv(1024)
                        if data:    
                            print('Forwarding: ' + data.decode("utf-8"))
                            c2s_q.put(data)
                    except Exception as e:
                        print(e)
                        break
                
                print('Client disconnected')
        		
# A thread that consumes data
def forwarder_socket_send(c2s_q, s2c_q):
    while True:
	    # Get some data
        if (not c2s_q.empty()):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST_BROKER, PORT_BROKER))
                while True:
                    try:
                        if (not c2s_q.empty()):
                            data = c2s_q.get()
                            print('Sending: ' + data.decode("utf-8"))
                            s.sendall(data)
                        response = s.recv(1024)
                        if response:
                            print('Received: ' + response.decode("utf-8"))
                            s2c_q.put(response)
                    except Exception as e:
                            print(e)
                            break                

		
# Create the shared queue and launch both threads
c2s_q = Queue() # Client to server queue
s2c_q = Queue() # Server to client queue

t1 = Thread(target = forwarder_socket_send, args =(c2s_q, s2c_q))
t2 = Thread(target = forwarder_server_socket_receive, args =(c2s_q, s2c_q))
t1.start()
t2.start()
