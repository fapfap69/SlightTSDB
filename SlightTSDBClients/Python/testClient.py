#!/usr/bin/env python
import socket
import time


RATE = 0.1
TCP_IP = '127.0.0.1'
TCP_PORT = 8080
BUFFER_SIZE = 16000


counter = 9999
while counter > 0:
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((TCP_IP, TCP_PORT))
	s.send( "{\"method\":\"setdpval\", \"params\": {\"dpId\": 3000, \"values\": [ %d , 0.0, %f]} }" % (counter, counter / 1000) )
	data = s.recv(BUFFER_SIZE)
	
	counter = counter -1
	time.sleep(RATE)
	s.close()

