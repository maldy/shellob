#!/usr/bin/python

import cgi
import socket
import pickle

HOST = 'localhost'
PORT = 20001

def recv_delim(socket, buf_len, delim):
	data = ""
	while True:
		recv_data = socket.recv(buf_len)
		data += recv_data
		if delim in data:
			return data[:-1]

def send_msg(socket, msg, delim):
	msg_len = len(msg)
	bytes_sent = 0
	while bytes_sent < msg_len:
		sent = socket.send( msg + delim )
		bytes_sent += sent
		msg = msg[sent+1:]

	return bytes_sent 



print "Content-Type: text/html\n\n"

print "<html>"
print "<head><title>The ESPNSTAR Football Search Engine - Results</title></head>"
print "<body>"
print "<h2>The ESPNSTAR Football Search Engine</h2>"
print "<p>Enter a query!</p>"
print "<ul><li>Try a single term, or multiple terms</li>"
print "<li>Use \" \" for phrase queries</li></ul>"
print "<form method = \"POST\" action=\"results.py\">"
print "Query: <input type = TEXT name = \"query\" size=60>"
print "<input type=\"SUBMIT\" value=\"Go\"></form>"
print "<br/><hr>"
print "<h3>Results</h3>"

The_Form = cgi.FieldStorage()
query = The_Form['query'].value
print "Query: ", query
s = socket.socket()
host = socket.gethostname()
port = PORT
s.connect(('10.109.27.150', port))

msg = pickle.dumps(query)
send_msg(s, msg, '\x01')
msg = recv_delim(s, 10000, '\x01')
s.close()
serp = pickle.loads(msg)
print "<ol>"
for doc in serp:
	(title, url, snippet) = doc
	print "<li><h4><a href=\"", url, "\" target=\"_blank\">", title, "</a></h4>"
	print "<p>", snippet, "</p></li>"

print "</body>"
print "</html>"
