#!/usr/bin/env python

"""
shellob.py

     !      !
   \._\____/_./
   _._/    \_._
    .-\____/-.
   /   oOOo   \
       <  >
    
A slightly simple webcrawler.
    
"""
__version__ = "0.5"
__authors__ = "maldy (lordmaldy at gmail dot com), Vishaka (vishakadatta at\
gmail dot com)"

import re

import socket
import time
import errno

# stuff you'll have to install - all available with python setuptools
from mechanize import Browser, HTTPError, URLError, BrowserStateError
import pymongo


espn_regex = re.compile(r'/football/')
fixture_regex = re.compile(r'/fixtures/')

PORT = 10000
URL_TIMEOUT = 60		#Time-out to wait for page to load

class Crawler():

	def __init__(self):
		self.br = Browser()
		self.br.set_handle_redirect(True)
		self.queue_server = {"host": "localhost", "port": PORT}
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)	
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		try: 
			self.sock.connect((self.queue_server['host'], self.queue_server['port']))
		except socket.error, (value, message):
			print "Could not connect to queue server at " + self.queue_server['host'] +\
					":" + str(self.queue_server['port'])
			print message
			return

	def recv_delim(self, buf_len, delim):
		data = ""
		while True:
			recv_data = self.sock.recv(buf_len)
			data += recv_data
			if delim in data:
				return data[:-1]

	def send_msg(self, msg, delim):
		msg_len = len(msg)
		bytes_sent = 0

		while bytes_sent < msg_len:
			sent = self.sock.send( msg + delim )
			bytes_sent += sent
			msg = msg[sent+1:]

		return bytes_sent 

	def crawl(self):

		while True:
			# grab the next url off the queue server
			buf_left = 10000 

			url_msg = self.recv_delim( 4096, '\0')
			depth_end = url_msg.find('\1')

			url = url_msg[depth_end+1:]
			depth = int(url_msg[0:depth_end])
	
			print str(time.ctime()) + " URL received from queue server ->" + url +\
			" Depth : " + str(depth)
			
			# fetch url contents (filter stuff here)
			try : 
				response = self.br.open(url,timeout=URL_TIMEOUT)

				if response:
					print "Crawl successful"
					crawler_ack = 's'
				else:
					print "Crawl failed - Timeout"
					crawler_ack = 'f'
			except HTTPError, e: 
				print "Crawl failed - HTTP error"
				if e.code >= 400 and e.code<= 417:
					crawler_ack = 'd'
				else:
					crawler_ack = 'f'
			except URLError:
				print "Crawl failed - Could not open page"
				crawler_ack = 'f'
					
			links_found = []
			if crawler_ack is 's':
				try:
					links_found = list( self.br.links() )
				except BrowserStateError:
					print "Crawl failed - Mechanize error"
					crawler_ack = 'd'
				except socket.timeout:
					print "Crawl failed - links() timed out"
					crawler_ack = 'f'

			crawler_msg = crawler_ack + "*"

			for link in links_found:
				if espn_regex.search(link.absolute_url) and not \
						fixture_regex.search(link.absolute_url):
					
					depth = link.absolute_url.count('/') - 4 
					if link.absolute_url[-1] is not '/':
						depth += 1
						link.absolute_url += '/'

					url_msg = str(depth) + '\1' + link.absolute_url + '*'
					crawler_msg += url_msg
		
			bytes_sent = self.send_msg( crawler_msg, '\0' )

		self.sock.close()
	
def main():
	# thread and unleash shellob onto the unsuspecting site. Amok!
	c = Crawler()
	c.crawl()
	
if __name__ == "__main__":
	main()


##############################
# Code Snippet dump - ignore #
##############################
"""
		response = self.br.open(url)
		for link in self.br.links():
			print link
		response.seek(0)
		html = response.read()
		root = lxml.html.fromstring(html)
		for link in root.iterlinks():
			print link
"""
