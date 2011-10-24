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
from datetime import datetime 
import errno

# stuff you'll have to install - all available with python setuptools
from mechanize import Browser, HTTPError, URLError, BrowserStateError
import pymongo
import lxml.html

mongo_host = 'localhost'
mongo_port = 27017

espn_regex = re.compile(r'http://(www.){,1}espnstar.com/football/')
bad_regex = re.compile(r'/fixtures/{,1}|/URL/{,1}|/galleries/{,1}|/videos/{,1}|/[0-9]/{,1}')

PORT = 10000
URL_TIMEOUT = 600		#Time-out to wait for page to load

#Instead of inserting the entire html page into db, we parse the content and 
#insert only the title and the text in the page. 
def parse_doc(html_file):
	title = ""
	doc = ""
	html = lxml.html.fromstring(html_file)
	title_el = html.xpath('//title')
	if title_el:
		title = title_el[0].text_content()
	div_el = html.find_class('freestyle-text')
	if div_el:
		doc = div_el[0].text_content()
	return (title, re.sub(r'[\r\n]', ' ', doc))

class Crawler():

	def __init__(self):
		self.br = Browser()
		self.br.set_handle_redirect(True)
		self.queue_server = {"host": "localhost", "port": PORT}
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)	

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

		connection = pymongo.Connection(mongo_host, mongo_port)
		db = connection.test_corpus

		while True:
			# grab the next url off the queue server
			buf_left = 10000 

			url_msg = self.recv_delim( 4096, '\0')
			depth_end = url_msg.find('\1')

			url = url_msg[depth_end+1:]
			depth = int(url_msg[0:depth_end])
	
			print str(datetime.utcnow()) + " URL received from queue server ->" + url +\
			" Depth : " + str(depth)
			
			# fetch url contents (filter stuff here)
			try : 
				response = self.br.open(url,timeout=URL_TIMEOUT)
				
				if response:
					crawler_ack = 's'
				else:
					print "Crawl failed - Timeout"
					crawler_ack = 'f'
			except HTTPError, e: 
				#Much internal debate on this, but "d" is the best
				#response I believe rather than enqueuing it again and again.
				print "Crawl failed - HTTP error"
				crawler_ack = 'd'
			except URLError:
				print "Crawl failed - Could not open page"
				crawler_ack = 'f'
					
			links_found = []
			if crawler_ack is 's':
				try:
					links_found = list( self.br.links() )
				except BrowserStateError:
					print "Crawl failed - Mechanize error"
					crawler_ack = 'f'
				except socket.timeout:
					print "Crawl failed - links() timed out"
					crawler_ack = 'f'

			crawler_msg = crawler_ack + "*"
			depth += 1	#All links in this page are at lower depth.

			for link in links_found:
				if espn_regex.search(link.absolute_url) and not \
						bad_regex.search(link.absolute_url):
					
					if link.absolute_url[-1] is not '/':
						link.absolute_url += '/'

					url_msg = str(depth) + '\1' + link.absolute_url + '*'
					crawler_msg += url_msg
		
			bytes_sent = self.send_msg( crawler_msg, '\0' )

			if response and len(links_found) > 0 and crawler_ack == 's':
				html = response.read()
				(title, body) = parse_doc(html)

				#URL normalization. End all URLs with a '/'.
				if url[-1] != "/":
					url += "/"

				post = {"url": url, "crawl_time": datetime.utcnow(), "title" : title,\
						    "body" : body}

				db.pages.update({"url": url},post, True)

		self.sock.close()
		connection.disconnect()
	
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
