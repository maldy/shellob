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
import gc

# stuff you'll have to install - all available with python setuptools
import urllib2
import lxml.html
from lepl.apps._test.rfc3696 import HttpUrl
import pickle

espn_regex = re.compile(r'http://(www.){,1}espnstar.com/football/')
bad_regex =\
re.compile(r'/fixtures/{,1}|/URL/{,1}|/galleries/{,1}|/videos/{,1}|/[0-9]/{,1}|/matchcast/{,1}')

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

def get_links(base_url, html_data):
	html = lxml.html.fromstring(html_data)	
	html.make_links_absolute( base_url )

	link_info = html.iterlinks()
	links = []
	for link in link_info:
		links.append( link[2] )

	return links

class Crawler():

	def __init__(self):
		self.queue_server = {"host": "localhost", "port": PORT}
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)	

		try: 
			self.sock.connect((self.queue_server['host'], self.queue_server['port']))
		except socket.error, (value, message):
			print "Could not connect to queue server at " + self.queue_server['host'] +\
					":" + str(self.queue_server['port'])
			print message
			return

	def recv_msg(self, buf_len, delim):
		data = ""
		while True:
			recv_data = self.sock.recv(buf_len)
			data += recv_data.decode('utf-8')
			if delim in data:
				return data[:-1]

	def send_msg(self, msg, delim):
		msg = msg.encode('utf-8')
		msg_len = len(msg)
		bytes_sent = 0

		while bytes_sent < msg_len:
			sent = self.sock.send( msg + delim )
			bytes_sent += sent
			msg = msg[sent+1:]

		return bytes_sent 

	def crawl(self):
		valid_url_check = HttpUrl()

		while True:
			# grab the next url off the queue server
			url_msg = self.recv_msg( 4096, '\0')

			if url_msg == "":
				print "Queue server terminated connection"
				break

			depth_end = url_msg.find('\1')
			url = url_msg[depth_end+1:]
			depth = int(url_msg[0:depth_end])
			del(depth_end)
	
			print str(datetime.utcnow()) + " URL received from queue server ->" + url +\
			" Depth : " + str(depth)

			# fetch url contents (filter stuff here)
			try : 
				response = urllib2.urlopen(url,timeout=URL_TIMEOUT)
				response_html = response.read()
				#response.read(1)
				response.fp._sock.recv = None
				response.close()
				
				if response:
					crawler_ack = 's'
				else:
					print "Crawl failed - Timeout"
					crawler_ack = 'f'
			except urllib2.HTTPError: 
				#Much internal debate on this, but "d" is the best
				#response I believe rather than enqueuing it again and again.
				print "Crawl failed - HTTP error"
				crawler_ack = 'd'
			except urllib2.URLError:
				print "Crawl failed - Could not open page"
				crawler_ack = 'f'
					
			if crawler_ack == 's':
				links_found = get_links( url, response_html ) 

			crawler_msg = crawler_ack + "*"
			depth += 1	#All links in this page are at lower depth.
			
			#Prepare the db post.
			if len(links_found) > 0 and crawler_ack == 's':
				if response_html:
					(title, body) = parse_doc(response_html)

					#URL normalization. End all URLs with a '/'.
					if url[-1] != u'/':
						url += u'/'

					post = url + '\4' + str(datetime.utcnow()) + '\4' + title + '\4' +\
									body

			for link in links_found:
				url = link.encode('utf-8')
				espn_regex_match = espn_regex.search(url)
				bad_regex_match = bad_regex.search(url) 
				valid_url = valid_url_check(url)
				if espn_regex.search(url) and not \
						bad_regex.search(url) and \
						valid_url == True:		
					
					if url[-1] != u'/' or url[-1] != '/':
						url += u'/'

					url_msg = str(depth) + '\1' + url + '*'
					crawler_msg += url_msg
		
			crawler_msg += '\2' + post
			bytes_sent = self.send_msg( crawler_msg, '\0' )

		self.sock.close()
	
def main():
	# thread and unleash shellob onto the unsuspecting site. Amok!
	c = Crawler()
	#gc.set_debug(gc.DEBUG_LEAK)
	c.crawl()
	
if __name__ == "__main__":
	main()
