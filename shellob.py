#!/usr/bin/env python

"""
shellob.py

     !      !
   \._\____/_./
   _._/    \_._
    .-\____/-.
   /   oOOo   \
       <  >
    
A simple webcrawler.

Set up a mongodb server, and have different instances of this program running.
Each program will recursively fetch as many urls as it can, upto the python stack limit (1000 at a time)

    
"""
__version__ = "0.0.2"
__author__ = "maldy (lordmaldy at gmail dot com), darthshak (darthshak at gmail dot com)"

import re
import datetime

# stuff you'll have to install - all available with python setuptools
import mechanize				# a browser emulator and screen scraper
import pymongo					# mongo client
import eventlet					# for thread concurrency

mongod_host = '10.42.43.10'			# our mongodb server
mongod_port = 27017
espn_regex = re.compile(r'/football/')		# make sure to crawl only espnstar.com/football pages (includes subdomains like beta.espnstar.com)
fixtures_regex = re.compile(r'/fixtures/')	# the fixtures pages are crawler traps - avoid

# check if current url has already been crawled before
def seen(url, connection=None):
	if connection is None:
		connection = pymongo.Connection(mongod_host, mongod_port)
	db = connection.espn_corpus
	seen = db.pages.find_one({'url':url},{'url':1})
	if seen:
		return True
	return False

# fetch recursively
def fetch(url, pool):
	print "fetching " + url + "... "
	br = mechanize.Browser()	# automatically respects the Robot Exclusion Policy (robots.txt)
	with eventlet.Timeout(5, False):
		try:
			response = br.open(url)
			connection = pymongo.Connection(mongod_host, mongod_port)
			db = connection.espn_corpus
			html = response.read()
			post = {"url": url, "crawl_time": datetime.datetime.utcnow(), "content": html}
			posts = db.pages
			posts.update({"url": url},post, True)
 			
		except Exception:
			raise
			
	links = br.links()
	for link in links:
		if espn_regex.search(link.absolute_url) and not fixtures_regex.search(link.absolute_url) and not seen(link.absolute_url):
			print url + " -> " + link.absolute_url
			url_post = {"url": link.absolute_url}
			url_posts = db.seen_urls
			url_posts.update({'url':link.absolute_url},url_post, True)
			connection.disconnect()			
			pool.spawn_n(fetch, link.absolute_url, pool)

# 'pool' is our non-blocking concurrent event manager
def crawl(start_url):
	pool = eventlet.GreenPool()
	try:
		fetch(start_url, pool)
	except Exception, e:
		print "Exception raised: %s" %e
	pool.waitall()
	return
		
def main():
	# provide a seed url list / queue unleash shellob onto the unsuspecting site.
	# provide a different seed list for different crawler instances to have them work in parallel.

	crawled_list = ["http://www.espnstar.com/football/"]

	for link in crawled_list:
		link = link.strip().encode("utf-8")
		crawl(link)

if __name__ == "__main__":
	main()
