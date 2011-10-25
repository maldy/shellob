#!/usr/bin/env python

import socket, threading
from datetime import datetime
from traceback import print_exc
from time import time
from sys import exc_info
import Queue
import pickle
import re
import pymongo

from pygraph.classes.digraph import digraph

REVISIT_TIMEOUT = 900			#Time-out (in seconds) to reattempt a failed crawl.
CRAWLER_TIMEOUT = 900 		#Time-out after which crawler is assumed dead.
DISK_SAVE_INTERVAL = 1200 #Interval after which important data is saved to disk.

LISTEN_PORT = 10000

SEED_URL = "http://www.espnstar.com/football/"

item_regex = re.compile(r'/(item)([0-9]*)/')

#Set up connection to corpus database.
corpus_connection = pymongo.Connection()
corpus_db = corpus_connection.espn_corpus
tempID_db = corpus_db.tempIDs
graph_db = corpus_db.graph

class DbEmpty(Exception):
	def __init__(self):
		self.value = "Database is empty."

	def __str__(self):
		return repr(self.value)

#Assigns a docID to given URL. Adds entries to the docID->url and url->docID
#maps. Returns the docID that was assigned.
def assign_docID(url):
	next_docID = docID_url_map[u'next_docID']

	acquire_lock(["docID_url", "url_docID"])
	url_docID_map[unicode(url)] = unicode(next_docID)
	docID_url_map[unicode(next_docID)] = unicode(url)

	docID_url_map[u'next_docID'] = next_docID + 1

	#Save docID->url maps to db.
	tempID_db.update({"url" : url}, {"url" : url, "docID" : next_docID}, True)
	tempID_db.update({"url" : "next_docID"}, {"url" : "next", "docID" : next_docID + 1},\
								True)
	corpus_connection.end_request()

	release_lock(["docID_url", "url_docID"])
	return next_docID 

def acquire_lock(lock_names):
	[locks[name].acquire() for name in lock_names]

def release_lock(lock_names):
	[locks[name].release() for name in lock_names]

def restore_queue(queue_file_name):
	queue = Queue.PriorityQueue()
	queue_dict = {}

	queue_file = open(queue_file_name, "r")

	try : 
		queue_dict = pickle.load(queue_file)
	
		if queue_file_name is file_names["roster"] :
			for key in queue_dict.keys():
				depth, parent = queue_dict[key]
				queue.put( (depth, parent, key)  )
		elif queue_file_name is file_names["failed"]:
			for key in queue_dict.keys():
				depth, parent, timestamp = queue_dict[key]
				queue.put( (depth, parent, key, timestamp) )
	finally :
		queue_file.close()

	return (queue, queue_dict)

def restore_set(set_name):
	result = set([])

	set_file = open(file_names[set_name], "r")
	strings = set_file.read().split('\n')
	strings.remove('')
	
	if set_name == "item_nos":
		[result.add(int(item_no)) for item_no in strings]
	else:
		[result.add(unicode(string)) for string in strings]

	set_file.close()
	return result 

#Restores graph, URL->ID and ID->URL maps
def restore_from_db():
	docID_url_map = {}
	url_docID_map = {}
	web_graph = digraph()

	docID_cursor = tempID_db.find()	
	edge_cursor = graph_db.find()

	#For some reason, no graph data in the db.
	if edge_cursor.count() == 0 or docID_cursor.count() == 0:
		raise DbEmpty()

	print edge_cursor.count()
	i = 0	
	while i < docID_cursor.count():
		docID_url_entry = docID_cursor[i]
		url = docID_url_entry['url']
		docID = unicode(docID_url_entry['docID'])

		docID_url_map[docID] = url 
		url_docID_map[url] = docID
		web_graph.add_node( docID )
		i += 1

	i = 0
	while i < edge_cursor.count():
		edge_entry = edge_cursor[i]
		from_docID = edge_entry['from']
		to_docID = edge_entry['to']

		web_graph.add_edge( [from_docID, to_docID] )
		i += 1

	return (web_graph, url_docID_map, docID_url_map)

class Logger(threading.Thread):
	def __init__(self, start_time):
		self.start_time = start_time
		self.continue_logging = True
		threading.Thread.__init__(self)

	def run(self):	
		last_save_time = self.start_time
		while self.continue_logging:
			if time() - last_save_time > DISK_SAVE_INTERVAL:
				write_to_disk()
				last_save_time = time()

	def end(self):
		self.continue_logging = False

#Use appropriate write method for data type.
def write_data(var_string): 
	try : 
		file_name = file_names[var_string]
	except KeyError:
		return

	fd = open( file_name, "w" )

	data = globals()[string_var_map[var_string]]
	acquire_lock([var_string])

	if type(data).__name__ == "dict":
		pickle.dump(data, fd)
	elif type(data).__name__ == "set":
		[fd.write(str(item) + "\n") for item in data]

	release_lock([var_string])
	fd.close()

def write_to_disk():
	[write_data(var_string) for var_string in var_strings]

	print "Wrote to disk"

class HandlerWrapper(threading.Thread):
	def __init__(self, client,client_id, conn_addr):
		self.client = client
		self.client_id = client_id
		self.conn_addr = conn_addr
		self.continue_running = True
		threading.Thread.__init__(self)

	def run(self):
		handler = ClientHandler(self.client, self.client_id, self.conn_addr)
		while self.continue_running:
			handler.start()

		#Connection termination.
		self.client.send("\0")

	def end(self):
		self.continue_running = False

class ClientHandler():
	def __init__(self, client, client_id, conn_addr):
		# Deals exclusively with a single crawler instance. 
		# 'ready' - If crawler is currently idle. If yes, send next
		# roster URL to crawler.
		self.client = client
		self.client_id = client_id
		self.conn_addr = conn_addr
		self.log_file = "crawler" + str(self.client_id)
		self.url_to_crawl_info = {}
							 
	def unpack_url_info(self):
		depth, url, parent_url = self.url_to_crawl_info['depth'],\
															self.url_to_crawl_info['url'],\
											 				self.url_to_crawl_info['parent']
		return (depth, url, parent_url)

	def print_log(self, msg):
		log_file = open("log/" + self.log_file, "a")
		msg_prefix = "[" + str(datetime.utcnow()) + "] " + str(self.conn_addr[0]) +\
								":" + str(self.conn_addr[1]) + " "
		log_file.write(msg_prefix +  msg + "\n")

	#Wrappers for the send and recv function to ensure proper delivery
	#of messages.
	def recv_msg(self, buf_len, delim):
		data = ""
		try : 
			while True:
				recv_data = self.client.recv(buf_len)
				data += recv_data
				if delim in data:
					return data[:-1]
		except : 
			excName = exc_info()[0].__name__
			self.handle_socket_error( excName ) 
			self.client.close()
			exit(-1)	#Thread exit. Connection already dead, so no cleanup required. 

	def send_msg(self, msg, delim):
		msg_len = len(msg)
		bytes_sent = 0

		try:
			while bytes_sent < msg_len:
				sent = self.client.send( msg + delim )
				bytes_sent += sent
				msg = msg[sent+1:]

			return bytes_sent 
		except :
			excName = exc_info()[0].__name__
			self.handle_socket_error( excName ) 
			self.client.close()
			exit(-1)	#Thread exit. 

	#If connection fails for any reason, we restore the URL back to the failed
	#queue or roster queue as the case may be.
	def handle_socket_error( self, excName ):
		depth, url, parent_url = self.unpack_url_info() 

		failed_queue.put( (depth, parent_url, url, time()) )
		failed_dict[url] = ( depth, parent_url, time() )

		if excName == "timeout":
			self.print_log("Timed out")
		else:
			self.print_log("Socket error")

	def convert_to_db_post( self, post ):
		fragments = post.split('\4')
		db_post = {"url" : fragments[0], "crawl_time" : fragments[1],\
						"title" : fragments[2], "body" : fragments[3] }
		return db_post

	def handle_ack( self, ack, post ):
		depth, url, parent_url = self.unpack_url_info()

		log_msg = "-" 

		if ack[0] == "s":
			log_msg += " (Complete)"
			self.print_log( log_msg ) 
			visited_set.add(url.strip())

			db_post = self.convert_to_db_post( post )
			corpus_db.pages.update( {"url": url}, db_post, True )
			corpus_connection.end_request()

		#If ack indicates a crawl which failed due to HTTP reasons.
		elif ack[0] == "f":
			log_msg += " (Failed)"
			self.print_log( log_msg )

			#If the crawl failed, we add it to the failed queue as well
			#as the failed list.
			acquire_lock(["failed"])

			if not failed_dict.has_key(url):
				failed_queue.put( (depth, url, parent_url, time() ) )
				failed_dict[url] = ( depth, parent_url, time() )

			release_lock(["failed"])

	#Rudimentary duplicate checking. For espnstar.com/football, all news
	#URLs are of the form item<num>/<title>. 
	#In this function, we check the item<num> part of the URL against a 
	#list of seen item numbers (couldn't resist). 
	#Returns True if already seen, False otherwise
	def is_dup(self, url):
		re_obj = item_regex.search(url)

		if re_obj:
			item, number = re_obj.groups()
			number = int(number)
			if number in item_nos_set:
				return True
			else:
				item_nos_set.add( number )
				return False
		else:	#Well, we really dont know in this case...
			return False
	
	#Gets next URL to be crawled.
	#Returns (depth, url, parent_url) where,
	#depth -> Depth of URL in the BFS tree.
	#url -> URL to be crawled.
	#parent -> URL from which this one was reached.
	def get_next_url(self):
		url = ""
		parent_url = ""
		depth = 0

		#We first check if we can re-attempt any of the failed crawls.
		#We preferentially pick out those crawls that failed at the least depth.
		#Only those crawls that failed more than REVISIT_TIMEOUT seconds ago
		#are reattempted. 
		#'depth' - Depth of the URL sent to the crawler.	
		#'url' - URL sent to the crawler.
		try:
			failed_url_info = failed_queue.get_nowait()
			depth, url, parent_url, timestamp = failed_url_info

			acquire_lock(["failed"])
			if time() - timestamp > REVISIT_TIMEOUT:
				failed_dict.pop(url)
			else:
				failed_queue.put(failed_url_info)
				failed_dict[url] = ( depth, parent_url, timestamp )
			release_lock(["failed"])
			
		except Queue.Empty:
			pass

		if len(url) == 0:
			try:  
				url_info = roster_queue.get_nowait()
				depth, parent_url, url = url_info

				acquire_lock(["roster"])
				roster_dict.pop(url)
				release_lock(["roster"])
			except Queue.Empty:
				pass

		self.url_to_crawl_info = {'depth' : depth, 'url' : unicode(url), \
										'parent' : unicode(parent_url)}

	#Writes page title, content, etc. to corpus.


	#Parse response from crawler. Takes depth, url arguments for
	#the sake of handling the ack message too.
	def parse_crawler_messages(self, response ):
		msgs_post = response.split("\2")
		msgs = msgs_post[0]
		post = msgs_post[1]

		msgs = msgs.split("*")
		ack = msgs[0]

		self.handle_ack( ack, post )
		depth, url, parent_url = self.unpack_url_info()
		url = unicode(url)
		parent_url = unicode(url)

		#Parent URL is now the URL that was just crawled.
		parent_url = url
		msgs = msgs[1:]
		
		p_docID = unicode(url_docID_map[parent_url])

		acquire_lock(["grand", "failed", "graph", "roster", "item_nos"])

		for msg in msgs:
			if msg:
				depth_end = msg.index('\1')
				depth = int(msg[:depth_end])
				url = unicode(msg[depth_end+1:].strip())

				if self.is_dup(url):
					continue

				if url not in grand_set:
					grand_set.add( url )
					docID = unicode(assign_docID( url ))
					web_graph.add_node( docID )
				else:
					docID = unicode(url_docID_map[url])

				if p_docID != docID and not web_graph.has_edge([p_docID, docID]):
					web_graph.add_edge([p_docID, docID])

					#Graph db update
					graph_db.insert( {"from" : p_docID, "to" : docID}, True )
					corpus_connection.end_request()

				if url not in visited_set and not roster_dict.has_key(url)\
						and not failed_dict.has_key(url):
					roster_queue.put( (depth, parent_url, url), True )

					roster_dict[url] = (depth, parent_url)
	
		release_lock(["grand", "failed", "graph", "roster", "item_nos"])

	def start(self):
		self.get_next_url()
		depth, url, parent_url = self.unpack_url_info() 

		#If the url field is empty, it means there is nothing new left to crawl.
		#Crawling is complete!
		#Else, we have to crawl.
		if url:
			log_msg = "Asking crawler " + str(self.client_id) +\
						" to visit " + url + " at depth " +\
							str(depth)

			self.print_log( log_msg )

			#Depth is packaged along with link. It's easier to ask crawler to
			#increment the depth than ask queue server to keep track of current depth.
			#Message format is : | Depth | \1 | URL | \0 |

			url_msg = str(int(depth)) + '\1' + url
	
			#Attempt to reconnect if the crawler has timed out.
			bytes_sent = self.send_msg(url_msg, '\0')

		else:
			self.print_log( "Out of URLs. Completed crawling!" )
			exit(0)

		#We block on the response from the crawler.
		#The first 1 byte of the response is an ack from the crawler.
		#If the crawl was successful, the rest of the response consists
		#of all the URLs found on that page.
		#If the crawl failed for any reason, the ack is the only information
		#present in the message.
		crawler_response = self.recv_msg( 50000, '\0' )	

		self.parse_crawler_messages( crawler_response )

class Queue_server():
	def __init__(self, host, port):
		self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listen_socket.bind((host, port))

	def start(self):
		client_id = 0

		client_threads_list = []

		while True:
			self.listen_socket.listen(5)
			conn_socket, conn_addr = self.listen_socket.accept()
			print conn_addr

			conn_socket.settimeout( CRAWLER_TIMEOUT )

			# Assign client to handler
			client_thread = HandlerWrapper(conn_socket, client_id, conn_addr)

			client_id += 1
			client_threads.append( client_thread )
			#client_thread.daemon = True
			client_thread.start()
	
client_threads = []

var_strings = ["roster", "failed", "grand", "graph", "docID_url",\
							"url_docID", "item_nos", "visited"]
locks = {}

#Create locks for shared data structures.
for name in var_strings:
	locks[name] = threading.Lock()

file_names = { "roster" : "qdata/roster", "failed" : "qdata/failed",\
							 "visited" : "qdata/visited", "grand" : "qdata/grand",\
							 "item_nos" : "qdata/item_nos" }
							
string_var_map = {"roster" : "roster_dict", "failed" : "failed_dict",\
		"visited" : "visited_set", "grand" : "grand_set",\
		"url_docID" : "url_docID_map", "docID_url" : "docID_url_map",\
		"graph" : "web_graph",\
		"item_nos" : "item_nos_set"}

#Each entry in the roster_queue is of the form ( depth, parent_url, url )
#where "depth" is the distance from the seed URL.
#parent_url is the parent from which this url was reached.

#Each entry in failed_queue is of the form (depth, parent_url, url, timestamp) 
#where "timestamp" is the time at which previous crawl occurred. 

#List of URLs already crawled.
#Each entry is of the form (depth, parent_url, url, timestamp)
#timestamp -> Time of crawl.

#Grand list of URLs i.e. URLs known to exist.

try : 
	print "Loading data from previous crawl. Might take a few minutes..."
	failed_queue, failed_dict = restore_queue( file_names["failed"] )
	roster_queue, roster_dict = restore_queue( file_names["roster"] )
	visited_set = restore_set("visited" )
	grand_set = restore_set("grand" )
	item_nos_set = restore_set("item_nos")

	web_graph, url_docID_map, docID_url_map = restore_from_db()
except : 
	print_exc()
	print "Error loading data from previous crawl. Starting afresh."

	failed_queue = Queue.PriorityQueue()
	roster_queue = Queue.PriorityQueue()
	roster_dict = {}
	failed_dict = {}
	item_nos_set = set([])
	visited_set = set([])
	grand_set = set([])
	web_graph = digraph( )
	docID_url_map = { u'next_docID' : 0 }
	url_docID_map = {}

	roster_queue.put( (0, "root", SEED_URL) )		
	roster_dict[SEED_URL] = (0, "root")
	grand_set.add(SEED_URL)
	_docID = assign_docID(SEED_URL)
	web_graph.add_node(unicode(_docID))

server = Queue_server('localhost',LISTEN_PORT)

try : 
	logger = Logger(time())
	logger.start()
	print "Loading complete. Started server."
	server.start()
except : 
	print "Caught termination request."

#Cleanup code.
for thread in client_threads:
	thread.end()
	thread.join(DISK_SAVE_INTERVAL)
	print "Terminated " + thread.getName()

logger.end()
logger.join()
#Do one last log update.
write_to_disk()
corpus_connection.disconnect()
print "Final write to disk"
exit(0)
