#!/usr/bin/env python

import socket, threading
from datetime import datetime
from traceback import print_exc
from time import time
from sys import exc_info
import Queue
import pickle
import re

from pygraph.classes.digraph import digraph
from pygraph.readwrite.markup import write, read

REVISIT_TIMEOUT = 900			#Time-out (in seconds) to reattempt a failed crawl.
CRAWLER_TIMEOUT = 900 		#Time-out after which crawler is assumed dead.
DISK_SAVE_INTERVAL = 600 #Interval after which important data is saved to disk.

LISTEN_PORT = 10000

SEED_URL = "http://www.espnstar.com/football/"

item_regex = re.compile(r'/(item)([0-9]*)/')

#Assigns a docID to given URL. Adds entries to the docID->url and url->docID
#maps. Returns the docID that was assigned.
def assign_docID(url):
	next_docID = docID_url_map['next_docID']

	acquire_lock(["docID_url", "url_docID"])
	url_docID_map[url] = unicode(next_docID)
	docID_url_map[unicode(next_docID)] = url

	docID_url_map['next_docID'] = next_docID + 1
	release_lock(["docID_url", "url_docID"])
	return next_docID 

def acquire_lock(lock_names):
	[locks[name].acquire() for name in lock_names]

def release_lock(lock_names):
	[locks[name].release() for name in lock_names]

def restore_queue_from_file(queue_file_name):
	queue = Queue.PriorityQueue()
	queue_dict = {}

	try:
		queue_file = open(queue_file_name, "r")
	except IOError:
		return (queue, queue_dict)

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
	except : 
		print_exc()
	finally :
		queue_file.close()

	return (queue, queue_dict)

def restore_list_from_file(set_file_name):
	url_set = set([])

	try: 
		url_file = open(set_file_name, "r")
	except IOError:
		return url_set

	strings = url_file.read().split('\n')

	for string in strings:
		url_set.add(string)

	url_file.close()
	return url_set

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
	fd = open( file_names[var_string], "w" )

	data = globals()[string_var_map[var_string]]
	acquire_lock([var_string])

	if type(data).__name__ == "dict":
		pickle.dump(data, fd)
	elif type(data).__name__ == "set":
		[fd.write(str(item) + "\n") for item in data]
	elif type(data).__name__ == "digraph":
		graph_xml = write( data )
		fd.write( graph_xml )

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
		depth, url, parent_url, url_type = self.url_to_crawl_info['depth'],\
																			self.url_to_crawl_info['url'],\
											 								self.url_to_crawl_info['parent'],\
																			self.url_to_crawl_info['type']
		return (depth, url, parent_url, url_type)

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
		depth, url, parent_url, url_type = self.unpack_url_info() 

		if url_type == "failed":
			failed_queue.put( (depth, parent_url, url, datetime.utcnow()) )
		elif url_type == "roster":
			#We increase the depth so that the crawler doesn't constantly
			#time out on the same URL.
			#Note that roster_queue does not need timestamp information.
			roster_queue.put( (depth + 1e-6, parent_url, url) )

		if excName == "timeout":
			self.print_log("Timed out")
		else:
			self.print_log("Socket error")

	def handle_ack( self, ack ):
		depth, url, parent_url, url_type = self.unpack_url_info()

		log_msg = "-" 

		acquire_lock(["failed"])

		if ack[0] == "s":
			log_msg += " (Complete)"
			self.print_log( log_msg ) 
			visited_set.add(url.strip())

			#Since URL was crawled successfully, removed it from the failed list.
			if url_type == "failed":
				failed_dict.pop(url)
		
		#If ack indicates a crawl which failed due to HTTP reasons.
		elif ack[0] == "f":
			log_msg += " (Failed)"
			self.print_log( log_msg )

			#If the crawl failed, we add it to the failed queue as well
			#as the failed list.
			if not failed_dict.has_key(url):
				failed_queue.put( (depth, url, parent_url, time() ) )
				failed_dict[url] = (depth, parent_url, time() )

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
	#Returns (depth, url, url_type) where,
	#depth -> Depth of URL in the BFS tree.
	#url -> URL to be crawled.
	def get_next_url(self):
		url = ""
		parent_url = ""
		depth = 0
		url_type = ""

		#We first check if we can re-attempt any of the failed crawls.
		#We preferentially pick out those crawls that failed at the least depth.
		#Only those crawls that failed more than REVISIT_TIMEOUT seconds ago
		#are reattempted. 
		#'depth' - Depth of the URL sent to the crawler.	
		#'url' - URL sent to the crawler.
		try:
			failed_url_info = failed_queue.get_nowait()
			timestamp = failed_url_info[3]

			if time() - timestamp > REVISIT_TIMEOUT:
				depth = failed_url_info[0]
				url = failed_url_info[1]
				parent_url = failed_url_info[2]
				url_type = "failed"
			else:
				failed_queue.put(failed_url_info)
			
		except Queue.Empty:
			pass

		if len(url) == 0:
			try:  
				url_info = roster_queue.get_nowait()
				depth, parent_url, url = url_info
				url_type = "roster"
			except Queue.Empty:
				pass

		self.url_to_crawl_info = {'depth' : depth, 'url' : url, \
										'parent' : parent_url, 'type' : url_type}

	#Parse response from crawler. Takes depth, url, url_type arguments for
	#the sake of handling the ack message too.
	def parse_crawler_messages(self, response ):
		msgs = response.split("*")
		ack = msgs[0]

		self.handle_ack( ack )
		depth, url, parent_url, url_type = self.unpack_url_info()

		#Parent URL is now the URL that was just crawled.
		parent_url = url
		msgs = msgs[1:]
		
		p_docID = url_docID_map[parent_url]

		acquire_lock(["grand", "graph", "failed", "roster", "item_nos"])

		for msg in msgs:
			if msg:
				depth_end = msg.index('\1')
				depth = int(msg[:depth_end])
				url = msg[depth_end+1:].strip()

				if url not in grand_set and not self.is_dup(url):
					grand_set.add( url )
					docID = assign_docID( url )
					espn_graph.add_node(unicode(docID))	
				else:
					docID = url_docID_map[url]

				#Add new node and edge to graph.
				print str(p_docID) + " -> " + str(docID)
				if not espn_graph.has_edge([unicode(p_docID), unicode(docID)])\
						and p_docID != docID:
					espn_graph.add_edge([unicode(p_docID), unicode(docID)])

				if url not in visited_set and not roster_dict.has_key(url)\
						and not failed_dict.has_key(url):
					roster_queue.put( (depth, parent_url, url), True )

					roster_dict[url] = (depth, parent_url)
	
		release_lock(["grand", "graph", "failed", "roster", "item_nos"])

	def start(self):
		self.get_next_url()
		depth, url, parent_url, url_type = self.unpack_url_info() 

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
		crawler_response = self.recv_msg( 10000, '\0' )	

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

#Create locks for shared data structures.
var_strings = ["roster", "failed", "grand", "graph", "docID_url",\
							"url_docID", "item_nos", "visited"]
locks = {}

for name in var_strings:
	locks[name] = threading.Lock()

file_names = { "roster" : "qdata/roster", "failed" : "qdata/failed",\
							 "visited" : "qdata/visited", "grand" : "qdata/grand",\
							 "url_docID" : "qdata/url_docID",\
							 "docID_url" : "qdata/docID_url",\
							 "graph" : "qdata/graph.xml",\
							 "item_nos" : "qdata/item_nos" }
							
string_var_map = {"roster" : "roster_dict", "failed" : "failed_dict",\
		"visited" : "visited_set", "grand" : "grand_set",\
		"url_docID" : "url_docID_map", "docID_url" : "docID_url_map",
		"graph" : "espn_graph", "item_nos" : "item_nos_set"}

#Each entry in the roster_queue is of the form ( depth, parent_url, url )
#where "depth" is the distance from the seed URL.
#parent_url is the parent from which this url was reached.
roster_queue, roster_dict = restore_queue_from_file( file_names["roster"] )

#Each entry in failed_Queue is of the form (depth, parent_url, url, timestamp) 
#where "timestamp" is the time at which previous crawl occurred. 
failed_queue, failed_dict = restore_queue_from_file( file_names["failed"] )

#List of URLs already crawled.
#Each entry is of the form (depth, parent_url, url, timestamp)
#timestamp -> Time of crawl.
visited_set = restore_list_from_file( file_names["visited"] )

#Grand list of URLs i.e. URLs known to exist.
grand_set = restore_list_from_file( file_names["grand"] )

item_nos_set = set([])
try :
	item_file = open( file_names["item_nos"], "r")
	str_items = set(item_file.read().split('\n'))
	str_items.remove('')

	[item_nos_set.add(int(item_no)) for item_no in str_items]
except IOError:
	pass 

#ESPN graph of URLs crawled. Made from visited_set.
#Read graph from file.
try :
	graph_file = open( file_names["graph"], "r")
	graphstr = graph_file.read()
	espn_graph = read(graphstr)
except IOError:
	espn_graph = digraph( )

#ID->URL for each node in espn_graph. 
try :
	docID_url_file = open(file_names["docID_url"], "r")
	docID_url_map = pickle.load(docID_url_file)
except IOError:
	docID_url_map = {'next_docID' : 0}

#URL->ID for each node in espn_graph
try :
	url_docID_file = open(file_names["url_docID"], "r")
	url_docID_map = pickle.load(url_docID_file)
except IOError:
	url_docID_map = {}

if roster_queue.empty():
	roster_queue.put( (0, "root", SEED_URL) )		

if len(grand_set) is 0:
	grand_set.add(SEED_URL)
	_docID = assign_docID(SEED_URL)
	espn_graph.add_node(unicode(_docID))

server = Queue_server('localhost',LISTEN_PORT)

try : 
	logger = Logger(time())
	logger.start()
	server.start()
except : 
	for thread in client_threads:
		thread.end()
		thread.join()

	logger.end()
	logger.join()
	#Do one last log update.
	write_to_disk()
	print "Final write to disk"
	exit(0)
