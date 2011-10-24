#!/usr/bin/env python

import socket, threading
from datetime import datetime
from traceback import print_exc
from time import time
import Queue
import pickle
import re

from pygraph.classes.digraph import digraph
from pygraph.readwrite.markup import write, read

REVISIT_TIMEOUT = 60		#Time-out (in seconds) to reattempt a failed crawl.
CRAWLER_TIMEOUT = 1800 	#Time-out after which crawler is assumed dead.
DISK_SAVE_INTERVAL = 60 #Interval after which important data is saved to disk.
MAX_TIMEOUTS = 1 

LISTEN_PORT = 10000

ROSTER_FILE = "qdata/roster"
FAILED_FILE = "qdata/failed"
VISITED_FILE = "qdata/visited"
GRAND_FILE = "qdata/grand"
URL_DOCID_FILE = "qdata/url_docID"
DOCID_URL_FILE = "qdata/docID_url"
GRAPH_FILE = "qdata/espn_graph.xml"
ITEM_FILE = "qdata/item_nos"

SEED_URL = "http://www.espnstar.com/football/"

#The next docID that will be assigned.
next_docID = 0

roster_lock = threading.Lock()
failed_lock = threading.Lock()
grand_lock = threading.Lock()
graph_lock = threading.Lock()
docID_maps_lock = threading.Lock()
item_nos_lock = threading.Lock()

item_regex = re.compile(r'/(item)([0-9]*)/')

#Assigns a docID to given URL. Adds entries to the docID->url and url->docID
#maps. Returns the docID that was assigned.
def assign_docID(url):
	global next_docID 

	docID_maps_lock.acquire()
	url_docID_map[url] = next_docID
	docID_url_map[next_docID] = url

	next_docID += 1
	docID_url_map['next_docID'] = next_docID
	docID_maps_lock.release()
	return next_docID - 1

def restore_queue_from_file(queue_file_name):
	queue = Queue.PriorityQueue()
	queue_dict = {}

	try:
		queue_file = open(queue_file_name, "r")
	except IOError:
		return (queue, queue_dict)

	try : 
		queue_dict = pickle.load(queue_file)
	
		if queue_file_name is ROSTER_FILE :
			for key in queue_dict.keys():
				depth, parent = queue_dict[key]
				queue.put( (depth, parent, key)  )
		elif queue_file_name is FAILED_FILE:
			for key in queue_dict.keys():
				depth, parent, timestamp = queue_dict[key]
				queue.put( (depth, parent, key, timestamp) )
	except : 
		queue_file.close()
	return (queue, queue_dict)

def restore_list_from_file(set_file_name):
	url_set = set([])

	try: 
		url_file = open(set_file_name, "r")
	except IOError:
		return url_set

	url_str = url_file.read().split('\n')
	for url in url_str:
		url_set.add(url)

	url_file.close()
	return url_set

class Logger(threading.Thread):
	def __init__(self, start_time):
		self.start_time = start_time
		threading.Thread.__init__(self)

	def run(self):	
		last_save_time = self.start_time
		while True:
			last_save_time = write_to_disk(last_save_time)

def write_to_disk(last_save_time):
	if time() - last_save_time > DISK_SAVE_INTERVAL:
		try : 
			grand_file = open( GRAND_FILE, "w" )
			failed_file = open( FAILED_FILE, "w" )
			roster_file = open( ROSTER_FILE, "w" )
			visited_file = open( VISITED_FILE, "w" )
			url_docID_file = open( URL_DOCID_FILE, "w" )
			docID_url_file = open( DOCID_URL_FILE, "w" )
		except : 
			print_exc()

		try : 
			roster_lock.acquire()
			pickle.dump(roster_dict, roster_file)
			roster_lock.release()

			failed_lock.acquire()
			pickle.dump(failed_dict, failed_file)
			failed_lock.release()

			docID_maps_lock.acquire()
			pickle.dump( url_docID_map, url_docID_file )
			pickle.dump( docID_url_map, docID_url_file ) 
			docID_maps_lock.release()
		except : 
			print_exc()

		try : 
			graph_file = open( GRAPH_FILE, "w" )

			graph_lock.acquire()
			graph_xml_str = write( espn_graph )
			graph_lock.release()

			graph_file.write( graph_xml_str )
		except : 
			print_exc()

		for url in visited_list:
			visited_file.write(url + "\n")

		for url in grand_list:
			grand_file.write(url + "\n")

		print "Wrote to disk"
		last_save_time = time()

	return last_save_time

class HandlerWrapper(threading.Thread):
	def __init__(self, client,client_id, conn_addr):
		self.client = client
		self.client_id = client_id
		self.conn_addr = conn_addr
		threading.Thread.__init__(self)

	def run(self):
		handler = ClientHandler(self.client, self.client_id, self.conn_addr)
		handler.start()

class ClientHandler():
	def __init__(self, client, client_id, conn_addr):
		# Deals exclusively with a single crawler instance. 
		# 'ready' - If crawler is currently idle. If yes, send next
		# roster URL to crawler.
		self.client = client
		self.client_id = client_id
		self.conn_addr = conn_addr
		self.crawler_timed_out = False
		self.n_timeouts = 0
		self.log_file = "crawler" + str(self.client_id)
							 
	def print_log(self, msg):
		log_file = open("log/" + self.log_file, "a")
		msg_prefix = "[" + str(datetime.utcnow()) + "] " + str(self.conn_addr[0]) +\
								":" + str(self.conn_addr[1]) + " "
		log_file.write(msg_prefix +  msg + "\n")

	#Wrappers for the send and recv function to ensure proper delivery
	#of messages.
	def recv_delim(self, buf_len, delim):
		data = ""
		while True:
			recv_data = self.client.recv(buf_len)
			data += recv_data
			if delim in data:
				return data[:-1]

	def send_msg(self, msg, delim):
		msg_len = len(msg)
		bytes_sent = 0

		while bytes_sent < msg_len:
			sent = self.client.send( msg + delim )
			bytes_sent += sent
			msg = msg[sent+1:]

		return bytes_sent 

	#If connection fails for any reason, we restore the URL back to the failed
	#queue or roster queue as the case may be.
	def handle_socket_error( self, url_info ):
		depth, url, parent_url, url_type = url_info['depth'], url_info['url'],\
											 								url_info['parent'], url_info['type']

		if url_type == "failed":
			failed_queue.put( (depth, parent_url, url, datetime.utcnow()) )
		elif url_type == "roster":
			#We increase the depth so that the crawler doesn't constantly
			#time out on the same URL.
			#Note that roster_queue does not need timestamp information.
			roster_queue.put( (depth + 1e-6, parent_url, url) )

	def handle_ack( self, ack, url_info ):
		depth, url, parent_url, url_type = url_info['depth'], url_info['url'],\
											 								url_info['parent'], url_info['type']
		log_msg = "-" 

		failed_lock.acquire()

		if ack[0] == "s":
			log_msg += " (Complete)"
			self.print_log( log_msg ) 
			visited_list.add(url.strip())

			#Since URL was crawled successfully, removed it from the failed list.
			if url_type == "failed":
				print url_info
				failed_dict.pop(url)
		
		#If ack indicates a crawl which failed due to HTTP reasons.
		elif ack[0] == "f":
			log_msg += " (Failed)"
			self.print_log( log_msg )

			#If the crawl failed, we add it to the failed queue as well
			#as the failed list.
			if not failed_dict.has_key(url):
				print "Putting " + url + " in failed queue."
				failed_queue.put( (depth, parent_url, url, time() ) )
				failed_dict[url] = (depth, parent_url, time() )

		failed_lock.release()

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
		if self.crawler_timed_out is False:

			try:
				failed_url_info = failed_queue.get_nowait()
				timestamp = failed_url_info[3]

				if time() - timestamp > REVISIT_TIMEOUT:
					print "From failed"
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
					print "From roster"
					url_info = roster_queue.get_nowait()
					depth, parent_url, url = url_info
					url_type = "roster"
				except Queue.Empty:
					pass

		url_info = {'depth' : depth, 'url' : url, 'parent' : parent_url,\
								'type' : url_type}
		print "Sent to crawler"
		print str(url_info)
		return url_info 

	#Parse response from crawler. Takes depth, url, url_type arguments for
	#the sake of handling the ack message too.
	def parse_crawler_messages(self, response, url_info ):
		msgs = response.split("*")
		ack = msgs[0]

		self.handle_ack( ack, url_info )
		depth, url = url_info['depth'], url_info['url'],

		new_parent_url = url_info['url']

		msgs = msgs[1:]
		
		grand_lock.acquire()
		roster_lock.acquire()
		failed_lock.acquire()
		graph_lock.acquire()

		p_docID = url_docID_map[new_parent_url]

		for msg in msgs:
			if msg:
				depth_end = msg.index('\1')
				depth = int(msg[:depth_end])
				url = msg[depth_end+1:].strip()

				if url not in grand_list and not self.is_dup(url):
					grand_list.add( url )
					docID = assign_docID( url )
					espn_graph.add_node(docID)	
				else:
					docID = url_docID_map[url]

				#Add new node and edge to graph.
				if not espn_graph.has_edge([p_docID, docID]):
					espn_graph.add_edge([p_docID, docID])

				if url not in visited_list and not roster_dict.has_key(url)\
						and not failed_dict.has_key(url):
					roster_queue.put( (depth, new_parent_url, url), True )
					roster_dict[url] = (depth, new_parent_url)
	
		grand_lock.release()
		roster_lock.release()
		failed_lock.release()
		graph_lock.release()

	def start(self):
		self.print_log( "--------------------------" )
		self.print_log( "New connection accepted" )

		while True:
			url_to_crawl_info = self.get_next_url()
			depth = url_to_crawl_info['depth']
			url = url_to_crawl_info['url']
			parent_url = url_to_crawl_info['parent']
			url_type = url_to_crawl_info['type']

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
		
				try:
					#Attempt to reconnect if the crawler has timed out.
					bytes_sent = self.send_msg(url_msg, '\0')
				except socket.timeout:
					log_msg = " " + url + " (Crawler timed out)"
					self.crawler_timed_out = True
					self.handle_socket_error( url_to_crawl_info ) 
					self.n_timeouts += 1

					self.print_log( log_msg )

				except socket.error:
					self.handle_socket_error( url_to_crawl_info ) 

					self.print_log( log_msg )
					break		#Evidently a break-down in the connection. Better to end it.

			else:
				self.print_log( "Out of URLs. Completed crawling!" )
				break
	
			#We block on the response from the crawler.
			#The first 1 byte of the response is an ack from the crawler.
			#If the crawl was successful, the rest of the response consists
			#of all the URLs found on that page.
			#If the crawl failed for any reason, the ack is the only information
			#present in the message.
			try : 
				crawler_response = self.recv_delim( 10000, '\0' )	
				if self.crawler_timed_out is True:
					self.crawler_timed_out = False
					self.n_timeouts = 0

			except socket.timeout:
				self.n_timeouts += 1
				self.crawler_timed_out = True

				if self.n_timeouts is 1:
					log_msg = " " + url + " (Crawler timed out)"
					self.print_log( log_msg )

				if self.n_timeouts == MAX_TIMEOUTS:
					self.handle_socket_error( url_to_crawl_info ) 
					log_msg = " Maximum number of time outs reached. Terminating"\
									+ " connection."
					self.print_log( log_msg )
					break

			except socket.error:
				self.handle_socket_error( url_to_crawl_info ) 

				self.print_log( log_msg )
				break
	
			self.parse_crawler_messages( crawler_response, url_to_crawl_info )

		self.print_log( "Connection terminated\n" )
		print "Connection terminated"
		self.client.close()

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
			client_threads_list.append(client_thread)
			client_thread.start()
	
#Each entry in the roster_queue is of the form ( depth, parent_url, url )
#where "depth" is the distance from the seed URL.
#parent_url is the parent from which this url was reached.
roster_queue, roster_dict = restore_queue_from_file( ROSTER_FILE )

#Each entry in failed_Queue is of the form (depth, parent_url, url, timestamp) 
#where "timestamp" is the time at which previous crawl occurred. 
failed_queue, failed_dict = restore_queue_from_file( FAILED_FILE )

#List of URLs already crawled.
#Each entry is of the form (depth, parent_url, url, timestamp)
#timestamp -> Time of crawl.
visited_list = restore_list_from_file( VISITED_FILE )

#Grand list of URLs i.e. URLs known to exist.
grand_list = restore_list_from_file( GRAND_FILE )

item_nos_set = set([])
try :
	item_file = open( ITEM_FILE, "r")
	str_items = item_file.read().split('\n')
	for str_item in str_items:
		item_no = int(str_item)
		item_nos_set.add(item_no)
except IOError:
	pass 

#ESPN graph of URLs crawled. Made from visited_list.
#Read graph from file.
try :
	graph_file = open( GRAPH_FILE, "r")
	graphstr = graph_file.read()
	espn_graph = read(graphstr)
except IOError:
	espn_graph = digraph( )

#ID->URL for each node in espn_graph. 
try :
	docID_url_file = open(DOCID_URL_FILE, "r")
	docID_url_map = pickle.load(docID_url_file)
	node_id = docID_url_map['latest_id']
except IOError:
	docID_url_map = {'next_id' : 1}

#URL->ID for each node in espn_graph
try :
	url_docID_file = open(URL_DOCID_FILE, "r")
	url_docID_map = pickle.load(url_docID_file)
except IOError:
	url_docID_map = {}

if roster_queue.empty():
	roster_queue.put( (0, "root", SEED_URL) )		

if len(grand_list) is 0:
	grand_list.add(SEED_URL)
	_docID = assign_docID(SEED_URL)
	espn_graph.add_node(_docID)

server = Queue_server('localhost',LISTEN_PORT)

logger = Logger(time())
logger.start()
server.start()
