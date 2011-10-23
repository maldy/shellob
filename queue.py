#!/usr/bin/env python

import socket, time, threading
import Queue
import pickle

from pygraph.classes.digraph import digraph
from pygraph.readwrite.markup import write, read

REVISIT_TIMEOUT = 900		#Time-out (in seconds) to reattempt a failed crawl.
CRAWLER_TIMEOUT = 1800 	#Time-out after which crawler is assumed dead.
DISK_SAVE_INTERVAL = 10 #Interval after which important data is saved to disk.
MAX_TIMEOUTS = 1 

LISTEN_PORT = 10000

ROSTER_FILE = "qdata/roster_file"
FAILED_FILE = "qdata/failed_file"
VISITED_FILE = "qdata/visited_file"
GRAND_FILE = "qdata/grand_file"
GRAPH_FILE = "qdata/espn_graph.xml"

SEED_URL = "http://www.espnstar.com/football/"

def restore_queue_from_file(queue_file_name):
	queue = Queue.PriorityQueue()
	queue_dict = {}

	try:
		queue_file = open(queue_file_name, "r")
	except IOError:
		return (queue, queue_dict)

	queue_dict = pickle.load(queue_file)
	
	if queue_file_name is ROSTER_FILE :
		for key in queue_dict.keys():
			depth, parent = queue_dict[key]
			queue.put( (depth, parent, key)  )
	elif queue_file_name is FAILED_FILE:
		for key in queue_dict.keys():
			depth, parent, timestamp = queue_dict[key]
			queue.put( (depth, parent, key, timestamp) )
	
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

def write_to_disk():
	grand_file = open( GRAND_FILE, "w" )
	failed_file = open( FAILED_FILE, "w" )
	roster_file = open( ROSTER_FILE, "w" )
	visited_file = open( VISITED_FILE, "w" )
	graph_file = open( GRAPH_FILE, "w" )

	pickle.dump(roster_dict, roster_file)
	pickle.dump(failed_dict, failed_file)

	graph_xml_str = write( espn_graph )
	graph_file.write( graph_xml_str )

	for url in visited_list:
		visited_file.write(url + "\n")

	for url in grand_list:
		grand_file.write(url + "\n")

	print "Wrote to disk"

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
		self.start_time = time.time()
		self.last_save_time = time.time()
							 
	def print_log(self, msg):
		log_file = open("log/" + self.log_file, "a")
		msg_prefix = "[" + time.ctime(time.time()) + "] " + str(self.conn_addr[0]) +\
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

		if url_type is "failed":
			failed_queue.put( (depth, parent_url, url, time.time()) )
		elif url_type is "roster":
			#We increase the depth so that the crawler doesn't constantly
			#time out on the same URL.
			#Note that roster_queue does not need timestamp information.
			roster_queue.put( (depth + 1e-6, parent_url, url) )

	def handle_ack( self, ack, url_info ):
		depth, url, parent_url, url_type = url_info['depth'], url_info['url'],\
											 								url_info['parent'], url_info['type']
		log_msg = url

		if ack[0] is "s":
			log_msg += " (Complete)"
			self.print_log( log_msg ) 
			visited_list.add(url.strip())

			if url_type is "failed":
				failed_dict.pop(url)

			#Add new node to graph.
			self.add_new_node( url )
			if parent_url is not "root":
				self.add_new_edge( parent_url, url )
		
		#If ack indicates a crawl which failed due to HTTP reasons.
		elif ack[0] is "f":
			log_msg += " (Failed)"
			self.print_log( log_msg )

			#If the crawl failed, we add it to the failed queue as well
			#as the failed list.
			if not failed_dict.has_key(url):
				failed_queue.put( (depth, parent_url, url, time.time() ) )
				failed_dict[url] = (depth, parent_url, time.time() )

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
				if time.time() - timestamp > REVISIT_TIMEOUT:
					depth = failed_url_info[0]
					url = failed_url_info[1]
					parent_url = failed_url_info[2]
					url_type = "failed"
				else:
					failed_queue.put(failed_url_info)
			
			except Queue.Empty:
				pass

			if not url:
				try:  
					url_info = roster_queue.get_nowait()
					depth, parent_url, url = url_info
					url_type = "roster"
				except Queue.Empty:
					pass

		url_info = {'depth' : depth, 'url' : url, 'parent' : parent_url,\
								'type' : url_type}
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
		
		for msg in msgs:
			if msg:
				depth_end = msg.index('\1')
				depth = int(msg[:depth_end])
				url = msg[depth_end+1:].strip()

				if url not in grand_list:
					grand_list.add( url )

				if url not in visited_list and not roster_dict.has_key(url):
					roster_queue.put( (depth, new_parent_url, url), True )
					roster_dict[url] = (depth, new_parent_url)
	
	def add_new_node( self, url ):
		if not url_id_map.has_key(url):
			node_id = id_url_map['next_id']
			id_url_map['next_id'] += 1

			id_url_map[node_id] = url
			url_id_map[url] = node_id

			espn_graph.add_node(node_id)

	def add_new_edge(self, base_url, url):
		base_node_id = url_id_map[base_url]

		url_node_id = url_id_map[url]
		espn_graph.add_edge([base_node_id, url_node_id])

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

			if time.time() - self.last_save_time > DISK_SAVE_INTERVAL:
				write_to_disk()
				self.last_save_time = time.time()

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
	id_url_file = open("id_url_map", "r")
	id_url_map = pickle.load(url_id_file, 2)
	node_id = id_url_map['latest_id']
except IOError:
	id_url_map = {'next_id' : 1}

#URL->ID for each node in espn_graph
try :
	url_id_file = open("url_id_map", "r")
	url_id_map = pickle.load(url_id_file, 2)
except IOError:
	url_id_map = {}

if roster_queue.empty():
	roster_queue.put( (0, "root", SEED_URL) )		

if len(grand_list) is 0:
	grand_list.add(SEED_URL)

server = Queue_server('localhost',LISTEN_PORT)
server.start()
