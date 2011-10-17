#!/usr/bin/env python

import socket, time, threading
import Queue
from pygraph.classes.graph import graph
from pygraph.readwrite.markup import write

REVISIT_TIMEOUT = 900		#Time-out (in seconds) to reattempt a failed crawl.
CRAWLER_TIMEOUT = 60 	#Time-out after which crawler is assumed dead.
MAX_TIMEOUTS = 1 

LISTEN_PORT = 10000

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
		log_file = open(self.log_file, "a")
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
	#url_info is a 3-tuple (depth, timestamp, url)
	def handle_socket_error( self, url_info, url_type ):
		print "Handling error"
		if url_type is "failed":
			failed_queue.put( url_info )
		elif url_type is "roster":
			#We increase the depth so that the crawler doesn't constantly
			#time out on the same URL.
			#Note that roster_queue does not need timestamp information.
			roster_queue.put( (url_info[0] + 1e-6, url_info[2]) )
	

	def handle_ack( self, ack, depth, url, url_type ):
		log_msg = str(time.ctime()) + " " + url

		if ack[0] is "s":
			log_msg += " (Complete)"
			self.print_log( log_msg ) 
			print log_msg 
			visited_list.add(url.strip())
			if url_type is "failed":
				failed_list.remove(url)
		
		#If ack indicates a crawl which failed due to HTTP reasons.
		elif ack[0] is "f":
			log_msg += " (Failed)"
			print log_msg
			self.print_log( log_msg )

			#If the crawl failed, we add it to the failed queue as well
			#as the failed list.
			if url not in failed_list:
				failed_queue.put( (depth, time.time(), url) )
				failed_list.add( url )

		#A crawl which failed due to an error from Mechanize.	We assume
		#that such an error will only repeat and don't bother enqueuing
		#it again.
		elif ack[0] is "d":
			log_msg += " (Mechanize error. Ignored)"
			print log_msg
			self.print_log( log_msg )
			grand_list.remove( url )

	#Gets next URL to be crawled.
	#Returns (depth, url, url_type) where,
	#depth -> Depth of URL in the BFS tree.
	#url -> URL to be crawled.
	#url_type -> Can be "failed" if there was a failed crawl on this URL before
	#or can be "roster" if it is a new unvisted URL.
	def get_next_url(self):
		url = ""

		#We first check if we can re-attempt any of the failed crawls.
		#We preferentially pick out those crawls that failed at the least depth.
		#Only those crawls that failed more than REVISIT_TIMEOUT seconds ago
		#are reattempted. 
		#'depth' - Depth of the URL sent to the crawler.	
		#'url' - URL sent to the crawler.
		if self.crawler_timed_out is False:
			url = ""
			depth = 0
			url_type = ""

			try:
				failed_url_info = failed_queue.get_nowait()

				timestamp = failed_url_info[1]
				if time.time() - timestamp > REVISIT_TIMEOUT:
					depth = failed_url_info[0]
					url = failed_url_info[2]
					url_type = "failed"
				else:
					failed_queue.put(failed_url_info)
			
			except Queue.Empty:
				pass

				#In case the chosen failed crawl is too recent, we put it 
				#back on the failed queue.
				#We choose from the roster queue instead.
				try:  
					url_info = roster_queue.get_nowait()
					depth, url = url_info
					url_type = "roster"
				except Queue.Empty:
					pass

		return (url, depth, url_type)

	#Parse response from crawler. Takes depth, url, url_type arguments for
	#the sake of handling the ack message too.
	def parse_crawler_messages(self, response, depth, url, url_type):
		print response
		msgs = response.split("*")
		ack = msgs[0]

		self.handle_ack( ack, depth, url, url_type )
		msgs = msgs[1:]
		
		for msg in msgs:
			if msg:
				try:
					depth_end = msg.index('\1')
					depth = int(msg[:depth_end])
					url = msg[depth_end+1:]

					if url.strip() not in grand_list:
						grand_list.add( url )

					if url.strip() not in visited_list and \
					 	 url.strip() not in roster_list:
						roster_queue.put( (depth, url), True )
						roster_list.add( url )

				except ValueError:
					log_msg = "Badly formed message from crawler" + str(self.client_id) +\
										"No depth information present" 

					self.print_log( log_msg )
					print log_msg + "\n"
					pass

	def start(self):
		self.print_log( "--------------------------" )
		self.print_log( "New connection accepted" )

		while True:

			url_info = self.get_next_url()
			depth, url, url_type = url_info

			#If the url field is empty, it means there is nothing new left to crawl.
			#Crawling is complete!
			#Else, we have to crawl.
			if url:
				log_msg = "Asking crawler " + str(self.client_id) +\
							" to visit " + url + " at depth " + str(depth)
	
				self.print_log( log_msg )
				print time.ctime() + " " + log_msg

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
					self.handle_socket_error( (depth, time.time(), url), url_type ) 
					self.n_timeouts += 1

					print time.ctime() + " " + log_msg
					self.print_log( log_msg )

				except socket.error, e:
					value, message = e
					self.handle_socket_error( (depth, time.time(), url), url_type ) 

					print time.ctime() + " " + log_msg
					self.print_log( log_msg )

			else:
				print "Out of URLs. Completed crawling!"
				break
	
			#We block on the response from the crawler.
			#The first 1 byte of the response is an ack from the crawler.
			#If the crawl was successful, the rest of the response consists
			#of all the URLs found on that page.
			#If the crawl failed for any reason, the ack is the only information
			#present in the message.
			try : 
				print "Waiting"
				crawler_response = self.recv_delim( 10000, '\0' )	
				if self.crawler_timed_out is True:
					self.crawler_timed_out = False
					self.n_timeouts = 0

			except socket.timeout:
				self.n_timeouts += 1
				self.crawler_timed_out = True

				if self.n_timeouts is 1:
					log_msg = " " + url + " (Crawler timed out)"
					print time.ctime() + " " + log_msg
					self.print_log( log_msg )

				if self.n_timeouts == MAX_TIMEOUTS:
					self.handle_socket_error( (depth, time.time(), url), url_type ) 
					log_msg = " Maximum number of time outs reached. Terminating"\
									+ " connection."
					print time.ctime() + " " + log_msg
					self.print_log( log_msg )
					break

			except socket.error, e:
				value, message = e
				self.handle_socket_error( (depth, time.time(), url), url_type ) 

				print time.ctime() + " " + log_msg
				self.print_log( log_msg )
				continue
	
			self.parse_crawler_messages( crawler_response, depth, url, url_type )

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
	
#Crawling is performed as BFS.

#Each entry in the roster_queue is of the form (depth, url) where
#"depth" is the distance from the seed URL.

#roster_queue = get_roster_from_db()
roster_queue = Queue.PriorityQueue()
roster_list = set([])

#Each entry in failed_Queue is of the form (depth, time_stamp, url) where
#"time_stamp" is the time at which previous crawl occurred. 

#failed_queue = get_failed_from_db()
failed_queue = Queue.PriorityQueue()
failed_list = set([])

#List of URLs already crawled.
visited_list = set([]) 

#Grand list of URLs i.e. URLs known to exist.
grand_list = set([])

#Need to modify seed_url so that BFS can be resumed.
#seed_url = get_seed_url_from_db()
seed_url = "http://www.espnstar.com/football/"

roster_queue.put( (0, seed_url) )		
grand_list.add(seed_url)
server = Queue_server('localhost',LISTEN_PORT)
server.start()
