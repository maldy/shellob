#!/usr/bin/env python

# Author : darthshak 
# Mail : vishakadatta [at] gmail [dot] com
# Downloads all chats from a user's folder. 
# Before the script is run, the user must enable access to chat logs through
# IMAP on the GMail account. 
#
# We directly access the inbox labelled "[Gmail]/Chats". After obtaining a list 
# of message IDs corresponding to the chat, we download each chat.
# After all the necessary parsing, we create a file (or push into DB) for each person that the
# user has chatted with, and store all chats in order of their message IDs,
# which corresponds to a chronological order. Whether this order is efficient in
# terms of access time and stuff remains to be seen.

import imaplib, getpass
import email.parser

#TODO 
#1) Build parser for output of email.parser. Need to use jabber.py and stuff.
#2) Decide on storage scheme for corpus.
#3) Add exception handling for all of this stuff.
class Indexer():
	def __init__(self):
		#Read user name and password from command line. 			
		#Little hesitant to store details on disk until I find a generally accepted
		#way to do so.
		username =	raw_input("User name : ")
		password = getpass.getpass("Password : ")

		self.mailbox = imaplib.IMAP4_SSL('imap.gmail.com', 993)
		self.mailbox.login( username, password )
		self.msg_parser = email.parser.Parser()

	#Get list of message IDs corresponding to chat logs. This requires set up of
	#Gmail to allow IMAP access to these logs.
	def get_chatIDs(self):

		#The mailbox containing Chats is readonly. 
		imap_response = self.mailbox.select( '[Gmail]/Chats', True )	
		if imap_response[0] != 'OK':
			print "Could not locate chat logs. Are you sure you have configured "+\
						"access to the logs?"
			return;

		#Get all message IDs corresponding to chats.
		imap_response = self.mailbox.search( '', 'ALL' )
		if imap_response[0] != 'OK':
			print "Could not locate chat logs for some reason. Perhaps all "+\
						"your chats are not logged?"
			return;

		msgIDs = imap_response[1][0].split()
		for msgID in msgIDs:			
			print "Retrieved message " + msgID + "/" + str(len(msgIDs))
			#Retrieves entire message. 
			rtrv_msg = self.mailbox.fetch( msgID, '(RFC822)' )
			#Parse message.
			parsed_msg = self.msg_parser.parsestr( rtrv_msg[1][0][1] )

			#info = get_info( parsed_msg )
			#db_insert( info )

def main():
	indexer_obj = Indexer()
	indexer_obj.get_chatIDs()

if __name__ == '__main__':
	main()
