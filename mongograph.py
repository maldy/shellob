#!/usr/bin/env python
#Reads parent and child URL information from corpus to construct metadata for
#graph

import pymongo 
import pickle
import traceback

from pygraph.classes.digraph import digraph
from pygraph.readwrite.markup import write

mongo_host = '10.109.27.150'
mongo_port = 27017

connection = pymongo.Connection(mongo_host, mongo_port)
db = connection.final_espn_corpus_new

#Receive all tuples from corpus in the form (parent, child).
#We later draw an edge between "parent" and "child"
url_data = db.pages.find( {}, { "parent" : True, "url" : True } )

_results = url_data.count()
url_graph_data = []

espn_graph = digraph()

#Inserting each docID as a node into the web graph.
i = 1
while i <= 26671:
	espn_graph.add_node( i )
	i += 1

i = 1		#Skipping the first entry, whose parent is "root". We do not insert 
#"root" into the graph.

while i < n_results:
	entry = url_data[i]
	parent_url = entry['parent']		#Parent 
	url = entry['url']							#Child

	#URLs were not normalized to end with a slash at the end. We do an OR
	#query for both versions of the URL. 
	if parent_url[-1] == u'/':
		alt = parent_url[:-1]
	else:
		alt = parent_url + u'/'

	#Find docID corresponding to child URL.
	url_docID_entry = db.docIDs.find( {"url" : entry['url']} )[0]
	url_docID = int(url_docID_entry['docID'])

	try : 
		parent_docID_entry = db.docIDs.find( { '$or' : [ {"url" : parent_url}, {"url" : alt } ] } )[0]
		parent_docID = int(parent_docID_entry['docID'])
	except : 
		i += 1
		continue

	espn_graph.add_edge( [parent_docID, url_docID] )
	i += 1
	
graph_xml = write(espn_graph)

graph_xml_file = open("graph_xml", "w")
graph_xml_file.write(graph_xml)

connection.disconnect()
