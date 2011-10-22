#!/usr/bin/env python

import pymongo

connection = pymongo.Connection("localhost", 27017)
db = connection.final_espn_corpus_new

docID = 0
entries = db.pages.find({},{"url" : 1})
for entry in entries:
	docID += 1
	url = entry["url"]
	post = {"docID" : docID, "url": url}
	posts = db.docIDs
	posts.update({"docID": docID}, post, True)
