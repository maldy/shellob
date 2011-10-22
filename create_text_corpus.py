#!/usr/bin/env python

import re
import pymongo
import lxml.html

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


connection = pymongo.Connection("localhost", 27017)
db = connection.final_espn_corpus_new

docID = 0

text_corpus = open('text_corpus', 'w')

entries = db.pages.find()
for entry in entries:
	(title, doc) = parse_doc(entry['content'])
	docID += 1
	print str(docID) #, " - ", title.encode('utf-8')
	print >> text_corpus, '\x03'.join((str(docID), title, entry['url'], doc)).encode('utf-8')
text_corpus.close()
