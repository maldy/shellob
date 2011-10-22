#!/usr/bin/env python

import re
import lxml.html
import StringIO
from simpleStemmer import SimpleStemmer
from collections import defaultdict
from array import array
import gc
import math
import pymongo

class Indexer:

	def __init__(self):

		self.doc_index_filepath = 'main_index'
		self.title_index_filepath = 'title_index'
		self.stemmer = SimpleStemmer()
		self.doc_index = defaultdict(list)
		self.title_index = defaultdict(list)
		self.doc_tf = defaultdict(list)
		self.doc_df = defaultdict(int)
		self.title_tf = defaultdict(list)
		self.title_df = defaultdict(int)
		self.numDocs = 0
		self.stopwords = dict.fromkeys(['a','able','about','across','after','all','almost','also','am','among','an','and','any','are','as','at','be','because','been','but','by','can','cannot','could','dear','did','do','does','either','else','ever','every','for','from','get','got','had','has','have','he','her','hers','him','his','how','however','i','if','in','into','is','it','its','just','least','let','like','likely','may','me','might','most','must','my','neither','no','nor','not','of','off','often','on','only','or','other','our','own','rather','said','say','says','she','should','since','so','some','than','that','the','their','them','then','there','these','they','this','tis','to','too','twas','us','wants','was','we','were','what','when','where','which','while','who','whom','why','will','with','would','yet','you','your'])

	def parse_doc(self, html_file):
		title = ""
		doc = ""
		html = lxml.html.fromstring(html_file)
		title_el = html.xpath('//title')
		if title_el:
			title = title_el[0].text_content()
		div_el = html.find_class('freestyle-text')
		if div_el:
			doc = div_el[0].text_content()
		return (title, doc)

	def tokenize(self, line):
		line = line.lower()
		line = re.sub(r'[^a-z0-9 ]', ' ', line) # replace non-alphanumeric characters with spaces
		line = line.split()
		line = [word for word in line if word not in self.stopwords]	# eliminate stopwords
		line = [self.stemmer.stem(word) for word in line]
		return line

	def write_index(self, filepath, index, tf, df):
		index_file = open(filepath, 'w')
		print >> index_file, self.numDocs
		self.numDocs = float(self.numDocs)
		for term in index.iterkeys():
			postings_list = []
			for posting in index[term]:
				docID = posting[0]
				positions = posting[1]
				postings_list.append(':'.join([str(docID), ','.join(map(str,positions))]))
			postings_data = ';'.join(postings_list)
			tf_data = ','.join(map(str, tf[term]))
			idf_data = '%.4f' % (self.numDocs / df[term])
			print >> index_file, '|'.join((term, postings_data, tf_data, idf_data))
		index_file.close()

	def build_term_index(self, docID, terms):
		term_dict_page = {}
		for position, term in enumerate(terms):
			try:
				term_dict_page[term][1].append(position)
			except:
				term_dict_page[term] = [docID, array('I', [position])]
		return term_dict_page

	def tf_idf_weights(self, term_dict_page, tf, df):
		# normalize the doc vector
		norm = 0
		for term, posting in term_dict_page.iteritems():
			norm += len(posting[1])**2
		norm = math.sqrt(norm)

		# calculate tf and idf weights
		for term, posting in term_dict_page.iteritems():
			tf[term].append('%.4f' % (len(posting[1])/norm))
			df[term] += 1
		return (tf, df)

	def merge_to_index(self, term_dict_page, index):
		for term_page, posting_page in term_dict_page.iteritems():
			index[term_page].append(posting_page)
		return index

	def create_indexes(self):
		gc.disable()

		docID = 0
		self.numDocs = 0

		# main loop creating the index

		connection = pymongo.Connection("localhost", 27017)
		db = connection.final_espn_corpus_new
		entries = db.pages.find()
		for entry in entries:
			(title, doc) = self.parse_doc(entry['content'])
			docID += 1
			print "DocID", docID, ": ", title.encode('utf-8'), " [", entry['url'].encode('utf-8'), "]"
			self.numDocs += 1
			lines = '\n'.join ((title, doc))
			doc_terms = self.tokenize(lines)
			title_terms = self.tokenize(title)

			doc_dict_page = self.build_term_index(docID, doc_terms)
			title_dict_page = self.build_term_index(docID, title_terms)

			(self.doc_tf, self.doc_df) = self.tf_idf_weights(doc_dict_page, self.doc_tf, self.doc_df)
			(self.title_tf, self.title_df) = self.tf_idf_weights(title_dict_page, self.title_tf, self.title_df)

			self.doc_index = self.merge_to_index(doc_dict_page, self.doc_index)
			self.title_index = self.merge_to_index(title_dict_page, self.title_index)

			'''
			# build the index for the current page
			term_dict_page = {}
			for position, term in enumerate(terms):
				try:
					term_dict_page[term][1].append(position)
				except:
					term_dict_page[term] = [docID, array('I', [position])]
			'''
			'''
			# normalize the document vector
			norm = 0
			for term, posting in term_dict_page.iteritems():
				norm += len(posting[1])**2
			norm = math.sqrt(norm)

			# calculate tf and idf weights	
			for term, posting in term_dict_page.iteritems():
				self.tf[term].append('%.4f' % (len(posting[1])/norm))
				self.df[term] += 1
			'''
			'''
			# merge the current page index with the main index
			for term_page, posting_page in term_dict_page.iteritems():
				self.main_index[term_page].append(posting_page)
			'''
		gc.enable()
		self.write_index(self.doc_index_filepath, self.doc_index, self.doc_tf, self.doc_df)
		self.write_index(self.title_index_filepath, self.title_index, self.title_tf, self.title_df)

def main():
	i = Indexer()
	i.create_indexes()

if __name__ == "__main__":
	main()
