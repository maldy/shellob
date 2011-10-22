#!/usr/bin/env python

import re
from simpleStemmer import SimpleStemmer
from collections import defaultdict
from itertools import izip
import copy
import math

import socket
import pickle

PREFERRED_SNIPPET_LENGTH = 35
MAX_SNIPPET_LENGTH = 50
SNIPPET_MATCH_WINDOW_SIZE = 5
HOST = 'localhost'
PORT = 20001

def recv_delim(client, buf_len, delim):
	data = ""
	while True:
		recv_data = client.recv(buf_len)
		data += recv_data
		if delim in data:
			return data[:-1]

def send_msg(client, msg, delim):
	msg_len = len(msg)
	bytes_sent = 0
	while bytes_sent < msg_len:
		sent = client.send( msg + delim )
		bytes_sent += sent
		msg = msg[sent+1:]

	return bytes_sent 


class Snippetizer:

	def normalize_term(self, term):
		return term.strip('!,.?').lower()	

	def get_normalized_terms(self, query):
		terms = [self.normalize_term(term) for term in query.split()] + \
			[self.normalize_term(term) + 's' for term in query.split()] + \
			[self.normalize_term(term) + 'es' for term in query.split()]
		return terms

	def list_range(self, x):
		return max(x) - min(x)

	def get_window(self, positions, indices):
		return [word_positions[index] for word_positions, index in \
       		     	izip(positions, indices)]

	def get_min_index(self, positions, window):
		min_indices = (window.index(i) for i in sorted(window))
		for min_index in min_indices:
			if window[min_index] < positions[min_index][-1]:
				return min_index

	def get_shortest_term_span(self, positions):
		indices = [0]*len(positions)
		min_window = window = self.get_window(positions, indices)
		while True:
			min_index = self.get_min_index(positions, window)
			if min_index==None:
				return min_window
			indices[min_index] += 1
			window = self.get_window(positions, indices)
			if self.list_range(min_window) > self.list_range(window):
				min_window = window
			if self.list_range(min_window) == len(positions):
				return min_window

	def generate_term_positions(self, doc, query):
		terms = query.split()
		positions = [[] for j in range(len(terms))]
		for i, word in enumerate(doc.split()):
			for term in terms:
				if self.normalize_term(word) in self.get_normalized_terms(term):
					positions[terms.index(term)].append(i)
					break
		positions = [x for x in positions if x]
		return positions

	def shorten_snippet(self, doc, query):
		flattened_snippet_words = []
		normalized_terms = self.get_normalized_terms(query)
		last_term_appearance = 0
		skipping_words = False
		for i, word in enumerate(doc.split()):
			if word in normalized_terms:
				last_term_appearance = 1
				skipping_words = False
			if i - last_term_appearance > SNIPPET_MATCH_WINDOW_SIZE:
				if not skipping_words:
					flattened_snippet_words.append("...")
					skipping_words = True
				continue
			flattened_snippet_words.append(word)
		return ' '.join(flattened_snippet_words)

	def get_snippet(self, doc, query):
		positions = self.generate_term_positions(doc, query)
		if not positions:
			return ' '.join(doc.split()[0:PREFERRED_SNIPPET_LENGTH]).strip()
		span = self.get_shortest_term_span(positions)
		start = max(0, span[0] - (PREFERRED_SNIPPET_LENGTH / 2))
		end = min(len(doc.split()), span[len(positions) - 1] + (PREFERRED_SNIPPET_LENGTH / 2))
		snippet = ' '.join(doc.split()[start:end+1])
		if (end - start > MAX_SNIPPET_LENGTH):
			snippet = self.shorten_snippet(snippet, query)
		return '...' + snippet.strip() + '...'


class SearchServer:

	def __init__(self):
		self.text_corpus_filepath = 'text_corpus'
		self.doc_index_filepath = 'main_index'
		self.title_index_filepath = 'title_index'
		self.page_ranks_filepath = 'page_ranks'
		self.page_rank_dict = {}
		self.numDocs = None
		self.corpus = {}
		self.doc_index = {}
		self.title_index = {}
		self.doc_tf = {}
		self.doc_idf = {}
		self.title_tf = {}
		self.title_idf = {}
		self.snippetizer = Snippetizer()
		self.stemmer = SimpleStemmer()
		self.stopwords = dict.fromkeys(['a','able','about','across','after','all','almost','also','am','among','an','and','any','are','as','at','be','because','been','but','by','can','cannot','could','dear','did','do','does','either','else','ever','every','for','from','get','got','had','has','have','he','her','hers','him','his','how','however','i','if','in','into','is','it','its','just','least','let','like','likely','may','me','might','most','must','my','neither','no','nor','not','of','off','often','on','only','or','other','our','own','rather','said','say','says','she','should','since','so','some','than','that','the','their','them','then','there','these','they','this','tis','to','too','twas','us','wants','was','we','were','what','when','where','which','while','who','whom','why','will','with','would','yet','you','your'])

	def tokenize(self, line):
		line = line.lower()
		line = re.sub(r'[^a-z0-9 ]', ' ', line) # replace non-alphanumeric characters with spaces
		line = line.split()
		line = [word for word in line if word not in self.stopwords]	# eliminate stopwords
		line = [self.stemmer.stem(word) for word in line]
		return line

	def read_text_corpus(self, filepath):
		corpus = {}
		corpus_file = open(filepath, 'r')
		for line in corpus_file:
			line = line.rstrip()
			(docID, title, url, doc) = line.split('\x03')
			corpus[docID] = {'title': title, 'url': url, 'doc': doc}
		return corpus	

	def read_index(self, index_filepath):
		index_file = open(index_filepath, 'r')
		self.numDocs = int(index_file.readline().rstrip().split('.')[0])
		index = {}
		tf = {}
		idf = {}
		for line in index_file:
			line = line.rstrip()
			(term, postings, line_tf, line_idf) = line.split('|')
			postings = postings.split(';')
			postings = [post.split(':') for post in postings]
			postings = [ [int(post[0]), map(int, post[1].split(','))] for post in postings ]
			index[term] = postings
			line_tf = line_tf.split(',')
			tf[term] = map(float, line_tf)
			idf[term] = float(line_idf)
		index_file.close()
		return (index, tf, idf)

	def read_indexes(self):
		print "Reading text corpus..."
		self.corpus = self.read_text_corpus(self.text_corpus_filepath)
		print "Reading page rank info..."
		pagerank_fp = open(self.page_ranks_filepath, "r")
		self.page_rank_dict = pickle.load(pagerank_fp)
		pagerank_fp.close()
		print "Reading document index (may take a few minutes)..."
		(self.doc_index, self.doc_tf, self.doc_idf) = self.read_index(self.doc_index_filepath)
		# print "Reading title index..."
		# (self.title_index, self.title_tf, self.title_idf) = self.read_index(self.title_index_filepath)
		print "Ready! Listening on localhost:20001"
 
	def intersect_lists(self, lists):
		if len(lists) == 0:
			return []
		lists.sort(key = len)
		return list(reduce(lambda x, y: set(x)&set(y), lists))

	def get_postings(self, terms):
		return [self.doc_index[term] for term in terms]

	def get_docs_from_postings(self, postings):
		return [ [x[0] for x in post] for post in postings]

	def dot_product(self, vector1, vector2):
		if len(vector1) != len(vector2):
			return 0
		return sum([ x*y for (x,y) in zip(vector1, vector2) ])

	def rank_documents(self, terms, docs):
		doc_vectors = defaultdict(lambda: [0]*len(terms))
		query_vector = [0]*len(terms)
		for term_index, term in enumerate(terms):
			if term not in self.doc_index:
				continue
			query_vector[term_index] = self.doc_idf[term]
			for doc_index, (doc, postings) in enumerate(self.doc_index[term]):
				if doc in docs:
					doc_vectors[doc][term_index] = self.doc_tf[term][doc_index]

		doc_scores = [ [self.dot_product(cur_doc_vector, query_vector) + self.page_rank_dict[doc]*1E6, doc] for doc, cur_doc_vector in doc_vectors.iteritems() ]
		doc_scores.sort(reverse=True)
		intermediate_docs = [str(x[1]) for x in doc_scores][:100]
		seen_titles = set()
		result_docs = []
		for document in intermediate_docs:
			if self.corpus[document]['title'] not in seen_titles:
				result_docs.append([self.corpus[document]['title'], self.corpus[document]['url'], self.snippetizer.get_snippet(self.corpus[document]['doc'], ' '.join(terms).strip())])
				seen_titles.add(self.corpus[document]['title'])
#		result_docs = [(self.corpus[x]['title'], self.corpus[x]['url'], self.snippetizer.get_snippet(self.corpus[x]['doc'], ' '.join(terms).strip())) for x in result_docs]
		return result_docs[:10]

	def one_term_query(self, q):
		original_query = q
		q = self.tokenize(q)
		if len(q)==0:
			print ''
			return
		elif len(q) > 1:
			return self.free_term_query(original_query)
		term = q[0]
		if term not in self.doc_index:
			print ''
			return ''
		else:
			postings = self.doc_index[term]
			docs = [x[0] for x in postings]
			return self.rank_documents(q, docs)

	def free_term_query(self, q):
		q = self.tokenize(q)
		if len(q)==0:
			print ''
			return ''
		li = set()
		for term in q:
			try:
				postings = self.doc_index[term]
				docs = [x[0] for x in postings]
				li = li|set(docs)
			except:	# term not in index
				pass
		li = list(li)
		return self.rank_documents(q, li)

	def phrase_query(self, q):
		original_query = q
		q = self.tokenize(q)
		if len(q) == 0:
			print ''
			return ''
		elif len(q) == 1:
			return self.one_term_query(original_query)
		phrase_docs = self.phrase_query_docs(q)
		return self.rank_documents(q, phrase_docs)

	def phrase_query_docs(self, q):
		phrase_docs = []
		length = len(q)
		for term in q:
			if term not in self.doc_index:
				return []
		postings = self.get_postings(q)
		docs = self.get_docs_from_postings(postings)
		docs = self.intersect_lists(docs)
		for i in xrange(len(postings)):
			postings[i] = [x for x in postings[i] if x[0] in docs]
		postings = copy.deepcopy(postings)
		for i in xrange(len(postings)):
			for j in xrange(len(postings[i])):
				postings[i][j][1] = [x - i for x in postings[i][j][1]]
		result = []
		for i in xrange(len(postings[0])):
			li = self.intersect_lists([x[i][1] for x in postings])
			if li == []:
				continue
			else:
				result.append(postings[0][i][0])
		return result

	def parse_query(self, query):
		if '"' in query:
			return self.phrase_query(query)
		elif len(query.split()) > 1:
			return self.free_term_query(query)
		else:
			return self.one_term_query(query)

	def listen(self):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		host = socket.gethostname()
		port = PORT
		s.bind((host, port))
		while True:
			s.listen(5)
			c, addr = s.accept()
			msg = recv_delim(c, 512, '\x01')
			query = pickle.loads(msg)
			print addr, ' >> ', query
			if query!='':
				serp = self.parse_query(query)
				msg = pickle.dumps(serp)
				send_msg(c, msg, '\x01')
			c.close()
			print "Done."

def main():
	s = SearchServer()
	s.read_indexes()
	s.listen()

if __name__ == '__main__':
	main()
