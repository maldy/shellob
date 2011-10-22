#!/usr/bin/env python
#Page-rank computation given a web-graph G. 
#
#This script reads the XML graph file created by mongograph.py 
#This method of computing page-rank exploits the sparsity of the web graph. 
#We store only the products of each matrix-vector multiplication thus reducing
#storage requirements and increasing speed. 
#

from pygraph.classes.digraph import digraph
from pygraph.readwrite.markup import read
import pymongo
import numpy
import pickle

def dec(x):
	return int(x) - 1

#Write to disk
def write_to_disk(p):
	i = 0
	page_ranks = {}
	while i < p.size:
		page_ranks[i+1] = p.item(i)
		i += 1

page_rank_file = open("page_ranks", "w")
pickle.dump( page_ranks, page_rank_file )
class PGraph:
  def __init__(self,n, espn_graph):
    self.size = n
    self.in_links = {}
    self.number_out_links = {}
    self.dangling_pages = {}

    for j in xrange(n):
      self.in_links[j] = []
      self.number_out_links[j] = 0

		#26671 documents in the corpus
    for docID in range(1,26672):
			index = docID - 1 

			udocID = unicode(docID)			#docIDs stored as unicode strings.
			self.in_links[index] = map( dec, espn_graph.incidents(udocID) )
			self.number_out_links[index] = len( espn_graph.neighbors(udocID) )

			if self.number_out_links[index] == 0:
				self.dangling_pages[index] = True

#	
#s = 0.85 is probability that user follows a link on the page.
#t = 0.15 that the user chooses an altogether different page when the page
#has no outgoing links.
#	
def step(g,p,s=0.85):
  n = g.size
  v = numpy.matrix(numpy.zeros((n,1)))
  inner_product = sum([p[j] for j in g.dangling_pages.keys()])

  for j in xrange(n):
    v[j] = s*sum([p[k]/g.number_out_links[k]

    for k in g.in_links[j]])+s*inner_product/n+(1-s)/n

  # We rescale the return vector, so it remains a
  # probability distribution even with floating point
  # roundoff.
  return v/numpy.sum(v)  

def pagerank(g,s=0.85,tolerance=0.00001):

  n = g.size
  p = numpy.matrix(numpy.ones((n,1)))/n
  iteration = 1
  change = 2
  while change > tolerance:
    print "Iteration: %s" % iteration
    new_p = step(g,p,s)
    change = numpy.sum(numpy.abs(p-new_p))
    print "Change in l1 norm: %s" % change
    p = new_p
    iteration += 1
  return p

#Reading graph from disk.
graph_xml_file = open("graph_xml")
graph_str = graph_xml_file.read()

#Converting to pygraph format.
espn_graph = read(graph_str)

#Converting to form suitable for pagerank computation.
pagerank_graph = PGraph(26671, espn_graph)
p = pagerank(pagerank_graph)


