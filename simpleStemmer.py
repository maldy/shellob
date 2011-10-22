#!/usr/bin/env python

class SimpleStemmer:

	def __init__(self):
		self.grammar_suffix = ['ing' ,'ly', 'ed', 'ion', 'ions', 'ies', 'ive', 'es', 's', 'ment']

	def stem(self, word):
		word = word.lower()
		for suffix in self.grammar_suffix:
			if word.endswith(suffix) and len(word) - len(suffix) > 2:
				word = word[:-len(suffix)]
				break
		return word

