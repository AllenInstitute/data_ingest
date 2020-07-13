from triple import *

class TriplesHolder(object):
	def __init__(self, subject_start, subject):
		self.subject = '<' + str(subject_start ) + str(subject) + '>'

		self.triples = []

		# self.predicates = []
		# self.objects = []
		self.prefixes = []

	def add_data(self, predicate, object_value):
		self.triples.append(Triple(self.subject, str(predicate), '"' + str(object_value) + '"'))

		# self.predicates.append(str(predicate))
		# self.objects.append('"' + str(object_value) + '"')

	def add_prefix(self, prefix):
		self.prefixes.append(prefix)

	def number_of_triples(self):
		return len(self.triples)

	def get_predicate_object(self, index):
		if index < 0 or index >= self.number_of_triples():
			raise Exception('index ' + str(index) + ' out of range. Index should be between 0 and ' + str(self.number_of_triples()))

		triple = self.triples[index]

		return triple.predicate, triple.object

	def is_last_index(self, i):
		return ((i + 1) == self.number_of_triples())
