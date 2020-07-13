from pymantic import sparql
import os
from validation import *
from controlled_vocab_validation import *
from triples_holder import *

INDEX_TO_FIRST = 0
INDEX_TO_SECOND = 1
SINGLE_RESULT = 1
LAST_ITEM = -1

class BlazeGraph(object):
	def __init__(self, settings):
		self.blaze_graph_server = settings['blaze_graph_server']
		self.ingest_prefixes = settings['ingest_prefixes']
		self.ingest_prefix = settings['ingest_prefix']
		self.subject_start = settings['subject_start']
		self.server = self.connect_to_server()

	def connect_to_server(self):
		return sparql.SPARQLServer(self.blaze_graph_server)

	def run_sparql_query(self, query):
		return self.server.query(query)

	def run_sparql_update(self, query):
		# print('running query', query)

		return self.server.update(query)

	def get_table_data(self, table_name):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?subject '
		query+= 'WHERE {' 
		query+= '?subject '+ str(IngestLib.add_prefix(self.ingest_prefix, 'table_name')) + ' "' + str(table_name) + '" ;'
		query+= '}'

		# print(query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']


		ingests = []

		for binding in bindings:
			
			subject = binding['subject']['value']

			ingests.append(self.find_all_by_subject(subject))

		return ingests

	def get_selected_data(self, triple_data, select_attributes):
		selected = []

		for triples in triple_data:
			data = {}

			for triple in triples:
				# print('triple', triple)

				attribute = self.get_attribute(triple.predicate)
				if attribute in select_attributes:
					data[self.get_attribute(triple.predicate)] = triple.object

			selected.append(data)

		# print('selected', selected)

		return selected

	def get_attribute(self, uri):
		return uri.split('/')[LAST_ITEM]

	def get_data_for_page(self, schema, table_name):
		select_attributes = {}
		for column_name in schema:
			select_attributes[column_name] = True

		table_data = self.get_table_data(table_name)

		data = self.get_selected_data(table_data, select_attributes)

		return data

	#carefull with this one
	def delete_all_data(self):
		query = 'DELETE WHERE { ?s ?p ?o};'
		self.run_sparql_update(query)

	def add_or_update_attributes(self, unique_key, attributes, table_name):
		triple_holder = TriplesHolder(self.subject_start, IngestLib.add_prefix(self.ingest_prefix, unique_key))
		triple_holder.add_prefix(self.ingest_prefixes)

		for predicate, object_value in attributes.items():
			triple_holder.add_data(predicate, object_value)

		triple_holder.add_data('rdf:label', triple_holder.subject)
		triple_holder.add_data(IngestLib.add_prefix(self.ingest_prefix, 'uid'), unique_key)
		triple_holder.add_data(IngestLib.add_prefix(self.ingest_prefix, 'table_name'), table_name)

		query = self.build_insert_query(triple_holder)

		return query

	def insert_default_id_key(self, identifier):
		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+=' INSERT DATA { ' + IngestLib.add_prefix(self.ingest_prefix, 'id_key_count')
		query+= ' ' + IngestLib.add_prefix(self.ingest_prefix, identifier)
		query+= ' 0 .'
		query+=' }'

		# print('query insert', query)

		self.run_sparql_update(query)

	def increment_key(self, identifier, result):
		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+=' DELETE { ' + IngestLib.add_prefix(self.ingest_prefix, 'id_key_count') + ' ' + IngestLib.add_prefix(self.ingest_prefix, identifier) + ' ' + str(result) + ' . }'

		increment_result = int(result)
		increment_result+=1

		query+=' INSERT { ' + IngestLib.add_prefix(self.ingest_prefix, 'id_key_count') + ' ' + IngestLib.add_prefix(self.ingest_prefix, identifier) + ' ' + str(increment_result) + ' .}'
		query+=' WHERE { ' + IngestLib.add_prefix(self.ingest_prefix, 'id_key_count') + ' ' + IngestLib.add_prefix(self.ingest_prefix, identifier) + ' ' + str(result) + ' .}'

		# print(query)
		self.run_sparql_update(query)

		return increment_result

	def get_next_id(self, identifier):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= ' '
		query+= 'SELECT ?object '
		query+= 'WHERE {' 
		query+= IngestLib.add_prefix(self.ingest_prefix, 'id_key_count') + ' ' + IngestLib.add_prefix(self.ingest_prefix, identifier) + ' ?object .'
		query+= ' }'

		# print('query', query)

		result = None

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']
		
		if len(bindings) > SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		elif  len(bindings) == SINGLE_RESULT:
			current_id = bindings[INDEX_TO_FIRST]['object']['value']

			result = identifier + '_' + str(self.increment_key(identifier, current_id))

		return result


	def get_next_id_key(self, identifier):
		id_key = self.get_next_id(identifier)

		if id_key is None:
			self.insert_default_id_key(identifier)
			id_key = self.get_next_id(identifier)

		return id_key



	def find_distinct_objects_by_predicate(self, predicate_prefix, predicate):
		results = []

		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT distinct ?object '
		query+= 'WHERE {' 
		query+= '?subject ' + IngestLib.add_prefix(predicate_prefix, predicate) + ' ?object'
		query+= '}'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']
		
		for binding in bindings:
			results.append(binding['object']['value'])

		return results

	def find_all_by_predicate(self, predicate_prefix, predicate):
		triples = []

		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?subject ?object '
		query+= 'WHERE {' 
		query+= '?subject ' + IngestLib.add_prefix(predicate_prefix, predicate) + ' ?object'
		query+= '}'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		for binding in bindings:
			triples.append(Triple(binding['subject']['value'], predicate, binding['object']['value']))

		return triples

	def find_all_by_subject(self, subject):
		triples = []

		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?predicate ?object '
		query+= 'WHERE {' 
		query+= '<' + subject + '> ?predicate ?object'
		query+= '}'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']
		for binding in bindings:
			# print('binding', binding)
			triples.append(Triple(subject, binding['predicate']['value'], binding['object']['value']))

			# print('triple', Triple(subject, binding['predicate']['value'], binding['object']['value']))
		return triples


	def find_by_label(self, label):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?subject '
		query+= 'WHERE {' 
		query+= '?subject rdf:label "' + str(label) + '"'
		query+= '}'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		subject = None

		if len(bindings) != SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		else:
			subject = bindings[INDEX_TO_FIRST]['subject']['value']

		return Triple(subject, 'rdf:label', subject)

	def find_label_by_predicate_object(self, predicate, object_value):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?object '
		query+= 'WHERE {' 
		query+= '?subject ' + predicate + ' "' + object_value + '" . '
		query+= '?subject rdf:label ?object'
		query+= '}'

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		subject = None

		if len(bindings) != SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		else:
			object_value = bindings[INDEX_TO_FIRST]['object']['value']

		return Triple(object_value, 'rdf:label', object_value)

	def insert_join(self, first_triple, second_triple):
		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+=' INSERT DATA { ' + first_triple.subject
		query+= ' rdf:has_part '
		query+= ' ' + second_triple.subject
		query+= ' .'
		query+=' };'


		# self.run_sparql_update(query)

		return query



	def add_controlled_vocab_joins(self, json_file, json_data):
		print('adding joins...')
		joins = json_data['joins']

		query = ''

		for join in joins:
			ControlledVocabValidation.validate_join(json_file, join)
			# file_name = join['file_name']
			table_one = join['table_one']
			table_two = join['table_two']
			values = join['values']



			for value in values:

				first_value = value[INDEX_TO_FIRST]
				second_value = value[INDEX_TO_SECOND]

				first_predicate = first_value[INDEX_TO_FIRST]
				first_object = first_value[INDEX_TO_SECOND]

				second_predicate = second_value[INDEX_TO_FIRST]
				second_object = second_value[INDEX_TO_SECOND]

				first_triple = self.find_label_by_predicate_object(first_predicate, first_object)
				second_triple = self.find_label_by_predicate_object(second_predicate, second_object)

				query+= self.insert_join(first_triple, second_triple)

		self.run_sparql_update(query)

	def build_insert_query(self, triple_holder):
		query = ''

		for prefix in triple_holder.prefixes:
			query+=prefix

		query+=' INSERT DATA { ' + triple_holder.subject

		for i in range(triple_holder.number_of_triples()):
			predicate, object_value = triple_holder.get_predicate_object(i)
			query+= ' ' + predicate 
			query+= ' ' + object_value

			if triple_holder.is_last_index(i):
				query+=' .'
			else:
				query+=' ;'

		query+=' };'

		return query

	def insert_controlled_vocab(self, json_file, json_file_template, json_file_extra_fields, json_file_extra_global_fields):
		print('adding controlled vocabulary...')

		Validation.validate_controlled_vacab_json(json_file)

		json_data = IngestLib.get_json_data_from_file(json_file)
		json_data_template = IngestLib.get_json_data_from_file(json_file_template)
		json_data_extra_fields = IngestLib.get_json_data_from_file(json_file_extra_fields)
		json_data_extra_global_fields = IngestLib.get_json_data_from_file(json_file_extra_global_fields)

		ControlledVocabValidation.validate_extra_fields(json_data, json_file, json_data_extra_fields, json_file_extra_fields)

		tables = json_data['tables']

		full_qeury = ''

		for table in tables:


			table_name = table['table_name']
			values = table['values']

			ControlledVocabValidation.validate_table(table_name, json_file, values, json_data_template, json_file_template)

			for attributes in values:
				unique_key = self.get_next_id_key(table_name)

				#add extra fields to attributes
				if table_name in json_data_extra_fields:
					for key, value in json_data_extra_fields[table_name].items():
						attributes[key] = value

				for key, value in json_data_extra_global_fields.items():
					attributes[key] = value

				full_qeury+=self.add_or_update_attributes(unique_key, attributes, table_name)

		self.run_sparql_update(full_qeury)

		self.add_controlled_vocab_joins(json_file, json_data)

def main():
	print("testing blaze graph")

	blazegraph = BlazeGraph()

	print('finished...')

if __name__ == "__main__":
	main()