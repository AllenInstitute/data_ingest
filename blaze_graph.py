from pymantic import sparql
import os
from validation import *
from controlled_vocab_validation import *
from triples_holder import *
from ingest_lib import *
from datetime import datetime
# import urllib
from urllib.request import urlopen
import json
import numpy as np
import math

INDEX_TO_FIRST = 0
INDEX_TO_SECOND = 1
SINGLE_RESULT = 1
LAST_ITEM = -1
SINGLE_INCREMENT = 1

class BlazeGraph(object):
	def __init__(self, settings):
		self.blaze_graph_server = settings['blaze_graph_server']
		self.ingest_prefixes = settings['ingest_prefixes']
		self.ingest_prefix = settings['ingest_prefix']
		self.subject_start = settings['subject_start']
		self.uid_service = settings['uid_service']
		self.server = self.connect_to_server()
		self.uid_keys = []

	def connect_to_server(self):
		return sparql.SPARQLServer(self.blaze_graph_server)

	def run_sparql_query(self, query):
		return self.server.query(query)

	def run_sparql_update(self, query):
		# print('running query', query)

		return self.server.update(query)

	def set_uploader_uid(self, ingest_uid, uploader_uid):
		uploader_triple = self.find_by_uid(uploader_uid)
		ingest_triple = self.find_by_uid(ingest_uid)

		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+=' DELETE { <' + ingest_triple.subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'uploader') + ' ?object .}'
		query+=' INSERT { <' + ingest_triple.subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'uploader') + ' ' + uploader_triple.get_object_for_insert() + ' . }'
		query+=' WHERE { <' + ingest_triple.subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'uploader') + ' ?object .}'

		# print('query', query)

		self.run_sparql_update(query)

	def get_subject_by_namespace_and_column(self, namespace, column_name, id_value):
		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= ' '
		query+= 'SELECT ?subject '
		query+= 'WHERE {' 
		query+= ' ?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'name_space') + ' "' + str(namespace) + '" . '
		query+= ' ?subject ' + IngestLib.add_prefix(self.ingest_prefix, column_name) + ' "' + str(id_value) + '" . '
		query+= ' }'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		subject = None

		if len(bindings) > SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		elif len(bindings) == SINGLE_RESULT:
			subject = bindings[INDEX_TO_FIRST]['subject']['value']

		if subject is None:
			print('query', query)

		return subject

	def get_file_record_uids(self, subject):
		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= ' '
		query+= 'SELECT ?object '
		query+= 'WHERE {' 
		query+= ' ?subject ' + 'rdf:has_part' + ' "' + subject + '" . '
		query+= ' ?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'uid') + ' ?object . '
		query+= ' }'

		# print('query', query)

		result = []

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']
		
		for binding in bindings:
			
			object_value = binding['object']['value']

			result.append(object_value)

		return result


	def get_name_space_data(self, name_space, uids):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?subject '
		query+= 'WHERE {' 
		query+= '?subject '+ str(IngestLib.add_prefix(self.ingest_prefix, 'name_space')) + ' "' + str(name_space) + '" .'

		if uids is not None:
			query+= '?subject di:uid ?uid . '
			query+='FILTER ( '

			index = 0
			for uid in uids:
				query+= '?uid = "' + uid + '" '
				if index + 1 != len(uids):
					query+=' || '

				index+=1
			query+=')'

		query+= '}'

		# print(query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']


		ingests = []

		for binding in bindings:
			
			subject = binding['subject']['value']

			ingests.append(self.find_all_by_subject(subject))

		return ingests

	def set_uploaded_at(self, ingest_uid):
		ingest_triple = self.find_by_uid(ingest_uid)

		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+=' DELETE { <' + ingest_triple.subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'uploaded_at') + ' ?object .}'
		query+=' INSERT { <' + ingest_triple.subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'uploaded_at') + ' "' + str(datetime.now()) + '" . }'
		query+=' WHERE { <' + ingest_triple.subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'uploaded_at') + ' ?object .}'

		# print('query', query)

		self.run_sparql_update(query)

	def set_ingest_state(self, ingest_uid, state):
		ingest_triple = self.find_by_uid(ingest_uid)

		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+=' DELETE { <' + ingest_triple.subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'status') + ' ?object .}'
		query+=' INSERT { <' + ingest_triple.subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'status') + ' "' + state + '" . }'
		query+=' WHERE { <' + ingest_triple.subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'status') + ' ?object .}'

		# print('query', query)

		self.run_sparql_update(query)


	def get_selected_data(self, triple_data, select_attributes):
		selected = []

		for triples in triple_data:
			data = {}

			first_time = True

			for triple in triples:
				if first_time:
					if 'subject' in select_attributes:
						data['subject'] = triple.subject

					if 'file_record_uids' in select_attributes:
						file_record_ids = self.get_file_record_uids(triple.subject)
						print('file_record_ids', file_record_ids)

						data['file_record_uids'] = [','.join(file_record_ids), len(file_record_ids)]

					first_time = False


				attribute = self.get_attribute(triple.predicate)
				if attribute in select_attributes:
					data[self.get_attribute(triple.predicate)] = triple.object

			selected.append(data)

		# print('selected', selected)

		return selected

	def get_attribute(self, uri):
		return uri.split('/')[LAST_ITEM]

	def get_tabular_data(self, name_space):
		name_space_data = self.get_name_space_data(name_space, None)

		tabular_data = []

		schema = {}
		rows = []

		for triples in name_space_data:
			first_time = True
			row = {}
			for triple in triples:
				if first_time:
					first_time = False

					schema['subject'] = True
					row['subject'] = triple.subject


				column_name = self.get_attribute(triple.predicate)

				schema[column_name] = True
				row[column_name] = triple.object

			rows.append(row)

		return schema, rows

	def get_data_for_page(self, schema, name_space, uids):
		select_attributes = {}

		for column_name in schema:
			select_attributes[column_name] = True

		name_space_data = self.get_name_space_data(name_space, uids)

		data = self.get_selected_data(name_space_data, select_attributes)

		return data

	def delete_all_data_by_ingest(self, ingest_uid):
		query = ''

		self.remove_ids_by_ingest_uid(ingest_uid)

		for prefix in self.ingest_prefixes:
			query+=prefix


		# query = 'DELETE WHERE { ?subject <' + str(self.subject_start) + 'ingest_id> "' + str(ingest_id) + '" . '
		query += 'DELETE WHERE { ?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'ingest_uid') + ' "' + str(ingest_uid) + '" . '
		query += '?subject ?predicate ?object . }'

		# print('query', query)

		self.run_sparql_update(query)

	#carefull with this one
	def delete_all_data(self):
		query = 'DELETE WHERE { ?s ?p ?o};'
		self.run_sparql_update(query)

	def add_or_update_attributes(self, unique_key, attributes, name_space, fast_query, ingest_triple=None):
		query = ''

		triple_holder = TriplesHolder(self.subject_start, IngestLib.add_prefix(self.ingest_prefix, unique_key))
		triple_holder.add_prefix(self.ingest_prefixes)

		for predicate, object_value in attributes.items():
			triple_holder.add_data(predicate, object_value)

		triple_holder.add_data('rdf:label', triple_holder.subject)
		triple_holder.add_data(IngestLib.add_prefix(self.ingest_prefix, 'uid'), unique_key)
		triple_holder.add_data(IngestLib.add_prefix(self.ingest_prefix, 'name_space'), name_space)

		if ingest_triple is not None:
			# print('ingest_triple.attributes', ingest_triple.attributes)
			triple_holder.add_data(IngestLib.add_prefix(self.ingest_prefix, 'ingest_uid'), ingest_triple.attributes['uid'])

		query = self.build_insert_query(triple_holder)
		
		if not fast_query:
			# print('query', query)
			self.run_sparql_update(query)

		return query

	def remove_ids_by_ingest_uid(self, ingest_uid):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= ' '
		query+= 'SELECT ?object '
		query+= 'WHERE {' 
		query+= ' ?s ' + IngestLib.add_prefix(self.ingest_prefix, 'ingest_uid') + ' "' + str(ingest_uid)+ '" . '
		query+= ' ?s ' + IngestLib.add_prefix(self.ingest_prefix, 'uid') + ' ?object . '
		query+= ' }'

		# print('query', query)

		result = None

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		uids = []

		for binding in bindings:
			uids.append(str(binding['object']['value']))
	

		self.delete_uids(uids)


	def get_id_block(self, identifier, number_of_keys):
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
		elif len(bindings) == SINGLE_RESULT:

			result = int(bindings[INDEX_TO_FIRST]['object']['value'])
			self.increment_key(identifier, result, number_of_keys)
		else:
			self.insert_default_id_key(identifier)
			result = 0
			self.increment_key(identifier, result, number_of_keys)

		return result


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

	def increment_key(self, identifier, result, increment):
		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+=' DELETE { ' + IngestLib.add_prefix(self.ingest_prefix, 'id_key_count') + ' ' + IngestLib.add_prefix(self.ingest_prefix, identifier) + ' ' + str(result) + ' . }'

		increment_result = int(result)
		increment_result+=increment

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

			result = identifier + '_' + str(self.increment_key(identifier, current_id, SINGLE_INCREMENT))

		return result

	def get_next_id_keys(self, identifier, number_of_keys):
		start = self.get_id_block(identifier, number_of_keys)
		keys = []

		for value in range(start, start + number_of_keys):
			keys.append(identifier + '_' + str(value))

		return keys

	def get_next_id_key(self, identifier):
		id_key = self.get_next_id(identifier)

		if id_key is None:
			self.insert_default_id_key(identifier)
			id_key = self.get_next_id(identifier)

		return id_key

	def get_uploader_uid_by_name(self, name):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?object '
		query+= 'WHERE {' 
		query+= '?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'name') + ' "' + str(name) + '" . '
		query+= '?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'name_space') + ' "uploaders" . '
		query+= '?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'uid') + ' ?object .'
		query+= '}'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		uid = None

		if len(bindings) != SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		else:
			uid = bindings[INDEX_TO_FIRST]['object']['value']

		return uid


	def get_ingest_uid_by_name(self, name):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?object '
		query+= 'WHERE {' 
		query+= '?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'name') + ' "' + str(name) + '" .'
		query+= '?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'name_space') + ' "ingests" . '
		query+= '?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'uid') + ' ?object .'
		query+= '}'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		uid = None

		if len(bindings) != SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		else:
			uid = bindings[INDEX_TO_FIRST]['object']['value']

		return uid


	def find_by_uid(self, uid):
		# print(IngestLib.add_prefix(self.ingest_prefix, 'uid') )

		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?subject '
		query+= 'WHERE {' 
		query+= '?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'uid') + ' "' + str(uid) + '"'
		query+= '}'

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		subject = None

		if len(bindings) != SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		else:
			subject = bindings[INDEX_TO_FIRST]['subject']['value']

		return Triple(subject, IngestLib.add_prefix(self.ingest_prefix, 'uid'), str(uid))

	def get_triple_holder(self, triples, subject, unique_key):

		triple_holder = TriplesHolder('', subject)
		for triple in triples:
			triple_holder.add_data(self.get_attribute(triple.predicate), triple.object)


		return triple_holder

	def get_by_uids(self, uids):
		schema = {}
		rows = []
		name_space_data = []

		for uid in uids:
			triple_holder = self.get_triples_from_uid(uid)

			triples = triple_holder.triples
			first_time = True
			row = {}
			for triple in triples:
				if first_time:
					first_time = False

					schema['subject'] = True
					row['subject'] = triple_holder.raw_subject

				column_name = self.get_attribute(triple.predicate)

				schema[column_name] = True
				row[column_name] = triple.object

			rows.append(row)

		return schema, rows

	def get_triples_from_uid(self, ingest_uid):

		triple = self.find_by_uid(ingest_uid)

		# triples = self.find_all_by_subject(IngestLib.add_prefix(self.subject_start + self.ingest_prefix, triple.subject))
		triples = self.find_all_by_subject(triple.subject)

		return self.get_triple_holder(triples, triple.subject, triple.object)

	def get_all_triples(self):
		results = []

		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?subject ?predicate ?object '
		query+= 'WHERE {' 
		query+= '?subject ?predicate ?object .'
		query+= '}'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']
		
		for binding in bindings:
			attributes = {}
			attributes['subject'] = binding['subject']['value']
			attributes['predicate'] = binding['predicate']['value']
			attributes['object'] = binding['object']['value']

			results.append(attributes)

		return results

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

	def find_uid_by_predicate_object(self, predicate, object_value, name_space):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?subject ?object '
		query+= 'WHERE {' 
		query+= '?subject ' + predicate + ' "' + object_value + '" . '
		query+= '?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'name_space') + ' "' + name_space + '" . '
		query+= '?subject ' + IngestLib.add_prefix(self.ingest_prefix, 'uid') + ' ?object'
		query+= '}'

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		subject = None

		if len(bindings) != SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		else:
			object_value = bindings[INDEX_TO_FIRST]['object']['value']
			subject = bindings[INDEX_TO_FIRST]['subject']['value']

		return Triple(subject, IngestLib.add_prefix(self.ingest_prefix, 'uid'), object_value)

	def find_uid_by_object_and_namespace(self, predicate, object_value, name_space):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?object '
		query+= 'WHERE {' 
		query+= '?s ' + IngestLib.add_prefix(self.ingest_prefix, 'name_space') + " '" + str(name_space) + "' . "
		query+= '?s ' + IngestLib.add_prefix(self.ingest_prefix, predicate) + " '" + str(object_value) + "' . "
		query+= '?s ' + IngestLib.add_prefix(self.ingest_prefix, 'uid') + ' ?object . '
		query+= '}'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		uid = None

		if len(bindings) != SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		else:
			uid = bindings[INDEX_TO_FIRST]['object']['value']

		return uid

	def find_uid_by_subject(self, subject):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		query+= 'SELECT ?object '
		query+= 'WHERE {' 
		query+= '<'+ subject + '> ' + IngestLib.add_prefix(self.ingest_prefix, 'uid') + ' ?object'
		query+= '}'

		# print('query', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']

		uid = None

		if len(bindings) != SINGLE_RESULT:
			raise Exception('Expected query to return a single result but it did not ' + str(query))
		else:
			uid = bindings[INDEX_TO_FIRST]['object']['value']

		return uid

	def insert_join(self, first_triple, second_triple):
		query = ''

		for prefix in self.ingest_prefixes:
			query+=prefix

		query+=' INSERT DATA { <' + first_triple.subject + '>'
		query+= ' rdf:has_part '
		query+= ' <' + second_triple.subject + '>'
		query+= ' .'
		query+=' }'


		# self.run_sparql_update(query)

		return query



	def insert_join_data(self, values, name_space_one, name_space_two):
		query = ''

		for value in values:

			# print('value', value)

			first_value = value[INDEX_TO_FIRST]
			second_value = value[INDEX_TO_SECOND]

			first_predicate = first_value[INDEX_TO_FIRST]
			first_object = first_value[INDEX_TO_SECOND]

			second_predicate = second_value[INDEX_TO_FIRST]
			second_object = second_value[INDEX_TO_SECOND]

			# print('name_space_one', name_space_one)
			# print('first_predicate', first_predicate)
			# print('first_object', first_object)

			# print('name_space_two', name_space_two)
			# print('second_predicate', second_predicate)
			# print('second_object', second_object)



			first_triple = self.find_uid_by_predicate_object(first_predicate, first_object, name_space_one)
			second_triple = self.find_uid_by_predicate_object(second_predicate, second_object, name_space_two)

			query = self.insert_join(first_triple, second_triple)

			# print('query', query)

			self.run_sparql_update(query)

		# self.run_sparql_update(query)

	def build_insert_query(self, triple_holder):
		query = ''

		for prefix in triple_holder.prefixes:
			query+=prefix

		query+=' INSERT DATA { ' + triple_holder.subject

		for i in range(triple_holder.number_of_triples()):
			predicate, object_value = triple_holder.get_predicate_object(i)

			query+= ' ' + str(predicate) 
			query+= ' ' + str(object_value)

			if triple_holder.is_last_index(i):
				query+=' .'
			else:
				query+=' ;'

		query+=' };'

		# print('query query', query)

		return query

	def get_id_keys(self, number_of_keys):

		key_url = self.uid_service + 'type=get_new_uids&number_of_uids=' + str(number_of_keys)

		# print('key_url', key_url)

		with urlopen(key_url) as url:
			data = json.loads(url.read())

			uids = []

			if 'state' not in data or 'message' not in data or 'uids' not in data:
				raise Exception('something went wrong getting new uids -- ' + str(key_url))

			elif data['state'] != 'Success':
				raise Exception('something went wrong getting new uids -- ' + str(data['message']) )

			else:
				uids = data['uids']


			if len(uids) != number_of_keys:
				raise Exception('Expected retrieved wrong number of uids ' + str(number_of_keys) + ' != ' + str(len(uids)))

			# print('uids', uids)

			return uids

	def delete_uids(self, uids=None):

		if uids == None:
			uids = self.uid_keys

		for uid_key in uids:
			key_url = self.uid_service + 'type=remove_uid&uid=' + str(uid_key)

			with urlopen(key_url) as url:
				data = json.loads(url.read())

				if 'state' not in data:
					raise Exception('something went wrong getting new uids -- ' + str(key_url))

				elif data['state'] != 'Success':
					raise Exception('something went wrong getting new uids -- ' + str(data['message']) )

	def finalize_uids(self):
		for uid_key in self.uid_keys:
			key_url = self.uid_service + 'type=finalize_uid&uid=' + str(uid_key)

			with urlopen(key_url) as url:
				data = json.loads(url.read())

				if 'state' not in data:
					raise Exception('something went wrong getting new uids -- ' + str(key_url))

				elif data['state'] != 'Success':
					raise Exception('something went wrong getting new uids -- ' + str(data['message']) )

	def get_tarql_insert(self, triple_holder, select_clause, shape_clause, where_clause):
		publication_tarql = ''

		for prefix in self.ingest_prefixes:
			publication_tarql+=prefix

		# print('publication_tarql', publication_tarql)

		publication_tarql+= ' INSERT {' + shape_clause + '} '
		publication_tarql+= ' WHERE {'
		publication_tarql+= where_clause
		publication_tarql+= ' VALUES (' + select_clause + ') {( %s )}'
		publication_tarql+='} ; '

		return publication_tarql

	def get_extra_fields(self):
		extra_fields = ['uid', 'name_space', 'data_type', 'ingest_uid']

		extra_shape_clause = []
		extra_select_clause = []
		
		for extra_field in extra_fields:
			extra_shape_clause.append(IngestLib.add_normal_field(extra_field, self.ingest_prefix))
			extra_select_clause.append('?' + extra_field)

		return extra_shape_clause, extra_select_clause

	def insert_csv_data(self, values, name_space, ingest_triple, file_ingest):
		schema = file_ingest.schema
		current_uid_keys = self.get_id_keys(len(values))
		extra_shape_clause, extra_select_clause = self.get_extra_fields()

		query = ''

		index = 0
		for row in values:

			shape_clause = file_ingest.shape_clause.copy()
			where_clause = file_ingest.where_clause.copy()
			select_clause = file_ingest.select_clause.copy()

			shape_clause+=extra_shape_clause
			select_clause+=extra_select_clause

			# print('file_ingest.joins', file_ingest.joins)

			# print('\nbefore shape_clause', shape_clause)

			for join in list(file_ingest.joins.keys()):
				column_name = file_ingest.get_join_table_column(join)

				if row[column_name] is not None:
					extra_joins = file_ingest.extra_joins


					shape_clause.append(extra_joins[join]['extra_shape_clause'])
					where_clause.append(extra_joins[join]['extra_where_clause'])

			tarql_insert = self.get_tarql_insert(ingest_triple, ' '.join(select_clause),  ' '.join(shape_clause), ' '.join(where_clause))

			unique_key = current_uid_keys[index]
			insert_values = []
			for column_name in schema:

				if column_name in row:
					value = None

					if row[column_name] is not None and isinstance(row[column_name], str) and "'" in row[column_name]:
						value = "'" + str(row[column_name].replace("'", "\\'")) + "'"
					else:
						value = "'" + str(row[column_name]) + "'"

					insert_values.append(value)
				else:
					raise Exception('Could not find column_name ' + str(column_name) + ' in csv file ' + str(name_space))

			insert_values.append("'" + str(unique_key) + "'")
			insert_values.append("'" + str(name_space) + "'")
			insert_values.append("'ingest_data'")
			insert_values.append("'" + str(ingest_triple.attributes['uid']) + "'")

			query_string = tarql_insert % ' '.join(insert_values)
			# print('unique_key', unique_key)

			# query = query_string.replace("SUB_UID", "'" + unique_key + "'")
			query_string = query_string.replace("SUB_UID", "'" + unique_key + "'")

			for join in list(file_ingest.joins.keys()):
				reference_table = file_ingest.get_join_reference_table(join)
				reference_table_column = file_ingest.get_join_reference_table_column(join)
				column_uid = file_ingest.get_join_column_uid(join)
				column_name = file_ingest.get_join_table_column(join)

				if column_name not in row:
					raise Exception('Could not find column_name ' + str(column_name) + ' in csv file ' + str(name_space))

				# if row[column_name] is not None and row[column_name] is not np.nan:
				if row[column_name] is None or row[column_name] is np.nan or (isinstance(row[column_name], (int, float)) and math.isnan(row[column_name])):
					pass
				else:

					subject = self.get_subject_by_namespace_and_column(reference_table, reference_table_column, row[column_name])

					if subject is None:
						raise Exception('Expecting an id of ' + str(row[column_name]) + ' to exist in ' + str(reference_table) + ' with predicate ' + str(reference_table_column) + ' but it did not')

					# print('subject', subject)
					query_string = query_string.replace(column_uid, subject)


			query+= query_string

			# print('\n\nquery_string', query_string)
			# self.run_sparql_update(query_string)


			index+=1

		# print('query insert', query)
		self.run_sparql_update(query)

	def get_export_data(self, file_ingest):
		query = ''
		for prefix in self.ingest_prefixes:
			query+=prefix

		select_clause = file_ingest.select_clause
		shape_clause = file_ingest.shape_clause
		optional_shape = file_ingest.optional_shape

		# print('before', shape_clause)

		for index in range(len(select_clause)):
			if file_ingest.matches_join(select_clause[index]):
				resolved = IngestLib.remove_prefix(file_ingest.joins[select_clause[index][1:]]['predicate'])

				# print('replace', select_clause[index][1:])
				# print('the', shape_clause[index + 1])
				shape_clause[index + 1] = shape_clause[index + 1].replace(select_clause[index][1:], resolved)
				select_clause[index] = '?' + resolved

		# print('after', shape_clause)

		query+= ' SELECT DISTINCT ' + ' '.join(select_clause)
		query+= ' WHERE {'

		if file_ingest.get_join_length() == 0:
			query+= ' '.join(shape_clause)
		else:
			optional_clause = optional_shape['optional_clause']
			optional_shape_clause = optional_shape['optional_shape_clause']
			query+= ' ?' + file_ingest.subject + ' a ' + IngestLib.add_prefix(self.ingest_prefix, file_ingest.subject.capitalize()) + ' ; '
			query+= ' '.join(optional_shape_clause)
			query+= ' OPTIONAL { ?' + file_ingest.subject + ' ' + ' '.join(optional_clause) + ' }'

			# print('optional_clause', optional_clause)
		query+= '}'

		# if file_ingest.subject == 'license':
		# print('\n\nquery', query)

		query_results = self.run_sparql_query(query)

		bindings = query_results['results']['bindings']
		# print('bindings', bindings)
		# print('************')
		results = []
		header_row = []
		first_time = True

		# if file_ingest.subject == 'license':
		# 	print('file_ingest.schema', file_ingest.schema)
		# 	print('select_clause', select_clause)
		

		for binding in list(bindings):
			row = []

			# if file_ingest.subject == 'license':
			# 	print('binding.keys()', binding.keys())

			# for key in list(binding.keys()):
			returned_key = list(binding.keys())
			for select_key in select_clause:
				key = IngestLib.remove_first_char(select_key)

				if key in returned_key:
					stored_value = ''
					value = ''

					if file_ingest.is_resolved(key):
						# print('find by uid subject', binding[key]['value'])
						value = self.find_uid_by_subject(binding[key]['value'])
					elif file_ingest.replace_primary_key and file_ingest.primary_key == key:
						# print('primary_key', key)
						# value = binding[key]['value']
						# print('file_name', file_ingest.subject, binding[key]['value'].replace("'", "\\'"))
						# .replace("'", "\\'")) + "'"
						value = self.find_uid_by_object_and_namespace(key, binding[key]['value'].replace("'", "\\'"), file_ingest.subject)
						# print('value', value)
					else:
						value = binding[key]['value']

					if value is not None and value != 'None':
						stored_value = value

					if ',' in stored_value:
						stored_value = '"' + stored_value + '"'

					row.append(stored_value)


				else:
					row.append('')

				if first_time:
					if key in file_ingest.resolved_mapping:
						header_row.append(file_ingest.resolved_mapping[key])
					else:
						header_row.append(key)

					

			if first_time:
				first_time = False

			results.append(row)

		return header_row, results


	def insert_data(self, values, name_space, ingest_triple):

		# uid_keys = self.get_next_id_keys(name_space, len(values))
		# uid_keys = self.get_id_keys(len(values))
		# self.uid_keys+= self.get_id_keys(len(values))
		current_uid_keys = self.get_id_keys(len(values))
		self.uid_keys+= current_uid_keys

		fast_query = True

		# if name_space == 'project':
		# 	fast_query = False

		index = 0
		query = ''
		for attributes in values:
			# unique_key = self.get_next_id_key(name_space)
			unique_key =current_uid_keys[index]
			attributes[IngestLib.add_prefix(self.ingest_prefix,'data_type')] = 'ingest_data'
			
			query+= self.add_or_update_attributes(unique_key, attributes, name_space, fast_query, ingest_triple)
			index+=1

		if fast_query:
			self.run_sparql_update(query)

	def insert_controlled_vocab(self, json_file, json_file_template, json_file_extra_fields, json_file_extra_global_fields):
		print('adding controlled vocabulary...')

		Validation.validate_controlled_vacab_json(json_file)

		json_data = IngestLib.get_json_data_from_file(json_file)
		json_data_template = IngestLib.get_json_data_from_file(json_file_template)
		json_data_extra_fields = IngestLib.get_json_data_from_file(json_file_extra_fields)
		json_data_extra_global_fields = IngestLib.get_json_data_from_file(json_file_extra_global_fields)

		ControlledVocabValidation.validate_extra_fields(json_data, json_file, json_data_extra_fields, json_file_extra_fields)

		name_spaces = json_data['name_spaces']

		full_qeury = ''

		for name_space_record in name_spaces:
			name_space = name_space_record['name_space']
			values = name_space_record['values']

			ControlledVocabValidation.validate_name_space(name_space, json_file, values, json_data_template, json_file_template)

			# uid_keys = self.get_next_id_keys(name_space, len(values))

			current_uid_keys = self.get_id_keys(len(values))
			self.uid_keys+= current_uid_keys

			index = 0
			for attributes in values:
				# unique_key = self.get_next_id_key(name_space)
				unique_key = current_uid_keys[index]

				if(IngestLib.add_prefix(self.ingest_prefix,'created_at') in attributes):
					attributes[IngestLib.add_prefix(self.ingest_prefix,'created_at')] = datetime.now()

				#add extra fields to attributes
				if name_space in json_data_extra_fields:
					for key, value in json_data_extra_fields[name_space].items():
						attributes[key] = value

				for key, value in json_data_extra_global_fields.items():
					attributes[key] = value

				full_qeury+=self.add_or_update_attributes(unique_key, attributes, name_space, True)

				index+=1

		self.run_sparql_update(full_qeury)

		# self.add_controlled_vocab_joins(json_file, json_data)

def main():
	print("testing blaze graph")

	blazegraph = BlazeGraph()

	print('finished...')

if __name__ == "__main__":
	main()