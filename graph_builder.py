import os
import sys
from owlready2 import *
import rdflib
from aiosdk.table_rdf import TableRdf
import pandas as pd

INDEX_TO_FIRST_JOIN = 0
INDEX_TO_SECOND_JOIN = 1

class GraphBuilder(object):

	def __init__(self, csv_files, join_csv_files, graph_folder_path):
		self.csv_files = csv_files
		self.join_csv_files = join_csv_files
		self.graph_folder_path = graph_folder_path

		self.table_reader = TableRdf()

		self.build_graph()

	def add_csv_files_to_graph(self):	
		for csv_file in self.csv_files:
			self.csv_handler(csv_file)

	def add_joins_to_graph(self):
		for join_csv_file in self.join_csv_files:
			self.csv_join_handler(join_csv_file)

	def build_graph(self):
		print('building graph')

		self.add_csv_files_to_graph()
		self.add_joins_to_graph()

	def get_filename_without_extension(self, filename):
		return os.path.splitext(os.path.basename(filename))[0]

	def get_join_keyword(self, csv_file):

		if '_' not in csv_file:
			raise Exception('Expected join csv file ' + str(csv_file) + ' to contain a underscore character but it did not')

		join_parts = self.get_filename_without_extension(csv_file).split('_')


		return join_parts[INDEX_TO_FIRST_JOIN], join_parts[INDEX_TO_SECOND_JOIN]

	def get_keyword(self, csv_file):
		return '_' + self.get_filename_without_extension(csv_file)

	def write_out_graph(self, csv_file, keyword):
		current_table_reader = TableRdf()

		file_name = self.get_filename_without_extension(csv_file)

		path = os.path.join(self.graph_folder_path, file_name + str('.ttl'))

		df = current_table_reader.read_csv(csv_file)

		current_table_reader.dataframe_to_rdf(df, keyword)

		with open(path, 'w') as f:
			current_table_reader.write_rdf(path, format='ttl')

	def csv_handler(self, csv_file):
		print('processing', csv_file)

		keyword = self.get_keyword(csv_file)

		#add to main graph
		df = self.table_reader.read_csv(csv_file)
		self.table_reader.dataframe_to_rdf(df, keyword)

		self.write_out_graph(csv_file, keyword)

		#Access the in-memory RDF graph directly
		graph = self.table_reader.graph

		# print('*******************8')
		# print(graph.serialize(format='ttl').decode('utf8'))

	def csv_join_handler(self, join_csv_file):
		join_one, join_two = self.get_join_keyword(join_csv_file)

		# print('csv_file', csv_file)
		# print('join_one', join_one)
		# print('join_two', join_two)

		df = self.table_reader.read_csv(join_csv_file)

		print('csv_file_join', join_csv_file)

		self.table_reader.dataframe_to_rdf_joins(df, join_one, join_two, join_csv_file)

		# self.table_reader


