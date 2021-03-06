from validation import *
from ingest_lib import *
from csv_ingest import *
import zipfile
import shutil
import glob
import os
from datetime import datetime
import pandas as pd
import math
import numpy as np
import traceback

SETTINGS_FOLDER = './settings'
SETTINGS_FILE = 'settings.json'
MIN_ROW_LENGTH = 1

class Ingest(object):
	def __init__(self, uploader_uid, ingest_uid, zip_file):
		try:
			settings_file = os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
			self.zip_file = zip_file

			self.settings = IngestLib.get_json_data_from_file(settings_file)
			self.ingest_prefix = self.settings['ingest_prefix']

			self.blaze_graph = BlazeGraph(self.settings)
			self.validation = Validation(self.settings)
			self.validation.run_default_validation(uploader_uid, ingest_uid, zip_file)
			self.ingest_triple = self.validation.ingest_triple

			self.template = self.ingest_triple.get_attribute('template')
			self.storage_directory = self.ingest_triple.get_attribute('storage_directory')

			self.blaze_graph.set_uploader_uid(ingest_uid, uploader_uid)
			self.blaze_graph.set_ingest_state(ingest_uid, 'uploading')
			self.blaze_graph.set_uploaded_at(ingest_uid)

			self.extract_zip()
			self.store_data()

			# raise Exception('Test')

			self.blaze_graph.set_ingest_state(ingest_uid, 'uploaded')
			self.blaze_graph.finalize_uids()

		except Exception as e:
			print('Error!',e)
			traceback.print_exc()
			self.blaze_graph.set_ingest_state(ingest_uid, 'upload failed')
			self.blaze_graph.delete_uids()


	def extract_zip(self):
		print('extracting ' + str(self.zip_file) + ' to ' + str(self.storage_directory))

		with zipfile.ZipFile(self.zip_file,"r") as zip_ref:

			#this extract the zip into a new folder in the storage_directory
			zip_ref.extractall(self.storage_directory)

			#the rest of this is the bring the files directory into the storage_directory and to clean up everything
			folder_name = os.path.basename(os.path.splitext(self.storage_directory)[0])

			unzipped_file_path = os.path.join(self.storage_directory, folder_name)
			if os.path.isdir(unzipped_file_path):

				for file in glob.glob(unzipped_file_path + '/*'):
					correct_location = os.path.join(self.storage_directory, os.path.basename(file))
					
					shutil.copyfile(file,  correct_location)
					os.remove(file)

				os.rmdir(unzipped_file_path)

	def store_data(self):
		errors = self.validation.validate_files(self.storage_directory, self.template)

		if len(errors) > 0:
			raise Exception('Error', errors)

		#remove all results from a previous run...
		self.blaze_graph.delete_all_data_by_ingest(self.ingest_triple.get_uid())

		self.insert_file_records(self.storage_directory, self.template)
		self.insert_data(self.storage_directory, self.template)
		# self.insert_joins(self.storage_directory, self.template)

		#create a file record for each file

	def insert_file_records(self, storage_directory, template):
		template_data = IngestLib.get_json_data_from_file(template)

		files = template_data['files']

		values = []

		name_space = 'file_records'

		for file in files:
			file_name = file['file_name']

			file_path = os.path.join(storage_directory, file_name)

			instance = {}
			instance[IngestLib.add_prefix(self.ingest_prefix,'required')] = file['required']
			instance[IngestLib.add_prefix(self.ingest_prefix,'file_type')] = file['file_type']
			instance[IngestLib.add_prefix(self.ingest_prefix,'data_type')] = file['data_type']
			instance[IngestLib.add_prefix(self.ingest_prefix,'file_name')] = file['file_name']
			instance[IngestLib.add_prefix(self.ingest_prefix,'storage_directory')] = storage_directory
			instance[IngestLib.add_prefix(self.ingest_prefix,'file_path')] = file_path
			instance[IngestLib.add_prefix(self.ingest_prefix,'exists')] = os.path.exists(file_path)
			instance[IngestLib.add_prefix(self.ingest_prefix,'created_at')] = datetime.now()
			instance[IngestLib.add_prefix(self.ingest_prefix,'size_in_bytes')] = os.path.getsize(file_path)
			instance[IngestLib.add_prefix(self.ingest_prefix,'md5sum')] = IngestLib.get_md5(file_path)

			instance['rdf:has_part'] = self.ingest_triple.raw_subject

			values.append(instance)


		self.blaze_graph.insert_data(values, name_space, self.ingest_triple)

	def insert_data(self, storage_directory, template):
		template_data = IngestLib.get_json_data_from_file(template)

		files = template_data['files']

		file_ingests = []


		for file in files:
			file_type = file['file_type']

			if file_type == 'csv':
				file_ingests.append(CsvIngest(file['subject'], file['required'], file['file_type'], file['data_type'], file['file_name'], file['select_clause'], file['shape_clause'], file['where_clause'], file['schema'], file['joins'], file['extra_joins'], file['primary_key'], file['replace_primary_key'], file['optional_shape']))
			else:
				raise Exception('file_type', str(file_type), ' not supported')


		prev_ingests = {}

		
		not_finished = True

		while not_finished:
			temp_file_ingests = []
			current_len = len(file_ingests)

			while file_ingests:
				file_ingest = file_ingests.pop()

				if file_ingest.meets_dependencies(prev_ingests):
					print('inserting', file_ingest.file_name)

					file_path = os.path.join(storage_directory, file_ingest.file_name)

					values, name_space = self.get_csv_data(file_path)
					
					# print('name_space', name_space)
					# self.blaze_graph.insert_data(values, name_space, self.ingest_triple)
					self.blaze_graph.insert_csv_data(values, name_space, self.ingest_triple, file_ingest)

					prev_ingests[file_ingest.subject] =  True
				else:
					temp_file_ingests.append(file_ingest)

			for file_ingest in temp_file_ingests:
				file_ingests.append(file_ingest)

			if current_len == len(file_ingests):
				file_names = []
				missing_dependencies = {}
				for file_ingest in file_ingests:
					file_names.append(file_ingest.file_name)

					missing_dependencies = file_ingest.get_missing_dependencies(missing_dependencies, prev_ingests)

				raise Exception('Error, could not resolve dependencies for ' + ' '.join(file_names) + ' --- missing dependencies: ' + str(list(missing_dependencies.keys())))
			elif len(file_ingests) == 0:
				not_finished = False




		# for file in files:
		# 	required = file['required']
		# 	file_type = file['file_type']
		# 	data_type = file['data_type']
		# 	file_name = file['file_name']

		# 	file_path = os.path.join(storage_directory, file_name)

		# 	if data_type == 'data':
		# 		if file_type == 'csv':
		# 			print('inserting', file_name)
		# 			select_clause = file['select_clause']
		# 			shape_clause = file['shape_clause']
		# 			where_clause = file['where_clause']
		# 			schema = file['schema']

		# 			values, name_space = self.get_csv_data(file_path)
		# 			# self.blaze_graph.insert_data(values, name_space, self.ingest_triple)
		# 			self.blaze_graph.insert_csv_data(values, name_space, self.ingest_triple, select_clause, shape_clause, where_clause, schema)

		# 		else:
		# 			raise Exception('file_type', str(file_type), ' not supported')

	def insert_joins(self, storage_directory, template):
		template_data = IngestLib.get_json_data_from_file(template)

		files = template_data['files']

		for file in files:
			required = file['required']
			file_type = file['file_type']
			data_type = file['data_type']
			file_name = file['file_name']

			file_path = os.path.join(storage_directory, file_name)

			if data_type == 'join':
				join_name_space_one = file['join_name_space_one']
				join_name_space_two = file['join_name_space_two']

				if file_type == 'csv':
					print('inserting', file_name)

					values = self.add_csv_joins_data(file_path)

					self.blaze_graph.insert_join_data(values, join_name_space_one, join_name_space_two)

				else:
					raise Exception('file_type', str(file_type), ' not supported')


	def get_csv_data(self, csv_file):
		values = []
		name_space = IngestLib.get_filename_without_extension(os.path.basename(csv_file))

		df = pd.read_csv(csv_file, header = 0, encoding='ISO-8859-1')
		# df = pd.read_csv(csv_file, header = 0, encoding='ascii')
		# df = pd.read_csv(csv_file, header = 0, encoding='utf-8')
		# df = pd.read_csv(csv_file, header = 0, encoding='cp1252')

		# encoding = "cp1252"
		# encoding = "ISO-8859-1"
		schema = df.columns

		# print('schema', schema)

		# with open(csv_file, 'r', encoding='ascii', errors='surrogateescape') as input_file:
		for index, row in df.iterrows():
			instance = {}
			for column_name in schema:
				# column_name = column_name.strip()
				# print(column_name, '-->', row[column_name])

				# if (isinstance(row[column_name], float) and math.isnan(row[column_name])) or math.isnan(row[column_name]):
				if row[column_name] is None or row[column_name] is np.nan or (isinstance(row[column_name], (int, float)) and math.isnan(row[column_name])):
					# instance[IngestLib.add_prefix(self.ingest_prefix, column_name.strip())] = None
					instance[column_name.strip()] = None
				elif isinstance(row[column_name], (float, int)):
					# instance[IngestLib.add_prefix(self.ingest_prefix, column_name)] = row[column_name]
					instance[column_name] = row[column_name]
				else:
					# instance[IngestLib.add_prefix(self.ingest_prefix, column_name)] = row[column_name].encode('utf-8', 'surrogateescape').decode('utf-8', 'replace')
					# instance[IngestLib.add_prefix(self.ingest_prefix, column_name)] = row[column_name]
					instance[column_name] = row[column_name].encode('utf-8', 'surrogateescape').decode('utf-8', 'replace')


			values.append(instance)
				

		# 		print(column_name, '-->', row[column_name])

		# 	print(index, row)
		# 	print()

		# with open(csv_file, 'r', encoding='utf-8', errors='surrogateescape') as input_file:
		# 	input_lines = input_file.readlines()

		# 	line_number = 1
		# 	schema = None

		# 	for input_line in input_lines:
		# 		if line_number == 1:
		# 			schema = input_line.split(',')
		# 		else:
		# 			input_line = input_line.strip()
		# 			# columns = IngestLib.parse_line(input_line, line_number, csv_file)
		# 			# columns = input_line.split(',')

		# 			if len(input_line) > MIN_ROW_LENGTH:
		# 				if len(columns) != len(schema):
		# 					raise Exception('Expected length of the columns ' + str(len(columns)) + ' to be the same as schema length ' + str(len(schema)) + ' for ' + str(csv_file) + ' but it was not (line ' + str(line_number) + ')')


		# 				instance = {}
						
		# 				for column_index in range(len(schema)):
		# 					instance[IngestLib.add_prefix(self.ingest_prefix, schema[column_index].strip())] = columns[column_index].strip().encode('utf-8', 'surrogateescape').decode('utf-8', 'replace')

		# 				values.append(instance)

		# 		line_number+=1

		return values, name_space

	def add_csv_joins_data(self, csv_file):
		df = pd.read_csv(csv_file, header = 0, encoding='ISO-8859-1')
		schema = df.columns

		values = []

		for index, row in df.iterrows():

			instance = []
			for column_name in schema:
				instance.append([IngestLib.add_prefix(self.ingest_prefix, column_name), row[column_name].encode('utf-8', 'surrogateescape').decode('utf-8', 'replace')])

			values.append(instance)

		

		# with open(txt_file, 'r', encoding='ascii', errors='surrogateescape') as input_file:
		# 	input_lines = input_file.readlines()
			
		# 	line_number = 1
		# 	schema = None

		# 	for input_line in input_lines:
		# 		if line_number == 1:
		# 			schema = input_line.split(',')
		# 		else:

		# 			input_line = input_line.strip()
		# 			# columns = IngestLib.parse_line(input_line, line_number, txt_file)
		# 			colunns = input_line.split(',')

		# 			if len(input_line) > MIN_ROW_LENGTH:
		# 				# print('************')
		# 				# print('columns', columns, len(columns))
		# 				# print('schema', schema, len(schema))

		# 				instance = []


		# 				if len(columns) != len(schema):
		# 					raise Exception('Expected length of the columns ' + str(len(columns)) + ' to be the same as schema length ' + str(len(schema)) + ' for ' + str(txt_file) + ' but it was not (line ' + str(line_number) + ')')
						
		# 				for column_index in range(len(schema)):
		# 					instance.append([schema[column_index].strip(), columns[column_index]].strip())

		# 				values.append(instance)
		# 		line_number+=1

			# instances = {}
			# instances['file_name'] = txt_file
			# instances['name_space_one'] = name_space_one
			# instances['name_space_two'] = name_space_two
			# instances['values'] = values

			# joins.append(instances)

		# print('values', values)

		return values