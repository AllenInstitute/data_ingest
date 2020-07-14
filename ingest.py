from validation import *
from ingest_lib import *
import zipfile
import shutil
import glob
import os
from datetime import datetime

SETTINGS_FOLDER = './settings'
SETTINGS_FILE = 'settings.json'
MIN_ROW_LENGTH = 1
INDEX_TO_FIRST_TABLE = 0
INDEX_TO_SECOND_TABLE = 1

class Ingest(object):
	def __init__(self, uploader_uid, ingest_uid, zip_file):
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

		self.extract_zip()
		self.store_data()

	def extract_zip(self):
		print('extracting ' + str(self.zip_file) + ' to ' + str(self.storage_directory))

		self.storage_directory

		with zipfile.ZipFile(self.zip_file,"r") as zip_ref:

			#this extract the zip into a new folder in the storage_directory
			zip_ref.extractall(self.storage_directory)

			#the rest of this is the bring the files directory into the storage_directory and to clean up everything
			folder_name = os.path.basename(os.path.splitext(self.zip_file)[0])

			unzipped_file_path = os.path.join(self.storage_directory, folder_name)

			for file in glob.glob(unzipped_file_path + '/*'):
				correct_location = os.path.join(self.storage_directory, os.path.basename(file))
				shutil.copyfile(file,  correct_location)
				os.remove(file)

			os.rmdir(unzipped_file_path)

	def store_data(self):
		self.validation.validate_file_existance(self.storage_directory, self.template)

		#remove all results from a previous run...
		self.blaze_graph.delete_all_data_by_ingest(self.ingest_triple.get_uid())

		self.insert_file_records(self.storage_directory, self.template)
		# self.insert_data(self.storage_directory, self.template)
		# self.insert_joins(self.storage_directory, self.template)

		#create a file record for each file

	def insert_file_records(self, storage_directory, template):
		template_data = IngestLib.get_json_data_from_file(template)

		files = template_data['files']

		values = []

		table_name = 'file_records'

		for file in files:
			file_name = file['file_name']

			file_path = os.path.join(storage_directory, file_name)

			instance = {}
			instance[IngestLib.add_prefix(self.ingest_prefix,'required')] = file['required']
			instance[IngestLib.add_prefix(self.ingest_prefix,'file_type')] = file['file_type']
			instance[IngestLib.add_prefix(self.ingest_prefix,'data_type')] = file['data_type']
			instance[IngestLib.add_prefix(self.ingest_prefix,'schema')] = file['schema']
			instance[IngestLib.add_prefix(self.ingest_prefix,'file_name')] = file['file_name']
			instance[IngestLib.add_prefix(self.ingest_prefix,'storage_directory')] = storage_directory
			instance[IngestLib.add_prefix(self.ingest_prefix,'file_path')] = file_path
			instance[IngestLib.add_prefix(self.ingest_prefix,'exists')] = os.path.exists(file_path)
			instance[IngestLib.add_prefix(self.ingest_prefix,'created_at')] = datetime.now()
			instance['rdf:has_part'] = self.ingest_triple.subject


			values.append(instance)


		self.blaze_graph.insert_data(values, table_name, self.ingest_triple)

	def insert_data(self, storage_directory, template):
		template_data = IngestLib.get_json_data_from_file(template)

		files = template_data['files']

		for file in files:
			required = file['required']
			file_type = file['file_type']
			data_type = file['data_type']
			schema = file['schema']
			file_name = file['file_name']

			file_path = os.path.join(storage_directory, file_name)

			if data_type == 'data':
				if file_type == 'csv_txt':
					print('inserting', file_name)

					values, table_name = self.get_txt_csv_data(file_path, schema)
					self.blaze_graph.insert_data(values, table_name, self.ingest_triple)

				else:
					raise Exception('file_type', str(file_type), ' not supported')

	def insert_joins(self, storage_directory, template):
		template_data = IngestLib.get_json_data_from_file(template)

		files = template_data['files']

		for file in files:
			required = file['required']
			file_type = file['file_type']
			data_type = file['data_type']
			schema = file['schema']
			file_name = file['file_name']

			file_path = os.path.join(storage_directory, file_name)

			if data_type == 'join':
				join_table_one = file['join_table_one']
				join_table_two = file['join_table_two']

				if file_type == 'csv_txt':
					print('inserting', file_name)

					values = self.add_txt_joins_data(file_path, schema)

					self.blaze_graph.insert_join_data(values, join_table_one, join_table_two)

				else:
					raise Exception('file_type', str(file_type), ' not supported')


	def get_txt_csv_data(self, txt_file, schema):
		values = []
		table_name = IngestLib.get_filename_without_extension(os.path.basename(txt_file))

		with open(txt_file, 'r') as input_file:
			input_lines = input_file.readlines()

			line_number = 1

			for input_line in input_lines:
				input_line = input_line.strip()
				columns = IngestLib.parse_line(input_line, line_number, txt_file)

				if len(input_line) > MIN_ROW_LENGTH:
					if len(columns) != len(schema):
						raise Exception('Expected length of the columns ' + str(len(columns)) + ' to be the same as schema length ' + str(len(schema)) + ' for ' + str(txt_file) + ' but it was not (line ' + str(line_number) + ')')


					instance = {}
					
					for column_index in range(len(schema)):
						instance[schema[column_index]] = columns[column_index]


					values.append(instance)

				line_number+=1

		return values, table_name

	def add_txt_joins_data(self, txt_file, schema):
		values = []

		with open(txt_file, 'r') as input_file:
			input_lines = input_file.readlines()
			
			line_number = 1

			for input_line in input_lines:
				input_line = input_line.strip()
				columns = IngestLib.parse_line(input_line, line_number, txt_file)

				if len(input_line) > MIN_ROW_LENGTH:
					# print('************')
					# print('columns', columns, len(columns))
					# print('schema', schema, len(schema))

					instance = []


					if len(columns) != len(schema):
						raise Exception('Expected length of the columns ' + str(len(columns)) + ' to be the same as schema length ' + str(len(schema)) + ' for ' + str(txt_file) + ' but it was not (line ' + str(line_number) + ')')
					
					for column_index in range(len(schema)):
						instance.append([schema[column_index], columns[column_index]])

					values.append(instance)
				line_number+=1

			# instances = {}
			# instances['file_name'] = txt_file
			# instances['table_one'] = table_one
			# instances['table_two'] = table_two
			# instances['values'] = values

			# joins.append(instances)

		# print('values', values)

		return values