# #!/usr/bin/env python
import os
import json
import glob
from ingest_lib import *

MIN_ROW_LENGTH = 1
INDEX_TO_FIRST_TABLE = 0
INDEX_TO_SECOND_TABLE = 1
COLUMNS_IN_JOIN_TXT = 2

class ControlledVocabWriter(object):
	def __init__(self, ingest_prefix):
		self.ingest_prefix = ingest_prefix
		self.row_ids = {}


	def get_uploaders(self):
		table_name = 'uploaders'

		uploader_one = {}
		uploader_one[IngestLib.add_prefix(self.ingest_prefix,'name')] = 'Nathan Sjoquist'
		uploader_one[IngestLib.add_prefix(self.ingest_prefix,'role')] = 'Superadmin'

		uploader_two = {}
		uploader_two[IngestLib.add_prefix(self.ingest_prefix,'name')] = 'Carol Thompson'
		uploader_two[IngestLib.add_prefix(self.ingest_prefix,'role')] = 'Superadmin'

		uploader_values = []
		uploader_values.append(uploader_one)
		uploader_values.append(uploader_two)

		uploaders = {}
		uploaders['table_name'] = table_name
		uploaders['values'] = uploader_values

		return uploaders


	def get_ingestion_instances(self):
		table_name = 'ingestions'

		ingestion_instance_one = {}
		ingestion_instance_one['di:name'] = 'upload part 1'
		ingestion_instance_one['di:storage_directory'] = '/scratch/allen/storage_directory/part_one'
		ingestion_instance_one['di:template'] = '/scratch/allen/data_ingest/templates/part_one.json'
		ingestion_instance_one['di:description'] = 'Floreat Domus'

		ingestion_instance_two = {}
		ingestion_instance_two['di:name'] = 'upload part 2'
		ingestion_instance_two['di:storage_directory'] = '/scratch/allen/storage_directory/part_two'
		ingestion_instance_two['di:template'] = '/scratch/allen/data_ingest/templates/part_one.json'
		ingestion_instance_two['di:description'] = 'Hinc lucem et pocula sacra'

		ingestion_instance_three = {}
		ingestion_instance_three['di:name'] = 'upload part 3'
		ingestion_instance_three['di:storage_directory'] = '/scratch/allen/storage_directory/part_three'
		ingestion_instance_three['di:template'] = '/scratch/allen/data_ingest/templates/part_one.json'
		ingestion_instance_three['di:description'] = 'twiddly dee tweedle dum'

		ingestion_instance_values = []
		ingestion_instance_values.append(ingestion_instance_one)
		ingestion_instance_values.append(ingestion_instance_two)
		ingestion_instance_values.append(ingestion_instance_three)

		ingestion_instances = {}
		ingestion_instances['table_name'] = table_name
		ingestion_instances['values'] = ingestion_instance_values

		return ingestion_instances

	def get_headers(self):
		headers = {}
		headers['archive'] = 'di:archive_uri,di:archive_name'
		headers['grant'] = 'di:grant_name,di:grant_uri'
		headers['investigator'] = 'di:investigator_name,di:missing,di:orcid'
		headers['modality'] = 'di:modality_name'
		headers['project_archive'] = 'di:project_name,di:archive_uri'
		headers['project_grant'] = 'di:project_name,di:grant_name'
		headers['project_investigator'] = 'di:project_name,di:investigator_name'
		headers['project_modality'] = 'di:project_name,di:modality_name'
		headers['project_species'] = 'di:project_name,di:species_name'
		headers['project_technique'] = 'di:project_name,di:technique_name'
		headers['protocol_uri'] = 'di:uri'
		headers['species'] = 'di:species_name'
		headers['technique'] = 'di:technique_name'
		headers['project'] = 'di:project_name,di:title,di:unknown,di:description,di:specimen_count,di:specimen_type'
		headers['project_protocol'] = 'di:project_name,di:protocol_uri'
		headers['protocol'] = 'di:protocol_uri'

		return headers

	def add_txt_file_template_info(self, txt_folder_path, tables):
		#get the text files
		path = os.path.join(txt_folder_path, '*' + str('.txt'))

		headers = self.get_headers()

		for txt_file in glob.glob(path):
			with open(txt_file, 'r') as input_file:
				input_lines = input_file.readlines()

				table_name = IngestLib.get_filename_without_extension(os.path.basename(txt_file))

				if table_name not in headers:
					raise Exception('Missing header inforomation for ' + str(table_name))

				header = headers[table_name].split(',')

				template = {}

				template['table_name'] = table_name
				template['required_fields'] = header
				template['required_files'] = []

				tables.append(template)

	def add_txt_joins_data(self, joins, txt_join_folder_path):
		path = os.path.join(txt_join_folder_path, '*' + str('.txt'))

		headers = self.get_headers()

		for txt_file in glob.glob(path):
			with open(txt_file, 'r') as input_file:
				input_lines = input_file.readlines()

				table_names = IngestLib.get_filename_without_extension(os.path.basename(txt_file))

				if table_names not in headers:
					raise Exception('Missing header inforomation for ' + str(table_names))

				header = headers[table_names].split(',')

				table_names_values = table_names.split('_')

				if len(table_names_values) != COLUMNS_IN_JOIN_TXT:
					raise Exception('Expected join table name "' + str(table_names) +'" to contain ' + str(COLUMNS_IN_JOIN_TXT) + ' table names when split on "_" but it contained ' + str(len(table_names_values)))


				table_one = 'di:' + table_names_values[INDEX_TO_FIRST_TABLE]
				table_two = 'di:' + table_names_values[INDEX_TO_SECOND_TABLE]

				values = []
				line_number = 1

				for input_line in input_lines:
					input_line = input_line.strip()
					columns = IngestLib.parse_line(input_line, line_number, txt_file)

					# print('columns', columns)
					# print('columns len', len(columns))

					if len(input_line) > MIN_ROW_LENGTH:
						if len(columns) != len(header):
							raise Exception('Expected length of the columns ' + str(len(columns)) + ' to be the same as schema length ' + str(len(header)) + ' for ' + str(txt_file) + ' but it was not (line ' + str(line_number) + ')')

						instance = []
						
						for column_index in range(len(header)):
							# instance[header[column_index]] = columns[column_index]
							instance.append([header[column_index], columns[column_index]])

						values.append(instance)

					line_number+=1

				instances = {}
				instances['file_name'] = txt_file
				instances['table_one'] = table_one
				instances['table_two'] = table_two
				instances['values'] = values

				joins.append(instances)


	def add_txt_data(self, tables, txt_folder_path):
		#get the text files
		path = os.path.join(txt_folder_path, '*' + str('.txt'))

		headers = self.get_headers()

		for txt_file in glob.glob(path):
			with open(txt_file, 'r') as input_file:
				input_lines = input_file.readlines()

				table_name = IngestLib.get_filename_without_extension(os.path.basename(txt_file))

				if table_name not in headers:
					raise Exception('Missing header inforomation for ' + str(table_name))

				header = headers[table_name].split(',')

				values = []
				line_number = 1

				for input_line in input_lines:
					input_line = input_line.strip()
					columns = IngestLib.parse_line(input_line, line_number, txt_file)

					# print('columns', columns)
					# print('columns len', len(columns))

					if len(input_line) > MIN_ROW_LENGTH:

						if len(columns) != len(header):
							raise Exception('Expected length of the columns ' + str(len(columns)) + ' to be the same as schema length ' + str(len(header)) + ' for ' + str(txt_file) + ' but it was not (line ' + str(line_number) + ')')


						instance = {}
						
						for column_index in range(len(header)):
							instance[header[column_index]] = columns[column_index]


						values.append(instance)

					line_number+=1

				instances = {}
				instances['table_name'] = table_name
				instances['values'] = values

				tables.append(instances)


	def write_controlled_vocab_json_file(self, file_path, txt_folder_path, txt_join_folder_path):
		controlled_vocab = {}
		tables = []
		joins = []

		tables.append(self.get_uploaders())
		tables.append(self.get_ingestion_instances())

		self.add_txt_data(tables, txt_folder_path)
		self.add_txt_joins_data(joins, txt_join_folder_path)

		controlled_vocab['tables'] = tables
		controlled_vocab['joins'] = joins


		with open(file_path, 'w') as outfile:
			json.dump(controlled_vocab, outfile, indent=2)

	def get_uploader_template(self):
		uploader_template = {}
		uploader_template['table_name'] = 'uploaders'
		uploader_template['required_fields'] = ['di:name', 'di:role']
		uploader_template['required_files'] = []

		return uploader_template

	def get_ingestion_instance_extra_fields(self):
		ingestion_instance_extra = {}
		ingestion_instance_extra['di:status'] = 'pending'
		ingestion_instance_extra['di:locked'] = 'False'
		ingestion_instance_extra['di:uploader'] = 'None'
		ingestion_instance_extra['di:number_of_files'] = 0

		return ingestion_instance_extra


	def get_ingestion_instance_template(self):
		ingestion_instance_template = {}
		ingestion_instance_template['table_name'] = 'ingestions'
		ingestion_instance_template['required_fields'] = ['di:name', 'di:storage_directory', 'di:template', 'di:description']
		ingestion_instance_template['required_files'] = ['di:template']

		return ingestion_instance_template

	def write_controlled_vocab_template(self, file_path, txt_folder_path):
		controlled_vocab_template = {}
		tables = []

		tables.append(self.get_uploader_template())
		tables.append(self.get_ingestion_instance_template())
		self.add_txt_file_template_info(txt_folder_path, tables)


		controlled_vocab_template['tables'] = tables


		with open(file_path, 'w') as outfile:
			json.dump(controlled_vocab_template, outfile, indent=2)

	def write_global_extra_fields_file(self, file_path):
		extra_fields = {}
		extra_fields['di:data_type'] = 'controlled_vocabulary'

		with open(file_path, 'w') as outfile:
			json.dump(extra_fields, outfile, indent=2)

	def write_extra_fields_file(self, file_path):
		extra_fields = {}
		extra_fields['ingestions'] = self.get_ingestion_instance_extra_fields()

		with open(file_path, 'w') as outfile:
			json.dump(extra_fields, outfile, indent=2)

	def write_controlled_vocab(self, controlled_vocab_file, controlled_vocab_template_file, controlled_vocab_extra_fields_file, txt_folder_path, controlled_vocab_global_extra_fields_file, txt_join_folder_path):
		self.write_controlled_vocab_json_file(controlled_vocab_file, txt_folder_path, txt_join_folder_path)
		self.write_controlled_vocab_template(controlled_vocab_template_file, txt_folder_path)
		self.write_extra_fields_file(controlled_vocab_extra_fields_file)
		self.write_global_extra_fields_file(controlled_vocab_global_extra_fields_file)