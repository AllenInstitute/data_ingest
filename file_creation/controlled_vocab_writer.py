# #!/usr/bin/env python
import os
import json
import glob
from ingest_lib import *

MIN_ROW_LENGTH = 1
INDEX_TO_FIRST_TABLE = 0
INDEX_TO_SECOND_TABLE = 1
COLUMNS_IN_JOIN_TXT = 2

UPLOADERS_TABLE = 'uploaders'
INGESTS_TABLE = 'ingests'

class ControlledVocabWriter(object):
	def __init__(self, ingest_prefix):
		self.ingest_prefix = ingest_prefix
		self.row_ids = {}


	def get_uploaders(self):
		table_name = UPLOADERS_TABLE

		uploader_one = {}
		uploader_one[IngestLib.add_prefix(self.ingest_prefix,'name')] = 'Nathan Sjoquist'
		uploader_one[IngestLib.add_prefix(self.ingest_prefix,'role')] = 'Superadmin'

		uploader_two = {}
		uploader_two[IngestLib.add_prefix(self.ingest_prefix,'name')] = 'Carol Thompson'
		uploader_two[IngestLib.add_prefix(self.ingest_prefix,'role')] = 'Superadmin'

		uploader_three = {}
		uploader_three[IngestLib.add_prefix(self.ingest_prefix,'name')] = 'Jimmy Mathews'
		uploader_three[IngestLib.add_prefix(self.ingest_prefix,'role')] = 'Superadmin'

		uploader_values = []
		uploader_values.append(uploader_one)
		uploader_values.append(uploader_two)

		uploaders = {}
		uploaders['table_name'] = table_name
		uploaders['values'] = uploader_values

		return uploaders

	def get_ingestion_instances(self):
		table_name = INGESTS_TABLE

		ingestion_instance_one = {}
		ingestion_instance_one['di:name'] = 'project inventory metadata'
		ingestion_instance_one['di:storage_directory'] = '/scratch/allen/storage_directory/project_inventory'
		ingestion_instance_one['di:template'] = '/scratch/allen/data_ingest/templates/project_inventory.json'
		ingestion_instance_one['di:description'] = 'Project inventory metadata upload'

		ingestion_instance_values = []
		ingestion_instance_values.append(ingestion_instance_one)

		ingestion_instances = {}
		ingestion_instances['table_name'] = table_name
		ingestion_instances['values'] = ingestion_instance_values

		return ingestion_instances

	def write_controlled_vocab_json_file(self, file_path):
		controlled_vocab = {}
		tables = []
		joins = []

		tables.append(self.get_uploaders())
		tables.append(self.get_ingestion_instances())

		controlled_vocab['tables'] = tables
		controlled_vocab['joins'] = joins


		with open(file_path, 'w') as outfile:
			json.dump(controlled_vocab, outfile, indent=2)

	def get_uploader_template(self):
		uploader_template = {}
		uploader_template['table_name'] = UPLOADERS_TABLE
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
		ingestion_instance_template['table_name'] = INGESTS_TABLE
		ingestion_instance_template['required_fields'] = ['di:name', 'di:storage_directory', 'di:template', 'di:description']
		ingestion_instance_template['required_files'] = ['di:template']

		return ingestion_instance_template

	def write_controlled_vocab_template(self, file_path):
		controlled_vocab_template = {}
		tables = []

		tables.append(self.get_uploader_template())
		tables.append(self.get_ingestion_instance_template())
		# self.add_txt_file_template_info(txt_folder_path, tables)

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
		extra_fields[INGESTS_TABLE] = self.get_ingestion_instance_extra_fields()

		with open(file_path, 'w') as outfile:
			json.dump(extra_fields, outfile, indent=2)

	def write_controlled_vocab(self, controlled_vocab_file, controlled_vocab_template_file, controlled_vocab_extra_fields_file, controlled_vocab_global_extra_fields_file):
		self.write_controlled_vocab_json_file(controlled_vocab_file)
		self.write_controlled_vocab_template(controlled_vocab_template_file)
		self.write_extra_fields_file(controlled_vocab_extra_fields_file)
		self.write_global_extra_fields_file(controlled_vocab_global_extra_fields_file)