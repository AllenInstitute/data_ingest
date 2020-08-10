# #!/usr/bin/env python
import os
import json
import glob
from ingest_lib import *

MIN_ROW_LENGTH = 1
INDEX_TO_FIRST_NAMESPACE = 0
INDEX_TO_SECOND_NAMESPACE = 1
COLUMNS_IN_JOIN_TXT = 2

UPLOADERS_NAMESPACE = 'uploaders'
INGESTS_NAMESPACE = 'ingests'

class ControlledVocabWriter(object):
	def __init__(self, ingest_prefix):
		self.ingest_prefix = ingest_prefix
		self.row_ids = {}


	def get_uploaders(self):
		name_space = UPLOADERS_NAMESPACE

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
		uploader_values.append(uploader_three)

		# tests = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't']
		# for test in tests:
		# 	uploader = {}
		# 	uploader[IngestLib.add_prefix(self.ingest_prefix,'name')] = test
		# 	uploader[IngestLib.add_prefix(self.ingest_prefix,'role')] = test
		# 	uploader_values.append(uploader)

		uploaders = {}
		uploaders['name_space'] = name_space
		uploaders['values'] = uploader_values

		return uploaders

	def get_ingestion_instances(self):
		name_space = INGESTS_NAMESPACE

		ingestion_instance_one = {}
		ingestion_instance_one['di:name'] = 'test project inventory metadata'
		ingestion_instance_one['di:storage_directory'] = '/scratch/allen/storage_directory/test_project_inventory'
		ingestion_instance_one['di:template'] = '/scratch/allen/data_ingest/templates/project_inventory.json'
		ingestion_instance_one['di:description'] = 'Project inventory metadata upload'
		ingestion_instance_one['di:created_at'] = None
		ingestion_instance_one['di:uploaded_at'] = None

		ingestion_instance_two = {}
		ingestion_instance_two['di:name'] = 'project inventory metadata'
		ingestion_instance_two['di:storage_directory'] = '/scratch/allen/storage_directory/project_inventory'
		ingestion_instance_two['di:template'] = '/scratch/allen/data_ingest/templates/project_inventory.json'
		ingestion_instance_two['di:description'] = 'test project inventory metadata upload'
		ingestion_instance_two['di:created_at'] = None
		ingestion_instance_two['di:uploaded_at'] = None

		ingestion_instance_values = []
		ingestion_instance_values.append(ingestion_instance_one)
		ingestion_instance_values.append(ingestion_instance_two)

		ingestion_instances = {}
		ingestion_instances['name_space'] = name_space
		ingestion_instances['values'] = ingestion_instance_values

		return ingestion_instances

	def write_controlled_vocab_json_file(self, file_path):
		controlled_vocab = {}
		name_spaces = []
		joins = []

		name_spaces.append(self.get_uploaders())
		name_spaces.append(self.get_ingestion_instances())

		controlled_vocab['name_spaces'] = name_spaces
		controlled_vocab['joins'] = joins


		with open(file_path, 'w') as outfile:
			json.dump(controlled_vocab, outfile, indent=2)

	def get_uploader_template(self):
		uploader_template = {}
		uploader_template['name_space'] = UPLOADERS_NAMESPACE
		uploader_template['required_fields'] = ['di:name', 'di:role']
		uploader_template['required_files'] = []

		return uploader_template

	def get_ingestion_instance_extra_fields(self):
		ingestion_instance_extra = {}
		ingestion_instance_extra['di:status'] = 'pending'
		ingestion_instance_extra['di:locked'] = 'False'
		ingestion_instance_extra['di:uploader'] = 'None'

		return ingestion_instance_extra


	def get_ingestion_instance_template(self):
		ingestion_instance_template = {}
		ingestion_instance_template['name_space'] = INGESTS_NAMESPACE
		ingestion_instance_template['required_fields'] = ['di:name', 'di:storage_directory', 'di:template', 'di:description']
		ingestion_instance_template['required_files'] = ['di:template']

		return ingestion_instance_template

	def write_controlled_vocab_template(self, file_path):
		controlled_vocab_template = {}
		name_spaces = []

		name_spaces.append(self.get_uploader_template())
		name_spaces.append(self.get_ingestion_instance_template())
		# self.add_txt_file_template_info(txt_folder_path, name_spaces)

		controlled_vocab_template['name_spaces'] = name_spaces


		with open(file_path, 'w') as outfile:
			json.dump(controlled_vocab_template, outfile, indent=2)

	def write_global_extra_fields_file(self, file_path):
		extra_fields = {}
		extra_fields['di:data_type'] = 'controlled_vocabulary'

		with open(file_path, 'w') as outfile:
			json.dump(extra_fields, outfile, indent=2)

	def write_extra_fields_file(self, file_path):
		extra_fields = {}
		extra_fields[INGESTS_NAMESPACE] = self.get_ingestion_instance_extra_fields()

		with open(file_path, 'w') as outfile:
			json.dump(extra_fields, outfile, indent=2)

	def write_controlled_vocab(self, controlled_vocab_file, controlled_vocab_template_file, controlled_vocab_extra_fields_file, controlled_vocab_global_extra_fields_file):
		self.write_controlled_vocab_json_file(controlled_vocab_file)
		self.write_controlled_vocab_template(controlled_vocab_template_file)
		self.write_extra_fields_file(controlled_vocab_extra_fields_file)
		self.write_global_extra_fields_file(controlled_vocab_global_extra_fields_file)