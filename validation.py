import json
from blaze_graph import *
from ingest_lib import *
import pandas as pd
import numpy as np
import math

class Validation(object):
	def __init__(self, settings):
		self.settings = settings

		self.blaze_graph = BlazeGraph(settings)
		self.passed_validation = True
		self.validation_checks = {}
		self.ingest_triple = None

	def validate_uid_presence(self, triple_holder, uid):
		if 'uid' not in triple_holder.attributes or uid not in triple_holder.attributes['uid']:
			raise Exception('Could not find record with uid of ' + str(uid))

	def validate_uploader(self, uploader_uid):
		triple_holder = self.blaze_graph.get_triples_from_uid(uploader_uid)

		self.validate_uid_presence(triple_holder, uploader_uid)

	def validate_field_exits(self, triple_holder, attribute, uid):
		if attribute not in triple_holder.attributes:
			raise Exception('Could not find attribute of ' + str(attribute) + ' for uid ' + str(uid))

	def validate_ingest(self, ingest_uid):
		self.ingest_triple = self.blaze_graph.get_triples_from_uid(ingest_uid)

		self.validate_uid_presence(self.ingest_triple, ingest_uid)

		template = self.ingest_triple.get_attribute('template')
		self.validate_file_exists(template)

	def validate_zip_file(self, zip_file):
		self.validate_file_exists(zip_file)

	def run_default_validation(self, uploader_uid, ingest_uid, zip_file):
		self.validate_uploader(uploader_uid)
		self.validate_ingest(ingest_uid)
		self.validate_zip_file(zip_file)

	def validate_file_exists(self, file_path):
		if not os.path.exists(file_path):
			raise Exception('Expected file to exist at ' + str(file_path) + ' but it does not')

	def validate_files(self, storage_directory, template):
		errors = []
		errors+= self.validate_file_existance(storage_directory, template)
		errors+= self.validate_schema(storage_directory, template)
		errors+= self.validate_joins(storage_directory, template)

		return errors

	def validate_schema(self, storage_directory, template):
		errors = []

		template_data = IngestLib.get_json_data_from_file(template)

		files = template_data['files']
		for file in files:
			try:
				required = file['required']

				if required:
					file_name = file['file_name']

					file_path = os.path.join(storage_directory, file_name)
					
					if os.path.exists(file_path):
						true_schema = (file['schema'])


						df = pd.read_csv(file_path, header = 0, encoding='ISO-8859-1')
						schema = df.columns

						if len(true_schema) != len(schema):
							raise Exception('schema in file ' + str(file_path) + ' is not the same length as the template schema: ' + str(schema))

						for index in range(len(true_schema)):
							if true_schema[index] != schema[index]:
								raise Exception('column with index ' + str(index) + ' in file ' + str(file_path) + ' is not the same length as in the template schema: ' + str(true_schema[index]) + ' != ' + str(schema[index]))

			except Exception as e:
				errors.append(str(e))

		return errors

	def validate_joins(self, storage_directory, template):
		errors = []

		template_data = IngestLib.get_json_data_from_file(template)

		#todo validate this?
		files = template_data['files']

		tables = {}
		table_primary_keys = {}

		for file in files:
			try:
				required = file['required']
				primary_key = file['primary_key']

				if required and primary_key is not None:
					file_name = file['file_name']
					subject = file['subject']

					file_path = os.path.join(storage_directory, file_name)
					
					if os.path.exists(file_path):
						primary_keys = {}

						df = pd.read_csv(file_path, header = 0, encoding='ISO-8859-1')

						for index, row in df.iterrows():
							primary_keys[row[primary_key]] = True
	
						tables[subject] = primary_keys
						table_primary_keys[subject] = primary_key
				
			except Exception as e:
				errors.append(str(e))

		for file in files:
			try:
				required = file['required']
				primary_key = file['primary_key']

				if required and primary_key is None:
					file_name = file['file_name']
					subject = file['subject']
					joins = file['joins']

					file_path = os.path.join(storage_directory, file_name)
					
					if os.path.exists(file_path):
						primary_keys = {}

						df = pd.read_csv(file_path, header = 0, encoding='ISO-8859-1')
						schema = df.columns

						for join in list(joins.keys()):
							table_column = joins[join]['table_column']

							if table_column not in schema:
								raise Exception('Expected column named ' + str(table_column) + ' to be in ' + str(file_path))

						for index, row in df.iterrows():

							try:
								for join in list(joins.keys()):

									reference_table = joins[join]['reference_table']
									reference_table_column = joins[join]['reference_table_column']
									table_column = joins[join]['table_column']

									value = row[table_column]

									if value is None or value is np.nan or (isinstance(value, (int, float)) and math.isnan(value)):
										pass

									else:

										if table_column not in row:
											raise Exception('Expected column named ' + str(table_column) + ' to be in ' + str(file_path))

										elif reference_table not in tables or reference_table not in table_primary_keys:
											raise Exception('Expected a table named ' + str(reference_table) + ' to exist but it did not')

										elif table_primary_keys[reference_table] != reference_table_column:
											raise Exception('Expected table named ' + str(reference_table) + ' to have a primary key of ' + str(reference_table) + ' but it did not')

										elif value not in tables[reference_table]:
											raise Exception('Expected primary key of ' + str(value) + ' to be in table ' + str(reference_table) + ' but it was not for file ' + str(file_path))


							except Exception as e:
								errors.append(str(e)) 
				
			except Exception as e:
				errors.append(str(e))

		return errors

	def validate_file_existance(self, storage_directory, template):
		errors = []

		template_data = IngestLib.get_json_data_from_file(template)

		#todo validate this?
		files = template_data['files']
		for file in files:
			required = file['required']
			if required:

				# file_type = file['file_type']
				# data_type = file['data_type']
				# schema = file['schema']
				file_name = file['file_name']


				file_path = os.path.join(storage_directory, file_name)
				try:
					self.validate_file_exists(file_path)
				except Exception as e:
					errors.append(str(e))

		return errors


	@staticmethod
	def validate_controlled_vacab_json(json_file):
		json_data = IngestLib.get_json_data_from_file(json_file)

		if 'name_spaces' not in json_data:
			raise Exception('Expected json_file ' + str(json_file) + '  to contain the field "name_spaces" but it does not')

		name_spaces = json_data['name_spaces']

		for name_space in name_spaces:
			if 'name_space' not in name_space:
				raise Exception('Expected json_file ' + str(json_file) + '  to contain a the field "name_space" for each name_space but it does not')

			if 'values' not in name_space:
				raise Exception('Expected json_file ' + str(json_file) + '  to contain a the field "values" for each name_space but it does not')