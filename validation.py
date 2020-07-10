import json
from blaze_graph import *
from ingest_lib import *

class Validation(object):
	def __init__(self, settings):
		self.settings = settings

		self.blaze_graph = BlazeGraph(settings)
		self.passed_validation = True
		self.validation_checks = {}
		self.template = None

	def validate_user(self, user):
		print('validating user', user)

	def validate_ingest_selection(self, ingest_selection):
		print('validating ingest_selection', ingest_selection)

		#look up template
		self.template = 'TODO'

	def validate_zip_file(self, zip_file):
		print('validating zip_file', zip_file)

	def run_default_validation(self, user, ingest_selection, zip_file):
		self.validate_user(user)
		self.validate_ingest_selection(ingest_selection)
		self.validate_zip_file(zip_file)


	@staticmethod
	def validate_controlled_vacab_json(json_file):
		json_data = IngestLib.get_json_data_from_file(json_file)

		if 'tables' not in json_data:
			raise Exception('Expected json_file ' + str(json_file) + '  to contain the field "tables" but it does not')

		tables = json_data['tables']

		unique_keys = {}

		for table in tables:
			if 'table_name' not in table:
				raise Exception('Expected json_file ' + str(json_file) + '  to contain a the field "table_name" for each table but it does not')

			if 'values' not in table:
				raise Exception('Expected json_file ' + str(json_file) + '  to contain a the field "values" for each table but it does not')

			values = table['values']
			for value in values:
				if 'unique_key' not in value:
					raise Exception('Expected json_file ' + str(json_file) + '  to contain a the field "unique_key" for each table but it does not')

				unique_key = value['unique_key']

				if unique_key in unique_keys:
					raise Exception('Expected json_file ' + str(json_file) + '  to contain a unique_key for each value but found more than one instance of ' + str(unique_key))

				unique_keys[unique_key] = True