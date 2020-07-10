import json
import os


class ControlledVocabValidation(object):
	def __init__(self, settings):
		self.settings = settings

		self.blaze_graph = BlazeGraph(settings)
		self.passed_validation = True
		self.validation_checks = {}
		self.template = None

	@staticmethod
	def has_field(json_file, json_data, field, table_name):
		if field not in json_data:
			raise Exception('Expected "' + str(field) + '" field to exist in all values in the "' + str(table_name) + '" table within ' + str(json_file) + ' but it does not')

	@staticmethod
	def has_join_field(json_file, json_data, field):
		if field not in json_data:
			raise Exception('Expected "' + str(field) + '" fjoin ield to exist within ' + str(json_file) + ' but it does not')

	@staticmethod
	def file_exists(filename):
		if not os.path.exists(filename):
			raise Exception('Expected filename to exist at ' + str(filename) + ' but it does not')

	@staticmethod
	def get_validations_from_template_data(table_name, json_data_template, json_file_template):
		validations = {}

		if 'tables' not in json_data_template:
			raise Exception('Expected "tables" to be in file ' + str(json_file_template) + ' but it was not')

		tables = json_data_template['tables']

		for table in json_data_template['tables']:
			if 'required_files' not in table:
				raise Exception('Expected "' + str(table['required_files']) + '" to exist in all values in table ' + str(table_name) + ' in file ' + str(json_file_template) + ' but it did was not')

			if 'required_fields' not in table:
				raise Exception('Expected "' + str(table['required_fields']) + '" to exist in all values in table ' + str(table_name) + ' in file ' + str(json_file_template) + ' but it did was not')


			current_validations = {}
			current_validations['required_files'] = table['required_files']
			current_validations['required_fields'] = table['required_fields']

			validations[table['table_name']] = current_validations

		return validations

	@staticmethod
	def validate_join(json_file, value):
		ControlledVocabValidation.has_join_field(json_file, value, 'file_name')
		ControlledVocabValidation.has_join_field(json_file, value, 'table_one')
		ControlledVocabValidation.has_join_field(json_file, value, 'table_two')
		ControlledVocabValidation.has_join_field(json_file, value, 'values')

	@staticmethod
	def validate_extra_fields(json_data, json_file, json_data_extra_fields, json_file_extra_fields):

		if 'tables' not in json_data:
			raise Exception('Expected "tables" to be in file ' + str(json_file) + ' but it was not')

		tables = json_data['tables']
		valid_table_names = {}

		for table in tables:
			if 'table_name' not in table:
				raise Exception('Expected "table_name" field to exist in all value row in file ' + str(json_file) + ' but it did not')

			valid_table_names[table['table_name']] = True


		for table_name in list(json_data_extra_fields.keys()):


			if table_name not in valid_table_names:
				raise Exception('Expected table name "' + str(table_name) + '" in "' + str(json_file_extra_fields) + ' to match a table name in "' + str(json_file) + '" but it did not')

	@staticmethod
	def validate_table(table_name, json_file, json_data, json_data_template, json_file_template):


		validations = ControlledVocabValidation.get_validations_from_template_data(table_name, json_data_template, json_file_template)

		if table_name not in validations:
			raise Exception('Invalid table name: ' + str(table_name) + ' Please add this table to the template file')
		else:
			current_validations = validations[table_name]
			required_files = current_validations['required_files']
			required_fields = current_validations['required_fields']

			#make sure required fields exist
			for required_field in required_fields:
				for value in json_data:
					ControlledVocabValidation.has_field(json_file, value, required_field, table_name)

			#make sure required files exist
			for required_file in required_files:
				for value in json_data:
					ControlledVocabValidation.has_field(json_file, value, required_file, table_name)
					ControlledVocabValidation.file_exists(value[required_file])