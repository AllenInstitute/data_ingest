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
	def has_field(json_file, json_data, field, name_space):
		if field not in json_data:
			raise Exception('Expected "' + str(field) + '" field to exist in all values in the "' + str(name_space) + '" name_space within ' + str(json_file) + ' but it does not')

	@staticmethod
	def has_join_field(json_file, json_data, field):
		if field not in json_data:
			raise Exception('Expected "' + str(field) + '" fjoin ield to exist within ' + str(json_file) + ' but it does not')

	@staticmethod
	def file_exists(filename):
		if not os.path.exists(filename):
			raise Exception('Expected filename to exist at ' + str(filename) + ' but it does not')

	@staticmethod
	def get_validations_from_template_data(name_space, json_data_template, json_file_template):
		validations = {}

		if 'name_spaces' not in json_data_template:
			raise Exception('Expected "name_spaces" to be in file ' + str(json_file_template) + ' but it was not')

		name_spaces = json_data_template['name_spaces']

		for name_space in json_data_template['name_spaces']:
			if 'required_files' not in name_space:
				raise Exception('Expected "' + str(name_space['required_files']) + '" to exist in all values in name_space ' + str(name_space) + ' in file ' + str(json_file_template) + ' but it did was not')

			if 'required_fields' not in name_space:
				raise Exception('Expected "' + str(name_space['required_fields']) + '" to exist in all values in name_space ' + str(name_space) + ' in file ' + str(json_file_template) + ' but it did was not')


			current_validations = {}
			current_validations['required_files'] = name_space['required_files']
			current_validations['required_fields'] = name_space['required_fields']

			validations[name_space['name_space']] = current_validations

		return validations

	@staticmethod
	def validate_extra_fields(json_data, json_file, json_data_extra_fields, json_file_extra_fields):

		if 'name_spaces' not in json_data:
			raise Exception('Expected "name_spaces" to be in file ' + str(json_file) + ' but it was not')

		name_spaces = json_data['name_spaces']
		valid_name_spaces = {}

		for name_space in name_spaces:
			if 'name_space' not in name_space:
				raise Exception('Expected "name_space" field to exist in all value row in file ' + str(json_file) + ' but it did not')

			valid_name_spaces[name_space['name_space']] = True


		for name_space in list(json_data_extra_fields.keys()):


			if name_space not in valid_name_spaces:
				raise Exception('Expected name_space name "' + str(name_space) + '" in "' + str(json_file_extra_fields) + ' to match a name_space name in "' + str(json_file) + '" but it did not')

	@staticmethod
	def validate_name_space(name_space, json_file, json_data, json_data_template, json_file_template):


		validations = ControlledVocabValidation.get_validations_from_template_data(name_space, json_data_template, json_file_template)

		if name_space not in validations:
			raise Exception('Invalid name_space name: ' + str(name_space) + ' Please add this name_space to the template file')
		else:
			current_validations = validations[name_space]
			required_files = current_validations['required_files']
			required_fields = current_validations['required_fields']

			#make sure required fields exist
			for required_field in required_fields:
				for value in json_data:
					ControlledVocabValidation.has_field(json_file, value, required_field, name_space)

			#make sure required files exist
			for required_file in required_files:
				for value in json_data:
					ControlledVocabValidation.has_field(json_file, value, required_file, name_space)
					ControlledVocabValidation.file_exists(value[required_file])