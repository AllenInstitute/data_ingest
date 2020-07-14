import json
from blaze_graph import *
from ingest_lib import *

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

	def validate_file_existance(self, storage_directory, template):
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
				self.validate_file_exists(file_path)

	@staticmethod
	def validate_controlled_vacab_json(json_file):
		json_data = IngestLib.get_json_data_from_file(json_file)

		if 'tables' not in json_data:
			raise Exception('Expected json_file ' + str(json_file) + '  to contain the field "tables" but it does not')

		tables = json_data['tables']

		for table in tables:
			if 'table_name' not in table:
				raise Exception('Expected json_file ' + str(json_file) + '  to contain a the field "table_name" for each table but it does not')

			if 'values' not in table:
				raise Exception('Expected json_file ' + str(json_file) + '  to contain a the field "values" for each table but it does not')