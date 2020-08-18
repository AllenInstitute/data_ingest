from ingest_lib import *
from blaze_graph import *
from csv_ingest import *

SETTINGS_FOLDER = './settings'
SETTINGS_FILE = 'settings.json'

class Export(object):
	def __init__(self, template, storage_directory):
		print('exporting to ' + str(storage_directory))

		settings_file = os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
		self.settings = IngestLib.get_json_data_from_file(settings_file)
		self.ingest_prefix = self.settings['ingest_prefix']
		self.blaze_graph = BlazeGraph(self.settings)
		self.export_files(template, storage_directory)

		print('finished...')

	def get_file_ingests(self, files):
		file_ingests = []

		for file in files:
			file_type = file['file_type']
			if file_type == 'csv':
				file_ingests.append(CsvIngest(file['subject'], file['required'], file['file_type'], file['data_type'], file['file_name'], file['select_clause'], file['shape_clause'], file['where_clause'], file['schema'], file['joins'], file['extra_joins'], file['primary_key'], file['replace_primary_key'], file['optional_shape']))
			else:
				raise Exception('file_type', str(file_type), ' not supported')

		return file_ingests


	def export_files(self, template, storage_directory):
		template_data = IngestLib.get_json_data_from_file(template)

		file_ingests = self.get_file_ingests(template_data['files'])

		print('exporting', len(file_ingests))
		for file_ingest in file_ingests:
			export_file = os.path.join(storage_directory, file_ingest.file_name)
			print('export_file', export_file)

			header_row, csv_lines = self.blaze_graph.get_export_data(file_ingest)

			#if no results
			if len(header_row) == 0:
				header_row = file_ingest.schema


			with open(export_file, 'w') as out_file:
				out_file.write(','.join(header_row) + '\n')

				for csv_line in csv_lines:
					out_file.write(','.join(csv_line) + '\n')


