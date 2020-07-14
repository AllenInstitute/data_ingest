# #!/usr/bin/env python

import json
from ingest_lib import *

class ProjectInventoryTemplateWriter(object):
	def __init__(self, file_path, ingest_prefix):

		self.headers = self.get_headers()
		self.ingest_prefix = ingest_prefix


		data = {}

		files  = []
		files.append(self.get_archive_data())
		files.append(self.get_investigator_data())
		files.append(self.get_project_archive_data())
		files.append(self.get_project_investigator_data())
		files.append(self.get_project_protocol_data())
		files.append(self.get_project_technique_data())
		files.append(self.get_protocol_data())
		files.append(self.get_technique_data())
		files.append(self.get_grant_data())
		files.append(self.get_modality_data())
		files.append(self.get_project_grant_data())
		files.append(self.get_project_modality_data())
		files.append(self.get_project_species_data())
		files.append(self.get_project_data())
		files.append(self.get_species_data())

		data['files'] = files

		with open(file_path, 'w') as outfile:
			json.dump(data, outfile, indent=2)

	def get_headers(self):
		headers = {}
		headers['archive'] = 'archive_uri,archive_name'
		headers['grant'] = 'grant_name,grant_uri'
		headers['investigator'] = 'investigator_name,missing,orcid'
		headers['modality'] = 'modality_name'
		headers['project_archive'] = 'project_name,archive_uri'
		headers['project_grant'] = 'project_name,grant_name'
		headers['project_investigator'] = 'project_name,investigator_name'
		headers['project_modality'] = 'project_name,modality_name'
		headers['project_species'] = 'project_name,species_name'
		headers['project_technique'] = 'project_name,technique_name'
		headers['protocol_uri'] = 'uri'
		headers['species'] = 'species_name'
		headers['technique'] = 'technique_name'
		headers['project'] = 'project_name,title,unknown,description,specimen_count,specimen_type'
		headers['project_protocol'] = 'project_name,protocol_uri'
		headers['protocol'] = 'protocol_uri'

		return headers

	def get_archive_data(self):
		schema = self.headers['archive']

		return IngestLib.create_template_validation(True, 'csv_txt', 'data', schema, self.ingest_prefix, 'archive.txt', None)

	def get_investigator_data(self):
		schema = self.headers['investigator']

		return IngestLib.create_template_validation(True, 'csv_txt', 'data', schema, self.ingest_prefix, 'investigator.txt', None)

	def get_project_archive_data(self):
		schema = self.headers['project_archive']

		return IngestLib.create_template_validation(True, 'csv_txt', 'join', schema, self.ingest_prefix, 'project_archive.txt', ['project','archive'])

	def get_project_investigator_data(self):
		schema = self.headers['project_investigator']

		return IngestLib.create_template_validation(True, 'csv_txt', 'join', schema, self.ingest_prefix, 'project_investigator.txt', ['project','investigator'])

	def get_project_protocol_data(self):
		schema = self.headers['project_protocol']

		return IngestLib.create_template_validation(True, 'csv_txt', 'join', schema, self.ingest_prefix, 'project_protocol.txt', ['project','protocol'])

	def get_project_technique_data(self):
		schema = self.headers['project_technique']

		return IngestLib.create_template_validation(True, 'csv_txt', 'data', schema, self.ingest_prefix, 'project_technique.txt', ['project','technique'])

	def get_protocol_data(self):
		schema = self.headers['protocol']

		return IngestLib.create_template_validation(True, 'csv_txt', 'data', schema, self.ingest_prefix, 'protocol.txt', None)

	def get_technique_data(self):
		schema = self.headers['technique']

		return IngestLib.create_template_validation(True, 'csv_txt', 'data', schema, self.ingest_prefix, 'technique.txt', None)

	def get_grant_data(self):
		schema = self.headers['grant']

		return IngestLib.create_template_validation(True, 'csv_txt', 'data', schema, self.ingest_prefix, 'grant.txt', None)

	def get_modality_data(self):
		schema = self.headers['modality']

		return IngestLib.create_template_validation(True, 'csv_txt', 'data', schema, self.ingest_prefix, 'modality.txt', None)

	def get_project_grant_data(self):
		schema = self.headers['project_grant']

		return IngestLib.create_template_validation(True, 'csv_txt', 'join', schema, self.ingest_prefix, 'project_grant.txt', ['project','grant'])

	def get_project_modality_data(self):
		schema = self.headers['project_modality']

		return IngestLib.create_template_validation(True, 'csv_txt', 'join', schema, self.ingest_prefix, 'project_modality.txt', ['project','modality'])

	def get_project_species_data(self):
		schema = self.headers['project_species']

		return IngestLib.create_template_validation(True, 'csv_txt', 'join', schema, self.ingest_prefix, 'project_species.txt', ['project','species'])

	def get_project_data(self):
		schema = self.headers['project']

		return IngestLib.create_template_validation(True, 'csv_txt', 'data', schema, self.ingest_prefix, 'project.txt', None)

	def get_species_data(self):
		schema = self.headers['species']

		return IngestLib.create_template_validation(True, 'csv_txt', 'data', schema, self.ingest_prefix, 'species.txt', None)
