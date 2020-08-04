# #!/usr/bin/env python

import json
from ingest_lib import *

class ProjectInventoryTemplateWriter(object):
	def __init__(self, file_path, ingest_prefix):

		# self.headers = self.get_headers()
		self.ingest_prefix = ingest_prefix


		data = {}

		files  = []
		files.append(self.get_data_collection())
		files.append(self.get_modality())
		files.append(self.get_organization())
		files.append(self.get_person())
		files.append(self.get_species())
		files.append(self.get_specimen_type())
		files.append(self.get_technique())

		# files.append(self.get_contact_person_realized_in_data_collection_project())
		# files.append(self.get_grant_is_specified_input_of_data_collection_project())
		
		# files.append(self.get_highlighted_web_resource_is_about_data_collection_project())
		# files.append(self.get_data_collection_is_specified_output_of_data_collection_project())
		# files.append(self.get_license())
		# files.append(self.get_data_collection_modality_classification())
		# files.append(self.get_license_is_about_data_collection_project())
		# files.append(self.get_data_collection_project_is_part_of_sub_program())
		
		# files.append(self.get_data_collection_project_is_related_to_data_collection_project())
		
		# files.append(self.get_data_collection_project_modality_classification())
		
		# files.append(self.get_data_collection_project_species_classification())
		# files.append(self.get_project())
		# files.append(self.get_data_collection_project_specimen_type_classification())
		# files.append(self.get_protocol())
		# files.append(self.get_data_collection_project_technique_classification())
		# files.append(self.get_protocol_is_about_data_collection_project())
		# files.append(self.get_data_collection_reported_specimen_count())
		# files.append(self.get_publication())
		# files.append(self.get_data_collection_species_classification())
		# files.append(self.get_publication_is_about_data_collection())
		# files.append(self.get_data_collection_specimen_type_classification())
		# files.append(self.get_publication_is_about_data_collection_project())
		# files.append(self.get_data_collection_technique_classification())
		
		# files.append(self.get_data_contributor_realized_in_data_collection_project())
		
		# files.append(self.get_data_creator_realized_in_data_collection_project())
		# files.append(self.get_sub_program_is_part_of_program())
		# files.append(self.get_data_publication_year_is_about_data_collection_project())
		
		# files.append(self.get_data_publisher_realized_in_data_collection_project())
		# files.append(self.get_web_resource())
		# files.append(self.get_grant())
		# files.append(self.get_web_resource_is_about_data_collection())


		data['files'] = files

		with open(file_path, 'w') as outfile:
			json.dump(data, outfile, indent=2)


	# def get_contact_person_realized_in_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'contact_person_realized_in_data_collection_project.csv')

	# def get_grant_is_specified_input_of_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'grant_is_specified_input_of_data_collection_project.csv')

	def get_data_collection(self):

		subject = 'data_collection'
		schema = ['data_collection_reference_id','data_collection_title','data_collection_short_title','data_collection_description','categorical_access_control_state','categorical_completion_state','last_updated_at_date']

		select_clause, shape_clause, where_clause = IngestLib.data_template_helper(subject, schema, self.ingest_prefix)

		return IngestLib.create_data_template_validation(True, 'csv', self.ingest_prefix, 'data_collection.csv', select_clause, shape_clause, where_clause, schema)

	# def get_highlighted_web_resource_is_about_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'highlighted_web_resource_is_about_data_collection_project.csv')

	# def get_data_collection_is_specified_output_of_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_is_specified_output_of_data_collection_project.csv')

	# def get_license(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'license.csv')

	# def get_data_collection_modality_classification(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_modality_classification.csv')

	# def get_license_is_about_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'license_is_about_data_collection_project.csv')

	# def get_data_collection_project_is_part_of_sub_program(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_project_is_part_of_sub_program.csv')

	def get_modality(self):
		subject = 'modality'
		schema = ['modality_name']

		select_clause, shape_clause, where_clause = IngestLib.data_template_helper(subject, schema, self.ingest_prefix)

		return IngestLib.create_data_template_validation(True, 'csv', self.ingest_prefix, 'modality.csv', select_clause, shape_clause, where_clause, schema)

	# def get_data_collection_project_is_related_to_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_project_is_related_to_data_collection_project.csv')

	def get_organization(self):
		subject = 'organization'
		schema = ['organization_reference_id','organization_name','ror_symbol']

		select_clause, shape_clause, where_clause = IngestLib.data_template_helper(subject, schema, self.ingest_prefix)

		return IngestLib.create_data_template_validation(True, 'csv', self.ingest_prefix, 'organization.csv', select_clause, shape_clause, where_clause, schema)

	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'organization.csv')

	# def get_data_collection_project_modality_classification(self): 
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_project_modality_classification.csv')

	def get_person(self):

		subject = 'person'
		schema = ['person_reference_id','person_name','person_given_name','person_family_name','orcid_symbol']

		select_clause, shape_clause, where_clause = IngestLib.data_template_helper(subject, schema, self.ingest_prefix)

		return IngestLib.create_data_template_validation(True, 'csv', self.ingest_prefix, 'person.csv', select_clause, shape_clause, where_clause, schema)

	# def get_data_collection_project_species_classification(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_project_species_classification.csv')

	# def get_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'project.csv')

	# def get_data_collection_project_specimen_type_classification(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_project_specimen_type_classification.csv')

	# def get_protocol(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'protocol.csv')

	# def get_data_collection_project_technique_classification(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_project_technique_classification.csv')

	# def get_protocol_is_about_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'protocol_is_about_data_collection_project.csv')

	# def get_data_collection_reported_specimen_count(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_reported_specimen_count.csv')

	# def get_publication(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'publication.csv')

	# def get_data_collection_species_classification(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_species_classification.csv')

	# def get_publication_is_about_data_collection(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'publication_is_about_data_collection.csv')

	# def get_data_collection_specimen_type_classification(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_specimen_type_classification.csv')

	# def get_publication_is_about_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'publication_is_about_data_collection_project.csv')

	# def get_data_collection_technique_classification(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_collection_technique_classification.csv')


	def get_species(self):
		subject = 'species'
		schema = ['species_name']

		select_clause, shape_clause, where_clause = IngestLib.data_template_helper(subject, schema, self.ingest_prefix)

		return IngestLib.create_data_template_validation(True, 'csv', self.ingest_prefix, 'species.csv', select_clause, shape_clause, where_clause, schema)

	# def get_data_contributor_realized_in_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_contributor_realized_in_data_collection_project.csv')

	def get_specimen_type(self):
		subject = 'specimen_type'
		schema = ['specimen_type_name']

		select_clause, shape_clause, where_clause = IngestLib.data_template_helper(subject, schema, self.ingest_prefix)

		return IngestLib.create_data_template_validation(True, 'csv', self.ingest_prefix, 'specimen_type.csv', select_clause, shape_clause, where_clause, schema)

	# def get_data_creator_realized_in_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_creator_realized_in_data_collection_project.csv')

	# def get_sub_program_is_part_of_program(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'sub_program_is_part_of_program.csv')

	# def get_data_publication_year_is_about_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_publication_year_is_about_data_collection_project.csv')

	def get_technique(self):
		subject = 'technique'
		schema = ['technique_name']

		select_clause, shape_clause, where_clause = IngestLib.data_template_helper(subject, schema, self.ingest_prefix)

		return IngestLib.create_data_template_validation(True, 'csv', self.ingest_prefix, 'technique.csv', select_clause, shape_clause, where_clause, schema)

	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'technique.csv')

	# def get_data_publisher_realized_in_data_collection_project(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'data_publisher_realized_in_data_collection_project.csv')

	# def get_web_resource(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'web_resource.csv')

	# def get_grant(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'grant.csv')

	# def get_web_resource_is_about_data_collection(self):
	# 	return IngestLib.create_template_validation(True, 'csv', 'data', self.ingest_prefix, 'web_resource_is_about_data_collection.csv')