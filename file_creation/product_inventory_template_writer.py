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
		files.append(self.get_web_resource())
		files.append(self.get_license())
		files.append(self.get_contact_person_realized_in_data_collection_project())
		files.append(self.get_grant_is_specified_input_of_data_collection_project())
		files.append(self.get_highlighted_web_resource_is_about_data_collection_project())
		files.append(self.get_data_collection_is_specified_output_of_data_collection_project())
		files.append(self.get_data_collection_modality_classification())
		files.append(self.get_license_is_about_data_collection_project())
		files.append(self.get_data_collection_project_is_part_of_sub_program())
		files.append(self.get_data_collection_project_is_related_to_data_collection_project())
		files.append(self.get_data_collection_project_modality_classification())
		files.append(self.get_data_collection_project_species_classification())
		files.append(self.get_project())
		files.append(self.get_data_collection_project_specimen_type_classification())
		files.append(self.get_protocol())
		files.append(self.get_data_collection_project_technique_classification())
		files.append(self.get_protocol_is_about_data_collection_project())
		files.append(self.get_data_collection_reported_specimen_count())
		files.append(self.get_publication())
		files.append(self.get_data_collection_species_classification())
		files.append(self.get_publication_is_about_data_collection())
		files.append(self.get_data_collection_specimen_type_classification())
		files.append(self.get_publication_is_about_data_collection_project())
		files.append(self.get_data_collection_technique_classification())
		files.append(self.get_data_contributor_realized_in_data_collection_project())
		files.append(self.get_data_creator_realized_in_data_collection_project())
		files.append(self.get_sub_program_is_part_of_program())
		files.append(self.get_data_publication_year_is_about_data_collection_project())
		files.append(self.get_data_publisher_realized_in_data_collection_project())
		files.append(self.get_grant())
		files.append(self.get_web_resource_is_about_data_collection())


		data['files'] = files

		with open(file_path, 'w') as outfile:
			json.dump(data, outfile, indent=2)


	def get_contact_person_realized_in_data_collection_project(self):

		subject = 'contact_person_realized_in_data_collection_project'
		schema = ['project_reference_id','person_reference_id','email_address','priority_order']
		joins = {}
		joins['project_reference_id'] =  {'reference_table':'project', 'predicate':'di:resoloved_project_reference_id',  'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['person_reference_id'] = {'reference_table':'person', 'predicate':'di:resolved_person_reference_id',  'table_column':'person_reference_id', 'reference_table_column':'person_reference_id', 'column_uid':'person_reference_uid'}

		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'contact_person_realized_in_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_grant_is_specified_input_of_data_collection_project(self):

		subject = 'grant_is_specified_input_of_data_collection_project'
		schema = ['project_reference_id','grant_reference_id','priority_order']
		primary_key = ''
		joins = {}
		joins['project_reference_id'] =  {'reference_table':'project', 'predicate':'di:resolved_project_reference_id',  'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['grant_reference_id'] = {'reference_table':'grant', 'predicate':'di:resolved_grant_reference_id',  'table_column':'grant_reference_id', 'reference_table_column':'grant_reference_id', 'column_uid':'grant_reference_uid'}

		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)
		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'grant_is_specified_input_of_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_collection(self):

		subject = 'data_collection'
		schema = ['data_collection_reference_id','data_collection_title','data_collection_short_title','data_collection_description','categorical_access_control_state','categorical_completion_state','last_updated_at_date']
		joins = {}
		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)

	def get_highlighted_web_resource_is_about_data_collection_project(self):

		subject = 'highlighted_web_resource_is_about_data_collection_project'
		schema = ['project_reference_id','web_resource_reference_id','priority_order']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['web_resource_reference_id'] = {'reference_table':'web_resource', 'predicate':'di:resolved_web_resource_reference_id', 'table_column':'web_resource_reference_id', 'reference_table_column':'web_resource_reference_id', 'column_uid':'web_resource_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'highlighted_web_resource_is_about_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_collection_is_specified_output_of_data_collection_project(self):

		subject = 'data_collection_is_specified_output_of_data_collection_project'
		schema = ['project_reference_id','data_collection_reference_id','priority_order']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['data_collection_reference_id'] = {'reference_table':'data_collection', 'predicate':'di:resolved_data_collection_reference_id', 'table_column':'data_collection_reference_id', 'reference_table_column':'data_collection_reference_id', 'column_uid':'data_collection_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_is_specified_output_of_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_license(self):
		subject = 'license'
		schema = ['license_reference_id','license_title','license_short_title','information_web_resource_reference_id']
		joins = {}
		joins['information_web_resource_reference_id'] = {'reference_table':'web_resource', 'predicate':'di:resolved_information_web_resource_reference_id', 'table_column':'information_web_resource_reference_id', 'reference_table_column':'web_resource_reference_id', 'column_uid':'information_web_resource_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'license.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)


	def get_data_collection_modality_classification(self):

		subject = 'data_collection_modality_classification'
		schema = ['data_collection_reference_id','modality_name']
		joins = {}
		joins['data_collection_reference_id'] = {'reference_table':'data_collection', 'predicate':'di:resolved_data_collection_reference_id', 'table_column':'data_collection_reference_id', 'reference_table_column':'data_collection_reference_id', 'column_uid':'data_collection_reference_uid'}

		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_modality_classification.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_license_is_about_data_collection_project(self):
		

		subject = 'license_is_about_data_collection_project'
		schema = ['project_reference_id','license_reference_id','priority_order']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['license_reference_id'] = {'reference_table':'license', 'predicate':'di:resolved_license_reference_id', 'table_column':'license_reference_id', 'reference_table_column':'license_reference_id', 'column_uid':'license_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'license_is_about_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_collection_project_is_part_of_sub_program(self):
		subject = 'data_collection_project_is_part_of_sub_program'
		schema = ['project_reference_id','sub_program_reference_id','priority_order']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['sub_program_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_sub_program_reference_id', 'table_column':'sub_program_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'sub_program_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_project_is_part_of_sub_program.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_modality(self):
		subject = 'modality'
		schema = ['modality_name']
		joins = {}
		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'modality.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_collection_project_is_related_to_data_collection_project(self):
		subject = 'data_collection_project_is_related_to_data_collection_project'
		schema = ['project_reference_id','related_project_reference_id','priority_order']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['related_project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_related_project_reference_id', 'table_column':'related_project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'subject_reference_uid'}

		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_project_is_related_to_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_organization(self):
		subject = 'organization'
		schema = ['organization_reference_id','organization_name','ror_symbol']
		joins = {}
		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'organization.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)

	def get_data_collection_project_modality_classification(self): 

		subject = 'data_collection_project_modality_classification'
		schema = ['project_reference_id','modality_name']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_project_modality_classification.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_person(self):

		subject = 'person'
		schema = ['person_reference_id','person_name','person_given_name','person_family_name','orcid_symbol']
		joins = {}
		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'person.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)

	def get_data_collection_project_species_classification(self):

		subject = 'data_collection_project_species_classification'
		schema = ['project_reference_id','species_name']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_project_species_classification.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_project(self):
		subject = 'project'
		schema = ['project_reference_id','project_title','project_short_title','project_description','project_type','doi_symbol','project_citation','information_web_resource_reference_id']
		joins = {}
		joins['information_web_resource_reference_id'] =  {'reference_table':'web_resource', 'predicate':'di:resolved_information_web_resource_reference_id',  'table_column':'information_web_resource_reference_id', 'reference_table_column':'web_resource_reference_id', 'column_uid':'web_resource_reference_uid'}

		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)
		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)

	def get_data_collection_project_specimen_type_classification(self):

		subject = 'data_collection_project_specimen_type_classification'
		schema = ['project_reference_id','specimen_type_name']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_project_specimen_type_classification.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_protocol(self):

		subject = 'protocol'
		schema = ['protocol_reference_id','protocol_title','protocol_short_title','view_web_resource_reference_id']
		joins = {}
		joins['view_web_resource_reference_id'] = {'reference_table':'web_resource', 'predicate':'di:resolved_view_web_resource_reference_id', 'table_column':'view_web_resource_reference_id', 'reference_table_column':'web_resource_reference_id', 'column_uid':'web_resource_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'protocol.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)

	def get_data_collection_project_technique_classification(self):

		subject = 'data_collection_project_technique_classification'
		schema = ['project_reference_id','technique_name']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_project_technique_classification.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_protocol_is_about_data_collection_project(self):
		subject = 'protocol_is_about_data_collection_project'
		schema = ['project_reference_id','protocol_reference_id','priority_order']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['protocol_reference_id'] = {'reference_table':'protocol', 'predicate':'di:resolved_protocol_reference_id', 'table_column':'protocol_reference_id', 'reference_table_column':'protocol_reference_id', 'column_uid':'protocol_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'protocol_is_about_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_collection_reported_specimen_count(self):
		subject = 'data_collection_reported_specimen_count'
		schema = ['data_collection_reference_id','specimen_type_name','reported_count']
		joins = {}
		joins['data_collection_reference_id'] = {'reference_table':'data_collection', 'predicate':'di:resolved_data_collection_reference_id', 'table_column':'data_collection_reference_id', 'reference_table_column':'data_collection_reference_id', 'column_uid':'data_collection_reference_uid'}

		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_reported_specimen_count.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_publication(self):
		subject = 'publication'
		schema = ['publication_reference_id','publication_title','publication_year','first_author_reference_id','doi_symbol','pubmed_id']
		joins = {}
		joins['first_author_reference_id'] = {'reference_table':'person', 'predicate':'di:resolved_first_author_reference_id', 'table_column':'first_author_reference_id', 'reference_table_column':'person_reference_id', 'column_uid':'person_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'publication.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)

	def get_data_collection_species_classification(self):
		subject = 'data_collection_species_classification'
		schema = ['data_collection_reference_id','species_name']
		joins = {}
		joins['data_collection_reference_id'] = {'reference_table':'data_collection', 'predicate':'di:resolved_data_collection_reference_id', 'table_column':'data_collection_reference_id', 'reference_table_column':'data_collection_reference_id', 'column_uid':'data_collection_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_species_classification.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_publication_is_about_data_collection(self):

		subject = 'publication_is_about_data_collection'
		schema = ['data_collection_reference_id','publication_reference_id','priority_order']
		joins = {}
		joins['data_collection_reference_id'] = {'reference_table':'data_collection', 'predicate':'di:resolved_data_collection_reference_id', 'table_column':'data_collection_reference_id', 'reference_table_column':'data_collection_reference_id', 'column_uid':'data_collection_reference_uid'}
		joins['publication_reference_id'] = {'reference_table':'publication', 'predicate':'di:resolved_publication_reference_id', 'table_column':'publication_reference_id', 'reference_table_column':'publication_reference_id', 'column_uid':'publication_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'publication_is_about_data_collection.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_collection_specimen_type_classification(self):

		subject = 'data_collection_specimen_type_classification'
		schema = ['data_collection_reference_id','specimen_type_name']
		joins = {}
		joins['data_collection_reference_id'] = {'reference_table':'data_collection', 'predicate':'di:resolved_data_collection_reference_id', 'table_column':'data_collection_reference_id', 'reference_table_column':'data_collection_reference_id', 'column_uid':'data_collection_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_specimen_type_classification.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_publication_is_about_data_collection_project(self):

		subject = 'publication_is_about_data_collection_project'
		schema = ['project_reference_id','publication_reference_id','priority_order']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['publication_reference_id'] = {'reference_table':'publication', 'predicate':'di:resolved_publication_reference_id', 'table_column':'publication_reference_id', 'reference_table_column':'publication_reference_id', 'column_uid':'publication_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'publication_is_about_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_collection_technique_classification(self):

		subject = 'data_collection_technique_classification'
		schema = ['data_collection_reference_id','technique_name']
		joins = {}
		joins['data_collection_reference_id'] = {'reference_table':'data_collection', 'predicate':'di:resolved_data_collection_reference_id', 'table_column':'data_collection_reference_id', 'reference_table_column':'data_collection_reference_id', 'column_uid':'data_collection_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_collection_technique_classification.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)


	def get_species(self):
		subject = 'species'
		schema = ['species_name']
		joins = {}
		join_names = {}
		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'species.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)

	def get_data_contributor_realized_in_data_collection_project(self):

		subject = 'data_contributor_realized_in_data_collection_project'
		schema = ['project_reference_id','person_reference_id','organization_reference_id','agent_type','priority_order']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['person_reference_id'] = {'reference_table':'person', 'predicate':'di:resolved_person_reference_id', 'table_column':'person_reference_id', 'reference_table_column':'person_reference_id', 'column_uid':'person_reference_uid'}
		joins['organization_reference_id'] = {'reference_table':'organization', 'predicate':'di:resolved_organization_reference_id', 'table_column':'organization_reference_id', 'reference_table_column':'organization_reference_id', 'column_uid':'organization_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_contributor_realized_in_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_specimen_type(self):
		subject = 'specimen_type'
		schema = ['specimen_type_name']
		joins = {}
		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'specimen_type.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_creator_realized_in_data_collection_project(self):

		subject = 'data_creator_realized_in_data_collection_project'
		schema = ['project_reference_id','person_reference_id','organization_reference_id','agent_type','priority_order']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['person_reference_id'] = {'reference_table':'person', 'predicate':'di:resolved_person_reference_id', 'table_column':'person_reference_id', 'reference_table_column':'person_reference_id', 'column_uid':'person_reference_uid'}
		joins['organization_reference_id'] = {'reference_table':'organization', 'predicate':'di:resolved_organization_reference_id', 'table_column':'organization_reference_id', 'reference_table_column':'organization_reference_id', 'column_uid':'organization_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_creator_realized_in_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_sub_program_is_part_of_program(self):

		subject = 'sub_program_is_part_of_program'
		schema = ['sub_program_reference_id','program_reference_id']
		joins = {}
		joins['sub_program_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_sub_program_reference_id', 'table_column':'sub_program_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'sub_program_reference_uid'}
		joins['program_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_program_reference_id', 'table_column':'program_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'program_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'sub_program_is_part_of_program.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_publication_year_is_about_data_collection_project(self):

		subject = 'data_publication_year_is_about_data_collection_project'
		schema = ['project_reference_id','data_publication_year']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_publication_year_is_about_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_technique(self):
		subject = 'technique'
		schema = ['technique_name']
		joins = {}
		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'technique.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_data_publisher_realized_in_data_collection_project(self):

		subject = 'data_publisher_realized_in_data_collection_project'
		schema = ['project_reference_id','organization_reference_id']
		joins = {}
		joins['project_reference_id'] = {'reference_table':'project', 'predicate':'di:resolved_project_reference_id', 'table_column':'project_reference_id', 'reference_table_column':'project_reference_id', 'column_uid':'project_reference_uid'}
		joins['organization_reference_id'] = {'reference_table':'organization', 'predicate':'di:resolved_organization_reference_id', 'table_column':'organization_reference_id', 'reference_table_column':'organization_reference_id', 'column_uid':'organization_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'data_publisher_realized_in_data_collection_project.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_web_resource(self):
		subject = 'web_resource'
		schema = ['web_resource_reference_id','web_resource_type','web_resource_title','web_resource_short_title','universal_resource_locator']
		joins = {}
		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'web_resource.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)

	def get_web_resource_is_about_data_collection(self):
		subject = 'web_resource_is_about_data_collection'
		schema = ['data_collection_reference_id','web_resource_reference_id','priority_order']
		joins = {}
		joins['data_collection_reference_id'] = {'reference_table':'data_collection', 'predicate':'di:resolved_data_collection_reference_id', 'table_column':'data_collection_reference_id', 'reference_table_column':'data_collection_reference_id', 'column_uid':'data_collection_reference_uid'}
		joins['web_resource_reference_id'] = {'reference_table':'web_resource', 'predicate':'di:resolved_web_resource_reference_id', 'table_column':'web_resource_reference_id', 'reference_table_column':'web_resource_reference_id', 'column_uid':'web_resource_reference_uid'}


		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'web_resource_is_about_data_collection.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, False)

	def get_grant(self):
		subject = 'grant'
		schema = ['grant_reference_id','grant_title','grant_identifier','funding_agency_reference_id','awardee_reference_id','report_symbol']
		joins = {}
		joins['awardee_reference_id'] = {'reference_table':'organization', 'predicate':'di:resolved_awardee_reference_id', 'table_column':'awardee_reference_id', 'reference_table_column':'organization_reference_id', 'column_uid':'awardee_reference_uid'}
		joins['funding_agency_reference_id'] = {'reference_table':'organization', 'predicate':'di:resolved_funding_agency_reference_id', 'table_column':'funding_agency_reference_id', 'reference_table_column':'organization_reference_id', 'column_uid':'funding_agency_reference_uid'}

		select_clause, shape_clause, where_clause, extra_joins = IngestLib.data_template_helper(subject, schema, self.ingest_prefix, joins)

		return IngestLib.create_data_template_validation(subject, True, 'csv', self.ingest_prefix, 'grant.csv', select_clause, shape_clause, where_clause, schema, joins, extra_joins, True)