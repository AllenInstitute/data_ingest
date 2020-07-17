#!/usr/bin/env python
import sys
import flask
from flask import request


SETTINGS_FOLDER = '../settings'
SETTINGS_FILE = 'settings.json'
SINGLE_RESULT = 1

sys.path.append("../")
from blaze_graph import *

# Create the application.
APP = flask.Flask(__name__)

@APP.route('/')
@APP.route('/index.html')
def index():
	return flask.render_template('index.html', title='Index')

def get_results_label(results_length):
	number_label = 'results'

	if results_length == SINGLE_RESULT:
		number_label = 'result'

	return number_label

@APP.route('/ingests.html')
def ingests():
	settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
	settings = IngestLib.get_json_data_from_file(settings_file)
	blaze_graph = BlazeGraph(settings)

	uids = request.args.get('uids')
	if uids is not None:
		uids = uids.split(',')

	schema = ['subject', 'uid', 'name', 'status', 'uploader', 'description', 'locked', 'storage_directory', 'template']

	ingests = blaze_graph.get_data_for_page(schema, 'ingests', uids)

	results_length = len(ingests)
	number_label = get_results_label(results_length)

	return flask.render_template('ingests.html', title='Ingests', ingests=ingests, schema=schema, results_length=results_length, number_label=number_label)

@APP.route('/tables.html')
def tables():
	settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
	settings = IngestLib.get_json_data_from_file(settings_file)
	blaze_graph = BlazeGraph(settings)

	tables = blaze_graph.find_distinct_objects_by_predicate(settings['ingest_prefix'], 'table_name')

	results_length = len(tables)
	number_label = get_results_label(results_length)

	return flask.render_template('tables.html', title='Tables', tables=tables, results_length=results_length, number_label=number_label)

@APP.route('/uploaders.html')
@APP.route('/uploaders')
def uploaders():
	uids = request.args.get('uids')

	if uids is not None:
		uids = uids.split(',')
	settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
	settings = IngestLib.get_json_data_from_file(settings_file)
	blaze_graph = BlazeGraph(settings)

	schema = ['uid', 'name', 'role']

	uploaders = blaze_graph.get_data_for_page(schema, 'uploaders', uids)

	results_length = len(uploaders)
	number_label = get_results_label(results_length)

	return flask.render_template('uploaders.html', title='Uploaders', uploaders=uploaders, schema=schema, results_length=results_length, number_label=number_label)

@APP.route('/tabular.html')
def tabular():
	settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
	settings = IngestLib.get_json_data_from_file(settings_file)
	blaze_graph = BlazeGraph(settings)

	table_names = request.args.get('table_names')

	if table_names is not None:
		table_names = table_names.split(',')
	else:
		table_names = blaze_graph.find_distinct_objects_by_predicate(settings['ingest_prefix'], 'table_name')

	tabular_results = []

	results_length = 0
	number_of_tables = len(table_names)
	

	for table_name in table_names:
		tabular_result = {}
		tabular_result['table_name'] = table_name

		schema, rows = blaze_graph.get_tabular_data(table_name)

		tabular_result['schema'] = schema
		tabular_result['rows'] = rows

		results_length+=len(rows)

		tabular_results.append(tabular_result)

	number_label = get_results_label(results_length)
	print("tabular_results[0]['table_name']", tabular_results[0]['table_name'])
	
	# print('tabular_results[0]["table_name"]', tabular_results[0]["table_name"])
	# print('tabular_results[0]["schema"]', tabular_results[0]["schema"])
	print('tabular_results[0]["rows"]', tabular_results[0]["rows"])

	return flask.render_template('tabular.html', title='Tabular', tabular_results=tabular_results, results_length=results_length, number_label=number_label, number_of_tables=number_of_tables)

@APP.route('/triples.html')
def triples():
	settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
	settings = IngestLib.get_json_data_from_file(settings_file)
	blaze_graph = BlazeGraph(settings)

	triples = blaze_graph.get_all_triples()
	results_length = len(triples)
	schema = ['subject',  'predicate', 'object']
	number_label = get_results_label(results_length)

	print('number_label', number_label)


	return flask.render_template('triples.html', title='Triples', triples=triples, schema=schema, results_length=results_length, number_label=number_label)

@APP.route('/subject')
def subject():
	subject = request.args.get('subject')

	schema = ['subject', 'predicate', 'object']

	missing_subject = (subject == None)
	triples = []

	if not missing_subject:
		settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
		settings = IngestLib.get_json_data_from_file(settings_file)
		blaze_graph = BlazeGraph(settings)
		full_subject = IngestLib.add_prefix(blaze_graph.subject_start + blaze_graph.ingest_prefix, subject)
		triples = blaze_graph.find_all_by_subject(full_subject)

		if len(triples) == 0:
			full_subject = subject
			triples = blaze_graph.find_all_by_subject(full_subject)


		results_length = len(triples)
		number_label = get_results_label(results_length)

	return flask.render_template('subject.html', title='Subject', subject=subject, missing_subject=missing_subject, triples=triples, results_length=results_length, number_label=number_label, schema=schema, full_subject=full_subject)

@APP.route('/query.html')
def query():
	return flask.render_template('query.html', title='Query')

if __name__ == '__main__':
    APP.debug=True
    APP.run()