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

settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
settings = IngestLib.get_json_data_from_file(settings_file)
blaze_graph = BlazeGraph(settings)

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


	uids = request.args.get('uids')
	if uids is not None:
		uids = uids.split(',')

	schema = ['subject', 'file_record_uids', 'uid', 'name', 'status', 'uploader', 'description', 'locked', 'storage_directory', 'template', 'created_at', 'uploaded_at']

	ingests = blaze_graph.get_data_for_page(schema, 'ingests', uids)

	results_length = len(ingests)
	number_label = get_results_label(results_length)

	return flask.render_template('ingests.html', title='Ingests', ingests=ingests, schema=schema, results_length=results_length, number_label=number_label)

@APP.route('/name_spaces.html')
def name_spaces():

	name_spaces = blaze_graph.find_distinct_objects_by_predicate(settings['ingest_prefix'], 'name_space')

	results_length = len(name_spaces)
	number_label = get_results_label(results_length)

	return flask.render_template('name_spaces.html', title='name_spaces', name_spaces=name_spaces, results_length=results_length, number_label=number_label)

@APP.route('/uploaders.html')
@APP.route('/uploaders')
def uploaders():
	uids = request.args.get('uids')

	if uids is not None:
		uids = uids.split(',')

	schema = ['uid', 'name', 'role']

	uploaders = blaze_graph.get_data_for_page(schema, 'uploaders', uids)

	results_length = len(uploaders)
	number_label = get_results_label(results_length)

	return flask.render_template('uploaders.html', title='Uploaders', uploaders=uploaders, schema=schema, results_length=results_length, number_label=number_label)

@APP.route('/tabular.html')
def tabular():

	name_spaces = request.args.get('name_spaces')

	if name_spaces is not None:
		name_spaces = name_spaces.split(',')
	else:
		name_spaces = blaze_graph.find_distinct_objects_by_predicate(settings['ingest_prefix'], 'name_space')

	tabular_results = []

	results_length = 0
	number_of_name_spaces = len(name_spaces)
	

	for name_space in name_spaces:
		tabular_result = {}
		tabular_result['name_space'] = name_space

		schema, rows = blaze_graph.get_tabular_data(name_space)

		tabular_result['schema'] = schema
		tabular_result['rows'] = rows

		results_length+=len(rows)

		tabular_results.append(tabular_result)

	number_label = get_results_label(results_length)

	return flask.render_template('tabular.html', title='Tabular', tabular_results=tabular_results, results_length=results_length, number_label=number_label, number_of_name_spaces=number_of_name_spaces)

@APP.route('/triples.html')
def triples():

	triples = blaze_graph.get_all_triples()
	results_length = len(triples)
	schema = ['subject',  'predicate', 'object']
	number_label = get_results_label(results_length)


	return flask.render_template('triples.html', title='Triples', triples=triples, schema=schema, results_length=results_length, number_label=number_label)

@APP.route('/subject')
def subject():
	subject = request.args.get('subject')

	schema = ['subject', 'predicate', 'object']

	missing_subject = (subject == None)
	triples = []
	triples_data = []

	if not missing_subject:
		full_subject = IngestLib.add_prefix(blaze_graph.subject_start + blaze_graph.ingest_prefix, subject)
		triples = blaze_graph.find_all_by_subject(full_subject)


		if len(triples) == 0:
			full_subject = subject
			triples = blaze_graph.find_all_by_subject(full_subject)

			index = 0
			for triple in triples:
				triples_data.append([triple, 'triple_' + str(index)])
				index+=1


		results_length = len(triples)
		number_label = get_results_label(results_length)



	return flask.render_template('subject.html', title='Subject', subject=subject, missing_subject=missing_subject, triples_data=triples_data, results_length=results_length, number_label=number_label, schema=schema, full_subject=full_subject)

@APP.route('/query')
def query():
	return flask.render_template('query.html', title='Query')

@APP.route('/file_records.html')
def file_records():
	schema = []
	rows = []

	uids = request.args.get('uids')
	if uids is not None:
		uids = uids.split(',')
		schema, rows = blaze_graph.get_by_uids(uids)


	results_length = len(rows)
	number_label = get_results_label(results_length)

	return flask.render_template('file_records.html', title='File Records', results_length=results_length, number_label=number_label, schema=schema, rows=rows)

if __name__ == '__main__':
    APP.debug=True
    # APP.run()
    APP.run(host='0.0.0.0')