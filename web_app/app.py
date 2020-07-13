#!/usr/bin/env python
import sys
import flask


SETTINGS_FOLDER = '../settings'
SETTINGS_FILE = 'settings.json'
sys.path.append("../")
from blaze_graph import *

# Create the application.
APP = flask.Flask(__name__)

@APP.route('/')
@APP.route('/index.html')
def index():
	return flask.render_template('index.html', title='Index')

@APP.route('/ingests.html')
def ingests():
	settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
	settings = IngestLib.get_json_data_from_file(settings_file)
	blaze_graph = BlazeGraph(settings)

	schema = ['uid', 'name', 'status', 'uploader', 'description', 'locked', 'storage_directory', 'template']

	ingests = blaze_graph.get_data_for_page(schema, 'ingestions')

	return flask.render_template('ingests.html', title='Ingests', ingests=ingests, schema=schema)

@APP.route('/tables.html')
def tables():
	settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
	settings = IngestLib.get_json_data_from_file(settings_file)
	blaze_graph = BlazeGraph(settings)

	tables = blaze_graph.find_distinct_objects_by_predicate(settings['ingest_prefix'], 'table_name')

	print('tables', tables)

	return flask.render_template('tables.html', title='Tables', tables=tables)

@APP.route('/uploaders.html')
def uploaders():
	settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
	settings = IngestLib.get_json_data_from_file(settings_file)
	blaze_graph = BlazeGraph(settings)

	schema = ['uid', 'name', 'role']

	uploaders = blaze_graph.get_data_for_page(schema, 'uploaders')

	return flask.render_template('uploaders.html', title='Uploaders', uploaders=uploaders, schema=schema)

@APP.route('/tabular.html')
def tabular():
	return flask.render_template('tabular.html', title='Tabular')

@APP.route('/triples.html')
def triples():
	return flask.render_template('triples.html', title='Triples')

@APP.route('/query.html')
def query():
	return flask.render_template('query.html', title='Query')

if __name__ == '__main__':
    APP.debug=True
    APP.run()