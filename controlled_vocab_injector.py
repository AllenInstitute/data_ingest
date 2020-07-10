#!/usr/bin/env python
import glob
import os
from blaze_graph import *
from file_creation.controlled_vocab_writer import *
from file_creation.settings_writer import *

CONTROLLED_VOCABULARY_FOLDER = './controlled_vocabulary'
SETTINGS_FOLDER = './settings'
SETTINGS_FILE = 'settings.json'
CONTROLLED_VOCAB = 'controlled_vocab.json'
CONTROLLED_VOCAB_TEMPLATE = 'controlled_vocab_template.json'
CONTROLLED_VOCAB_EXTRA_FIELDS = 'controlled_vocab_extra_fields.json'
CONTROLLED_VOCAB_GLOBAL_EXTRA_FIELDS = 'controlled_vocab_global_extra_fields.json'
TXT_FILES_PATH = './source/txt_files'
TXT_JOIN_FILES_PATH = './source/txt_file_joins'

from ingest_lib import *

def write_input_files(controlled_vocab, controlled_vocab_template, controlled_vocab_extra_fields, controlled_vocab_global_extra_fields, settings_file):
	SettingsWriter.write_settings_file(settings_file)

	settings = IngestLib.get_json_data_from_file(settings_file)

	controlled_vocab_writer = ControlledVocabWriter(settings['ingest_prefix'])
	controlled_vocab_writer.write_controlled_vocab(controlled_vocab, controlled_vocab_template, controlled_vocab_extra_fields, TXT_FILES_PATH, controlled_vocab_global_extra_fields, TXT_JOIN_FILES_PATH)

	return settings

#create the directores if needed
def create_directories(controlled_vocab_folder, settings_folder):
	if not os.path.isdir(controlled_vocab_folder):
		os.mkdir(controlled_vocab_folder)

	if not os.path.isdir(settings_folder):
		os.mkdir(settings_folder)

def main():
	create_directories(CONTROLLED_VOCABULARY_FOLDER, SETTINGS_FOLDER)

	controlled_vocab = os.path.join(CONTROLLED_VOCABULARY_FOLDER, CONTROLLED_VOCAB)
	controlled_vocab_template = os.path.join(CONTROLLED_VOCABULARY_FOLDER, CONTROLLED_VOCAB_TEMPLATE)
	controlled_vocab_extra_fields = os.path.join(CONTROLLED_VOCABULARY_FOLDER, CONTROLLED_VOCAB_EXTRA_FIELDS)
	controlled_vocab_global_extra_fields = os.path.join(CONTROLLED_VOCABULARY_FOLDER, CONTROLLED_VOCAB_GLOBAL_EXTRA_FIELDS)
	settings_file =  os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)

	settings = write_input_files(controlled_vocab, controlled_vocab_template, controlled_vocab_extra_fields, controlled_vocab_global_extra_fields, settings_file)

	blaze_graph = BlazeGraph(settings)
	data_inject_directory = settings['data_inject_directory']

	#clear the graph
	blaze_graph.delete_all_data()

	blaze_graph.insert_controlled_vocab(controlled_vocab, controlled_vocab_template, controlled_vocab_extra_fields, controlled_vocab_global_extra_fields)

	print('finished')


if __name__ == "__main__":
	main()
