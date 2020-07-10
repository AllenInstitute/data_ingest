#!/usr/bin/env python
import glob
import os
from blaze_graph import *
from file_creation.controlled_vocab_writer import *
from file_creation.settings_writer import *

SETTINGS_FILE = './settings/settings.json'
UPLOADERS_FILE = './controlled_vocabulary/controlled_vocab.json'
CONTROLLED_VOCAB_TEMPLATE = './controlled_vocabulary/controlled_vocab_template.json'
CONTROLLED_VOCAB_EXTRA_FIELDS = './controlled_vocabulary/controlled_vocab_extra_fields.json'
CONTROLLEDL_VOCAB_GLOBAL_EXTRA_FIELDS = './controlled_vocabulary/controlled_vocab_global_extra_fields.json'
TXT_FILES_PATH = './source/txt_files'
TXT_JOIN_FILES_PATH = './source/txt_file_joins'

from ingest_lib import *

def write_input_files():
	SettingsWriter.write_settings_file(SETTINGS_FILE)

	settings = IngestLib.get_settings()

	controlled_vocab_writer = ControlledVocabWriter(settings['ingest_prefix'])
	controlled_vocab_writer.write_controlled_vocab(UPLOADERS_FILE, CONTROLLED_VOCAB_TEMPLATE, CONTROLLED_VOCAB_EXTRA_FIELDS, TXT_FILES_PATH, CONTROLLEDL_VOCAB_GLOBAL_EXTRA_FIELDS, TXT_JOIN_FILES_PATH)

	return settings

def main():
	settings = write_input_files()

	

	blaze_graph = BlazeGraph(settings)
	data_inject_directory = settings['data_inject_directory']

	#clear the graph
	blaze_graph.delete_all_data()

	blaze_graph.insert_controlled_vocab(UPLOADERS_FILE, CONTROLLED_VOCAB_TEMPLATE, CONTROLLED_VOCAB_EXTRA_FIELDS, CONTROLLEDL_VOCAB_GLOBAL_EXTRA_FIELDS)

	print('finished')


if __name__ == "__main__":
	main()
