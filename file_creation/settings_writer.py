# #!/usr/bin/env python

import json

class SettingsWriter(object):
	@staticmethod
	def write_settings_file(file_path):
		ingest_prefix = 'di:'

		prefixes = ['PREFIX '+ str(ingest_prefix) +' <http:/data_ingest.brain-map.org/>', 'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>']

		settings = {}
		settings['blaze_graph_server'] = 'http://192.168.1.102:9999/blazegraph/sparql'
		settings['data_inject_directory'] = './data_injects'
		settings['ingest_prefixes'] = ' '.join(prefixes)
		settings['ingest_prefix'] = ingest_prefix
		settings['subject_start'] = 'http://data_ingest/'
		settings['uid_service'] = 'http://127.0.0.1:8000/development/uid_service?'

		with open(file_path, 'w') as outfile:
			json.dump(settings, outfile, indent=2)
