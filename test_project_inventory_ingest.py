from ingest import *
from ingest_lib import *
from file_creation.product_inventory_template_writer import *

SETTINGS_FOLDER = './settings'
SETTINGS_FILE = 'settings.json'

def main():
	print("running test ingest")

	settings_file = os.path.join(SETTINGS_FOLDER, SETTINGS_FILE)
	settings = IngestLib.get_json_data_from_file(settings_file)

	zip_file = '/scratch/allen/metadata/data_inventory_test_data_20200727.zip'
	template = '/scratch/allen/data_ingest/templates/project_inventory.json'

	print('writing template', template)

	ProjectInventoryTemplateWriter(template, settings['ingest_prefix'])

	blaze_graph = BlazeGraph(settings)

	uploader_uid = blaze_graph.get_uploader_uid_by_name('Nathan Sjoquist')
	ingest_uid = blaze_graph.get_ingest_uid_by_name('test project inventory metadata')

	Ingest(uploader_uid, ingest_uid, zip_file)

	print('finished...')

if __name__ == "__main__":
	main()
