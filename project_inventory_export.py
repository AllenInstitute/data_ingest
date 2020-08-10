from export import *

def main():
	template = '/scratch/allen/data_ingest/templates/project_inventory.json'
	output_directory = '/scratch/allen/storage_directory/project_inventory_output'

	Export(template, output_directory)

if __name__ == "__main__":
	main()
