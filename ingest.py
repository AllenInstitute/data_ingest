#!/usr/bin/env python
import os
import sys
import glob
from graph_builder import *

GRAPH_FOLDER_PATH = 'source/graphs'
CSV_FOLDER_PATH = 'source/project_inventory'

def get_csv_files():
	join_csv_files = []
	csv_files = []

	#get the csv files
	path = os.path.join(CSV_FOLDER_PATH, '*' + str('.csv'))

	#seperate the csv file into normal files and join files
	for csv_file in glob.glob(path):
		file_name = os.path.basename(csv_file)

		if '_' in file_name:
			join_csv_files.append(csv_file)
		else:
			csv_files.append(csv_file)


	return csv_files, join_csv_files

def main():
	print("Running ingest")

	csv_files, join_csv_files = get_csv_files()

	graph_builder = GraphBuilder(csv_files, join_csv_files, GRAPH_FOLDER_PATH)

	# for csv_file in csv_files:
	# 	csv_handler(csv_file)
	# print(files)

	print('finished...')

if __name__ == "__main__":
	main()
