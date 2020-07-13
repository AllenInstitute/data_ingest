from ingest import *

def main():
	print("running test ingest")

	# blazegraph = BlazeGraph()

	user = 'Nathan'
	ingest_selection = 'test_ingest'
	zip_file = 'temp.zip'

	ingest = Ingest(user, ingest_selection, zip_file)

	print('finished...')

if __name__ == "__main__":
	main()
