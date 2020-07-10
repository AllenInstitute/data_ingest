from validation import *
from ingest_lib import *



class Ingest(object):
	def __init__(self, user, ingest_selection, zip_file):
		self.settings = IngestLib.get_settings()

		self.validation = Validation(self.settings)
		self.validation.run_default_validation(user, ingest_selection, zip_file)