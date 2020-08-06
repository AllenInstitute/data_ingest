

class CsvIngest(object):
	def __init__(self, subject, required, file_type, data_type, file_name, select_clause, shape_clause, where_clause, schema, joins, extra_joins):
		self.subject = subject
		self.required = required
		self.file_type = file_type
		self.data_type = data_type
		self.file_name = file_name
		self.select_clause = select_clause
		self.shape_clause = shape_clause
		self.where_clause = where_clause
		self.schema = schema
		self.joins = joins
		self.extra_joins = extra_joins

		self.dependencies = []

		for join in list(self.joins.keys()):
			self.dependencies.append(self.joins[join]['reference_table'])

	def get_missing_dependencies(self, missing_dependencies, prev_ingests):
		for dependency in self.dependencies:
			if dependency not in prev_ingests:
				missing_dependencies[dependency] = True

		return missing_dependencies

	def get_join_reference_table(self, join):

		return self.joins[join]['reference_table']

	def get_join_predicate(self, join):

		return self.joins[join]['predicate']

	def get_join_reference_table_column(self, join):

		return self.joins[join]['reference_table_column']

	def get_join_column_uid(self, join):

		return self.joins[join]['column_uid']

	def get_join_table_column(self, join):

		return self.joins[join]['table_column']
		
	def meets_dependencies(self, prev_ingests):
		result = True

		for dependency in self.dependencies:
			if dependency not in prev_ingests:
				result = False
				break

		return result