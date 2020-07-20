class Triple(object):
	def __init__(self, subject, predicate, object_value):
		self.subject = subject
		self.predicate = predicate
		self.object = object_value

	def __str__(self):
		return '(' + str(self.subject) + ', ' + str(self.predicate) + ", " +  str(self.object) + ')'

	def get_object_for_insert(self):
		# if isinstance(self.object, str):
		# 	return '"' + self.object + '"'
		# else:
		# 	return self.object

		return '"' + str(self.object) + '"'