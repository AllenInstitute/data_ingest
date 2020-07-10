class Triple(object):
	def __init__(self, subject, predicate, object_value):
		self.subject = subject
		self.predicate = predicate
		self.object = object_value

	def __str__(self):
		return '(' + str(self.subject) + ', ' + str(self.predicate) + ", " +  str(self.object) + ')'


