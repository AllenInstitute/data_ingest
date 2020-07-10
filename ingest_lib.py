import os
import json

SETTINGS_FILE = './settings/settings.json'

class IngestLib(object):
	@staticmethod
	def get_filename_without_extension(filename):
		return os.path.splitext(os.path.basename(filename))[0]

	@staticmethod
	def add_prefix(prefix, value):
		return (prefix + str(value))

	@staticmethod
	def parse_line(line, line_number, txt_file):
		results = []

		first_character = True
		in_quote = False
		add_item = False
		in_number = False

		# print('line', line)

		item = ''

		for char_number in range(len(line)):
			character = line[char_number]

			# print('character', character)

			next_character = None
			is_comma = (character == ',')
			is_quote = (character == '"')
			is_digit = (character.isdigit())


			if (char_number + 1) != len(line):
				next_character = line[char_number + 1]

			if add_item:
				results.append(item)
				item = ''
				in_quote = False
				in_number = False
				first_character = True
				add_item = False

				if next_character is not None and next_character == ',':
					add_item = True
				elif is_comma and next_character is None:
					results.append(item)

			#if is first charact in an item
			elif first_character:
				first_character = False

				if is_quote:
					in_quote = True
					first_item = False
				elif is_digit:
					in_number = True
					item+=character

				elif is_comma:
					# print('adding comma')

					results.append(item)
					first_character = True
				else:
					
					raise Exception('Error expected either quote or comma for line ' + str(line_number) + ' character number ' + str(char_number) + ' in file ' + str(txt_file))

			elif ((in_quote and is_quote) or (in_number and is_digit)) and (next_character is None or next_character == ','):
				add_item = True
				# results.append(item)
				# item = ''
				# in_quote = False
				# in_number = False
				# first_character = True
				# add_item = False

			elif is_comma:
				# escape the comma
				item+='/'
				item+=character
			else:
				item+=character

		if len(item) != 0:
			results.append(item)

		# print('results', results)

		return results

	@staticmethod
	def get_settings():
		return IngestLib.get_json_data_from_file(SETTINGS_FILE)

	@staticmethod
	def get_json_data_from_file(json_file):
		if not os.path.exists(json_file):
			raise Exception('Expected json_file to exist at ' + str(json_file) + ' but it does not')
		
		results = {}

		with open(json_file) as json_data:  
			results = json.load(json_data)

		return results