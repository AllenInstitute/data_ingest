import xml.etree.ElementTree as ET
import re
import copy

class CatalogHelper(object):
    XML_NAMESPACE_MAP = {
        'cat': 'urn:oasis:names:tc:entity:xmlns:xml:catalog'
    }
    URI_XPATH='./cat:group/cat:uri'
    IMPORT_PREFIX = re.compile(r'.*/([^_]+)_import\.(.+)$')

    def __init__(self, catalog_file):
        tree = ET.parse(catalog_file)
        root = tree.getroot()

        self._import_info = {}

        for uri in root.findall(
            CatalogHelper.URI_XPATH,
            namespaces=CatalogHelper.XML_NAMESPACE_MAP):
            name = uri.attrib['name']
            uri = uri.attrib['uri']

            m = CatalogHelper.IMPORT_PREFIX.match(uri)

            if m:
                prefix = m.group(1)
                fmt = m.group(2)

                if fmt == 'owl':
                    self._import_info[prefix] = {
                        'name': name,
                        'uri': uri,
                        'prefix': prefix,
                        'format': fmt
                    }

    def import_info(self):
        return copy.deepcopy(self._import_info)
