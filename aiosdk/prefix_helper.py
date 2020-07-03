import rdflib
import json
import copy

class PrefixHelper(object):
    PREFIX_KEY = '@context'

    def __init__(self,
                 prefix_jsonld_file,
                 prefixes=[],
                 iri='http://example.org/ontology#',
                 imports=[]):
        self.ontology = iri
        self.imports = imports

        with open(prefix_jsonld_file) as f:
            if prefixes and len(prefixes) > 0:
                self._prefixes = {
                    k: v for k,v in json.load(f)[PrefixHelper.PREFIX_KEY].items()
                    if k in prefixes
                }
            else:
                self._prefixes = copy.deepcopy(
                    json.load(f)[PrefixHelper.PREFIX_KEY]
                )

        self._prefix_graph = self.prefix_graph()

    def prefixes(self):
        return copy.deepcopy(self._prefixes)

    def prefix_graph(self):
        ns_graph = rdflib.Graph()

        for p,u in self._prefixes.items():
            ns_graph.namespace_manager.bind(
                p, rdflib.URIRef(u),
                override=True
            )

        return ns_graph

    def compact_url(self, term):
        '''
        parameters
        ----------
        term : string
            uri to shorten

        returns
        -------
        string: curie
        '''
        (curie_prefix, _, curie_lname) = \
            self._prefix_graph.namespace_manager.compute_qname(term)

        return ':'.join([curie_prefix, curie_lname])

    def sparql_prefix_string(self):
        prefix_string = '\n'.join(
            'PREFIX {}: <{}>'.format(p, u)
            for p, u in self._prefixes.items()
        )

        return prefix_string

    def turtle_prefix_string(self, add_ontology_and_imports=True):
        prefix_string = '\n'.join(
            '@prefix {}: <{}> .'.format(p, u)
            for p, u in self._prefixes.items()
        )

        if add_ontology_and_imports:
            ontology_string = '\n<{}> a owl:Ontology ;'.format(self.ontology)
    
            import_string = '    owl:imports ' + ',\n        '.join(
                '<{}>'.format(import_uri) for import_uri in self.imports
            ) + ' .'
        else:
            ontology_string = ''
            import_string = ''

        return '\n'.join([prefix_string, ontology_string, import_string])

    @classmethod
    def add_argparse_args(cls, parser):
        parser.add_argument(
            "--prefixes",
            nargs='*',
            help='space separated list of prefixes to use in the template'
        )

        parser.add_argument(
            "--iri",
            help='ontology iri'
        )

        parser.add_argument(
            "--imports",
            help="ontology module imports",
            action='append'
        )

        parser.add_argument(
            "--prefix-jsonld",
            help="where to find the CURIE prefix assignments"
        )