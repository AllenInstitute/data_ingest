from SPARQLWrapper import SPARQLWrapper, JSON, RDFXML
from .prefix_helper import PrefixHelper
from .term_helper import TermHelper
from io import StringIO
import rdflib
import pandas as pd
import re


class SparqlHelper(object):
    OBO_PREFIX_QUERY = '''\
    SELECT DISTINCT ?ontology_term ?label
{
    ?ontology_term rdfs:label ?label
    FILTER(isURI(?ontology_term))
    FILTER(REGEX(STR(?ontology_term), '^.*_[0-9][0-9][0-9][0-9][0-9][0-9][0-9]'))
}
'''

    def __init__(self, endpoint, prefix_jsonld, prefixes, remote=True, term_csv=None):
        self.remote = remote
        self.print_query = None

        self.term_helper = None
        self.set_labels_from_csv(term_csv)

        self.prefix_helper = PrefixHelper(prefix_jsonld, prefixes)
        self.prefix_string = self.prefix_helper.sparql_prefix_string()

        if self.remote:
            self.sparql = SPARQLWrapper(endpoint)
        else:
            self.sparql = rdflib.Graph()
            preprocessed_turtle = StringIO()
            self.term_helper.preprocess_turtle2(
                self.prefix_helper,
                endpoint,
                preprocessed_turtle)
            preprocessed_turtle.seek(0)
            self.sparql.parse(preprocessed_turtle, format='turtle')

    def create_term_csv(self, output):
        term_df = self.query_for_labels_dataframe()

        TermHelper.write_term_csv(output, term_df)

    def query_for_labels_dataframe(self):
        df = self.query_to_dataframe(
            SparqlHelper.OBO_PREFIX_QUERY
        )

        df['label'] = df['label'].str.replace('_', ' ')
        df['ontology_term'] = df['ontology_term'].apply(
            self.prefix_helper.compact_url
        )

        return df

    def set_labels_from_csv(self, term_file):
        self.term_helper = TermHelper(term_file)

    def query_for_labels(self, set_labels=False):
        df = self.query_for_labels_dataframe()

        labels = TermHelper.label_dataframe_to_dict(df)

        if set_labels is True:
            self.term_helper.labels = labels

        return labels

    def preprocess_query(self, sparql_string):
        if self.term_helper:
            sparql_string = self.term_helper.map_terms(sparql_string)

        sparql_string = '\n'.join([self.prefix_string, sparql_string])

        return sparql_string
 
    def clean_value(self, value):
        try:
            value = value['value']
        except:
            pass

        try:
            value = value.replace('http://ontology.brain-map.org/aio#', 'aio:')
        except:
            pass

        return value

    def sparql_graph_query(self, query_string):
        df = pd.DataFrame(self.sparql.query(query_string))

        df = df.applymap(lambda x: x.toPython() if x else None)

        return df

    def sparql_wrapper_query(self, query_string):
        ret = self.query_wrapper_query(query_string, JSON)

        return self.query_wrapper_postprocess(ret)

    def query_wrapper_query(self, query_string, format):
        self.sparql.setQuery(query_string)
        self.sparql.setReturnFormat(format)

        return self.sparql.query().convert()

    def query_wrapper_postprocess(self, ret):
        df = pd.DataFrame(ret['results']['bindings'], columns=ret['head']['vars'])

        return df.applymap(self.clean_value)

    def preprocess_and_interpolate(self, query_string, *params):
        if self.print_query == 'pre':
            print(query_string)

        if params:
            query_string = query_string % params

        query_string = self.preprocess_query(query_string)

        if self.print_query == 'post':
            print(query_string)

        return query_string

    def query_to_dataframe(self, query_string, *params, **kwargs):
        query_string = self.preprocess_and_interpolate(
            query_string, *params, **kwargs
        )

        if self.remote:
            return self.sparql_wrapper_query(query_string)
        else:
            return self.sparql_graph_query(query_string)

    def construct_to_graph(self, query_string, *params):
        query_string = self.preprocess_and_interpolate(query_string, *params)

        graph = self.query_wrapper_query(query_string, RDFXML)

        return graph
