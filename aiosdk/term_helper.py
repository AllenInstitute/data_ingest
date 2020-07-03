from .prefix_helper import PrefixHelper
from owlready2 import *
import rdflib
import pandas as pd
import argparse
import re

class TermHelper(object):
    def __init__(self, term_csv=None):
        if term_csv is None:
            self.labels = {}
        else:
            self.labels = TermHelper.get_labels_from_csv(term_csv)

    @classmethod
    def cleanup_characters(cls, id_string, lower=False):
        '''Change problem characters for ids to underscores.
    
        Parameters
        ----------
        lower: boolean
            downcase the result or not (default: False)

        Returns
        -------
        string :
            input cleaned and optionally downcased.
        '''
        cleaned = re.sub(r'[^0-9A-Za-z_]+', '_', str(id_string))

        if lower:
            return cleaned.lower()
        else:
            return cleaned


    @classmethod
    def find_term(cls, ont, label, prefix_mapping):
        term = ont.search_one(label=label)

        if term is None:
            term = ont.search_one(label=label.replace(' ', '_'))

        return TermHelper.curie(term.iri, prefix_mapping)

    @classmethod
    def curie(cls, iri, prefix_mapping):
        '''
        parameters
        ----------
        iri : string
            non-compact form
        prefix_mapping: string
            prefix and namespace separated by a colon and space
        '''
        (prefix, namespace) = re.split(r'\s*:\s+', prefix_mapping, maxsplit=1)

        return re.sub(namespace, (prefix + ':'), iri) 

    @classmethod
    def find_terms(cls, prefix_mapping, csv_file, ontology_file, output, term_file_output):
        term_df = pd.read_csv(csv_file)
        term_df.columns = ['ontology_term', 'label']
        ont = get_ontology(ontology_file).load()

        idx = term_df['ontology_term'].isnull().index

        term_df['ontology_term'] = term_df['label'].apply(
            lambda x: TermHelper.find_term(ont, x, prefix_mapping)
        ) 

        return term_df

    @classmethod
    def write_term_csv(cls, output, term_df):
        term_df.to_csv(output, index=False)

    @classmethod
    def write_termfile(cls, term_file_output, term_df):
        with open(term_file_output, 'w') as f:
            for idx, row in term_df.iterrows():
                f.write("{}     #  {}\n".format(
                    row['ontology_term'], row['label'])
                )

    @classmethod
    def label_dataframe_to_dict(cls, df):
        return {
            r['label'] : r['ontology_term'] for i,r in df.iterrows()
        }

    @classmethod
    def get_labels_from_csv(cls, term_file):
        term_df = pd.read_csv(term_file)
        term_df.columns = ['ontology_term', 'label']

        return TermHelper.label_dataframe_to_dict(term_df)

    def map_term(self, label):
        return self.labels[label]

    def map_terms(self, sparql):
        for k, v in self.labels.items():
            sparql = re.sub('`' + k + '`', v, sparql)

        return sparql

    def preprocess_turtle2(self, prefix_helper, inf, outf):
        outf.write(prefix_helper.turtle_prefix_string(False))

        for line in inf:
            outf.write(self.map_terms(line))

    @classmethod
    def preprocess_turtle(
        cls,
        infile,
        outfile,
        prefixes_jsonld,
        prefixes,
        term_file):

        prefix_helper = PrefixHelper(prefixes_jsonld, prefixes)
        term_helper = TermHelper(term_file)

        with open(infile) as inf:
            with open(outfile, 'w') as outf:
                outf.write(prefix_helper.turtle_prefix_string(False))

                for line in inf:
                    outf.write(term_helper.map_terms(line))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--command", required=False, default=None)
    parser.add_argument("--prefix", nargs='+')
    parser.add_argument("--prefixes-jsonld", required=False)
    parser.add_argument("--terms", required=False)
    parser.add_argument("--input")
    parser.add_argument("--ontology", required=False)
    parser.add_argument("--output")
    parser.add_argument("--term_file_output", required=False)

    args = parser.parse_args()

    if args.command == 'sub-labels':
        TermHelper.preprocess_turtle(
            args.input,
            args.output,
            args.prefixes_jsonld,
            args.prefix, #[ 'dash', 'rdf', 'rdfs', 'owl', 'schema', 'sh', 'xsd', 'aio', 'BFO', 'IAO', 'OBI', 'SO', 'so', 'oboI'],
            args.terms)   #'labels.csv')
    else:
        term_df = TermHelper.find_terms(
            args.prefix, args.input, args.ontology,
            args.output, args.term_file_output
        )
        TermHelper.write_term_csv(args.output, term_df)
        TermHelper.write_termfile(args.term_file_output, term_df)
