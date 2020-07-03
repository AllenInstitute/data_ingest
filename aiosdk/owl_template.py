from aiosdk.prefix_helper import PrefixHelper
from aiosdk.term_helper import TermHelper
import jinja2
import pandas as pd
import argparse


class OwlTemplate(object):
    def __init__(self, search_path, prefix_jsonld, template_name, prefixes=[], iri=None, imports=[], terms=None):
        loader = jinja2.FileSystemLoader(searchpath=search_path)
        self.env = jinja2.Environment(loader=loader)
        self.template = self.env.get_template(template_name)
        self.prefix_helper = PrefixHelper(
            prefix_jsonld,
            prefixes=prefixes,
            iri=iri,
            imports=imports
        )
        self.prefix_string = self.prefix_helper.turtle_prefix_string()
        if terms:
            self.term_helper = TermHelper(terms)
        else:
            self.term_helper = None

    def render_template(self, **kwargs):
        rendered_template =  self.template.render(**kwargs)

        if self.term_helper:
            rendered_template = self.term_helper.map_terms(rendered_template)

        return rendered_template

    def render_via_rdflib(self, df):
        import rdflib

        graph = rdflib.Graph()

        for i, row in df.iterrows():
            if i == 0:
                row['prefixes'] = self.prefix_string
            else:
                row['prefixes'] = ''

            graph.parse(data=self.render_template(**row), format='turtle')

        return graph

    def render_to_file(self, file_stream, df):
        for i, row in df.iterrows():
            if i == 0:
                render_prefixes = True
            else:
                render_prefixes = False

            file_stream.write(self.render_line(render_prefixes, **row))

    def render_line(self, render_prefixes, **kwargs):
        if render_prefixes:
            kwargs['prefixes'] = self.prefix_string
        else:
            kwargs['prefixes'] = ''

        return self.render_template(**kwargs)

    @classmethod
    def add_argparse_args(cls, parser):
        parser.add_argument(
            "--template-search-path",
            help="where to find the jinja2 templates"
        )

        PrefixHelper.add_argparse_args(parser)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    OwlTemplate.add_argparse_args(parser)

    parser.add_argument(
        "--csv-path",
        help='path to input csv file'
    )

    parser.add_argument(
        "--template-file-name",
        help='name of the jinja2 template file'
    )

    parser.add_argument(
        "--terms",
        help='csv file mapping from curies to labels'
    )

    parser.add_argument(
        "--output",
        help='path to output ttl file'
    )

    args = parser.parse_args()

    ot = OwlTemplate(
        args.template_search_path,
        args.prefix_jsonld,
        args.template_file_name,
        args.prefixes,
        args.iri,
        args.imports,
        args.terms
    )

    df = pd.read_csv(args.csv_path)
    df.fillna('', inplace=True)

    #graph = ot.render_via_rdflib(orgs)
    #print(graph.serialize(format='turtle').decode('utf8'))

    with open(args.output, 'w') as f:
        ot.render_to_file(f, df)
