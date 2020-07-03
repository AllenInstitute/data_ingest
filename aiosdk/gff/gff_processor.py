import argparse
from owlready2 import *
import rdflib
import pandas as pd
import re
from aiosdk.owl_template import OwlTemplate
from aiosdk.catalog_helper import CatalogHelper
from aiosdk.term_helper import TermHelper
from aiosdk.prefix_helper import PrefixHelper


class GffProcessor(object):
    '''Use BCBio.GFF to scan a Generic Feature Format file
    and produce OWL individuals.

    See: `GFF Parsing <https://biopython.org/wiki/GFF_Parsing>`_
    '''

    def __init__(self,
        gff_file,
        aio_file,
        reference_genome_term,
        owlready_sqlite_file,
        outfile,
        catalog,
        prefix_jsonld,
        template_search_path,
        gene_template,
        mrna_template,
        cds_template,
        crid_template,
        prefixes,
        iri,
        imports,
        terms
    ):
        ''' Prepare a local ontology to represent the GFF file.

        Parameters
        ----------
        gff_file : string
            path to the input to be processed
        aio_file : string
            path to the Allen Institute ontology
        reference_genome_term : string
            associate output file to this
        owlready_sqlite_file : string
            temporary backing database storage
        outfile : string
            rdf result in turtle format
        catalog : string
            OASIS format xml with cached imported ontoloy subsets
        '''

        self.render_prefixes = True
        self.gff_file = gff_file
        self.reference_genome_term = reference_genome_term
        self.sqlite3_file = owlready_sqlite_file

        self.catalog = CatalogHelper(catalog)
        self.import_info = self.catalog.import_info()

        default_world.set_backend(filename=self.sqlite3_file)

        self.file_path = outfile
        self.file_url = '#'

        self.term_classes = {}

        self.namespace_bindings = PrefixHelper(prefix_jsonld).prefixes()
        self.namespace_bindings[''] = self.file_url

        self.aio_ontology = get_ontology(aio_file).load()

        self.aio = self.aio_ontology.get_namespace(
            'http://ontology.brain-map.org/aio#'
        )

        self.so = get_ontology(self.import_info['so']['uri']).load()
        self.obi = get_ontology(self.import_info['obi']['uri']).load()

        self.onto = get_ontology(self.file_url)
        self.onto.imported_ontologies.append(self.obi)
        self.onto.imported_ontologies.append(self.so)
        self.onto.base_iri = 'http://example.org/gff3#'

        self.reference_genome = self.aio_ontology.search_one(
            iri=self.reference_genome_term
        )

        if self.reference_genome is None:
            print('could not find reference genome {}'.format(
                self.reference_genome_term)
            )

        self.name_class = self.obi.search_one(label='textual entity')
        self.polypeptide_class = self.obi.search_one(label='polypeptide')

        self.chromosomes = {}

        self.gene_template = OwlTemplate(
            template_search_path,
            prefix_jsonld,
            gene_template,
            prefixes,
            iri,
            imports,
            terms
        )

        self.crid_template = OwlTemplate(
            template_search_path,
            prefix_jsonld,
            crid_template,
            prefixes,
            iri,
            imports,
            terms
        )

        self.mrna_template = OwlTemplate(
            template_search_path,
            prefix_jsonld,
            mrna_template,
            prefixes,
            iri,
            imports,
            terms
        )

        self.cds_template = OwlTemplate(
            template_search_path,
            prefix_jsonld,
            cds_template,
            prefixes,
            iri,
            imports,
            terms
        )

    def determine_term_id(self, feature, qualifiers):
        '''File specific way of assigning an id.

        Parameters
        ----------
        feature : object
            BCBio.GFF3 feature
        qualifiers : dict
            from the feature
        '''
        term_id = None

        if 'transcript_name' in qualifiers:
            term_id = qualifiers['transcript_name'][0]
        else:
            term_id = feature.id

        return TermHelper.cleanup_characters(term_id)

    def determine_parent_id(self, feature, qualifiers):
        '''File specific way of assigning an id.

        Parameters
        ----------
        feature : object
            BCBio.GFF3 feature
        qualifiers : dict
            from the feature
        '''
        term_id = None

        if 'Parent' in qualifiers:
            parent_id = qualifiers['Parent'][0]

        return TermHelper.cleanup_characters(parent_id)

    @classmethod
    def parse_args(cls):
        '''Used by subclasses that implement main command functionality

        Returns
        -------
        object : parsed arguments
        '''
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--catalog',
            help='location of OASIS catalog file for imported ontologies'
        )
        parser.add_argument(
            "--input",
            help='location of GFF3 or GTF reference genome file'
        )
        parser.add_argument(
            '--gene-template',
            help='file name of jinja2 template'
        )
        parser.add_argument(
            '--crid-template',
            help='file name of jinja2 template'
        )
        parser.add_argument(
            '--mrna-template',
            help='file name of jinja2 template'
        )
        parser.add_argument(
            '--cds-template',
            help='file name of jinja2 template'
        )
        parser.add_argument(
            "--aio",
            help='location of Allen Institute Ontology'
        )
        parser.add_argument(
            "--term",
            help='a reference genome named individual'
        )
        parser.add_argument(
            "--sqlite3_file",
            help='sqlite3 database file created for intermediate storage'
        )
        parser.add_argument(
            "--terms",
            help='csv file mapping curies to labels')
        parser.add_argument(
            "--output",
            help='generated standalone ontology file for the GFF data'
        )

        OwlTemplate.add_argparse_args(parser)

        return parser.parse_args()
