from BCBio import GFF
import argparse
import pprint
from owlready2 import *
import rdflib
import pandas as pd
import re
from aiosdk.term_helper import TermHelper
from .gff_processor import GffProcessor


class ProcessRefSeqGff(GffProcessor):

    def process_gene_feature(self, out_handle, record, feature, qualifiers):
        type_iri = self.so.search_one(label=feature.type).iri
        type_curie = 'SO:' + type_iri.split('SO_')[1]
        reference_genome_curie = 'aio:' + self.reference_genome_term.split('aio#')[1]

        term_id = "aio:"+ self.determine_term_id(feature, qualifiers)
        chromosome_id = "aio:chromosome_{}".format(record.id)

        name_string = None
        name_curie = None
        if 'description' in qualifiers:
            name_string = qualifiers['description'][0]
            name_curie = 'aio:{}_name'.format(
                TermHelper.cleanup_characters(name_string)
            )

        symbol_curie = None
        symbol = None
        if 'gene' in qualifiers:
            symbol = qualifiers['gene'][0]
            symbol_curie = 'aio:{}_symbol'.format(
                TermHelper.cleanup_characters(symbol)
            )


        out_handle.write(self.gene_template.render_line(
            self.render_prefixes,
            identifier=term_id,
            gene=type_curie,
            chromosome_id=chromosome_id,
            reference_genome=reference_genome_curie,
            name_curie=name_curie,
            name_string=name_string,
            symbol_curie=symbol_curie,
            symbol_string=symbol
        ))
        self.render_prefixes=False

        gene_id = None

        if 'Dbxref' in qualifiers:
            for gene_id_xref in qualifiers['Dbxref']:
                m = re.match(r'^GeneID:(\d+)$', gene_id_xref)
                if m:
                    gene_id = m.group(0)
                    gene_symbol = m.group(1)
                    break

        out_handle.write(
            self.crid_template.render_line(
                False,
                identifier='aio:{}_id'.format(
                    TermHelper.cleanup_characters(gene_symbol)
                ),
                about=term_id,
                symbol='aio:{}_symbol'.format(
                    TermHelper.cleanup_characters(gene_id)
                ),
                registry_name='aio:NCBI_Gene_name',
                representation = gene_id,
                accession_id=None,
                version_number=None
            )
        )

    def process_region_feature(self, record, feature, qualifiers):
        term_class = self.class_for_feature_type(feature.type, qualifiers)
        term_id = self.determine_term_id(feature, qualifiers)
        term = self.create_term(term_class, term_id)
        term.label.append(feature.id)
        self.add_qualifier_triple(term, 'hasDbXref', qualifiers, 'Dbxref')
        term.hasDbXref.append(record.id)
        self.reference_genome.BFO_0000051.append(term)

        try:
            loc = feature.location
            term.start_location.append(loc.start.position)
            term.end_location.append(loc.end.position)
        except Exception as e:
            print('no location: {}'.format((e,)))

        self.chromosomes[record.id] = term

    def process_mrna_feature(self, out_handle, feature, qualifiers):
        type_iri = self.so.search_one(label=feature.type).iri
        type_curie = 'SO:' + type_iri.split('SO_')[1]
        reference_genome_curie = 'aio:' + self.reference_genome_term.split('aio#')[1]

        term_id = "aio:"+ self.determine_term_id(feature, qualifiers)

        if 'Parent' in qualifiers:
            gene_curie = 'aio:' + self.determine_parent_id(feature, qualifiers)
        else:
            gene_curie = None

        if 'product' in qualifiers:
            mrna_name = "{}, {}".format(qualifiers['product'][0], feature.type)
            mrna_name_curie = "aio:{}_name".format(
                TermHelper.cleanup_characters(mrna_name)
            )

        out_handle.write(self.mrna_template.render_line(
            self.render_prefixes,
            identifier=term_id,
            so_type=type_curie,
            gene=gene_curie,
            reference_genome=reference_genome_curie,
            name_id=mrna_name_curie,
            name_string=mrna_name
        ))

        genbank_symbol = None
        accession_id = None
        version_number = None

        if 'Dbxref' in qualifiers:
            for gene_id_xref in qualifiers['Dbxref']:
                m = re.match(r'^Genbank:(([NX]M_\d+)\.(\d+))$', gene_id_xref)
                if m:
                    genbank_symbol = m.group(1)
                    accession_id = m.group(2)
                    version_number = m.group(3)
                    break

        out_handle.write(
            self.crid_template.render_line(
                False,
                identifier='aio:{}_id'.format(
                    TermHelper.cleanup_characters(genbank_symbol)
                ),
                about=term_id,
                symbol='aio:{}_symbol'.format(
                    TermHelper.cleanup_characters(genbank_symbol)
                ),
                registry_name='aio:NCBI_Nucleotide_name',
                representation = genbank_symbol,
                accession_id=accession_id,
                version_number=version_number
            )
        )

    def process_cds_feature(self, out_handle, feature, qualifiers):
        type_iri = self.so.search_one(label=feature.type).iri
        type_curie = 'SO:' + type_iri.split('SO_')[1]
        reference_genome_curie = 'aio:' + self.reference_genome_term.split('aio#')[1]

        term_id = "aio:"+ self.determine_term_id(feature, qualifiers)

        mrna_curie = None
        if 'Parent' in qualifiers:
            mrna_curie = 'aio:' + self.determine_parent_id(feature, qualifiers)

        polypeptide_id = term_id + '_polypeptide'

        polypeptide_name_curie = None
        polypeptide_name_string = None

        if 'product' in qualifiers:
            polypeptide_name_string = qualifiers['product'][0]
            polypeptide_name_curie = 'aio:{}_name'.format(
                TermHelper.cleanup_characters(polypeptide_name_string)
            )

        out_handle.write(self.cds_template.render_line(
            False,
            identifier=term_id,
            so_type=type_curie,
            mrna_id=mrna_curie,
            reference_genome=reference_genome_curie,
            polypeptide_id=polypeptide_id,
            polypeptide_name_curie=polypeptide_name_curie,
            name_string=polypeptide_name_string
        ))

        crid = None
        crid_symbol = None
        genbank_symbol = None
        accession_id = None
        version_number = None

        if 'Dbxref' in qualifiers:
            for protein_id_xref in qualifiers['Dbxref']:
                m = re.match(r'^Genbank:(([NX]P_\d+)\.(\d+))$', protein_id_xref)
                if m:
                    genbank_symbol = m.group(1)
                    crid = 'aio:{}_id'.format(
                        TermHelper.cleanup_characters(genbank_symbol)
                    )
                    crid_symbol = 'aio:{}_symbol'.format(
                        TermHelper.cleanup_characters(genbank_symbol)
                    )
                    accession_id = m.group(2)
                    version_number = m.group(3)
                    break

            if crid is None:
                print('No genbank symbol: {}'.format(qualifiers['Dbxref']))
            else:
                out_handle.write(
                    self.crid_template.render_line(
                        False,
                        identifier=crid,
                        about=polypeptide_id,
                        symbol=crid_symbol,
                        registry_name='aio:NCBI_Protein_name',
                        representation = genbank_symbol,
                        accession_id=accession_id,
                        version_number=version_number
                    )
                )
        else:
            print('no Dbxref for {}'.format(term_id))

    def process(self):
        '''Iterate through the GFF3 file, creating terms and
        associated entities for each gene or chromosome record.
        '''
        count = 0

        with open(self.gff_file) as in_handle:
            with open(self.file_path, 'w') as out_handle:
                for record in GFF.parse(in_handle, target_lines=1):
                    for feature in record.features:
                        qualifiers = feature.qualifiers

                        if feature.type == 'gene':
                            self.process_gene_feature(out_handle, record, feature, qualifiers)
                        elif (
                            feature.type == 'region' and
                            'genome' in qualifiers and
                            qualifiers['genome'][0] == 'chromosome'
                        ):
                            pass
                            #self.process_region_feature(record, feature, qualifiers)
                        elif feature.type == 'mRNA':
                            self.process_mrna_feature(out_handle, feature, qualifiers)
                        elif feature.type == 'CDS':
                            self.process_cds_feature(out_handle, feature, qualifiers)
                        else:
                            pass
                            #print('skipping {} feature'.format(feature.type))

                    if count % 1000 == 0:
                        print(count)
    
                    count = count + 1

if __name__ == '__main__':
    args = ProcessRefSeqGff.parse_args()

    gffp = ProcessRefSeqGff(
        args.input,
        args.aio,
        args.term,
        args.sqlite3_file,
        args.output,
        args.catalog,
        args.prefix_jsonld,
        args.template_search_path,
        args.gene_template,
        args.mrna_template,
        args.cds_template,
        args.crid_template,
        args.prefixes,
        args.iri,
        args.imports,
        args.terms
    )

    gffp.process()
