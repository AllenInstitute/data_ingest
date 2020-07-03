from aiosdk.owl_template import OwlTemplate
import argparse
import re

class ProcessAssemblyReport(object):
    MATCH_PATTERN = r'^([0-9XY]+)\s+(\S+)\s+([0-9XY]+)\s+Chromosome\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s*$'

    def __init__(self, chromosome_template, crid_template, reference_genome):
        self.chromosome_template = chromosome_template
        self.crid_template = crid_template
        self.reference_genome = reference_genome

    def process_file(self, in_file_path, out_file_path):
        render_prefixes = True

        with open(in_file_path) as f:
            with open(out_file_path, 'w') as out:
                while True:
                    line = f.readline()
    # 1       assembled-molecule      1       Chromosome      CM000994.2      =       NC_000067.6     C57BL/6J        195471971       chr1
                    m = re.match(ProcessAssemblyReport.MATCH_PATTERN, line)

                    if m:
                        accession_id = m.group(6)
                        version_number = accession_id.split('.')[1]
                        chromosome_number = m.group(1)

                        chromosome_curie = 'aio:chromosome_{}'.format(accession_id)

                        out.write(
                            self.chromosome_template.render_line(
                                render_prefixes,
                                chromosome=chromosome_curie,
                                name='aio:chromosome_{}_name'.format(accession_id),
                                name_string=chromosome_number,
                                reference_genome=self.reference_genome
                            )
                        )
                        render_prefixes = False
                        out.write(
                            self.crid_template.render_line(
                                False,
                                identifier='aio:chromosome_{}_id'.format(accession_id),
                                about=chromosome_curie,
                                symbol='aio:{}_symbol'.format(m.group(1)),
                                registry_name='aio:NCBI_Nucleotide_name',
                                representation = chromosome_number,
                                accession_id=accession_id,
                                version_number=version_number
                            )
                        )

                    if not line:
                        break

    @classmethod
    def parse_args(cls):
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--template-search-path",
            help="where to find the jinja2 templates"
        )

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

        parser.add_argument(
            "--chromosome-template",
            help='name of the jinja2 template file'
        )

        parser.add_argument(
            "--crid-template",
            help='name of the jinja2 template file'
        )

        parser.add_argument(
            '--assembly-report',
            help='input assembly report file'
        )

        parser.add_argument(
            '--reference-genome',
            help='iri of the associated reference genome entity'
        )

        parser.add_argument(
            '--terms',
            help='file with curie to label mapping'
        )

        parser.add_argument(
            '--output',
            help='turtle file to write to'
        )

        return parser.parse_args()


if __name__ == '__main__':
    args = ProcessAssemblyReport.parse_args()

    chromosome_template = OwlTemplate(
        args.template_search_path,
        args.prefix_jsonld,
        args.chromosome_template,
        prefixes=args.prefixes,
        iri=args.iri,
        imports=args.imports,
        terms=args.terms
    )

    crid_template = OwlTemplate(
        args.template_search_path,
        args.prefix_jsonld,
        args.crid_template,
        args.prefixes,
        iri=args.iri,
        imports=args.imports,
        terms=args.terms
    )

    par = ProcessAssemblyReport(
        chromosome_template,
        crid_template,
        args.reference_genome
    )

    par.process_file(
        args.assembly_report,
        args.output
    )