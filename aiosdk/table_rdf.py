from aiosdk.term_helper import TermHelper
import pandas as pd
import rdflib
from rdflib import RDF
import re
import argparse


NUMBER_OF_JOIN_COLUMNS = 2
INDEX_TO_FIRST_JOIN = 0
INDEX_TO_SECOND_JOIN = 1

class TableRdf(object):
    namespace_uri = 'http://ontology.brain-map.org/tabular#'

    def __init__(self):
        self.tabular = rdflib.Namespace(TableRdf.namespace_uri)
        self.graph = rdflib.Graph()

    def read_csv(self, filepath):
        return pd.read_csv(filepath)

    def read_excel(self, filepath, sheet):
        return pd.read_excel( filepath, sheet_name=sheet)



    def dataframe_to_rdf_joins(self, df, join_one, join_two, join_csv_file, id_column=None, qtt=False):
        

        columns = list(df.columns)
        prop = {
            c: self.tabular[TermHelper.cleanup_characters(c, lower=True)]
            for c in columns
        }

        if qtt is True:
            start_row = 1
        else:
            start_row = 0

        if len(columns) != NUMBER_OF_JOIN_COLUMNS:
            raise Exception('Expected join file ' + str(join_csv_file) + ' to contain two columns but it has ' + str(len(columns)))

        # first_column = columns[INDEX_TO_FIRST_JOIN]
        # second_column = columns[INDEX_TO_SECOND_JOIN]

        for index, row in df.iterrows():
            # print('row', row)
            first_value = row[INDEX_TO_FIRST_JOIN]
            second_value = row[INDEX_TO_SECOND_JOIN]

            #search for subject with predicate (column name) and object (value)
            # ns1:project_name, "dong_antero"

            #make sure both exist
            #get subjects of both
            #create new records

            #join on tables
            #add new record

            # print(first_column, second_column)


        # for i in range(len(first_column)):
        # #     row = columns[]
        #     # print(columns[i][first_column][i], columns[second_column][i])
        #     print()


        # for column in columns:
        #     # print('column', df[column])
        #     print('column', column)

        #     # print(df[column][start_row:].dropna())
        #     # print('second', df[column][1])
        #     # print('df[column]', df[column][0], df[column][1])
        #     # pass
        #     # for row, value in df[column][start_row:].dropna().items():
        #     #     print('value', value)

        #     #     row_uri = self.tabular['row_%07d' % (row,)] + str(keyword)
        #     #     self.graph.add((row_uri, prop[column], rdflib.Literal(str(value).strip())))
        #     #     self.graph.add((row_uri, RDF.type, self.tabular['Row']))


    def dataframe_to_rdf(self, df, keyword='', id_column=None, qtt=False):
        columns = list(df.columns)
        prop = {
            c: self.tabular[TermHelper.cleanup_characters(c, lower=True)]
            for c in columns
        }

        if qtt is True:
            start_row = 1
        else:
            start_row = 0

        for column in columns:
            for row, value in df[column][start_row:].dropna().items():
                row_uri = self.tabular['row_%07d' % (row,)] + str(keyword)
                self.graph.add((row_uri, prop[column], rdflib.Literal(str(value).strip())))
                self.graph.add((row_uri, RDF.type, self.tabular['Row']))
                # self.graph.add((row_uri, RDF.type, rdflib.Literal('hotdog')))

                print('RDF.type', RDF.type)
                print('prop[column]', prop[column])

    def write_csv(self, df, filename):
        df.to_csv(filename,index=False)

    def write_rdf(self, filename, format='xml'):
        with open(filename, 'w') as f:
            f.write(self.graph.serialize(format=format).decode('utf8'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        help="qtt, dosdp, xslx, csv"
    )
    parser.add_argument(
        "--xlsx-input-file",
        help='spreadsheet for input',
        required=None,
        default=None
    )

    parser.add_argument(
        "--csv-input-file",
        help='for input',
        required=None,
        default=None
    )

    parser.add_argument(
        "--sheet-name",
        help='which sheet to write out',
        required=False,
        default=None
    )
    parser.add_argument(
        "--output",
        help='output file name'
    )

    args = parser.parse_args()

    table_rdf = TableRdf()
        
    if args.mode == 'csv':
        data = table_rdf.read_csv(
            args.csv_input_file
        )
    else:
        data = table_rdf.read_excel(
            args.xlsx_input_file,
            args.sheet_name
        )

    if args.mode in ['xslx', 'csv']:
        table_rdf.dataframe_to_rdf(data, qtt=False)
        table_rdf.write_rdf(args.output)
    elif args.mode == 'qtt':
        table_rdf.write_csv(data, args.output)
    elif args.mode == 'dosdp':
        table_rdf.write_csv(data, args.output)
    else:
        print('Unrecognized TableRdf mode: {}'.format(args.mode))

