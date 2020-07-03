from flask_appbuilder.models.generic import GenericSession
from aiosdk.prefix_helper import PrefixHelper
from aiosdk.sparql_helper import SparqlHelper
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import XSD
import pandas as pd
import re


class RdfSession(GenericSession):
    shape_query = '''
SELECT ?shape ?target_class ?prop ?name ?object_class ?min ?max ?inverse
WHERE { 
    ?shape a sh:NodeShape ;
           sh:targetClass ?target_class ;
           sh:property ?p
    .

    ?p sh:name ?name .

    {
        ?p sh:qualifiedValueShape [
           sh:class ?object_class
       ]
    } UNION {
        ?p sh:datatype ?object_class
    }

    {
        ?p sh:path ?prop .
        FILTER (!isBlank(?prop))
        BIND(false as ?inverse)
    } UNION {
        ?p sh:path [ sh:inversePath ?prop ]
        BIND(true as ?inverse)
    }

    OPTIONAL { ?p sh:qualifiedMinCount ?min }
    OPTIONAL { ?p sh:qualifiedMaxCount ?max }
}
VALUES ?target_class { %s }
'''

    shape_columns = [
        'shape', 'target_class', 'prop',
        'name', 'object_class', 'min', 'max', 'inverse'
    ]

    def __init__(self,
                 file_path, format='turtle',
                 prefix_jsonld=None, prefixes=None,
                 shacl=None, remote=False, term_csv=None):
        self.sparql_helper = SparqlHelper(
            file_path,
            prefix_jsonld,
            prefixes=prefixes,
            remote=remote)

        self.shacl = SparqlHelper(
            shacl,
            prefix_jsonld,
            prefixes=prefixes,
            remote=False,
            term_csv=term_csv
        )
        self.shacl.print_query = 'post'

        self.namespace_prefixes = PrefixHelper(prefix_jsonld).prefixes()

        super(RdfSession, self).__init__()

    def _add_object(self, model):
        self.add(model)

    def query(self, model_cls):
        """
            SQLAlchemy query like method
        """
        self._filters_cmd = list()
        self.query_filters = list()
        self._order_by_cmd = dict()
        self._offset = 0
        self._limit = 20

        self.query_class = model_cls

        return self

    def update(self, update_cmd):
        # todo: use filters
        delete_triples = []
        insert_triples = []
        bindings = {}
        n = 0

        for k,v in update_cmd.items():
            if k == 'id':
                bindings['id'] = URIRef(v)
            else:
                delete_triples.append('    ?id ?prop_{} ?old_value_{} .'.format(n, n))
                insert_triples.append('    ?id ?prop_{} ?new_value_{} .'.format(n, n))
                bindings['prop_{}'.format(n)] = URIRef(self.column_iris[k])
                bindings['old_value_{}'.format(n)] = Literal(v[0], datatype=XSD.string)
                bindings['new_value_{}'.format(n)] = Literal(v[1], datatype=XSD.string)
            n = n + 1

        delete_string = '\n'.join(delete_triples)
        insert_string = '\n'.join(insert_triples)

        if len(delete_triples) > 0:
            update_sparql = '''\
DELETE {
''' + delete_string + '''
} INSERT {
''' + insert_string + '''
} WHERE {
''' + delete_string + '''
}
'''
            print(update_sparql)
            print(bindings)

        result = self.sparql_helper.update(
            update_sparql,
            initNs=self.namespace_prefixes,
            initBindings=bindings
        )

        return True

    def get(self, pk):
        self.delete_all(self.query_class)

        results = self.get_all(entity_uri=pk)
        models = self.unpack_results(results, just_one=True)

        return models[0] # super(RdfSession, self).get(pk)

    def get_model_curie(self):
        try:
            model_curie = self.query_class.curie
        except:
            model_curie = self.query_class.get_curie()

        return model_curie

    def unpack_results(self, results, just_one=False):
        models = []
        vars, opts = self.build_property_shape_from_shacl()

        for i, result in results.iterrows():
            model = self.query_class()
            model.id = result[0]

            var_num = 1
            for var in vars:
                var = re.sub('^\?', '', var)
                setattr(model, var, result[var_num])
                var_num = var_num + 1

            models.append(model)

            if just_one:
                break

        return models

    def all(self):
        self.delete_all(self.query_class)

        model_curie = self.get_model_curie()

        results = self.get_all(
            model_curie,
            order_by = self._order_by_cmd,
            offset=self._offset,
            limit=self._limit
        )

        models = self.unpack_results(results)

        total = self.total(
            class_curie=model_curie,
            order_by=self._order_by_cmd
        )

            #self._add_object(line)

        return int(total), models# super(RdfSession, self).all()

    def total(self,
        class_curie,
        filters={},
        order_by={}, # {'label': 'ASC' },
    ):
        '''
        Parameters
        ----------
        class_curie : string
            compact url such as aio:organization
        order_by : dict
            like { 'label': 'ASC', 'state': 'DESC' }

        Returns
        -------
        int : number of filtered entities
        '''
        filter_string = self.build_filter_string(filters)

        query_string = """\
SELECT (count(?entity) as ?total)
    WHERE {
    ?entity a """ + class_curie + """ .
""" + filter_string + """
}
"""

        print(query_string)

        result_df = self.sparql_helper.query_to_dataframe(
            query_string,
        )

        return result_df.values[0][0]

    def build_filter_string(
            self,
            filters
        ):
        filter_number = 0
        filter_clauses = []

        for property, value in filters.items():
            filter_clauses.append(
                '    ?entity aio:{} ?filter_{} .'.format(
                    property, filter_number
                )
            )
            filter_clauses.append(
                '    FILTER(?filter_{} = "{}")'.format(
                    filter_number, value
                )
            )
            filter_number = filter_number + 1

        filter_string = '\n'.join(filter_clauses)

        return filter_string

    def build_order_by(self, order_by):
        order_clauses = []

        for order,direction in order_by.items():
            if direction == 'ASC':
                order_clause = 'ORDER BY ASC(?{})'.format(order)
            elif direction == 'DESC':
                order_clause = 'ORDER BY DESC(?{})'.format(order)
            else:
                raise Exception('unexpected order')
            order_clauses.append(order_clause)
        order_string = '\n'.join(order_clauses)

        return order_string

    def build_limit(self, limit):
        if limit is not None:
            limit_clause = 'LIMIT {}'.format(limit)
        else:
            limit_clause = ''

        return limit_clause

    def build_offset(self, offset):
        if offset is not None:
            offset_clause = 'OFFSET {}'.format(offset)
        else:
            offset_clause = ''

        return offset_clause

    def shape_dataframe(self, model_curie):
        df = self.shacl.query_to_dataframe(
            RdfSession.shape_query,
            model_curie
        )
        df.columns = RdfSession.shape_columns

        return df

    def build_property_shape_from_shacl(self):
        optional_pattern = "OPTIONAL { ?entity %s<%s> %s }"
        model_curie = self.get_model_curie()

        df = self.shape_dataframe(model_curie)

        result_variables = []
        optionals = []
        
        for i, row in df.iterrows():
            result_variable = "?{}".format(row['name'])
            result_variables.append(result_variable)

            if row['inverse'] is True:
                inverse = '^'
            else:
                inverse = ''

            optionals.append(
                optional_pattern % (inverse, row['prop'], result_variable)
            )

        return result_variables, '\n    '.join(optionals)

    def build_select_clause(self):
        select_extra, optionals = self.build_property_shape_from_shacl()

        return "SELECT DISTINCT ?entity " + ' '.join(select_extra)

    def combine_query_string(
        self,
        select_clause,
        shape_clause,
        filter_string,
        values_clause,
        order_string,
        limit_clause,
        offset_clause
    ):
        return select_clause + """
WHERE {
    ?entity a ?class .
""" + shape_clause + filter_string + '\n' + values_clause + """
}
""" + '\n'.join([order_string, limit_clause, offset_clause])

    def build_query(
        self,
        filters,
        values_clause,
        order_by,
        limit,
        offset
    ):
        select_clause = self.build_select_clause()
        select_extra, shape_clause = self.build_property_shape_from_shacl()
        filter_string = self.build_filter_string(filters)
        order_string = self.build_order_by(order_by)
        limit_clause = self.build_limit(limit)
        offset_clause = self.build_offset(offset)

        query_string = self.combine_query_string(
            select_clause=select_clause,
            shape_clause=shape_clause,
            filter_string=filter_string,
            values_clause=values_clause,
            order_string=order_string,
            limit_clause=limit_clause,
            offset_clause=offset_clause
        )

        return query_string


    def get_all(self,
                class_curie=None,
                filters={},
                order_by={}, # {'label': 'ASC' },
                limit=None,
                offset=None,
                entity_uri=None):
        '''
        Parameters
        ----------
        class_curie : string
            compact url such as aio:organization
        order_by : dict
            like { 'label': 'ASC', 'state': 'DESC' }
        limit : int
            zero based, for paging
        offset : int
            zero based, for paging
        '''
        if class_curie is not None:
            curie_prefix, curie_local_name = class_curie.split(':')
            namespace = self.namespace_prefixes[curie_prefix]
            values_clause = 'VALUES (?class) { (%s) }' % (
                class_curie,
            )

        if entity_uri is not None:
            values_clause = 'VALUES (?entity) { (%s) }' % (
                URIRef(entity_uri),
            )

        query_string = self.build_query(
            filters,
            values_clause,
            order_by,
            limit,
            offset
        )

        return self.sparql_helper.query_to_dataframe(
            query_string
        )
