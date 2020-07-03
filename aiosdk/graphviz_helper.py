import pandas as pd
import graphviz as gv
import rdflib
import re


class GraphvizHelper(object):
    property_query = '''\
SELECT DISTINCT  ?p
WHERE {
    { ?s ?p ?o }
}
'''

    class_query = '''\
CONSTRUCT {
    ?p a ?c
} WHERE {
    ?p a ?c
}
VALUES ?p { %s }
'''

    label_query = '''\
CONSTRUCT {
    ?p rdfs:label ?l
} WHERE {
    ?p rdfs:label ?l
}
'''

    render_query = '''\
SELECT DISTINCT ?s ?sl ?p ?pl ?o ?ol ?sc ?scl ?oc ?ocl
WHERE {
    ?s ?p ?o .
    OPTIONAL {
        ?s a ?sc
        FILTER(?sc != owl:NamedIndividual)
        OPTIONAL { ?sc rdfs:label ?scl }
    }
    OPTIONAL { 
        ?o a ?oc
        FILTER(?oc != owl:NamedIndividual)
        OPTIONAL { ?oc rdfs:label ?ocl }
    }
    OPTIONAL { ?s rdfs:label ?sl }
    OPTIONAL { ?p rdfs:label ?pl }
    OPTIONAL { ?o rdfs:label ?so }
    FILTER(?p != rdfs:label)
    FILTER(?p != rdf:type)
}'''

    node_query = '''\
SELECT DISTINCT ?n ?nc ?ncl ?property ?property_label ?literal
WHERE {
    { 
        ?n ?p ?o
    } UNION {
        ?s ?p2 ?n
    }
    FILTER(?p != rdfs:label)
    OPTIONAL {
        ?n a ?nc
        OPTIONAL { ?nc rdfs:label ?ncl }
    }
    OPTIONAL {
        ?n ?property ?literal
        OPTIONAL { ?property rdfs:label ?property_label }
        FILTER(isLiteral(?literal))
    }
}
ORDER BY ?n
'''

    node_attr = dict(
        style='filled',
        shape='record',
        align='left',
        fontsize='11',
        ranksep='0.1',
        height='0.2'
    )

    def __init__(self, ontology_sparql_helper, data_sparql_helper):
        self.ontology = ontology_sparql_helper
        self.sparql = data_sparql_helper

        self.graph = None

        self.property_label_graph = self.ontology.construct_to_graph(
            GraphvizHelper.label_query
        )

    def construct2(self, construct_where_string, values_string, *params):
        construct_query_string = """\
CONSTRUCT {
%s
} WHERE {
%s
}
%s
""" % (construct_where_string, construct_where_string, values_string)

        self.construct(construct_query_string, *params)

    def construct(self, construct_query_string, *params):
        self.graph = self.sparql.construct_to_graph(
            construct_query_string,
            *params
        )

        properties = [
            str(row[0])
            for row in self.graph.query(GraphvizHelper.property_query)
        ]

        property_string = ' '.join(properties)

        self.graph += self.property_label_graph

    def choose_label(self, preferred, alternate, default='x'):
        name = None

        if preferred is not None and preferred != '':
            name = preferred
        elif alternate is not None and alternate != '':
            name = alternate
        else:
            name = default

        if '#' in name:
            name = re.sub(r'^[^#]+', '', name)
        else:
            name = re.sub(r'^http://purl.obolibrary.org/obo/', '', name)

        return name

    def make_node_records_string(self, node_dict):
        node_records = [
            '<type> {}'.format(node_dict['type']),
            '<id> {}'.format(node_dict['id']),
        ]

        if 'label' in node_dict:
            node_records.append('<label> {}'.format(node_dict['label']))

        for k,v in node_dict.items():
            if k in ('type', 'id', 'label'):
                continue
    
            node_records.append('{%s|%s}' % (k,v))
    
        return '|'.join(node_records)

    def nodes(self, node_df):
        current_node = None
        node_dict = {}

        for i, r in node_df.iterrows():
            if current_node != r[0]:
                if current_node is not None:
                    node_records_string = self.make_node_records_string(node_dict)
                    self.gv_graph.node(
                        self.choose_label(current_node, ''),
                        r'{ %s }' % (node_records_string,)
                    )
                current_node = r[0]
                node_dict = { 'type': 'thing', 'id': r[0] }

            if r[1] or r[2]:
                node_dict['type'] = self.choose_label(r[2], r[1], 'Thing')

            if r[3] == rdflib.RDFS.label:
                node_dict['label'] = r[5]
            elif r[4] and r[5]:
                node_dict[self.choose_label(r[4], r[3], 'prop')] = r[5]

        node_records_string = self.make_node_records_string(node_dict)

        self.gv_graph.node(
            self.choose_label(current_node, ''),
            r'{ %s }' % (node_records_string,)
        )

    def edges(self, query_result_df):
        for i, r in query_result_df.iterrows():
            if isinstance(r[4], rdflib.term.Literal):
                continue

            source = self.choose_label(r[0], '')
            target = self.choose_label(r[4], '')
            edge_label = self.choose_label(r[3], r[2])

            self.gv_graph.edge(source, target, label=edge_label)

    def render(self):
        self.gv_graph = gv.Digraph(
            node_attr=GraphvizHelper.node_attr,
            graph_attr=dict(size="8,8", rankdir='BT'),
            strict=True
        )

        node_df = pd.DataFrame(self.graph.query(self.node_query)) 
        self.nodes(node_df)

        result_df = pd.DataFrame(self.graph.query(self.render_query))
        self.edges(result_df)

        return self.gv_graph