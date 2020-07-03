class QueryLibrary(object):
    chromosome_list_template = """\
SELECT ?chromosome ?name
WHERE {
  $n a aio:name .
  $n `denotes` ?chromosome .
  ?chromosome a `chromosome` .
  $n `has representation` ?name .
}
"""

    symbol_query = '''\
SELECT ?entity
WHERE {
    ?n `has representation` ?representation .
    ?n `denotes` ?entity .
    FILTER(REGEX(STR(?representation), '^%s$'))
}
'''

    sequence_features = '''\
CONSTRUCT {
   ?sf a owl:Class ;
       rdfs:label ?sfl .

   ?c2 rdfs:subClassOf ?c1 ;
       a owl:Class ;
       rdfs:label ?l .

   ?c2 so:member_of ?c3 .

   ?c2 so:part_of ?c4 .

   ?c2 so:derives_from ?c5 .
} WHERE {
   ?c1 rdfs:subClassOf* ?sf .

   ?sf a owl:Class ;
       rdfs:label ?sfl .

   ?c2 rdfs:subClassOf ?c1 ;
       a owl:Class ;
       rdfs:label ?l .

    OPTIONAL { 
        ?c2 rdfs:subClassOf ?r .
        ?r a owl:Restriction ;
           owl:onProperty so:member_of ;
           owl:someValuesFrom ?c3 .
    }

    OPTIONAL { 
        ?c2 rdfs:subClassOf ?r .
        ?r a owl:Restriction ;
           owl:onProperty so:part_of ;
           owl:someValuesFrom ?c4 .
    }

    OPTIONAL { 
        ?c2 rdfs:subClassOf ?r .
        ?r a owl:Restriction ;
           owl:onProperty so:derives_from ;
           owl:someValuesFrom ?c5 .
    }
} VALUES ?sf { `sequence feature` }
'''

    role_info = '''\
SELECT ?agent_name ?role_label ?role_type_label ?process_label ?process_type_label ?name

WHERE {

    ?role a aio:data_curation ;
          a [ rdfs:label ?role_type_label ] ;
          rdfs:label ?role_label ;
          `inheres in` ?agent .

    [] a aio:name ;
       `denotes` ?agent ;
       `has representation` ?agent_name .

    ?role `realized in` ?process .
    
    ?process a [ rdfs:label ?process_type_label ] ;
             rdfs:label ?process_label .

    ?reference_genome `is specified input of` ?process .

    ?text_entity `denotes` ?reference_genome ;
       `has representation` ?name .

    ?text_entity a aio:name .
}
'''

    reference_genome_info = '''\
SELECT ?reference_genome ?name ?process_label ?role_name ?curator_name

WHERE {

    ?reference_genome a `reference genome` ;
                      `is specified input of` ?process .

    [] `denotes` ?reference_genome ;
       `has representation` ?name .
    
    ?process a [ rdfs:label ?process_type_label ] ;
             rdfs:label ?process_label .
    
    ?role `realized in` ?process ;
          rdfs:label ?role_name ;
          a aio:data_curation ;
          `inheres in` ?curator .

        [] `denotes` ?curator ;
           a aio:name ;
           `has representation` ?curator_name .
}
'''

    chromosome_query = '''\
SELECT ?chromosome ?reference_genome
WHERE {
    ?chromosome `part of` ?reference_genome .
    ?reference_genome a `reference genome`
}
VALUES ?chromosome { %s }
'''

    gene_info = '''\
SELECT ?chromosome ?mrna ?reference_genome
WHERE {

    ?gene `part of` ?chromosome, ?reference_genome .

    ?chromosome a `chromosome` .
    
    ?reference_genome a `reference genome` ;

    OPTIONAL { ?mrna `member of` ?gene }
    
    VALUES ?gene { %s }
}
'''

    mrna_query = '''\
SELECT DISTINCT ?mrna ?polypeptide ?polypeptide_name

WHERE {
    ?t `is about`|`denotes` ?mrna ;
       a ?c .
    ?t `has representation`|(`has part`/`has representation`) ?representation .

    FILTER(?c != owl:NamedIndividual)
    ?c rdfs:label ?representation_class_label .

    ?cds a `CDS` ;
         `part of` ?mrna .

    ?polypeptide so:derives_from ?cds .

    [] `denotes` ?polypeptide ;
       `has representation` ?polypeptide_name .
}
VALUES ?mrna { %s }
'''

    representation_query = '''\
SELECT ?entity ?representation ?representation_entity ?representation_class ?registry ?accession_id ?version ?registry_id ?uri_prefix ?uri

WHERE {
    {
        ?representation_entity `denotes`|`is about` ?entity ;
                               a `centrally registered identifier` ;
                               `has part` [ a `centrally registered identifier symbol` ;
                                            `has representation` ?representation ] ;
                               `has part` [ `denotes` ?registry_id ;
                                            `has representation` ?registry ] .
        ?registry_id a `centrally registered identifier registry` .
            
        OPTIONAL {
            FILTER(?registry_id = aio:NCBI_Protein)
            BIND('https://www.ncbi.nlm.nih.gov/protein/' AS ?uri_prefix)
        } 

        BIND(CONCAT(?uri_prefix, ?representation) AS ?uri)

    } UNION {
        ?representation_entity `is about`|`denotes` ?entity ;
                               `has representation` ?representation .
    }

    ?representation_entity a [ rdfs:label ?representation_class ] .


    OPTIONAL {
        [] `part of` ?representation_entity ;
           a aio:accession_identifier ;
           `has representation` ?accession_id .
    }
    
    OPTIONAL {
        [] `part of` ?representation_entity ;
           a `version number` ;
           `has representation` ?version .
    }
}
VALUES ?entity { %s }
'''

    process_query = '''\
SELECT ?input_name ?release ?input_type_label ?process_name ?process_type_label

WHERE {

    ?input `is specified input of` ?process ;
           a [ rdfs:label ?input_type_label ] .

    ?process a [ rdfs:label ?process_type_label ] ;
             rdfs:label ?process_name .
    
    [] `denotes` ?input ;
       a aio:name ;
       `has representation` ?input_name .
    
    OPTIONAL { 
        [] `is about` ?input ;
           a aio:release_number ;
           `has representation` ?release
    }
}
'''
 
    class_names_query = """\
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?c ?l
    WHERE {
      ?c a owl:Class ;
         rdfs:label ?l
    }
    """

    string_regex_query = """\
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?c ?l
    WHERE {
      ?c a owl:Class ;
         rdfs:label ?l
      FILTER(REGEX(STR(?l), "^.*agent.*$"))

    }
    """

    named_individuals_query = """\
    prefix aio: <http://ontology.brain-map.org/aio#>
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?i ?class
    WHERE {
      ?i a owl:NamedIndividual .
      ?i a ?c .
      FILTER(?c != owl:NamedIndividual)
      ?c rdfs:label ?class .
    } limit 100
    """

    count_of_classes = """\
    prefix aio: <http://ontology.brain-map.org/aio#>
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?class (COUNT(?class) AS ?count)
    WHERE {
      ?i a owl:NamedIndividual .
      ?i a ?c .
      FILTER(?c != owl:NamedIndividual)
      ?c rdfs:label ?class .
    }
    GROUP BY ?class
    """

    # Count of ‘sequence feature’ individuals
    count_of_sequence_features = """\
    SELECT ?class (COUNT(?class) AS ?count)
    WHERE {
      ?i a ?c .
      ?c rdfs:label ?class .
      ?c rdfs:subClassOf* `sequence feature`
    }
    GROUP BY ?class
    """

    # Count of Data Transformations
    count_of_data_transformations = """\
    prefix aio: <http://ontology.brain-map.org/aio#>
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?class (COUNT(?class) AS ?count)
    WHERE {
      ?i a owl:NamedIndividual .
      ?i a ?c .
      FILTER(?c != owl:NamedIndividual)
      ?c rdfs:label ?class .
      ?c rdf:type|rdfs:subClassOf* [ rdfs:label ?super ]
      FILTER(REGEX(STR(?super), '^data transformation$'))
    }
    GROUP BY ?class
    """

    # Classes for a specific individual
    specific_individual = """\
    prefix aio: <http://ontology.brain-map.org/aio#>
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?classes
    WHERE {
      aio:NM_009334_accession_id rdf:type|rdfs:subClassOf ?classes .
    }
    """

    # Names and Labels for an specific entity
    names_and_labels = """\
    prefix aio: <http://ontology.brain-map.org/aio#>
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    prefix obo: <http://purl.obolibrary.org/obo/>

    SELECT ?entity ?label
    WHERE {
      {
        ?n obo:IAO_0000219 ?entity ;
           obo:OBI_0002815 ?label .
      } UNION {
        ?entity rdfs:label ?label .
      }
      FILTER(?entity = aio:GRC)
    }
    """

    # Select names or labels for several specific entities
    names_and_labels_specific = """\
    prefix aio: <http://ontology.brain-map.org/aio#>
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    prefix obo: <http://purl.obolibrary.org/obo/>

    SELECT ?entity ?label
    WHERE {
      {
        ?n obo:IAO_0000219 ?entity ;
           obo:OBI_0002815 ?label .
      } UNION {
        ?entity rdfs:label ?label .
      }
      VALUES ?entity { aio:GRC aio:NCBI aio:GRCm38_p3 }
    }
    """

    incoming_outgoing = """\
    prefix aio: <http://ontology.brain-map.org/aio#>
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    prefix obo: <http://purl.obolibrary.org/obo/>

    # Incoming and outgoing properties
    prefix aio: <http://ontology.brain-map.org/aio#>
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    prefix obo: <http://purl.obolibrary.org/obo/>

    # Incoming and outgoing associations with labels
    SELECT ?direction ?property ?property_label ?other_label
    WHERE {
      {
        ?entity ?property ?other .
        VALUES ?direction { "out" }
      } UNION {
        ?other ?property ?entity
        VALUES ?direction { "in" }
      }
      VALUES ?entity { aio:GRC }
      OPTIONAL { ?property rdfs:label ?property_label }
      OPTIONAL { ?other rdfs:label ?other_label }
    }
    ORDER BY ?direction
    """

    # Centrally Registered Ids
    crids = """\
    prefix aio: <http://ontology.brain-map.org/aio#>
    prefix owl: <http://www.w3.org/2002/07/owl#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    prefix obo: <http://purl.obolibrary.org/obo/>

    # obo:IAO_0000578 centrally registered identifier
    # obo:IAO_0000577 centrally registered identifier symbol
    # obo:BFO_0000051 has part
    # obo:IAO_0000136 is about
    # obo:OBI_0002815 has representation
    # obo:IAO_0000219 denotes
    SELECT ?CRID ?sym ?about ?representation ?registry
    WHERE {
        ?CRID a obo:IAO_0000578 ;
              obo:BFO_0000051 ?sym, ?rname;
              obo:IAO_0000136 ?about .
        ?sym a obo:IAO_0000577 ;
             obo:OBI_0002815 ?representation .
        ?rname obo:IAO_0000219 [ a obo:IAO_0000579 ] ;
               obo:OBI_0002815 ?registry .
    }
    """

    construct_reference_genome_process = '''\
CONSTRUCT {
    ?reference_genome a `reference genome` ;
                      `is specified input of` ?process .

    ?rname `denotes` ?reference_genome ;
           `has representation` ?name ;
           a ?rname_class .

    ?process a [ rdfs:label ?process_type_label ] ;
             rdfs:label ?process_label .

    ?role `realized in` ?process ;
          rdfs:label ?role_name ;
          a aio:data_curation ;
          `inheres in` ?curator .

    ?cname `denotes` ?curator ;
        a aio:name ;
        `has representation` ?curator_name .

    ?curator a ?curator_class .
}
WHERE {
    ?reference_genome a `reference genome` ;
                      `is specified input of` ?process .

    ?rname `denotes` ?reference_genome ;
           `has representation` ?name ;
           a ?rname_class .

    ?process a [ rdfs:label ?process_type_label ] ;
             rdfs:label ?process_label .

    ?role `realized in` ?process ;
          rdfs:label ?role_name ;
          a aio:data_curation ;
          `inheres in` ?curator .

    ?cname `denotes` ?curator ;
           a aio:name ;
           `has representation` ?curator_name ;
           a ?cname_class .

    ?curator a ?curator_class .
}
'''

    construct_role_info = '''\
CONSTRUCT {
    ?role a aio:data_curation ;
          a ?role_type ;
          rdfs:label ?role_label ;
          `inheres in` ?agent .

    ?role_type rdfs:label ?role_type_label .

    ?role `realized in` ?process ;
          rdfs:label ?process_label .

    ?process a ?process_type ;
             rdfs:label ?process_type_label .

    ?agent a ?agent_class .
} WHERE {
    ?role a aio:data_curation ;
          a ?role_type ;
          rdfs:label ?role_label ;
          `inheres in` ?agent .

    ?role_type rdfs:label ?role_type_label .

    ?role `realized in` ?process ;
          rdfs:label ?process_label .

    ?process a ?process_type ;
             rdfs:label ?process_type_label .

    ?agent a ?agent_class .
}
'''

    construct_crid_query = '''\
CONSTRUCT {
    ?crid ?is_about ?entity ;
          a `centrally registered identifier` ;
          `has part` ?crid_sym ;
          `has part` ?denotes_crid_registry .

    ?entity a ?e_class .

    ?crid_sym a `centrally registered identifier symbol` ;
              `has representation` ?representation .

    ?denotes_crid_registry `denotes` ?registry ;
                            a ?denotes_type ;
                           `has representation` ?registry_string .

    ?registry a `centrally registered identifier registry` .
} WHERE {
    ?crid ?is_about ?entity ;
          a `centrally registered identifier` ;
         `has part` ?crid_sym ;
         `has part` ?denotes_crid_registry .
    FILTER(?is_about = `is about` || ?is_about = `denotes`)

    ?entity a ?e_class .

    ?crid_sym a `centrally registered identifier symbol` ;
              `has representation` ?representation .

    ?denotes_crid_registry `denotes` ?registry ;
                           a ?denotes_type ;
                           `has representation` ?registry_string .

    ?registry a `centrally registered identifier registry` .
}
VALUES ?entity { %s }
'''

    top_level_clause = """\
    ?genome_annotation a aio:genome_annotation .
    
    ?reference_genome a `reference genome` ;
                      `is specified input of` ?process .

    ?chromosome `part of` ?reference_genome ;
                a `chromosome` .

    ?gene `part of` ?chromosome ;
          a ?gene_type .

    ?mrna `member of` ?gene ;
          a ?mrna_type .

    ?cds `part of` ?mrna ;
         a ?cds_type .

    ?polypeptide so:derives_from ?cds ;
         a ?polypeptide_type .

    ?crid `is about` ?polypeptide ;
          a `centrally registered identifier` ;
          `has part` ?crid_symbol ;
          `has part` ?registry_name .

    ?crid_symbol a `centrally registered identifier symbol` ;
                 `has representation` ?protein_symbol .

    ?version_number `part of` ?crid ;
                    a `version number` ;
                    `has representation` ?version .

    ?registry_name a aio:name ;
                   `denotes` ?registry .

    ?registry a `centrally registered identifier registry` .
    
    ?data_curation `realized in` ?process ;
                   `inheres in` ?organization .
"""

    top_level_values = "VALUES (?gene ?polypeptide) { (%s %s) }"

