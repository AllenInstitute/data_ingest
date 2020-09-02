Overview
========

Datasets triple store <==> UI and curation applications

**Datasets triple store**.
- Should natively support an entity class system. Hopefully gotten by selecting pertinent classes from a reference OWL ontology. Should be provably disjoint. This is for namespacing. Could be managed outside the triple store with pointer lists, or internally with RDF triples representing class membership.
- Each class should have a preferred primary key field. Will lead to warnings in case of duplication.
- Should internally manage validations at various levels. One level can be OWL consistency with respect to a reference ontology. The next level can be more detailed integrity testing for a given entity type, when this is not providd by OWL axioms (i.e. no missing fields).
- For use by people who are more interested in the infrastructure of the data model and its integrity than the real-life referents of the data items (projects, researchers).
- Should be capable of summary statistics with a view toward integrity of the store as a whole. Number of valid entities by name, number of modification events, set of contributors to modification events, number of allowed but undesirable configurations (e.g. a field which is designated as a preferred primary key is not unique among entities of a given type).
- Should be conservative, often rejecting requests by applications for updates and providing reason responses.

**UI and curation applications**.
- Should do all interfacing with external sources and targets, like CSV exports and reporting on specially queried subsets
- For use by people who are more interested in the real-life referents of the data items (projects, researchers) than the infrastructure of the data model and its integrity.

Types of application usage of API
=================================

Modification:
- **entity insert**. A brand new entity. *Requires*: Entity class (name or handle object?), and a dictionary of Data field classes with proposed values.
- **entity deprecate**. *Requires*: Some sort of reason code (e.g. entity no longer exists, or entity never existed, or entity no longer relevant).
- **relation insert**. *Requires*: Store-provided handles for source and target (see "Query" below), store-provided handle for the relation class.
- **relation deprecate**.
- **field modification**. Here I use "field" to mean the value of a named primitive / Data field of a given entity (or relation!) of interest. *Requires*: Store-provided handle

Query:
- **lookup handle**. The desired behavior for this function is to provide easy syntax for single-entity or single-relation lookups, without the full machinery of a query in a proper query language. It would return specially-designed handle objects whose functions are part of the exposed API module.

Examples:
```py
from ds_triple_store_api import *

store = TripleStore.get_connection()
# Will presumably have a hardcoded internal reference to an AI-internal server.
store['entitys preferred identifying field string value']
 # returns a handle object which knows its UID and various behaviors for display and phoning home to the triple store
store['entityclass name']
# returns some sort of iterator into the entities of this class (or else a special handle object for classes?)
store['identifying string value']['data field name']
# returns a handle object implementing primitive-wrapper capability
store['identifying string value']['relationclass name']
# returns a handle object implementing relation-wrapper capability, AND entity-wrapper capability (namely, a wrapper of the target of the relation)

store['identifying string value 1'] = Entity({'main name' : 'x', 'auxiliary data property name' : 'y'})
# translates inot a call to "entity insert"
# The right-hand side is, and is supposed to look like, an entity that only exists in local scope until after this statement succeeds. The store may reject this request.

e = store['identifying string value 2']
store['identifying string value 3']['relationclass name'] = e
# translates into a call to "relation insert"
```

Store-side implementation issues
================================

- **UID generation**. Store-provided handle objects should know their identity mainly by internally stored UIDs.
- **Version control and attribution**. Modifications events may get tagged with references to source attributions, something like user name and hostname of machine that generated the request, and IP, and unix-style timestamp.
- **Relations and inverses**. OWL knows how to model inverses of relations. In our use cases, we are not typically very sure whether the source or the target of a given relation is more likely to be the one focused on in a given case. I think this means that every relation should be registered at the same time as its inverse. Inverses are not a special type of relation, they are just relations themselves. Highly desirable to have names coming from ontology for both a relation and its inverse, unless we are sure we will never be starting from the target and seeking the source.

