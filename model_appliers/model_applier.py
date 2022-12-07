import re
from neointerface import NeoInterface
from data_providers.data_provider import DataProvider
from query_builders.query_builder import QueryBuilder


class ModelApplier(NeoInterface):
    """
    The purpose of the class is to refactor the data in the graph, extract_entities (create new nodes using data from other nodes)
    and link_entities (create relationships between existing nodes based on certain condition.
    The methods in the class utilize metadata nodes with labels Class and Property to get information on how to extract_entities
    and link_entities
    """
    RDFSLABEL = 'rdfs:label'

    def __init__(self, mode="schema_PROPERTY", *args, **kwargs):
        """
        :param mode: mode of MethodApplier to work either with schema_PROPERTY schema or MAPS_TO_CLASS schema
        :param args:
        :param kwargs:
        """
        assert mode is None or mode in ["schema_PROPERTY", "schema_CLASS"]
        super().__init__(*args, **kwargs)
        if not mode:
            res = self.query("MATCH (p:Property)<-[:MAPS_TO_PROPERTY]-() RETURN p LIMIT 1")
            if res:
                self.mode = "schema_PROPERTY"
            else:
                self.mode = "schema_CLASS"
            if self.debug:
                print(f"Setting ModelApplier mode to {self.mode}")
        else:
            self.mode = mode

    def delete_classes_entities(self, delete_rawdata=False) -> None:
        """
        Delete all nodes with labels corresponding to the existing Class Entities (such as
            'Study', 'Site', 'Race', 'Treatment', etc)

        If the flag delete_rawdata is True, then also eliminate all nodes with the label "Source Data Row".

        Note: all the relationships to the deleted nodes are deleted as well.

        :param delete_rawdata:  Flag indicating whether to also delete the nodes with the label "Source Data Row"
        :return:                None
        """
        # Get a list of all the values of the field "label" in nodes labeled "Class"
        list_of_labels_to_delete = self.get_single_field(field_name="label", labels="Class")  # List of Class entities

        if delete_rawdata:
            list_of_labels_to_delete.append("Source Data Row")  # Also mark "Source Data Row" nodes for deletion

        # print(list_of_labels_to_delete)    # EXAMPLE: ["Study", "Site", "Race", etc, possibly including "Source Data Row"]

        self.delete_nodes_by_label(delete_labels=list_of_labels_to_delete)

        if self.verbose:
            print("Nodes with the following labels have been deleted:", list_of_labels_to_delete)

    def define_refactor_indexes(self, where_map: dict = None) -> None:
        """
        Read classes and properties, and create index for most of them (except for some)

        In order to improve refactoring query performance indexes on the class_/properties need to be defined
        getting the list of labels and properties to define indexes on.

        :return: None
        """
        wh_list = []
        if not (where_map):
            where_map = {}
        else:
            wh_list, where_map = QueryBuilder.list_where_conditions_per_dict(where_map)
        wh = (("WHERE " + " AND ".join(wh_list)) if wh_list else "")
        if self.mode == "schema_PROPERTY":
            q = f"""
            MATCH (`Source Data Table`:`Source Data Table`)-[:HAS_COLUMN]->(`Source Data Column`:`Source Data Column`),
            (`Source Data Column`)-[:MAPS_TO_PROPERTY]->(Property:Property)<-[:HAS_PROPERTY]-(Class:Class)
            {wh}        
            RETURN DISTINCT Class.label as label, Class.create as create, Property.label as property
            """
        elif self.mode == "schema_CLASS":
            q = f"""
                MATCH (`Source Data Table`:`Source Data Table`)-[:HAS_COLUMN]->(`Source Data Column`:`Source Data Column`),
                (`Source Data Column`)-[:MAPS_TO_CLASS]->(Class:Class)
                {wh}        
                RETURN DISTINCT Class.label as label, Class.create as create, '{self.RDFSLABEL}' as property
                """
        res = self.query(q, where_map)
        for data in res:
            # handling Labels that are exceptions e.g. labels most likely to have many distinct values therefore does not make sense to create an index
            if not (data["create"] == True):
                self.create_index(data['label'], data['property'])

    def refactor_all(self, where_map=None) -> None:
        """
        Running full pipleline of refactoring the data (i.e. extract_entities, link_entities, etc.)
        :param where_map        a dictionary to be passed to extract_class_entities
        :return:        None
        """
        self.define_refactor_indexes()
        self.extract_class_entities(where_map=where_map)
        self.link_classes()
        self.create_is_a_rel_to_class()
        self.link_to_terms()

    def reshape_all(self, where_map=None, batch_size=1000) -> None:
        self.define_refactor_indexes()
        self.extract_class_entities()
        self.create_is_a_rel_to_class(batch_size=batch_size)
        self.link_via_is_a(batch_size=batch_size)
        self.link_to_terms()

    def refactor_selected(self, domains=None, classes=None) -> None:
        """
        Running a selective pipleline of refactoring the data (i.e. extract_entities, link_entities, etc.).
        Either Domains or Classes should not be None.
        :param where_map      a dictionary to be passed to extract_class_entities
        :param domains        a list of domains to be refactored
        :param classes        a list of classes to be refactored
        :return:        None
        """
        assert domains is not None or classes is not None, "No Domains or Classes were passed. To run a full pipeline of " \
                                                           "refactoring, use refactor_all()"

        where_map = {}
        if domains:
            where_map['Source Data Table'] = {'_domain_': domains}
        if classes:
            where_map['Class'] = {'label': classes}

        self.define_refactor_indexes(where_map=where_map)
        self.extract_class_entities(where_map=where_map)
        self.link_classes(domains=domains, classes=classes)

    def extract_class_entities(self, where_map=None, store_counts=True) -> None:
        """
        The 2 parts of the function are explained separately
        :param where_map      a dictionary to be passed to _extract_class_entities_part_1
        :return:    None
        """
        qres = self._extract_class_entities_part_1(where_map=where_map)
        self._extract_class_entities_part_2(qres)
        # only when store_counts=True we store counts of instances of classes on node with label 'Class'
        if store_counts:
            for lbl in set([lbl for res in qres for lbl in res['lbl']]):
                q = f"""
                    MATCH (x:`{lbl}`)
                    WITH count(x) as cnt
                    MATCH (c:Class{{label:$lbl}}) 
                    SET c.count = cnt 
                """
                self.query(q, {'lbl': lbl})

    def _extract_class_entities_part_1(self, where_map=None) -> [{}]:
        """
        Helper function to locate paths traversing the Neo4j graph from `Source Data Table` nodes to `Class` nodes,
        by way of `Source Data Column` and `Property` nodes,
        following the relationships `HAS_COLUMN`, `MAPS_TO_PROPERTY` and `HAS_PROPERTY`,
        but avoiding scenarios where an alternate path exists with a `MAPS_TO` relationship.

        In essence, this is a traversal from the lower-lever domain of "source tables" to
        the higher-level domain of "classes".

        The results of the traversal are compiled as a list of dictionaries, with a mix of gathered and created values

        :param where_map      a dictionary to be passed to QueryBuilder.list_where_conditions_per_dict to extend where condition
                              applied to meta-model nodes in the query.



        :return:    A list of dictionaries.  EXAMPLES of individual entries:
            {'mode': 'merge', 'domain': 'ADSL', 'coll': [['STUDYID', 'STUDYID']], 'lbl': 'Study'}
            {'mode': 'merge', 'domain': 'ADSL', 'coll': [['RACE', 'RACE'], ['RACEN', 'RACEN']], 'lbl': 'Race'}
            {'mode': 'merge', 'domain': 'ADAE', 'coll': [['ASTDT', 'ASTDT'], ['ASTTM', 'ASTTM'], ['AETERM', 'AETERM']], 'lbl': 'Adverse Event'}

            DICTIONARY KEYS:
                'mode':     the string "apoc.create.node" if the 'lbl' value (see below) occurs in the list self.labels_to_always_create;
                                otherwise, "apoc.merge.node"
                'domain':   the value of the "_domain_" attribute in the `Source Data Table` node
                'coll':     a 2-element list comprising the value of "_columnname_" in the `Source Data Column` node,
                                                    and the value of "label"  in the `Property` node
                'lbl':      a list of "label" attributes in the `Class` node (along with labels of the classes to which the class is SUBCLASS_OF)
        """
        if self.verbose:
            print(" ------ Refactoring loaded data per graph class_ definition.  EXECUTING PART 1 --------- ")
        wh_list = []
        if not (where_map):
            where_map = {}
        else:
            wh_list, where_map = QueryBuilder.list_where_conditions_per_dict(where_map)
        ### Processing MAPS_TO_PROPERTY
        # The MATCH part locates paths on the graph, from `Source Data Table` nodes to `Class` nodes;
        # the WHERE part excludes some of those paths based on the where_map paramtere;
        # the second WITH statement extracts some variables, and packages 2 variables as a "coll" pair;
        # the third WITH statement aliases "mode"

        if self.mode == "schema_PROPERTY":
            q_match = f"""
            MATCH (`Source Data Folder`:`Source Data Folder`)-[:HAS_TABLE]->(`Source Data Table`:`Source Data Table`),
            (`Source Data Table`)-[:HAS_COLUMN]->(`Source Data Column`:`Source Data Column`),
            (`Source Data Column`)-[:MAPS_TO_PROPERTY]->(property:Property)<-[:HAS_PROPERTY]-(Class:Class),
            p_classes=(Class)-[:SUBCLASS_OF*0..10]->(parent)
            WHERE NOT ( (parent)-[:SUBCLASS_OF]->() )
           {("AND " + " AND ".join(wh_list) if wh_list else "")}  
            """
        else:
            q_match = f"""
            MATCH (`Source Data Folder`:`Source Data Folder`)-[:HAS_TABLE]->(`Source Data Table`:`Source Data Table`),
            (`Source Data Table`)-[:HAS_COLUMN]->(`Source Data Column`:`Source Data Column`),
            (`Source Data Column`)-[:MAPS_TO_CLASS]->(Class:Class),
            p_classes=(Class)-[:SUBCLASS_OF*0..10]->(parent)
            WHERE NOT ( (parent)-[:SUBCLASS_OF]->() )
           {("AND " + " AND ".join(wh_list) if wh_list else "")}  
            WITH *, {{label:'{self.RDFSLABEL}'}} as property
            """
        q = f"""
           {q_match}                             
           WITH *, CASE WHEN Class.create = True THEN
                     'create'  
                   ELSE
                     'merge'
                   END AS mode
           ORDER BY `Source Data Table`, Class, `Source Data Column`
           WITH mode, `Source Data Table`, [c in nodes(p_classes) | c.label] as lbl, 
             collect([`Source Data Column`._columnname_, property.label]) as coll
           RETURN mode, `Source Data Table`._domain_ as domain, coll, lbl           
           """
        params = where_map
        if self.debug:
            print(q, params)
        qres = self.query(q, params)

        if self.debug:
            print("_extract_class_entities_part_1() created a list with the following ", len(qres), " elements: ")
            for r in qres:
                print("    ", r)
        ### Processing `Source Data Table` MAPS_TO_CLASS (extraction of 1 node per `Source Data Row` with no properties (CoreClass to link to)
        q2 = f"""
        MATCH (`Source Data Folder`:`Source Data Folder`)-[:HAS_TABLE]->(`Source Data Table`:`Source Data Table`),
           (`Source Data Table`)-[:MAPS_TO_CLASS]->(Class:Class),
           p_classes=(Class)-[:SUBCLASS_OF*0..10]->(parent)
           WHERE NOT ( ()-[:SUBCLASS_OF]->(Class) OR (parent)-[:SUBCLASS_OF]->() )
           {("AND " + " AND ".join(wh_list) if wh_list else "")}                          
           RETURN 
            CASE WHEN Class.create = True THEN
                'create'  
            ELSE
                'merge'
            END AS mode,
            `Source Data Table`._domain_ as domain,
            [] as coll,   
            [c in nodes(p_classes) | c.label] as lbl                        
        """
        qres_no_columns = self.query(q2, params)
        return qres + qres_no_columns

    def _extract_class_entities_part_2(self, qres) -> None:
        """
        Create new nodes connected to `Source Data Row` with "FROM_DATA" relationships as needed.

        For each dictionary in the list qres, look for `Source Data Table` nodes that have a "_domain_" attribute
            with a value matching that of the "domain" key in the dictionary,
            and connected to `Source Data Row` nodes with a ":HAS_DATA" relationship;
            also, require that the properties of the `Source Data Row` node overlap with the zero-th elements
            in the dictionary value for "coll".
            Whenever the above conditions are met, create a new node with label(s) specified
            by in "lbl", with the same attributes/value as the overlapping attributes
            found in the `Source Data Row` node, and pointing to the `Source Data Row` node with a
            relationship named "FROM_DATA"

        :param qres:    A list of dictionaries. For details, see _extract_class_entities_part_1()
        :return:        None
        """
        if self.verbose:
            print(" ------ Refactoring loaded data per graph class_ definition.  EXECUTING PART 2 --------- ")
            print("    LOOPING OVER ", len(qres), " entries in helper list:")

        for i, r in enumerate(qres):
            if self.debug:
                print(f"{i} Processing {r}")
            self.extract_entities(
                mode=r['mode'],
                label='Source Data Row',
                cypher='''
                    MATCH (f:`Source Data Table`{_domain_:$domain})-[:HAS_DATA]->(node:`Source Data Row`)
                    RETURN id(node)
                ''',
                cypher_dict={'domain': r['domain']},
                target_label=r["lbl"],
                property_mapping={x[0]: x[1] for x in r["coll"]},
                relationship='FROM_DATA',
                direction='<'
            )

    def link_classes(self, domains=None, classes=None) -> None:
        """
        Establish, among data nodes, relationships that echo the relationships
        among their corresponding (metadata) classes.

        Look for all pairs of "Class"-labeled nodes linked by a "<-[:FROM]-(:Relationship)-[:TO]->" relationship;
        if (left_c, right_c) is such a pair, then look for all triples of nodes fulfilling some requirements
        (described below) - and to all the found node, add relationships.

        Requirement for the sought triplets (left, sdr, right) of nodes, given the (left_c, right_c) pair above:
            1. the label of (left) equals the value of the "label" attribute of the node left_c
            2. the label of (right) equals the value of the "label" attribute of the node right_c
            3. the label of (sdr) is `Source Data Row`
            4. there's a relationship named FROM_DATA from (left) to (sdr), as well as from (right) to (sdr)

        The added relationship, if all the above requirements are met, is:
            A relationship from (left) to (right), named HAS_XYZ , where XYZ is the (upper-case version) of the
            value of the "label" attribute of the node right_c

        SEE DIAGRAM in repository: docs/link_classes.png


        :param classes    an optional list of classes to pass to _link_classes_part_1, in order to limit qres to specific classes
        :param domains    an optional list of domains to pass to _link_classes_part_1, in order to limit qres to specific domains
        :return: None
        """
        qres = self._link_classes_part_1(domains=domains, classes=classes)
        self._link_classes_part_2(qres)

    def _link_classes_part_1(self, domains=None, classes=None):
        """
        :param classes    an optional list of classes to define WHERE condition, to only return specified classes for linking
        :param domains    an optional list of domains to define WHERE condition, to only return specified domains for linking
        """

        conditions = []
        if domains:
            conditions.append(""" ( apoc.coll.intersection(apoc.text.split(left_c.from_domains,","), $domains) OR 
                                    apoc.coll.intersection(apoc.text.split(right_c.from_domains,","), $domains) )
                              """)

        if classes:
            conditions.append("(left_c.label in $classes OR right_c.label in $classes)")

        selection = "WHERE " + " and ".join(conditions) if conditions else ""

        q = f"""
            MATCH (left_c:Class)<-[:FROM]-(r:Relationship)-[:TO]->(right_c:Class)
            {selection}
            RETURN DISTINCT left_c.label as left_class, right_c.label as right_class, r.relationship_type as relationship
            """

        params = {"domains": domains, "classes": classes}
        if self.debug:
            print(q, params)

        qres = self.query(q, params)

        if self.debug:
            print("_link_classes_part_1() created a list with the following ", len(qres), " elements: ")
            for r in qres:
                print("    ", r)

        return qres

    def _link_classes_part_2(self, qres: list) -> None:
        if self.verbose:
            print("--------------- Linking classes ----------------------")
        for res in qres:
            self.link_entities(
                left_class=res['left_class'],
                right_class=res['right_class'],
                relationship=(res['relationship'] if res['relationship'] is not None else "_default_"),
                cond_via_node="Source Data Row",
                cond_left_rel="FROM_DATA>",
                cond_right_rel="<FROM_DATA",
            )

    # supporing the slternative refactoring (reshape_all)
    def create_is_a_rel_to_class(self, batch_size=10000):
        if self.verbose:
            print("--------------- Creating IS_A relationship ----------------------")
        q = """            
        call apoc.periodic.iterate(
        '
        MATCH (c:Class)        
        WHERE NOT c.label in ["Class", "Resource", "Variable", "Dataset", "Term"]
        CALL apoc.cypher.run("MATCH (x:`" + c.label +"`) RETURN x", {}) YIELD value
        WITH c, value["x"] as x
        RETURN c, x
        ',
        'MERGE (x)-[:IS_A]->(c)',        
        {batchSize:$batch_size, parallel:$parallel})
        YIELD total, batches, failedBatches, failedOperations 
        RETURN total, batches, failedBatches, failedOperations 
        """
        res = []
        r1 = self.query(q, {'parallel': True, 'batch_size': batch_size})
        res.append(r1)
        if r1:
            if r1[0]['failedBatches'] > 0:
                r2 = self.query(q, {'parallel': False, 'batch_size': batch_size})
                res.append(r2)
        if self.verbose:
            print(res)
        return (res)

    # supporing the slternative refactoring (reshape_all)
    def create_is_a_rel_to_class2(self, batch_size=10000):
        if self.verbose:
            print("--------------- Creating IS_A relationship ----------------------")
        q = """
        MATCH (c:Class)          
        WHERE c.label in $labels OR $labels = '*'    
        call apoc.periodic.iterate(
        'MATCH (x:`'+c.label+'`) WHERE NOT (x)-[:IS_A]->(:Class) RETURN x',                   
        'WITH x, $c as c MERGE (x)-[:IS_A]->(c)',        
        {batchSize:$batch_size, parallel:$parallel, params: {c:c}})
        YIELD total, batches, failedBatches, failedOperations 
        RETURN c.label as label, total, batches, failedBatches, failedOperations
        """
        res = []
        r1 = self.query(q, {'parallel': True, 'batch_size': batch_size, 'labels': '*'})
        res.append(r1)
        labels = []
        for r in r1:
            if r['failedBatches']:
                labels.append(r['label'])
        if labels:
            r2 = self.query(q, {'parallel': True, 'batch_size': batch_size, 'labels': labels})
            res.append(r2)
        if self.verbose:
            print(res)
        return res

    # supporing the slternative refactoring (reshape_all)
    def link_via_is_a(self, batch_size=10000):
        if self.verbose:
            print("--------------- Linking classes ----------------------")
        q = """
        call apoc.periodic.iterate(      
        '  
            MATCH (c1:Class)<-[:IS_A]-(ent1)-[:FROM_DATA]->(data)<-[:FROM_DATA]-(ent2)-[:IS_A]->(c2:Class),
            (c1)<-[:FROM]-(r:Relationship)-[:TO]->(c2)
            WHERE NOT (ent1)-[]->(ent2) //TODO: to check if if works (could be 2 rels btw same 2 nodes)
            RETURN c1, c2, r, ent1, ent2
        '
        ,
        '
            CALL apoc.merge.relationship(
                ent1, 
                CASE WHEN r.relationship_type IS NULL THEN
                    c2.label
                ELSE
                    r.relationship_type
                END, 
                {}, //identProps 
                {}, //props
                ent2, 
                {} //onMatchProps 
            ) 
            YIELD rel 
            RETURN rel
        '
        ,
        {batchSize:$batch_size, parallel:$parallel}
        )
        YIELD total, batches, failedBatches, failedOperations
        RETURN total, batches, failedBatches, failedOperations        
        """
        res = []
        r1 = self.query(q, {'parallel': True, 'batch_size': batch_size})
        res.append(r1)
        if r1:
            if r1[0]['failedBatches'] > 0:
                r2 = self.query(q, {'parallel': False, 'batch_size': batch_size})
                res.append(r2)
        if self.verbose:
            print(res)
        return res

    # supporing the slternative refactoring (reshape_all)
    def link_to_terms(self, batch_size=10000):
        q = """
                call apoc.periodic.iterate(      
                '  
                    MATCH (class:Class)-[:HAS_CONTROLLED_TERM]->(term:Term)
                    RETURN class, term
                '
                ,
                '
                    MATCH (class)<-[IS_A]-(class_instance)
                    WHERE term.`rdfs:label` = class_instance.`rdfs:label`
                    MERGE (class_instance)-[:Term]->(term)
                '
                ,
                {batchSize:$batch_size, parallel:$parallel}
                )
                YIELD total, batches, failedBatches, failedOperations
                RETURN total, batches, failedBatches, failedOperations        
                """
        res = self.query(q, {'parallel': True, 'batch_size': batch_size})
        if self.verbose:
            print(res)
        return res

    def delete_reshaped(self, batch_size=10000):
        if self.verbose:
            print(" ------------------ Deleting reshaped instances ------------------")
        q = f"""
        call apoc.periodic.iterate(
        '
            MATCH (n)-[:FROM_DATA]->(data)
            WHERE NOT n:Term
            RETURN n
        ',
            'DETACH DELETE(n)',        
        {{batchSize:$batch_size, parallel:$parallel}})
        YIELD total, batches, failedBatches, failedOperations
        RETURN total, batches, failedBatches, failedOperations                                                                    
        """
        res = []
        r1 = self.query(q, {'parallel': True, 'batch_size': batch_size})
        res.append(r1)
        if r1:
            if r1[0]['failedBatches'] > 0:
                r2 = self.query(q, {'parallel': False, 'batch_size': batch_size})
                res.append(r2)
        q = f"""
        call apoc.periodic.iterate(
        '
        MATCH (x:Term)-[r:FROM_DATA]->(data) 
        OPTIONAL MATCH (x)-[r2:IS_A]->(:Class)
        RETURN r, r2
        ',
        'DELETE(r) DELETE(r2)',        
        {{batchSize:$batch_size, parallel:$parallel}})
        YIELD total, batches, failedBatches, failedOperations
        RETURN total, batches, failedBatches, failedOperations               
        """
        r3 = self.query(q, {'parallel': True, 'batch_size': batch_size})
        res.append(r3)
        if r3:
            if r3[0]['failedBatches'] > 0:
                r4 = self.query(q, {'parallel': False, 'batch_size': batch_size})
                res.append(r4)
        if self.verbose:
            print(res)
        return res

    def label_entities(self,
                       class_='Observation',
                       add_label=None,
                       cond_via_rel='HAS_OBSERVATION',
                       cond_via_rel_direction='<',
                       cond_cypher=None,
                       cond_cypher_dict=None
                       ):
        # TODO: it is possible to used DataLoader.get_data for this use case for fetching data surrounding nodes to be labeled
        """
        Adds additional label on nodes with label {class_}
          the additional label is taken from property {add_label['Property']} of node with label {add_label['Class']}
          which is connected to the initial node with relationship of type {cond_via_rel} in direction {cond_via_rel_direction}
          or which satisfies the provided {cond_cypher} condition
          NOTE cond_via_rel, cond_via_rel_direction are disregarded in case of cond_cypher provided
        :param class_: label on the nodes on which new labels will be assigned
        :param add_label: where (from the graph) is the information about the label(to be assigned) is taken from
        :param cond_via_rel: if cond_cypher is not used then the information about the label(to be assigned) is looked-up
        in the nodes related to the initial with {cond_via_rel} relationship
        :param cond_via_rel_direction: direction of {cond_via_rel} relationship
        :param cond_cypher: None or a valid cypher query that must return neo4j variables named {class_.lower()} and {add_label["Class"].lower()}
        :param cond_cypher_dict: dictionary of additional parameters for cond_cypher (if required)
        :return:
        """
        assert type(add_label) == dict
        assert "Class" in add_label.keys()
        assert "Property" in add_label.keys()
        if not add_label:
            add_label = {'Class': 'Category', 'Property': 'name'}
        if cond_cypher:
            assert re.search(f'RETURN.*{class_.lower()}', cond_cypher)
            assert re.search(f'RETURN.*{add_label["Class"].lower()}', cond_cypher)
            if self.verbose:
                print(f"Using cypher condition add labels to link node {class_}; Cypher: {cond_cypher}")
            list_returns = [f'value["{rtrn}"] as `{rtrn}`' for rtrn in [class_.lower(), add_label['Class'].lower()]]
            periodic_part1 = f"""
            CALL apoc.cypher.run($cypher, $cypher_dict) YIELD value            
            RETURN {', '.join(set(list_returns))}                                        
            """

        else:
            assert cond_via_rel_direction in ["<", ">", "", None]
            d1 = ("<" if cond_via_rel_direction == "<" else "")
            d2 = (">" if cond_via_rel_direction == ">" else "")
            periodic_part1 = f"""
            MATCH (`{class_.lower()}`:`{class_}`){d1}-[:`{cond_via_rel}`]-{d2}(`{add_label['Class'].lower()}`:`{
            add_label['Class']}`)
            RETURN {class_.lower()}, {add_label['Class'].lower()}                
            """
        q = f"""              
                   call apoc.periodic.iterate(
                   '{periodic_part1}',
                   'CALL apoc.create.addLabels(`{class_.lower()}`, [' + $property + ']) YIELD node RETURN node',        
                   {{batchSize:10000, parallel:false, params: {{cypher: $cypher, cypher_dict: $cypher_dict}}}})
                   YIELD total, batches, failedBatches, failedOperations
                   RETURN total, batches, failedBatches, failedOperations
               """
        params = {'cypher': cond_cypher, 'cypher_dict': cond_cypher_dict,
                  'property': "`" + add_label['Class'].lower() + "`.`" + add_label['Property'] + "`"
                  }
        if self.debug:
            print("        Query : ", q)
            print("        Query parameters: ", params)
        self.query(q, params)
