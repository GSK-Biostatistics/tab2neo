import os
from neointerface import NeoInterface
import pandas as pd


class ModelManager(NeoInterface):
    """
    Python class to manage metadata nodes (such as nodes with label Class, Relationship, Term) along with relationships between them
    as well as to return information about them
    """
    URI_MAP = {
        "Class": {"properties": ["label"]},
        "Property": {"properties": ["Class.label", "label"]},
        "Relationship": {"properties": ["relationship_type"],
                         "neighbours": [
                             {"label": "Class", "relationship": "FROM", "property": "label"},
                             {"label": "Class", "relationship": "TO", "property": "label"},
                         ]},
        "Term": {"properties": ["Codelist Code", "Term Code"]},
        "Method": {"properties": ["parent_id", "id"]},
    }
    SCD = 50  # SUBCLASS_OF allowed Depth
    RDFSLABEL = "rdfs:label"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.verbose:
            print(f"---------------- {self.__class__} initialized -------------------")

    def gen_default_reltype(self, to_label: str) -> str:
        """
        Default relationship type name is generated from the label of the :TO Class
        """
        return f'{to_label}'

    def gen_default_reltypes_list(self, rels: list) -> [{}]:
        """
        Updates list of dicts with relationships {'from':...,'to':...,'type':...}
        replacing 'type':None with default relationship type
        """
        return [{**rel, **{'type': (rel['type'] if rel['type'] else self.gen_default_reltype(rel['to']))}}
                for rel in rels
                ]

    def create_class(self, classes, merge=True) -> [list]:
        """
        :param classes: Name, or list of names, to give to the new class(es) created
        :param merge:   boolean - if True use MERGE statement to create nodes to avoid duplicate classes
                            TODO: address question "would we want to ever allow multiple classes with the same name??"
        :return:        A list of lists that contain a single dictionary with keys 'label', 'neo4j_id' and 'neo4j_labels'
                        EXAMPLE: [ [{'label': 'A', 'neo4j_id': 0, 'neo4j_labels': ['Class']}],
                                   [{'label': 'B', 'neo4j_id': 1, 'neo4j_labels': ['Class']}]
                                 ]
        """
        assert type(merge) == bool

        if not type(classes) == list:
            classes = [classes]

        if merge:
            q = """
            WITH $classes as classes 
            UNWIND classes as class_name 
            CALL apoc.merge.node(['Class'], {label: class_name}, {}, {}) 
            YIELD node 
            RETURN node as class
            ORDER by node
            """
        else:
            q = """
            WITH $classes as classes 
            UNWIND classes as class_name 
            CALL apoc.create.node(['Class'], {label: class_name}) YIELD node 
            RETURN node as class
            ORDER by node
            """

        params = {'classes': classes}

        if self.debug:
            print(f"""
            query: {q}
            parameters: {params}
            """)

        return self.query_expanded(q, params)

    def set_short_label(self, label: str, short_label: str) -> None:
        "One the class with :Class{label:{label}} - sets property 'short_label value to the provided"
        q = """
        MATCH (c:Class)
        WHERE c.label = $label
        SET c.short_label = $short_label
        """
        params = {'label': label, 'short_label': short_label}
        self.query(q, params)

    def create_related_classes_from_list(self, rel_list: [[str, str, str]]) -> [str]:
        """
        Create `Class` and `Relationship` nodes between them, as specified by rel_list

        Given a list of relationship triplets, perform 2 operations:
        1)  Identify all the unique names, and create new nodes labeled "Class",
            each new node has one of the unique names stored in an attribute named "label"
        2)  Adds <-[:FROM]-(:Relationship{relationship_type:'...'})-[:TO]-> relationships to pairs of the newly-created nodes,
            as specified by the triplets in the elements of rel_list

        EXAMPLE:  if rel_list is  [  ["Study", "Site", "Site],  ["Study", "Subject", "Subject]  ]
                  then 3 new `Class`-labeled nodes will be created, with "label" attributes respectively
                  valued "Study", "Site" and "Subject",
                  plus <-[:FROM]-(:Relationship{relationship_type:'Site'})-[:TO]-> relationship from "Study" to "Site",
                  and <-[:FROM]-(:Relationship{relationship_type:'Subject'})-[:TO]-> relationship from "Study" to "Subject"

        :param rel_list: A list of 3-element lists, indicating a relationship among nodes of type `Class`
                         EXAMPLE:   [
                                        ["Study", "Site", "Site'],
                                        ["Study", "Subject", "Subject"],
                                        ["Subject", "Race", "Race"]
                                    ]
        :return:         List of all the class names; repeated ones are taken out
        """

        # Identify all the unique class names in inner elements of rel_list
        class_set = set()  # Empty set
        for rel in rel_list:
            # [:2] in order to get only the first two items (classes) out of rel_list; [2] is the relationship type
            class_set = class_set.union(rel[:2])  # The Set Union operation will avoid duplicates

        class_list = sorted(list(class_set))  # Convert the final set back to list

        q = f"""            
        UNWIND $rels as rel
        WITH rel[0] as left, rel[1] as right, rel[2] as type    
        WHERE apoc.meta.type(left) = apoc.meta.type(right) = 'STRING'  
        MERGE (ln:Class {{label:left}})
        MERGE (rn:Class {{label:right}})   
        MERGE (ln)<-[:FROM]-(:Relationship{{relationship_type:type}})-[:TO]->(rn)   
        """
        params = {"rels": [(r if len(r) == 3 else r + [self.gen_default_reltype(to_label=r[1])]) for r in rel_list]}
        self.query(q, params)

        return class_list

    def get_all_classes(self) -> [str]:
        ""
        return [c['Class'] for c in self.get_all_classes_with_nodeids()]

    def get_all_classes_with_nodeids(self, include_id=False, sort=True) -> [dict]:
        """
        Get all the existing Class names, optionally including their Neo4j ID, and optionally sorted
        :param include_id:  If True, also include the Neo4j ID's
        :param sort:        If True, sort the results by name
        :return:            A list of dictionaries, with keys "Class" (for the name) and "_id_Class"
                            EXAMPLE, with include_id=False:
                                [{'Class': 'car'}, {'Class': 'boat'}]
                            EXAMPLE, with include_id=True:
                                [{'Class': 'car', "_id_Class": 88}, {'Class': 'boat', "_id_Class": 91}]
        """
        q = '''
            MATCH (class:Class)
            RETURN class.label as Class, class.short_label as short_label
            '''

        if include_id:
            q += " , id(class) as _id_Class "

        if sort:
            q += " ORDER BY class.label"

        return self.query(q)

    def get_rels_from_labels(self, labels: list) -> [{}]:
        """
        Returns all the relationships (according to the schema) from the nodes with specified labels
        including the relationships of parent and child classes
        """
        q = f"""
        MATCH 
            (c1:Class)<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(c1low:Class)
        WHERE 
            c1.label in $labels AND
            NOT EXISTS ( (c1low)<-[:SUBCLASS_OF]-(:Class) )
        WITH c1low
        MATCH 
            path1 = (c1high:Class)<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(c1low:Class)
        WHERE             
            NOT EXISTS ( (:Class)<-[:SUBCLASS_OF]-(c1high) )
        WITH nodes(path1) as col1
        UNWIND col1 as c1       
        MATCH (x)<-[f:FROM]-(rr:Relationship)-[t:TO]->(y)
        WHERE x = c1 or y = c1
        RETURN {{from: x.label, to: y.label, type: rr.relationship_type}} as rel            
        ORDER BY rel['from'], rel['to'], rel['type']
        """
        params = {'labels': labels}
        res = self.query(q, params)
        return [x['rel'] for x in res]

    def get_labels_from_rels_list(self, rels_list: list) -> [str]:
        "Returns all the class labels from a list of relationships in the form {'from':...,'to':...,'type':...}"
        labels = []
        for rel in rels_list:
            for key in ['from', 'to']:
                if rel.get(key) not in labels:
                    labels.append(rel.get(key))
        return labels

    def get_rels_btw2(self, label1: str, label2: str):
        """
        Returns all the relationships (according to the schema) between nodes with sprcified labels {label1} and {label2}
        including the relationships of parent and child classes
        """
        q = f"""
        MATCH 
            (c1:Class)<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(c1low:Class),
            (c2:Class)<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(c2low:Class)
        WHERE 
            c1.label = $label1 AND c2.label = $label2 AND
            NOT EXISTS ( (c1low)<-[:SUBCLASS_OF]-(:Class) ) AND
            NOT EXISTS ( (c2low)<-[:SUBCLASS_OF]-(:Class) ) 
        WITH c1low, c2low
        MATCH 
            path1 = (c1high:Class)<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(c1low:Class),
            path2 = (c2high:Class)<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(c2low:Class)
        WHERE             
            NOT EXISTS ( (:Class)<-[:SUBCLASS_OF]-(c1high) ) AND
            NOT EXISTS ( (:Class)<-[:SUBCLASS_OF]-(c2high) ) 
        WITH nodes(path1) as col1, nodes(path2) as col2
        UNWIND col1 as c1
        UNWIND col2 as c2
        WITH c1, c2
        MATCH (x)<-[f:FROM]-(rr:Relationship)-[t:TO]->(y)
        WHERE (x = c1 and y = c2) or (y = c1 and x = c2) 
        RETURN {{from: x.label, to: y.label, type: rr.relationship_type}} as rel            
        ORDER BY rel['from'], rel['to'], rel['type']
        """
        params = {'label1': label1, 'label2': label2}
        res = self.query(q, params)
        return [x['rel'] for x in res]

    def infer_rels(self, labels: list, oclass_marker: str = "**", impute_relationship_type: bool = True):
        """
        Infers most appropriate relationship type (if exists) between each pair of $labels
        for generating cypher query according to the schema
        """
        q = f"""
        MATCH (a:Class), (b:Class)
        WHERE a.label in $labels and b.label in $labels 
        AND (
                ( EXISTS ( (a)-[:SUBCLASS_OF*0..{str(
            self.SCD)}]->()<-[:FROM]-(:Relationship)-[:TO]->()<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(b) ) )
                OR
                ( EXISTS ( (a)<-[:SUBCLASS_OF*0..{str(
            self.SCD)}]-()<-[:FROM]-(:Relationship)-[:TO]->()-[:SUBCLASS_OF*0..{str(self.SCD)}]->(b) ) )
            )
        OPTIONAL MATCH 
            p1 = (a)-[:SUBCLASS_OF*0..{str(
            self.SCD)}]->()<-[:FROM]-(r1:Relationship)-[:TO]->(crt_to1)<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(b)
        OPTIONAL MATCH
            p2 = (a)<-[:SUBCLASS_OF*0..{str(
            self.SCD)}]-()<-[:FROM]-(r2:Relationship)-[:TO]->(crt_to2)-[:SUBCLASS_OF*0..{str(self.SCD)}]->(b)
        WITH a, b, 
            collect(distinct {{
                path: p1, 
                rel_type: 
                    CASE WHEN r1.relationship_type is NULL and $impute_relationship_type THEN 
                        crt_to1.label
                    ELSE
                        r1.relationship_type
                    END,   
                short_label: r1.short_label, 
                tag: 'p1'
            }})
            +
            collect({{
                path: p2, 
                rel_type: 
                    CASE WHEN r2.relationship_type is NULL THEN 
                        crt_to2.label
                    ELSE
                        r2.relationship_type
                    END,   
                short_label: r2.short_label,               
                tag: 'p2'
            }})
            as coll
        UNWIND coll as map
        WITH a, b, map, size(nodes(map['path'])) as sz 
        WHERE not map['path'] is NULL
        WITH *
        ORDER BY map['tag'], sz //Here we prioritizing paths with SUBCLASS > over paths with < SUBCLASS rel. and then prioritize shorter paths  
        WITH a, b, collect(map) as coll //ideally we should always have only 1 path for each pair, but allowing here for >1
        WITH a, b, coll[0] as map        
        //WITH apoc.map.mergeList(
        //    [{{from: a.label, to: b.label, type: map['rel_type']}}] +
        //        CASE WHEN short_label in keys(a) THEN [{{from_tag: a.short_label}}] ELSE [] END +
        //        CASE WHEN short_label in keys(b) THEN [{{to_tag: b.short_label}}] ELSE [] END
        //) as rel
        WITH {{from: a.label, to: b.label, type: map['rel_type'], short_label: map['short_label']}} as rel
        WITH *
        ORDER BY rel['from'], rel['to'], rel['type']        
        RETURN rel
        """
        olabels = [label for label in labels if label.endswith(oclass_marker)]
        params = {
            'labels': [(label[:-(len(oclass_marker))] if label in olabels else label) for label in labels],
            'impute_relationship_type': impute_relationship_type
        }
        res = self.query(q, params)
        rels = []
        for r in res:
            dct = r['rel']
            if dct.get('from') in olabels or dct.get('to') in olabels:
                dct['optional'] = True
            rels.append(dct)
        return rels

    def translate_to_shortlabel(self, labels: list, rels: list, labels_to_pack, where_map: dict = None,
                                where_rel_map: dict = None, use_rel_labels=True):
        if not where_map:
            where_map = {}
        if not where_rel_map:
            where_rel_map = {}

        q = """
        MATCH (c:Class)
        WHERE c.label in $labels
        RETURN apoc.map.fromPairs(collect([
            c.label,
            CASE WHEN 'short_label' in keys(c) THEN  
                c.short_label
            ELSE
                c.label
            END
        ])) as map
        """
        params = {'labels': list(set(labels + [rel.get('from') for rel in rels] + [rel.get('to') for rel in rels]))}
        assert labels_to_pack is None or isinstance(labels_to_pack, dict)
        if labels_to_pack is not None:
            labels_lst = []
            for key, value in labels_to_pack.items():
                assert isinstance(value,
                                  (str, list)), f'Value in labels_to_pack is not string or list. It was: {type(value)}'
                labels_lst.append(key)
                if isinstance(value, str):
                    labels_lst.append(value)
                elif isinstance(value, list):
                    labels_lst.extend(value)
            params['labels'].extend(labels_lst)
        # print(f'PARAMS: {params}')

        res = self.query(q, params)
        map = res[0]['map']
        if use_rel_labels:
            for rel in rels:
                if rel.get('short_label'):
                    map[rel['to']] = rel.get('short_label')
        if not labels:
            labels = []
        labels = [{'label': label, 'short_label': map[label]} for label in labels]
        if not rels:
            rels = []
        rels = [{**rel, **{'from': map[rel['from']], 'to': map[rel['to']]}} for rel in rels]

        if labels_to_pack is not None:
            mapped_labels_to_pack = {}
            # print(f'MAP: {map}')
            for key, value in labels_to_pack.items():
                assert isinstance(value,
                                  (str, list)), f'Value in labels_to_pack is not string or list. It was: {type(value)}'
                if isinstance(value, str):
                    mapped_labels_to_pack[map[key]] = map[value]
                elif isinstance(value, list):
                    # print(f'KEY: {key}')
                    # print(f'VALUE: {value}')
                    assert key in map.keys(), f'{key} was not found as a key in {map}'
                    assert (True if i in map.keys() else False for i in
                            value), f'a value from {value} was not found as a key in {map}'
                    mapped_labels_to_pack[map[key]] = [map[i] for i in value]
                    # convert the fromclass/coreclass label to short label only
                    # leave the label as it needs to be long format for the generate_call function
            labels_to_pack = mapped_labels_to_pack

        if not where_map:
            where_map = {}
        where_map = {map[key]: item for key, item in where_map.items()}
        where_rel_map = {map[key]: item for key, item in where_rel_map.items()}
        return labels, rels, labels_to_pack, where_map, where_rel_map

    @staticmethod
    def arrows_dict_uri_dict_enrich(dct: dict, uri_map: dict):

        def _gen_new_prop_name(neighbour: dict):
            assert len({"label", "relationship", "property"}.intersection(neighbour.keys())) == 3
            return f"{neighbour['relationship']}.{neighbour['label']}.{neighbour['property']}"

        def _get_neighbour_id(nd: dict, neighbour: dict):
            assert len({"label", "relationship", "property"}.intersection(neighbour.keys())) == 3
            for rel in dct['relationships']:
                if nd["id"] in [rel["toId"]] and rel["type"] == neighbour["relationship"]:
                    return rel["fromId"]
                elif nd["id"] in [rel["fromId"]] and rel["type"] == neighbour["relationship"]:
                    return rel["toId"]
            return None

        def _get_neighbour_value(nd: dict, neighbour: dict):
            neighbour_id = _get_neighbour_id(nd, neighbour)
            for nd in dct['nodes']:
                if nd['id'] == neighbour_id:
                    return nd['properties'].get(neighbour["property"])

        def _enrich_node(nd: dict, neighbour: str):
            value = _get_neighbour_value(nd, neighbour)
            return {**nd, **{"properties": {**nd["properties"], **{_gen_new_prop_name(neighbour): value}}}}

        merge_on = {}
        for key, item in uri_map.items():
            merge_on[key] = item.get("properties").copy()
            if not merge_on[key]:
                merge_on[key] = []
            if item.get('neighbours'):
                for n in item['neighbours']:
                    new_nodes = []
                    for nd in dct['nodes']:
                        if key in nd['labels']:
                            new_nodes.append(_enrich_node(nd, n))
                        else:
                            new_nodes.append(nd)
                    dct = {**dct, **{"nodes": new_nodes}}
                    merge_on[key].append(_gen_new_prop_name(n))
        return dct, merge_on

    def get_class_ct(self, class_: str, ct_prop_name='rdfs:label'):
        q = """
        MATCH (c:Class) 
        WHERE c.label = $class_
        OPTIONAL MATCH (c)-[:HAS_CONTROLLED_TERM]->(t:Term)
        RETURN collect(DISTINCT t[$ct_prop_name]) as coll
        """
        params = {'class_': class_, 'ct_prop_name': ct_prop_name}
        res = self.query(q, params)
        if res:
            return res[0]['coll']
        else:
            return []

    def propagate_rels_to_parent_class(self):
        if self.verbose:
            print("Copying Relationships to 'parent' Classes where (child)-[:SUBCLASS_OF]->(parent)")
        self.query("""
        MATCH (c:Class)<-[r1:TO|FROM]-(r:Relationship)-[r2:TO|FROM]-(target:Class), (c)-[:SUBCLASS_OF*1..50]->(source:Class)
        WHERE type(r1) <> type(r2)
        WITH *,
        "
            WITH $source as source, $target as target
            MERGE (source)<-[:`"+type(r1)+"`]-(:Relationship{relationship_type:$type})-[:`"+type(r2)+"`]->(target)
            RETURN count(*)
        " as q, 
        {type: r.relationship_type, source: source , target: target} as params
        CALL apoc.cypher.doIt(q, params) YIELD value
        RETURN value, q, params         
        """)

    def remove_unmapped_classes(self):
        q = """
        MATCH (c:Class)
        WHERE NOT EXISTS (
            ()-[:MAPS_TO_CLASS]->()-[:SUBCLASS_OF*0..50]->(c)
        )
        OPTIONAL MATCH (c)<-[:TO|FROM]-(r:Relationship)
        REMOVE r:Relationship
        REMOVE c:Class
        """
        self.query(q)

    def remove_auxilary_term_labels(self):
        """
        To be used after reshaping - additional labels from Terms that have not been extracted from data are removed
        :return:
        """
        q = """
        MATCH (x:Term) 
        WHERE not exists ( (x)-[:FROM_DATA]->() )
        WITH x, labels(x) as coll
        UNWIND coll as lbl
        WITH *
        WHERE NOT lbl in ["Term", "Class"]
        CALL apoc.cypher.doIt(
        'WITH x REMOVE x:`'+lbl+'`',
        {x:x}
        ) yield value
        RETURN value
        """
        self.query(q)

    def export_model_ttl(self, folder: str, filename: str, include_mappings=False):
        uri_map1 = {
            "Data Extraction Standard": {"properties": "_tag_"},
            "Source Data Folder": {"properties": "_folder_"},
            "Source Data Table": {"properties": "_domain_"},
            "Source Data Column": {"properties": ["_domain_", "_columnname_"]}
        }
        uri_map2 = {key: item
                    for key, item in ModelManager.URI_MAP.items()
                    if key in ["Class", "Property", "Relationship"]}

        if include_mappings:
            self.rdf_generate_uri({**uri_map1, **uri_map2})
            rdf = self.rdf_get_subgraph(
                """
                MATCH (n:Class)
                call apoc.path.expand(n, 
                    'SUBCLASS_OF|HAS_CONTROLLED_TERM>|<MAPS_TO_CLASS|<HAS_COLUMN|<HAS_TABLE|<TO|<FROM', 
                    '+Class|+Term|+Relationship|+Data Extraction Standard|+Source Data Folder|+Source Data Table|+Source Data Column', 
                    0, 1 ) yield path
                RETURN path
                """
            )
        else:
            self.rdf_generate_uri(uri_map2)
            rdf = self.rdf_get_subgraph(
                """
                MATCH (n:Class)
                call apoc.path.expand(n, 'SUBCLASS_OF|HAS_CONTROLLED_TERM|<TO|<FROM', '+Class|+Relationship|+Term', 0, 1 ) yield path
                RETURN path
                """
            )
        rdf_file_path = os.path.join(folder, filename)
        with open(rdf_file_path, "w") as file:
            file.write(rdf)

    def create_custom_mappings_from_dict(self, groupings = None) -> None:
        """
        Function to support MethodApplier(mode="schema_PROPERTY")

        Create a set of nodes labeled "Class", all taken from the groupings dictionary; their names are
            stored in an attribute named "label".  EXAMPLES: "Study", "Site", "Race", "Adverse Event".

        Also, create a set of nodes labeled "Property"; likewise, their names are
            stored in an attribute named "label".  EXAMPLES: "RACE", "RACEN", "SITEID".

        In addition, create relationships named "HAS_PROPERTY", from each of the "Class" nodes to the appropriate
            "Property" nodes.   EXAMPLE:  The "Race" node (labeled "Class") links to the "RACE", "RACEN" nodes
            (labeled "Property")

        Finally,  create relationships named "MAPS_TO_PROPERTY", from `Source Data Column` nodes to `Property` nodes.
            These relationships are created based on matches between the "_columnname_" attribute on `Source Data Column` nodes
            and entries in the lists contained in groupings (e.g., "RACEN"); in some cases, further restrictions are applied,
            requiring the `Source Data Column` node to be linked to a particular `Source Data Table` node.

        :param groupings:   A dictionary.  The keys are either "*" (meaning no `Source Data Table` restriction)
                                           or the name of a specific `Source Data Table`.
                                           The values are dictionaries such as {"Race": ["RACE", "RACEN"]}
        :return:            None
        """
        if not groupings:
            groupings = {}

        # Loop over all the keys/values of the groupings dictionary
        for table, groupping in groupings.items():
            # EXAMPLES of table:  "ADSL" or "*" (the star indicates "all tables")
            # groupping is a dictionary with entries such as  "Race": ["RACE", "RACEN"]

            # Define the Cypher query, which depends on the value of the "table" variable in the outer loop;
            #       for examples of how the query shapes up, see below (inside inner loop)
            q = f"""                    
                MERGE (class:Class {{label:$class}})
                WITH * UNWIND $properties as property
                MERGE (class)-[:HAS_PROPERTY]->(p:Property {{label:property}})
                WITH *
                MATCH (sdc:`Source Data Column` {{_columnname_:property}})
                {
                "" if table == "*" else "<-[:HAS_COLUMN]-(sdt:`Source Data Table` {_domain_:$table})"
                }                     
                MERGE (sdc)-[:MAPS_TO_PROPERTY]->(p)
                """

            for class_, properties in groupping.items():
                # EXAMPLE of class_ : "Race"
                # EXAMPLE of properties: ["RACE", "RACEN"]

                params = {"table":table, "class": class_, "properties": properties}
                self.query(q, params)

                # EXAMPLE1 of query (involving a specific table, such as "ADSL")
                """
                    MERGE (class:Class {label:$class})
                    WITH * UNWIND $properties as property
                    MERGE (class)-[:HAS_PROPERTY]->(p:Property {label:property})
                    WITH *
                    MATCH (sdc:`Source Data Column` {_columnname_:property})
                    <-[:HAS_COLUMN]-(sdt:`Source Data Table` {_domain_:$table})                     
                    MERGE (sdc)-[:MAPS_TO_PROPERTY]->(p)
                """

                # EXAMPLE2 of query (involving any table, indicated by the "*" value of the table variable)
                """
                    MERGE (class:Class {label:$class})
                    WITH * UNWIND $properties as property
                    MERGE (class)-[:HAS_PROPERTY]->(p:Property {label:property})
                    WITH *
                    MATCH (sdc:`Source Data Column` {_columnname_:property})                     
                    MERGE (sdc)-[:MAPS_TO_PROPERTY]->(p)
                """
                # EXAMPLE of params: {'table': 'ADSL', 'class': 'Race', 'properties': ['RACE', 'RACEN']}

    def create_custom_rels_from_list(self, rels: [[str, str]], create_if_absent = False) -> None:
        """
        Adds "CLASS_RELATES_TO" relationships to pairs of nodes with "Class" label,
        based on the list passed as the argument "rels".
        Matches occur based on node attributes named "label".
        Optionally, create the Class nodes if needed.
        To create just a single relationship, see create_class_relationship()

        NOTE: this is a more general version of the method create_related_classes_from_list()

        :param rels:    A list of 2-element lists, indicating a relationship among nodes of type `Class`
                        EXAMPLE:   [
                                        ["Study", "Site"],
                                        ["Study", "Subject"],
                                        ["Subject", "Race"]
                                   ]
        :param create_if_absent: If True, the Class nodes specified in the argument "rel" get created as needed;
                                        otherwise, no relationships get created whenever their start or end class is missing
        :return:                 None
        """
        if rels is None or rels == []:
            return      # There's nothing to do

        if create_if_absent:
            q = f"""            
                UNWIND $rels as rel
                WITH rel[0] as left, rel[1] as right      
                WHERE apoc.meta.type(left) = apoc.meta.type(right) = 'STRING'  
                MERGE (ln:Class {{label:left}})
                MERGE (rn:Class {{label:right}})   
                MERGE (ln)-[:CLASS_RELATES_TO]->(rn)   
                """
        else:   # The Class nodes MUST be present, or no relationship gets created
            q = f"""            
                UNWIND $rels as rel
                WITH rel[0] as left, rel[1] as right    
                WHERE apoc.meta.type(left) = apoc.meta.type(right) = 'STRING'      
                MATCH (ln:Class {{label:left}}), (rn:Class {{label:right}})    
                MERGE (ln)-[:CLASS_RELATES_TO]->(rn)   
                """

        params = {"rels": rels}
        self.query(q, params)

    def create_model_from_data(
            self,
            data_label: str = "Source Data Row",
            data_table_label: str = "Source Data Table",
            domain_property: str = "_domain_",
            no_domain_label: str = "Thing",
            data_column_label: str = "Source Data Column",
            columnname_property: str = "_columnname_",
            exclude_properties: list = None,
    ):
        """
        Creates Class and Relationship nodes to represent a trivial schema to reshape data ingested e.g. with
        FileDataLoader - Tables loaded into nodes one row to one node, column names used as property names.

        data_labels: labels of the nodes where loaded data is stored (mm with use OR btw labels to fetch data nodes)
        domain_property: property where the name of the table/domain can be found
        """
        if exclude_properties is None:
            exclude_properties = ["_filename_", "_folder_"]
        q = f"""
        MATCH (data:`{data_label}`)<-[:HAS_DATA]-(dt:`{data_table_label}`)        
        WITH distinct dt, dt._domain_ as domain, keys(data) as ks
        WITH *, CASE WHEN domain IS NULL THEN $no_domain_label ELSE domain END as domain
        WITH dt, domain, [k in ks WHERE NOT k IN $exclude_properties] as ks
        WITH dt, domain, apoc.coll.flatten(apoc.coll.toSet(ks)) as ks
        MERGE (c:Class{{label: domain, short_label: domain, create: True}})
        MERGE (dt)-[:MAPS_TO_CLASS]->(c)
        WITH *
        UNWIND ks as k
        MATCH (dt)-[:HAS_COLUMN]->(col:`{data_column_label}`)
        WHERE col.`{columnname_property}` = k
        MERGE (c2:Class{{label: k, short_label: k}})        
        MERGE (c)<-[:FROM]-(r:Relationship{{relationship_type: k}})-[:TO]->(c2)
        MERGE (col)-[:MAPS_TO_CLASS]->(c2)        
        """
        params = {
            "exclude_properties": exclude_properties + [domain_property],
            "no_domain_label": no_domain_label
        }
        self.query(q, params)