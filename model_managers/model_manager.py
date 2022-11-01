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

    ## ---------------------------- Generate model from excel SDTM spec ----------------------------- ##
    def generate_excel_based_model(self, label_terms: bool = False, create_term_indexes: bool = False):
        """
        Run ExcelStandardLoader.load_standard() to prepare metadata from excel (and SDTM ontology form GitHub)
        in Neo4j
        :param create_term_indexes: weather to create indexes for each Class that HAS_CONTROLLED_TERM (can be done later
        during reshaping)
        :return: None
        """
        print("Creating indexes on Class and Term")
        self.create_index(label="Class", key="label")
        self.create_index(label="Class", key="short_label")
        self.create_index(label="Relationship", key="relationship_type")
        self.create_index(label="Term", key="Codelist Code")
        self.create_index(label="Term", key="Term Code")
        self.create_index(label="Term", key=ModelManager.RDFSLABEL)
        print("Creating indexes on Source Data Table and Source Data Column")
        self.create_index(label="Source Data Table", key="_domain_")
        self.create_index(label="Source Data Column", key="_columnname_")

        # Terms
        print("Mapping Term GSK Codes and NCI Codes together")
        q = f"""
        MATCH (t:Term)
        SET t.`{ModelManager.RDFSLABEL}` = t.Term
        SET t.`Codelist Code` = 
            CASE WHEN t.`NCI Codelist Code` IS NULL OR t.`NCI Codelist Code` = '' THEN 
                t.GSK_Codelist_Code
            ELSE
                t.`NCI Codelist Code`
            END
        SET t.`Term Code` = 
            CASE WHEN t.`NCI Term Code` IS NULL OR t.`NCI Term Code` = '' THEN 
                t.GSK_Term_Code
            ELSE
                t.`NCI Term Code`
            END
        """
        self.query(q)

        print("Creating Classes from Dataset and ObservationClass")
        # Datasets to Classes
        q = """
            MATCH (d:Dataset)
            SET d:Class
            SET d.label = d.Description
            SET d.short_label = d.Dataset
            SET d.create = True
            WITH *
            OPTIONAL MATCH (d)<-[:HAS_DATASET]-(oc:ObservationClass)
            WITH *, {
                INTERVENTIONS: 'Intervention',
                EVENTS: 'Event',  
                `FINDINGS ABOUT`: 'Finding About',
                FINDINGS: 'Finding',
                RELATIONSHIP: 'Relationship (SDTM)',
                `SPECIAL PURPOSE`: 'Special Purpose',
                `TRIAL DESIGN`: 'Trial Design'                             
            } as map
            SET oc:Class
            SET oc.label = map[oc.Class]
            SET oc.short_label = toUpper(map[oc.Class])
            MERGE (d)-[:SUBCLASS_OF]->(oc)
            //MERGE (sdt:`Source Data Table`{_domain_: d.Dataset}) //This is outsourced to automap_excel_based_model
            //MERGE (sdt)-[:MAPS_TO_CLASS]->(d)
            WITH DISTINCT oc
            MERGE (rec_class:Class{label: 'Record', short_label: 'RECORD'})
            MERGE (oc)-[:SUBCLASS_OF]->(rec_class)
            """
        self.query(q)

        print("Creating Class from Variable")  # when no DataElement exists
        q = """
        MATCH (d:Dataset)-[:HAS_VARIABLE]->(v:Variable)
        WHERE NOT 
            (
                (v.Variable starts with 'COVAL' AND v.Variable <> 'COVAL') 
                    OR
                (v.Variable starts with 'TSVAL' AND NOT v.Variable in ['TSVAL', 'TSVALCD', 'TSVALNF'])
            )// - should be handled separately
        AND NOT EXISTS 
            (
                (v)-[:IS_DATA_ELEMENT]->(:DataElement)
            )        
        SET v:Class
        SET v.label =                           
            CASE WHEN v.n_with_same_label > 1 THEN 
                d.Description + ' ' + v.Label
            ELSE
                v.Label
            END                
        SET v.short_label =             
            CASE WHEN v.n_with_same_name > 1 THEN 
                d.Dataset + v.Variable
            ELSE
                v.Variable
            END
        SET v.create = False 
        MERGE (d)<-[:FROM]-(:Relationship{relationship_type:v.Label})-[:TO]->(v)          
        """
        self.query(q)

        print("Creating Class from dataElement and Relationship from Variable")
        q = """
            MATCH (de:DataElement)
            OPTIONAL MATCH (de)-[:dataElementRole]->(dar:DataElementRole)
            WITH DISTINCT de,                                   
            {
                label: 
                    CASE de.dataElementName
                        WHEN 'USUBJID' THEN 'Subject'
                        WHEN 'STUDYID' THEN 'Study'
                    ELSE
                        de.dataElementLabel
                    END,
                short_label: de.dataElementName,
                create: 
                    CASE WHEN dar.label IN ['Identifier Variable', 'Result Qualifier']
                    AND NOT de.dataElementName in ['DOMAIN', 'STUDYID', 'USUBJID', 'EPOCH'] THEN
                        True                
                    ELSE
                        False
                    END
            } as map
            SET de:Class //nothing will be mapped to these classes - only to the parent class which is created below
            SET de.label = apoc.text.join([x in [de.vg, map['label']] WHERE NOT x IS NULL], " ")                   
            SET de.short_label = apoc.text.join([x in [de.vg_short, map['short_label']] WHERE NOT x IS NULL], " ")           
            SET de.create = map['create']
            WITH *
            CALL apoc.merge.node(['Class'], map, {}, {}) YIELD node as dehl 
            MERGE (de)-[:SUBCLASS_OF]->(dehl)
            WITH *      
            MATCH (d:Dataset)-[:HAS_VARIABLE]->(v:Variable)-[:IS_DATA_ELEMENT]->(de:DataElement)
            SET v:Relationship
            SET v.label = v.Label
            SET v.short_label = v.Variable
            SET v.relationship_type = map.label
            MERGE (dehl)<-[:TO]-(v)-[:FROM]->(d)  
            WITH *
            MATCH (v)-[:HAS_CONTROLLED_TERM]->(t:Term)
            MERGE (dehl)-[:HAS_CONTROLLED_TERM]->(t)
            """
        self.query(q)

        # Merging duplicate dehl Terms:
        # Note: this is rather a workaround
        # In the graph world for the case where 'Weight' is part of 4 codelists CVTEST, MOTEST, PCTEST, VSTEST
        # it would make more sense to have 1 codelist for all tests and value-level metadata based on the DOMAIN value
        q = """
        MATCH path=(dehl:Class)-[r:HAS_CONTROLLED_TERM]->(t:Term)
        WHERE NOT dehl:Variable
        WITH *
        ORDER BY dehl, t.`rdfs:label`, t.`Codelist Code`, t.`Term Code`
        WITH dehl, t.`rdfs:label` as Term, collect([r, t]) as coll
        WHERE size(coll) > 1
        WITH *, coll[0][-1] as template
        MERGE (dehl_term:Term{`Codelist Code`: 'P' + template.`Codelist Code`, `Term Code`: 'P' + template.`Term Code`})
        SET dehl_term.`rdfs:label` = template.`rdfs:label`
        WITH *
        MERGE (dehl)-[:HAS_CONTROLLED_TERM]->(dehl_term)
        WITH *
        UNWIND coll as pair
        WITH *, pair[0] as r, pair[1] as t
        MERGE (t)-[:TERM_POOLED_INTO]->(dehl_term)
        DELETE r
        """
        self.query(q)

        # Link Domain Abbreviation to all dehl classes
        q = """
        MATCH (de:DataElement)-[:SUBCLASS_OF]->(dehl:Class), (domain_class:Class{label:'Domain Abbreviation'})
        WHERE dehl <> domain_class AND NOT dehl.label in ['Subject', 'Study']
        MERGE (dehl)<-[:FROM]-(:Relationship{relationship_type:'DOMAIN'})-[:TO]->(domain_class)
        """
        self.query(q)

        if label_terms:
            # Labelling Terms
            q = """
            MATCH (c:Class)
            WHERE NOT (c:DataElement)-[:SUBCLASS_OF]->()
                AND NOT c.create //NOT labelling the classes that get always created (e.g. --ORRES), otherwise duplicates appear
                AND NOT (c:Variable AND c.Dataset STARTS WITH 'SUPP')
            WITH * 
            MATCH (c)-[:HAS_CONTROLLED_TERM]->(t:Term)
            WITH c, collect(t) AS coll
            WITH *
            CALL apoc.create.addLabels(coll, [c.label]) 
            YIELD node
            RETURN count(*)
            """
            self.query(q)

            if create_term_indexes:
                # Creating indexes for each Term label
                print("Creating indexes for each Term label")
                q = f"""
                MATCH (c:Class) 
                WHERE EXISTS ( (:Term)<-[:HAS_CONTROLLED_TERM]-(c) )
                RETURN c.label as label        
                """
                res = self.query(q)
                for r in res:
                    # print(f"Creating index for {r['label']}")
                    self.create_index(r['label'], ModelManager.RDFSLABEL)

        # Creating classes from SUPP domain Terms (only SUPPDM for now)
        print("Creating Class from SUPP domain Terms")
        q = """
        MATCH path=(t:Term)<-[:HAS_CONTROLLED_TERM]-(qnam:Variable),
        (qnam)<-[:HAS_VARIABLE]-(suppd:Dataset)<-[:SUPP_DATASET]-(d:Dataset)        
            WHERE qnam.Dataset STARTS WITH 'SUPP' AND qnam.Variable = 'QNAM'
            AND d.Dataset = 'DM' // TODO: to be removed when generalized for all SUPP domains
            //chellenges - (1) no 1:1 btw Term and `Decoded Value`; (2) no uniqueness of Term/`Decoded Value`: sz>1            
        //WITH t.`Decoded Value` as label
        WITH t.`Decoded Value` + ' (' + t.Term + ')' as label
        ,collect({t: t, qnam: qnam, suppd: suppd, d: d}) as coll         
        WITH *, size(coll) as sz
        UNWIND coll as map
        WITH label, map['t'] as t, map['qnam'] as qnam, map['suppd'] as suppd, map['d'] as d, sz
        FOREACH(_ IN CASE WHEN sz=1 THEN [1] ELSE [] END | 
            SET t:Class
            SET t.label = label
            SET t.short_label = t.Term
            SET t.create = False
            MERGE (d)<-[:FROM]-(:Relationship{relationship_type:t.label})-[:TO]->(t)             
        )                                  
        WITH *                
        MATCH path2 =(qnam)<-[:HAS_VARIABLE]-(ds:Dataset)-[:HAS_VARIABLE]->(qval:Variable)
            ,path3 = (qval)-[:HAS_VALUE_LEVEL_METADATA]->(vl:Valuelevel)-[:HAS_WHERE_CLAUSE]->(wc:`Where Clause`)
            ,path4 = (wc)-[:ON_VARIABLE]->(qnam)
            ,path5 = (wc)-[:ON_VALUE]->(t)
            ,path6 = (vl)-[:HAS_VL_TERM]->(vlterm:Term)
            WHERE qval.Variable = 'QVAL'
        MERGE (t)-[:HAS_CONTROLLED_TERM]->(vlterm)                       
        """
        self.query(q)

        # ------------------ LINKING-----------------------
        # (0)

        # -------- Creating  'qualifies' Relationship--------
        print("Creating Relationships based on SDTM ontology")
        q = """
        MATCH p=(x:Class)<-[:SUBCLASS_OF*0..1]-(:DataElement)-[:qualifies]->(:DataElement)-[:SUBCLASS_OF*0..1]->(y:Class)
        MERGE (subj)<-[:FROM]-(:Relationship{relationship_type:'QUALIFIES'})-[:TO]->(core)
        """
        self.query(q)

        # --------------- Custom links (business experience) -------------------:
        # additional 'qualifies' rel for business purposes:
        print("Creating additional Relationships based on business need")
        data = []
        data.append({'left': 'Subject', 'right': 'Study', 'rel': 'Study'})
        data.append({'left': 'Body System or Organ Class', 'right': 'Dictionary-Derived Term', 'rel': 'QUALIFIES'})
        data.append({'left': 'Visit Name', 'right': 'Visit Number', 'rel': 'QUALIFIES'})
        q = """
        UNWIND $data as row
        MATCH (left_c:Class), (right_c:Class)
        WHERE left_c.label = row['left'] and right_c.label = row['right']
        MERGE (left_c)<-[:FROM]-(:Relationship{relationship_type:row['rel']})-[:TO]->(right_c)
        """
        self.query(q, {'data': data})

        # (2)
        # -------- getting topics -------
        q = """
        MATCH (de:DataElement)-[:dataElementRole]->(der:DataElementRole)
        WHERE der.label = 'Topic Variable'
        RETURN de.dataElementLabel as topic_class
        """
        topics = self.query(q)
        # extending and updating topics:
        df_topics = pd.DataFrame(
            topics + [{"topic_class": "Dictionary-Derived Term"}]
        )
        # we rather use the long name as topic than the short name
        df_topics["topic_class"] = df_topics["topic_class"].replace({
            "Short Name of Measurement, Test or Examination": "Name of Measurement, Test or Examination"})
        topics = list(df_topics["topic_class"])

        # ------- getting Result Qualifiers and findings topics -------
        q = """
        MATCH (de:DataElement)-[:dataElementRole]->(der:DataElementRole),
        (de2:DataElement)-[:context]->(ctx:VariableGrouping)
        WHERE
          der.label = 'Result Qualifier'
          AND de2.dataElementLabel in $topics
          AND ctx.contextLabel = 'Findings Observation Class Variables'
        RETURN de.dataElementLabel as rq_class, de2.dataElementLabel as topic
        """
        df_resqs = pd.DataFrame(self.query(q, {"topics": topics}))

        # ------- linking Result Qualifiers to topics (Findings) -------
        print("Linking Result Qualifiers to Finding Topics")
        q = """
        UNWIND $data as row
        MATCH (c:Class), (c_topic:Class)
        WHERE c.label = row['rq_class'] AND c_topic.label = row['topic']
        MERGE (c_topic)<-[:FROM]-(r:Relationship)-[:TO]->(c)
        SET r.relationship_type = 'HAS_RESULT'
        """
        self.query(q, {"data": df_resqs.to_dict(orient="records")})

        # (3)
        # linking grouping classes to topics
        print("Linking grouping classes to topics")
        q = """
        MATCH (de:DataElement)-[:dataElementRole]->(der:DataElementRole)
        WHERE der.label = 'Grouping Qualifier'
        RETURN DISTINCT de.dataElementLabel as groupping_class
        """
        groupings = [res["groupping_class"] for res in self.query(q)]

        q = """
        MATCH (topic:Class), (gr:Class)
        WHERE topic.label in $topics and gr.label in $groupings
        MERGE (topic)<-[:FROM]-(:Relationship{relationship_type:'IN_CATEGORY'})-[:TO]->(gr)
        """
        self.query(q, {"topics": topics, "groupings": groupings})

        # (4)
        # category to subcategory
        print("Linking Category to Sub-Category")
        self.query("""
        MATCH (cat:Class), (scat:Class)
        WHERE cat.label = 'Category' and scat.label = 'Subcategory'
        MERGE (cat)<-[:FROM]-(:Relationship{relationship_type:'HAS_SUBCATEGORY'})-[:TO]->(scat)
        """)

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

    def automap_excel_based_model(self, domain: list, standard: str):
        # mapping to Dataset and Variable/DataElement classes
        q = """
        MATCH (sdt:`Source Data Table`), (ds:Dataset)
        WHERE sdt._domain_ = ds.Dataset
        MERGE (sdt)-[:MAPS_TO_CLASS]->(ds)
        WITH *
        MATCH (ds)-[:HAS_VARIABLE]->(v:`Variable`),
              (sdt)-[:HAS_COLUMN]->(sdc:`Source Data Column`)
        WHERE sdc._columnname_ = v.Variable
        CALL apoc.do.when(
            v:Class,
            '
            WITH sdc, v        
            MERGE (sdc)-[:MAPS_TO_CLASS]->(v)  
            '
            ,
            '
            WITH sdc, v
            MATCH (v)-[:IS_DATA_ELEMENT]->()-[:SUBCLASS_OF]->(dehl)
            MERGE (sdc)-[:MAPS_TO_CLASS]->(dehl)  
            '
            ,
            {sdc:sdc, v:v}
        ) YIELD value               
        RETURN *
        """
        self.query(q)

        # mapping to SUPP-- Term Classes
        q = """                
        MATCH (t:Term:Class), (sdc:`Source Data Column`)
        WHERE t.short_label = sdc._columnname_
        MERGE (sdc)-[:MAPS_TO_CLASS]->(t) 
        """
        self.query(q)

        # TODO: Bug. This should also match on domain/dataset, now it get's multiple matches for e.g. identifiers, timing etc.
        # setting order property on source data column
        q = f"""
                MATCH (sdc:`Source Data Column`), (v:`Variable`)
                WHERE sdc._columnname_ = v.Variable
                SET sdc.Order = v.Order
                """

        self.query(q)

        # add the Data Extraction Standard node to the db and attach it to the Source Data Tables
        q = """
            MERGE (sdf:`Data Extraction Standard`{_tag_:$standard})
            WITH sdf
            MATCH (sdt:`Source Data Table`)
            MERGE (sdf)-[:HAS_TABLE]->(sdt)
            """
        params = {'standard': standard}
        self.query(q, params)

        # set the SortOrder property for each source data table in the graph
        self.set_sort_order(domain=domain, standard=standard)
        # Extend the extraction metadata with MAPS_TO_COLUMN rel between relationship and source data column nodes
        self.extend_extraction_metadata(domain=domain, standard=standard)

    def set_sort_order(self, domain: list, standard: str):

        for dom in domain:
            q = """
            MATCH (sdf:`Data Extraction Standard`{_tag_:$standard})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:$domain})-[:HAS_COLUMN]->(sdc:`Source Data Column`)
            WITH sdc, sdt
            ORDER BY sdc.Order
            WITH collect(sdc._columnname_) AS col_order, sdt
            SET sdt.SortOrder = col_order
            """
            params = {'domain': dom, 'standard': standard}

            self.query(q, params)

            q = """
            MATCH (sdf:`Data Extraction Standard`{_tag_:$standard})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:$domain})-[:HAS_COLUMN]->(sdc:`Source Data Column`)
            WITH sdc, sdt
            ORDER BY sdc.Order
            WITH collect(sdc._columnname_) AS col_order, sdt
            SET sdt.SortOrder = col_order
            """
            params = {'domain': dom, 'standard': standard}

            self.query(q, params)

    def extend_extraction_metadata(self, domain: list, standard: str):
        # Adds the relationship MAPS_TO_COLUMN between the source data column node and the relationship that
        # sdc node's variable is pointing 'TO'. WHERE that relationship is 'FROM' a core class (ie FA, EX, VS, ... etc)
        for dom in domain:
            q = """
            MATCH (sdf:`Data Extraction Standard`{_tag_:$standard})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:$table})
            , (sdt)-[:HAS_COLUMN]->(sdc:`Source Data Column`)-[:MAPS_TO_CLASS]->(c:Class)<-[:TO]-(r:Relationship)
            , (r)-[:FROM]-(c2:Class)
            WHERE c2.short_label = $table
            MERGE (r)-[:MAPS_TO_COLUMN]-(sdc)
            """
            params = {'standard': standard, 'table': dom}
            self.query(q, params)

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

    def create_model_from_data(
            self,
            data_label: str = "Source Data Row",
            data_table_label: str = "Source Data Table",
            domain_property: str = "_domain_",
            data_column_label: str = "Source Data Column",
            columnname_property: str = "_columnname_",
            no_domain_label: str = "Thing",
            exclude_properties: list = ["_filename_", "_folder_"],
    ):
        """
        Creates Class and Relationship nodes to represent a trivial schema to reshape data ingested e.g. with
        FileDataLoader - Tables loaded into nodes one row to one node, column names used as property names.

        data_labels: labels of the nodes where loaded data is stored (mm with use OR btw labels to fetch data nodes)
        domain_property: property where the name of the table/domain can be found
        """
        q = f"""
        MATCH (data:`{data_label}`)<-[:HAS_DATA]-(dt:`{data_table_label}`)        
        WITH distinct dt, dt._domain_ as domain, keys(data) as ks
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
            "exclude_properties": exclude_properties + [domain_property]
        }
        self.query(q, params)