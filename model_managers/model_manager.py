import os
from neointerface import NeoInterface
from typing import List
import pandas as pd
import numpy as np
from logger.logger import logger


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
            logger.info(f"---------------- {self.__class__} initialized -------------------")

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

    def create_class(self, classes, merge=True, merge_on: List[str]=None) -> [list]:
        """
        :param classes:  List of string labels or a list of property dictionaries to give to the new class(es)
                         created. For example:
                         classes = ['class1', 'class2' ...] OR classes = [{"label": 'class1'}, {"label": 'class2'] ...]
                         Will both result in the creation of two new classes with labels class1 and class2 respectively.
        :param merge:    boolean - if True use MERGE statement to create nodes to avoid duplicate classes
                             TODO: address question "would we want to ever allow multiple classes with the same name??"
        :param merge_on: Optional list of property names MERGED on when classes is a list of property dictionaries. This
                         can be used to selectively rename certain properties on an existing node rather than
                         creating a new one, For example:

                            With classes = [{'label': 'class1', 'type': 'new_type'}] and merge on = ['label']
                            If a class node with 'label' = 'class1' already exists with 'type' = 'old_type' rather
                            than creating a new node 'class1' will be updated with 'type' = 'new_type'

                         When not merge_on is not set the default behaviour is to merge on all properties.
        :return:         A list of lists that contain a single dictionary with keys 'label', 'neo4j_id' and 'neo4j_labels'
                         EXAMPLE: [ [{'label': 'A', 'neo4j_id': 0, 'neo4j_labels': ['Class']}],
                                    [{'label': 'B', 'neo4j_id': 1, 'neo4j_labels': ['Class']}]
                                  ]
        """

        # Maintain backwards compatibility:
        if type(classes) == str:
            classes = [classes]

        assert type(classes) == list, "Classes must be a list"
        assert type(merge) == bool, "Merge must be a bool"
        if merge_on:
            assert merge, "Merge_on requires merge = true"

        if type(classes[0]) == str:
            # Note class_item is the result of unwinding $classes, which in this case is a list of string labels.
            apoc_action = f"""
                CALL apoc.{'merge' if merge else 'create'}.node(
                    ['Class'], {{label: class_item}}{', {}, {}' if merge else ''}
                ) YIELD node
            """
        elif type(classes[0]) == dict:
            # Note class_item is the result of unwinding $classes, which in this case is a list of property
            # dictionaries. So a new node would be created/merged with that given dictionary.
            if not merge_on:
                apoc_action = f"""
                    CALL apoc.{'merge' if merge else 'create'}.node(
                        ['Class'], class_item{', {}, {}' if merge else ''}
                    ) YIELD node
                """
            else:
                ident_props = f'{{`{merge_on[0]}`: class_item["{merge_on[0]}"]'
                for prop in merge_on[1:]:
                    ident_props += f', `{prop}`: class_item["{prop}"]'
                ident_props += '}'

                apoc_action = f"""
                    CALL apoc.merge.node(
                        ['Class'], {ident_props}, class_item, class_item
                    ) YIELD node
                """
                # Example resulting apoc_action format(merge):
                # With ident props = ['label'] the resulting action would be:
                # CALL apoc.merge.node(
                #   ['Class'], {`label`: class_item[`label`]}, class_item, class_item
                # ) YIELD node
                # Which matches on 'label then sets the properties to class_item
        else:
            raise AssertionError('Classes must be a list of strings or dict')

        q = f"""
        WITH $classes as classes 
        UNWIND classes as class_item 
        {apoc_action}
        RETURN node as class
        ORDER by node
        """

        params = {'classes': classes}

        if self.verbose:
            logger.debug(f"""
            query: {q}
            parameters: {params}
            """)

        return self.query(q, params, return_type='neo4j.Result')

    def delete_class(self, values: list, identifier='label'):
        """
        Deletes a class and any associated relationships or controlled terminology
        :param values: list of class labels or property values (if identifier is changed) to match classes for removal
        :param identifier: Class property to use in combination with values for identification.
        :return:
        """
        # TODO: address the issue of invalid Methods linked to the deleted entities.
        q = f"""
        MATCH (class:Class)
        WHERE class[$identifier] in $values
        OPTIONAL MATCH (class)-[:HAS_CONTROLLED_TERM]->(term:Term)
        OPTIONAL MATCH (class)-[:TO|FROM]-(rel:Relationship)
        DETACH DELETE class, term, rel
        """
        params = {'values': values, 'identifier': identifier}

        return self.query(q, params, return_type='neo4j.Result')

    def get_missing_classes(self, values: list, identifier='label'):
        """
        Samples a list of class property values to determine if a set of classes are missing from the database.
        :param values: List of property values eg: [class1, class2]
        :param identifier: Property name to be used when identifying classes eg: 'label'
        :return: A list of any values not found in neo4j.
        """

        q = f"""
        MATCH (c:Class)
        WHERE c.{identifier} in $values
        RETURN collect(c.{identifier}) as existing
        """
        params = {'values': values}
        existing_classes = self.query(q, params)[0].get('existing')

        missing_classes = set(values) - set(existing_classes)
        return missing_classes

    def set_short_label(self, label: str, short_label: str) -> None:
        "One the class with :Class{label:{label}} - sets property 'short_label value to the provided"
        q = """
        MATCH (c:Class)
        WHERE c.label = $label
        SET c.short_label = $short_label
        """
        params = {'label': label, 'short_label': short_label}
        self.query(q, params)

    def create_related_classes_from_list(self, rel_list: [[str, str, str]], identifier='label') -> [str]:
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
        :param identifier: String class property used to identify to & from classes
        :return: List of all the class names; repeated ones are taken out
        """

        # Identify all the unique class names in inner elements of rel_list
        class_set = set()  # Empty set
        for rel in rel_list:
            # [:2] in order to get only the first two items (classes) out of rel_list; [2] is the relationship type
            class_set = class_set.union(rel[:2])  # The Set Union operation will avoid duplicates

        class_list = sorted(list(class_set))  # Convert the final set back to list

        self.create_relationship(rel_list, identifier, match_classes=False)

        return class_list

    def create_subclass(self, subclass_list: List[List[str]], identifier='label', match_classes=True)-> [str]:
        """
        An additional label is added to a class node ('variable') if some of the variables need to be grouped of the class as defined in subclass_list.
        For example:
            With subclass_list = [ ['parent', 'child'] ]
            A new class label with name as "SUBCLASS_OF", as specified by the
            identifier will be created between "parent" and "child".
        """

        q= f"""
        UNWIND $class_list as class_
        CALL apoc.do.when(size(class_)<=2,
        "WITH class_[0] as class_label, class_[1] as subclass_label
        {'MATCH' if match_classes else 'MERGE'} (c1:Class {{`{identifier}`:class_label}})
        {'MATCH' if match_classes else 'MERGE'} (c2:Class {{`{identifier}`:subclass_label}})   
        MERGE (c1)<-[sub:SUBCLASS_OF]-(c2)
        RETURN collect([c1.`{identifier}`, c2.`{identifier}`]) as classes",
        "WITH class_[0] as class_label, class_[1] as subclass_label, class_[2] as cond
        {'MATCH' if match_classes else 'MERGE'} (c1:Class {{`{identifier}`:class_label}})
        {'MATCH' if match_classes else 'MERGE'} (c2:Class {{`{identifier}`:subclass_label}}) 
        MERGE (c1)<-[sub:SUBCLASS_OF]-(c2)
        SET sub.`conditions`= apoc.convert.toJson(cond)
        RETURN collect([c1.`{identifier}`, c2.`{identifier}`, sub.`conditions`]) as classes",
        {{class_:class_}})
        YIELD value
        RETURN collect(value.classes) as classes
        """
        res = self.query(q, {
            "class_list": [sc for sc in subclass_list]
        })

        if res:
            self.propagate_rels_to_child_class()
            self.propagate_terms_to_parent_class()
            result = [np.array(i).flatten().tolist() for i in res[0].get('classes')]
            return result
        else:
            return []
    
    def create_relationship(self, rel_list: List[List[str]], identifier='label', match_classes=True) -> [str]:
        """
        Create relationship nodes between two specified classes as defined in rel_list.
        For example:
        1.  With rel_list = [ ['class1', 'class2', 'example'] ]
            A new relationship node will be created between nodes with "label", as specified by the
            identifier, 'class1' and 'class2' with a relationship_type property = 'example'.
            This relationship node also includes 'FROM.Class.label' and 'TO.Class.label' properties
            regardless of the class identifier.

        2.  With rel_list = [ ['class1', 'class2', 'example','false'] ]
            A new relationship node will be created between nodes with "label", as specified by the
            identifier, 'class1' and 'class2' with a relationship_type property = 'example' and optional_relationship = 'false'.
            This relationship node also includes 'FROM.Class.label' and 'TO.Class.label' properties
            regardless of the class identifier.
        Note if no relationship type is included in example-1, a default is generated via gen_default_reltype() 
        and if no relationship type is included in example-2, optional_relationship would be considered as relationship type 
        and optional relationship would be ignored
        :param rel_list: A list of relationships represented as lists
        :param identifier: String class property used to identify to & from classes
        :param match_classes: Boolean, If false classes are merged rather than matched which will create them
                              if they do not already exist.
        :return: A list of created relationships = rel_list if all relationships were created successfully
        """

        q = f"""
        UNWIND $rels as rel
        CALL apoc.do.when(size(rel)<=3,
        "WITH rel[0] as from_identity, rel[1] as to_identity, rel[2] as rel_type
        {'MATCH' if match_classes else 'MERGE'} (from:Class {{`{identifier}`:from_identity}})
        {'MATCH' if match_classes else 'MERGE'} (to:Class {{`{identifier}`:to_identity}})   
        MERGE (from)<-[:FROM]-(rel_node:Relationship{{relationship_type:rel_type}})-[:TO]->(to)
        SET rel_node.`FROM.Class.label` = from.label
        SET rel_node.`TO.Class.label` = to.label
        RETURN collect([from.`{identifier}`, to.`{identifier}`, rel_node.relationship_type]) as rels", 
        "WITH rel[0] as from_identity, rel[1] as to_identity, rel[2] as rel_type, rel[3] as optional
        {'MATCH' if match_classes else 'MERGE'} (from:Class {{`{identifier}`:from_identity}})
        {'MATCH' if match_classes else 'MERGE'} (to:Class {{`{identifier}`:to_identity}})   
        MERGE (from)<-[:FROM]-(rel_node:Relationship{{relationship_type:rel_type, relationship_optional:optional}})-[:TO]->(to)
        SET rel_node.`FROM.Class.label` = from.label
        SET rel_node.`TO.Class.label` = to.label
        RETURN collect([from.`{identifier}`, to.`{identifier}`, rel_node.relationship_type, rel_node.relationship_optional]) as rels
        ",
        {{rel:rel}})
        YIELD value
        RETURN collect(value.rels) as rels
        """

        res = self.query(q, {
            "rels": [(r if len(r)>=3 else r + [self.gen_default_reltype(to_label=r[1])]) for r in rel_list]
        })

        if res:
            result = [np.array(i).flatten().tolist() for i in res[0].get('rels')]
            return result
        else:
            return []

    def delete_terms_of_parent_class(self, subclass_list: [[str, str]], identifier='label'):
        if self.verbose:
            logger.info("Deleting propgated terms of 'parent' classes where (child)-[:SUBCLASS_OF]->(parent) and the  subclass rel no longer exists")

        match_rel = f'(:Class{{`{identifier}`:child}})-[:SUBCLASS_OF*1..50]->(source:Class)'

        q=f"""
        UNWIND $subclass_rel as subclass_rel
        WITH subclass_rel[0] as parent, subclass_rel[1] as child
        MATCH (:Class{{`{identifier}`:child}})-[:HAS_CONTROLLED_TERM]->(term:Term), {match_rel}, (source)-[has_term:HAS_CONTROLLED_TERM]->(term)
        DETACH DELETE has_term 
        """

        params = {"subclass_rel": subclass_list}  
        return self.query(q, params, return_type='neo4j.Result')

    def delete_rels_of_child_class(self, subclass_list: [[str, str]], identifier='label'):
        if self.verbose:
            logger.info("Deleting propagated relationships from 'child' classes where (child)-[:SUBCLASS_OF]->(parent) and the subclass rel no longer exists")
        
        match_rel = '(source:Class)-[:SUBCLASS_OF*1..50]->(c)'

        q = f"""
        UNWIND $subclass_rel as subclass_rel
        WITH subclass_rel[0] as parent, subclass_rel[1] as child
        MATCH (c:Class{{`{identifier}`:parent}})<-[r1:TO|FROM]-(r:Relationship)-[r2:TO|FROM]-(target:Class), {match_rel}, (source)<-[:FROM]-(del_rel:Relationship{{`relationship_type`:r.relationship_type}})-[:TO]->(target)
        DETACH DELETE del_rel
        """

        params = {"subclass_rel": subclass_list}  
        return self.query(q, params, return_type='neo4j.Result')

    def delete_subclasses(self, subclass_list: [[str, str]], identifier='label'):
        """
        Deletes the propagated terms and relationship and subclass relationships between specified classes.
        :param rel_list: List of parent ad child classes to be deleted in the following format:
                         [parent_class, child_class]
                         For example: With identifier = 'label' and
                         subclass_list = [['class1', 'class2'], ...]
                         'SUBCLASS_OF'relationships between classes with labels = 'class1' and 'class2'
                         would be deleted.
        :param identifier: String class property to be used when identifying classes.
        :return:
        """

        self.delete_terms_of_parent_class(subclass_list, identifier)
        self.delete_rels_of_child_class(subclass_list, identifier)

        q = f"""
        UNWIND $subclass as subclass
        WITH subclass[0] as parent, subclass[1] as child
        MATCH (:Class{{`{identifier}`:parent}})<-[subclass_rel:SUBCLASS_OF]-(:Class{{`{identifier}`:child}})
        DETACH DELETE subclass_rel
        """
        params = {"subclass": subclass_list}
        return self.query(q, params, return_type='neo4j.Result')

    def delete_relationship(self, rel_list: [[str, str, str]], identifier='label'):
        """
        Deletes specified relationships between classes.
        :param rel_list: List of relationships to be deleted in the following format:
                         [from class prop value, to class prop value, relationship type]
                         For example: With identifier = 'label' and
                         rel_list = [['class1', 'class2', 'Example'], ...]
                         Relationships of type "Example" between classes with labels = 'class1' and 'class2'
                         would be deleted.
        :param identifier: String class property to be used when identifying classes.
        :return:
        """

        q = f"""
        UNWIND $rels as rel
        WITH rel[0] as from, rel[1] as to, rel[2] as type
        MATCH (:Class{{`{identifier}`:from}})<-[:FROM]-(rel:Relationship {{relationship_type:type}})-[:TO]->(:Class{{`{identifier}`:to}})
        DETACH DELETE rel
        """
        params = {"rels": rel_list}
        return self.query(q, params, return_type='neo4j.Result')

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

    def get_all_classes_props(self, props: [str]) -> [dict]:
        """
        Retrieve a list of property values for all classes.
        :param props: List of properties ro retrieve ie ['label', ...]
        :return: A list of dictionaries, with keys equal to the specified property name
                            EXAMPLE, with props=['prop1', 'prop2]:
                                [{'prop1': 'value', 'prop2': 'value'}, ...]
        """
        assert len(props) > 0, 'Must specify at least one property to return!'
        assert len(props) == len(set(props)), 'Specified props must not contain duplicates!'

        q_return = f"RETURN c.`{props[0]}` as `{props[0]}`"
        if len(props) != 1:
            for prop in props[1:]:
                q_return += f", c.`{prop}` as `{prop}`"

        q = f"""
        MATCH (c:Class)
        {q_return}
        """

        return self.query(q)

    def get_subclasses_where(self, where_clause=None, identifier='label') -> [{}]:
        
        q=f"""MATCH (c1:Class)<-[s:SUBCLASS_OF]-(c2:Class)
            {where_clause if where_clause else ""}
            RETURN {{parent: c1.`{identifier}`, child: c2.`{identifier}`, conditions:s.`conditions`}} as classes
            """

        res = self.query(q)

        return [x['classes'] for x in res]       


    def get_rels_where(self, where_clause=None, return_prop="label") -> [{}]:
        """
        Returns a list of dictionaries representing relationships between all classes or a subset of classes and/or
        relationships if filtered with a cypher where clause.
        :param where_clause: Optional string cypher where clause, For example:
                             `WHERE from_class.short_label IS NOT NULL` - Only relationships from_classes with a short_label
        :param return_prop: Property to identify class in returned relationships. For example with
                            return prop = "label", relationships will be in the format:
                            {from: from_class.label, to: to_class.label, type: rel.relationship_type, optional: rel.relationship_optional}
        :return: A list of dicts representing relationships eg:
                            [{from: from_class.label, to: to_class.label, type: rel.relationship_type, optional: rel.relationship_optional}, ...]
        """

        q = f"""
        MATCH (from_class:Class)<-[:FROM]-(rel:Relationship)-[:TO]->(to_class:Class)
        {where_clause if where_clause else ""}
        RETURN {{from: from_class.`{return_prop}`, to: to_class.`{return_prop}`, type: rel.relationship_type, optional: rel.relationship_optional}} as rel   
        """
        res = self.query(q)
        return [x['rel'] for x in res]

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
        RETURN {{from: x.label, to: y.label, type: rr.relationship_type, optional: rr.relationship_optional}} as rel            
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

    def get_rels_btw2(self, label1: str, label2: str, identifier='label'):
        """
        Returns all the relationships (according to the schema) between classes with identifier properties equal to
        {label1} and {label2} including the relationships of parent and child classes.
        """
        q = f"""
        MATCH 
            (c1:Class)<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(c1low:Class),
            (c2:Class)<-[:SUBCLASS_OF*0..{str(self.SCD)}]-(c2low:Class)
        WHERE 
            c1.`{identifier}` = $label1 AND c2.`{identifier}` = $label2 AND
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
        RETURN {{from: x.`{identifier}`, to: y.`{identifier}`, type: rr.relationship_type}} as rel            
        ORDER BY rel['from'], rel['to'], rel['type']
        """
        params = {'label1': label1, 'label2': label2}
        res = self.query(q, params)
        return [x['rel'] for x in res]

    def infer_rels(self, labels: list, labels_opt: list = None, impute_relationship_type: bool = True):
        """
        Infers most appropriate relationship type (if exists) between each pair of $labels
        for generating cypher query according to the schema
        """
        if not labels_opt:
            labels_opt = []  
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
        params = {
            'labels': labels,
            'impute_relationship_type': impute_relationship_type
        }
        res = self.query(q, params)
        rels = []
        for r in res:
            dct = r['rel']
            if dct.get('from') in labels_opt or dct.get('to') in labels_opt:
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

    def create_ct(self, controlled_terminology: dict, identifier='label', order_terms=True, merge_on=None):
        """
        Creates :Term nodes and links them to a specified class with a [:HAS_CONTROLLED_TERM] relationship.
        If order terms is True, an ascending Order property will be assigned to terms in the order they are created
        accounting for the order of existing terms (if any) and [:NEXT] relationships between terms following this order.
        For example:
            With identifier = 'label' and controlled_terminology =
            {
                'class1': [{'term_label': 'term1'}, {'term_label': 'term2'}],
                'class2': [{'term_label': 'term3'}]
            }
            3 new term nodes with 'term_label' properties equal to 'term1', 'term2' and 'term3' would be
            created. The class with 'label' = 'class1' would then be linked by [:HAS_CONTROLLED_TERM]
            relationships to 'term1' and 'term2' and similarly 'class2' would be linked to 'term3'

        :param controlled_terminology: A dictionary of classes and terms eg: {'class1': [{prop1: value, prop2: value}, ...]}
        :param identifier: String, property used when identifying classes to assign terms.
        :param order_terms: Bool, if true order properties and next relationships will be created between terms.
        :param merge_on: Optional List[string] term properties to merge on to prevent duplication
        :return: neo4j result object.
        """
        ident_props = None
        missing = self.get_missing_classes(list(controlled_terminology.keys()), identifier)
        assert not missing, f'Cannot create controlled terminology for nonexistent classes: {missing}'

        if merge_on:
            ident_props = f'{{`{merge_on[0]}`: term_props["{merge_on[0]}"]'
            for prop in merge_on[1:]:
                ident_props += f', `{prop}`: term_props["{prop}"]'
            ident_props += '}'

        # Create terms
        q1 = f"""
        UNWIND KEYS($terminology) as class_label
        MATCH (class:Class {{{identifier}: class_label}})
        WITH class, class_label
        UNWIND $terminology[class_label] as term_props
        CALL apoc.merge.node(['Term'], {f"{ident_props}, term_props, term_props" if ident_props else 'term_props, {}, {}'}) YIELD node
        CALL apoc.create.addLabels([node], [class.label]) YIELD node as term
        MERGE (class)-[:HAS_CONTROLLED_TERM]->(term)
        """
        res1 = self.query(q1, {'terminology': controlled_terminology}, return_type='neo4j.Result')

        if order_terms:
            # Create order property on new terms
            q2 = f"""
            UNWIND $labels as class_label
            MATCH (c:Class{{{identifier}: class_label}})
            OPTIONAL MATCH (c)-[:HAS_CONTROLLED_TERM]->(term:Term)
            WITH c, MAX(term.Order) as term_order
            WITH c, CASE WHEN term_order IS NULL THEN 1 ELSE term_order + 1 END as term_order
    
            MATCH (c)-[:HAS_CONTROLLED_TERM]->(t1:Term)
            WHERE t1.Order is NULL
            WITH c, term_order, t1 order by t1.`Codelist Code`, t1.`Term Code` 
            WITH c, collect(t1) as terms_to_order, term_order
            WITH *, apoc.coll.zip(terms_to_order, range(term_order, term_order + size(terms_to_order))) as pairs
            UNWIND pairs as pair
            WITH pair[0] as term_node, pair[1] as new_order, c, terms_to_order
            SET term_node.Order = new_order
            return c, terms_to_order
            """
            self.query(q2, {'labels': list(controlled_terminology.keys())})

            # Create next rel between terms
            q3 = f"""
            UNWIND $labels as class_label
            MATCH (c:Class{{{identifier}: class_label}})-[:HAS_CONTROLLED_TERM]->(t:Term)
            WITH c,t ORDER BY c.label, t.Order ASC
            WITH c, COLLECT(t) AS terms
            FOREACH (n IN RANGE(0, SIZE(terms)-2) |
                FOREACH (prev IN [terms[n]] |
                    FOREACH (next IN [terms[n+1]] |
                        MERGE (prev)-[:NEXT]->(next))))
            """
            self.query(q3, {'labels': list(controlled_terminology.keys())})

        return res1

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

    def get_class_ct_map(self, classes: list, ct_props: list = None, identifier='label'):
        """
        Return controlled terminology for a given list of classes.
        :param classes: list of class labels of class.identifier property values if using custom identifier
                        for example: with identifier = 'short_label', classes = ['shortlabel1', 'shortlabel2']
        :param ct_props: list of controlled terminology props to collect eg:
                         ['label', 'Codelist Code', 'Order']
                         defaults to ['rdfs:label']
        :param identifier: string used when identifying classes
        :return: Dictionary of classes and found controlled terminology for example:
                 with classes = ['class1', 'class2'] and ct_props = ['label'] a typical result might be:
                {'class1': [{'label': 'term1'}, {'label': 'term2'}], 'class2': [{{'label': 'term3'}}]}
                Where 'class1' has two terms labeled 'term1' and 'term2' and 'class2' has one term labeled 'term3'
        """

        if ct_props is None:
            ct_props = ['rdfs:label']
        elif type(ct_props) == str:
            ct_props = [ct_props]

        prop_collection = f'["{ct_props[0]}", term.`{ct_props[0]}`]'
        if len(ct_props) != 1:
            for prop in ct_props[1:]:
                prop_collection += f', ["{prop}", term.`{prop}`]'

        q = f"""
        UNWIND $classes as class
        MATCH (c:Class)-[:HAS_CONTROLLED_TERM]->(term:Term)
        WHERE c.`{identifier}` = class
        WITH c, collect(apoc.map.fromPairs([{prop_collection}])) as terms
        RETURN apoc.map.setKey({{}}, c.`{identifier}`, terms) as ct
        """

        params = {'classes': classes}
        res = self.query(q, params)

        data = {}
        if res:
            for term_dict in res:
                # Flattened result {class.identifer:[[term.ct_props[0], ...], [term.ct_props[0], ...]]}
                data.update(term_dict.get('ct'))
        return data

    def delete_ct(self, controlled_terminology: dict, ct_props: list, identifier='label'):
        """
        Deletes part of class specific controlled terminology.
        :param controlled_terminology: Dictionary of key: class identity - value: list of class specific term property
                                       values to delete. For example:
                                       with ct_props = ['Codelist Code'] controlled terminology might be:
                                      {'class1':[['code1'], ['code2']], class1':[['code2']]}
                                      Note these property values must align with the keys defined in term_props!
        :param ct_props: List of property names that define property values in a controlled terminology term,
                         For example:
                            With controlled terminology = {'class1':[['code1'], ['code2']], class1':[['code2']]}
                            ct_props would be ['Codelist Code'] indicating that we intending to delete terms
                            for 'class1' and 'class2' using 'Codelist Code' property value.
        :param identifier: string, property used to identify class
        :return: neo4j result object.
        """
        # TODO: Resolving [:NEXT] rel when deleting CT

        where_clause = f't.`{ct_props[0]}` = term_props[0]'
        for count, prop in enumerate(ct_props[1:], start=1):
            where_clause += f' AND t.`{prop}` = term_props[{count}]'

        q = f"""
        UNWIND KEYS($terminology) as class_label
        MATCH (c:Class)
        WHERE c.`{identifier}` = class_label
        UNWIND $terminology[class_label] as term_props
        MATCH (c)-[:HAS_CONTROLLED_TERM]-(t:Term)
        WHERE {where_clause}
        DETACH DELETE t
        """

        return self.query(q, {'terminology': controlled_terminology}, return_type='neo4j.Result')

    def get_all_ct(self, term_props: list, class_prop='label', derived_only=False):
        """
        Returns a list of dictionaries containing specified props for all controlled terminology.
        Example:
            With term_props = ['rdfs:label'] and class_prop = ['label'] the return format would be:
            [{'label': class1, 'rdfs:label': 'term1'}, {'label': class1, 'rdfs:label': 'term2'},
            {'label': class2, 'rdfs:label': 'term1'} ...]
            Repeated for all CT.
        :param term_props: Term properties to return eg: ['rdfs:label', 'Codelist Code']
        :param class_prop: Class property used to identify controlled terminology class
                            :param derived_only:
        :param derived_only: Bool, optional filter for classes where class.derived = 'true'.
        :return: List of dictionaries, see example above.
        """
        # TODO: consider converting to format similar to get_class_ct_map? ie {'class.label': [{'rdfs:label': 'term1'}]}
        assert len(term_props) >= 1, 'Must include at least 1 term_prop'
        assert class_prop not in term_props, 'Class prop cannot be in term props'

        term_return = f'term.`{term_props[0]}` as `{term_props[0]}`'
        for prop in term_props[1:]:
            term_return += f', term.`{prop}` as `{prop}`'

        q = f"""
        MATCH (c:Class)-[:HAS_CONTROLLED_TERM]->(term:Term)
        {'WHERE c.derived = "true"' if derived_only else ''}
        RETURN c.`{class_prop}` as `{class_prop}`, {term_return}
        """
        return self.query(q)

    def create_same_as_ct(self, same_as_terms: List[dict], term_identifiers: List[str], identifier='label'):
        """
        Creates a [:SAME_AS] relationship between two terms. For example:
            With term_identifiers = ['Codelist Code', 'Term Code']
            and same_as_terms = [
                {'from_class': 'class1', 'to_class': 'class2', # Class identifiers
                 'from_codelist_code': 'code_1', 'from_term_code': 'code_2', # Term 1 identifier
                 'to_codelist_code': 'code_3', 'to_term_code': 'code_4' # Term 2 identifier
                }
            ]
            A same as relationship would be created from a term of 'class1' with properties:
            `Codelist Code` = code_1 and `Term Code` = 'code_2' to the corresponding term for 'class2'.

            Note term identifiers in a same_as_terms dictionaries must be lowercase, use underscores instead of spaces
            and be prefixed with from_ or to_ a property listed in term_identifiers.

        :param same_as_terms: List of dictionaries defining same as terms between classes.
        :param term_identifiers: List of strings with term properties that guarantee uniqueness
        :param identifier: string class property used when matching classes
        :return: neo4j result object
        """

        where_clause = f"WHERE c1.`{identifier}` = new_term['from_class'] AND c2.`{identifier}` = new_term['to_class']"
        for term_prop in term_identifiers:
            clean_prop = term_prop.lower().replace(' ', '_')
            where_clause += f" AND t1.`{term_prop}` = new_term['from_{clean_prop}']"
            where_clause += f" AND t2.`{term_prop}` = new_term['to_{clean_prop}']"

        q = f"""
        UNWIND $same_as_terms as new_term
        MATCH (c1:Class)-[:HAS_CONTROLLED_TERM]-(t1:Term)
        MATCH (c2:Class)-[:HAS_CONTROLLED_TERM]-(t2:Term)
        {where_clause}
        MERGE (t1)-[:SAME_AS]->(t2)
        """
        return self.query(q, {'same_as_terms': same_as_terms}, return_type='neo4j.Result')

    def remove_same_as_ct(self, same_as_terms: List[dict], term_identifiers: List[str], identifier='label'):
        """
        Removes a [:SAME_AS] relationship between two terms, see "create_same_as_ct()" for format information.

        :param same_as_terms: List of dictionaries defining same as terms between classes.
        :param term_identifiers: List of strings with term properties that guarantee uniqueness
        :param identifier: string class property used when matching classes
        :return: neo4j result object
        """

        where_clause = f"WHERE c1.`{identifier}` = new_term['from_class'] AND c2.`{identifier}` = new_term['to_class']"
        for term_prop in term_identifiers:
            clean_prop = term_prop.lower().replace(' ', '_')
            where_clause += f"AND t1.`{term_prop}` = new_term['from_{clean_prop}']"
            where_clause += f"AND t2.`{term_prop}` = new_term['to_{clean_prop}']"

        q = f"""
        UNWIND $same_as_terms as new_term
        MATCH (c1:Class)-[:HAS_CONTROLLED_TERM]->(t1:Term)-[rel:SAME_AS]->(t2:Term)<-[:HAS_CONTROLLED_TERM]-(c2:Class)
        {where_clause}
        DETACH DELETE rel
        """

        return self.query(q, {'same_as_terms': same_as_terms}, return_type='neo4j.Result')

    def propagate_rels_to_parent_class(self):
            if self.verbose:
                logger.info("Copying Relationships to 'parent' Classes where (child)-[:SUBCLASS_OF]->(parent)")
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

    def propagate_rels_to_child_class(self):
        if self.verbose:
            logger.info("Copying Relationships to 'child' Classes where (child)-[:SUBCLASS_OF]->(parent)")
        
        match_rel = '(source:Class)-[:SUBCLASS_OF*1..50]->(c)'

        self.query(f"""
        MATCH (c:Class)<-[r1:TO|FROM]-(r:Relationship)-[r2:TO|FROM]-(target:Class), {match_rel}
        WHERE type(r1) <> type(r2)
        WITH *,
        "
            WITH $source as source, $target as target
            MERGE (source)<-[:`"+type(r1)+"`]-(:Relationship{{relationship_type:$type}})-[:`"+type(r2)+"`]->(target)
            RETURN count(*)
        " as q, 
        {{type: r.relationship_type, source: source , target: target}} as params
        CALL apoc.cypher.doIt(q, params) YIELD value
        RETURN value, q, params         
        """)  


    def propagate_terms_to_parent_class(self):
        if self.verbose:
            logger.info("Copying terms to 'parent' where (child)-[:SUBCLASS_OF]->(parent)")
        
        match_rel = '(c)-[:SUBCLASS_OF*1..50]->(source:Class)'

        self.query(f"""
        MATCH (c:Class)-[:HAS_CONTROLLED_TERM]->(term:Term), {match_rel}
        MERGE (source)-[:HAS_CONTROLLED_TERM]->(term) 
        RETURN term     
        """)
  

    def remove_unmapped_classes(self):
        if self.verbose:
            logger.info("Removing Unmapped Classes")
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
        if self.verbose:
            logger.info("Removing Auxilary Term Labels")
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
            exclude_properties: list = ["_filename_", "_folder_"],
    ):
        """
        Creates Class and Relationship nodes to represent a trivial schema to reshape data ingested e.g. with
        FileDataLoader - Tables loaded into nodes one row to one node, column names used as property names.

        data_labels: labels of the nodes where loaded data is stored (mm with use OR btw labels to fetch data nodes)
        domain_property: property where the name of the table/domain can be found
        """

        self.create_index("Class","label")
        self.create_index("Class","short_label")
        self.create_index("Relationship","relationship_type")
        self.create_index("Term","Codelist Code")
        self.create_index("Term","Term Code")

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
        
    def export_model_to_linkml(self):
        """
        Read the Class/Relationship/Term model from Neo4j and returns a linkml like dict.
        More about linkml:
            https://linkml.io/linkml/
            https://github.com/linkml/linkml
        One can then easily create a yaml file from the dict with the following code:
        import yaml
        cld_schema_dict = mm.export_model_to_linkml()
        yaml_serialized = yaml.dump(cld_schema_dict)
        with open(f"cld.yaml", "w") as f:
            f.write(yaml_serialized)        
        """
        
        q = """
        MATCH (c:Class)
        WITH c, apoc.map.fromPairs(
                [k in [k in ['label', 'short_label', 'derived', 'data_type', 'is_stat', 'uri'] WHERE NOT c[k] IS NULL] | [k,c[k]]]
            ) as class_map
        OPTIONAL MATCH (c)-[:FROM]-(r:Relationship)-[:TO]->(c2:Class)
        WITH c, class_map, {alias: r.relationship_type, name: c.label + " " + r.relationship_type, uri: r.uri} as _attr, c2
        ORDER BY c.label, c2.label, r.relationship_type
        WITH c, class_map, [x in collect( 
            apoc.map.fromPairs(
                [k in [k in ['alias', 'name', 'uri'] WHERE NOT _attr[k] IS NULL] | [k, _attr[k]]] + [['range', c2.label]]
            )
        ) WHERE NOT x = {range: NULL}]
        + CASE WHEN c.create = True THEN [] ELSE [{
            alias: 'rdfs:label', name:c.label + ' rdfs:label', 
            range: 
                CASE WHEN EXISTS ((c)-[:HAS_CONTROLLED_TERM]->(:Term)) THEN c.label + ' CT' ELSE coalesce(c.data_type, 'string') END
        }] END 
        as attributes
        WITH c, class_map, attributes
        WITH collect(apoc.map.merge(class_map, {attributes: attributes})) as classes
        OPTIONAL MATCH (c:Class)-[:HAS_CONTROLLED_TERM]->(t:Term)
        WITH classes, c, {permissible_values: apoc.map.fromPairs(collect(
            [t.`rdfs:label`, {description: t.`Codelist Code` + '_' + t.`Term Code`}]
        ))} as pv
        WITH classes, apoc.map.fromPairs(collect([c.label + ' CT', pv])) as enums        
        WITH {classes: classes, enums: enums} as res
        RETURN apoc.map.fromPairs([k in [k in keys(res) WHERE NOT (res[k] IS NULL or res[k] = {} or res[k] = [])] | [k, res[k]]]) as res
        """
        #TODO: currently linkml range for rdfs:label is set to Class.data_type, need to check that these are aligned with linkml types
        return self.query(q)[0]['res']
            
    def create_model_from_linkml(self, linkml_dict: dict):
        """
        Create Class/Relationship/Term schema from a linkml-like dict (see export_model_to_linkml above)
        One can read linkml schema from a yaml file with the following code:
        
        import yaml
        with open("cld.yaml", "r") as stream:
            cld_schema_dict = yaml.safe_load(stream)
            
        Args:
            linkml_dict (dict): linkml-like dict
        """
        classes = linkml_dict.get('classes')
        for class_ in classes:
            attrs = class_.pop("attributes")
            self.create_class([class_], merge_on=["label"])
            self.create_relationship(
                [
                    [class_.get("label"), attr_.get("range"), attr_.get("alias")] 
                    for attr_ in attrs
                    if not attr_.get("alias") == "rdfs:label"
                ]
            )
            
        ct = {}
        for class_, dct_1 in linkml_dict.get("enums").items():
            ct[class_[:-3]] = [
                {
                    'rdfs:label': rdfs_label,
                    'Codelist Code': dct_2.get("description").split("_")[0],
                    'Term Code': dct_2.get("description").split("_")[1]
                }
                for rdfs_label, dct_2 in dct_1.get("permissible_values").items()
            ]
        self.create_ct(
            ct,
            merge_on=['Codelist Code', 'Term Code']
        )

    def delete_from_graph(self):
        actions = [
            f"""
            MATCH (m:Method)
            OPTIONAL MATCH path = (m)-[r:METHOD_ACTION|NEXT*1..10]->(x:`Method`)
            OPTIONAL MATCH path2 = (x)-[r2]->(y)
            DETACH DELETE r2, x
            """,
            f"""
            MATCH (m:Method)
            OPTIONAL MATCH (m)-[r]-()
            DELETE r
            DELETE m
            """,
            f"""
            MATCH (class:Class)
            WHERE class.derived IS NOT NULL
            OPTIONAL MATCH (class)-[r:HAS_CONTROLLED_TERM]-(term:Term)
            OPTIONAL MATCH (class)-[r1:TO|FROM]-(rel:Relationship)-[r2:TO|FROM]-(class2:Class)
            DELETE r, r1
            DETACH DELETE term, class, rel
            """,
            f"""
            MATCH (term1:Term)-[r:SAME_AS]->(term2:Term)
            DELETE r
            """
        ]
        for q in actions:
            self.query(q)

    