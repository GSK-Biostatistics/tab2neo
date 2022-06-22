from neointerface import NeoInterface
import pandas as pd
import logging

class ModelManager(NeoInterface):
    """
    Python class to manage metadata nodes (such as nodes with label Class and Property) along with relationships between them
    as well as to return information about them
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.verbose:
            print (f"---------------- {self.__class__} initialized -------------------")



    ####################  TO CREATE/DELETE Classes or Properties  ####################

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
        assert type(merge)==bool

        if not type(classes) == list:
            classes = [classes]

        if merge:
            q = "WITH $classes as classes UNWIND classes as class_name CALL apoc.merge.node(['Class'], {label: class_name}, {}, {}) YIELD node RETURN node as class"
        else:
            q = "WITH $classes as classes UNWIND classes as class_name CALL apoc.create.node(['Class'], {label: class_name}) YIELD node RETURN node as class"

        params = {'classes': classes}

        if self.debug:
            print(f"""
            query: {q}
            parameters: {params}
            """)

        return self.query_expanded(q, params)



    def delete_class(self, class_:str):
        #TODO: add check EXISTS ( (class:Class{{label:$class_}})-[:HAS_PROPERTY]->() ), return -1
        q = "MATCH (class:Class{{label:$class_}}) DELETE class"
        params = {'label': class_}
        if self.debug:
            print(f"""
            query: {q}
            parameters: {params}
            """)
        self.query(q, params)



    def create_property(self, class_:str, property:str, merge=True) -> None:
        """
        Add the given Property to the specified Class

        :param class_:      Name of the Class to which the new Property is attached
        :param property:    Name of the new Property.  It must be a non-empty string.
        :param merge:       If True, then the Class is created if not present,
                                          and the Property is only created if not already present;
                            if False, then the Class must be already present (or nothing will be added),
                                           and the Property is always created
        :return:            None
        """
        if not class_:
            return      # Whether a Class node is matched or created, the Class name must always be present

        if not property:
            return      # Validate property to be a non-empty string

        op1 = ('MERGE' if merge else 'MATCH')
        op2 = ('MERGE' if merge else 'CREATE')
        q = f"""
        {op1} (class:Class {{label:$class_}})        
        WITH *
        {op2} (class)-[:HAS_PROPERTY]->(property:Property{{label:$property}})        
        """
        params = {
            'class_': class_,
            'property': property
        }
        if self.debug:
            print(f"""
            query: {q}
            parameters: {params}
            """)
        self.query(q, params)



    def delete_property(self, class_:str, property:str):
        q = """
        MATCH (class:Class{{label:$class_}})-[:HAS_PROPERTY]->(property:Property{{label:$property}})  
        DELETE property
        """
        params = {
            'class_': class_,
            'property': property
        }
        if self.debug:
            print(f"""
             query: {q}
             parameters: {params}
             """)
        self.query(q, params)



    ####################  TO RENAME Classes or Properties  ####################

    def rename_class(self, class_:str, new_class_label:str):
        q = """
        MATCH (class:Class {{label:$class_}})
        set class.label = $new_class_label
        """
        params = {
            'class_': class_,
            'new_class_label': new_class_label
        }
        if self.debug:
            print(f"""
            query: {q}
            parameters: {params}
            """)
        self.query(q, params)
        #currently existing instances of nodes with label of class.label will not be renamed (the method to be called before refactoring)
        #TODO: consider adding calling apoc.refactor.rename.label withing apoc.periodic.iterate to allow for renaming instances of the class_



    def rename_classes_by_ids(self, name_mapping: list) -> list:
        """
        Bulk-rename one or more Class nodes using the given mapping,
        a list of pairs which provides the new names for the nodes with the specified Neo4j IDs

        :param name_mapping:    EXAMPLE: [[123, 'new name for node 123'], [88, 'new name for node 88']]

        :return:                A list of dictionaries explaining the operations performed.
                                EXAMPLE: [{'mod': 'Class: A=>A new'}, {'mod': 'Class: B=>B new'}, {'mod': 'Class: C=>C new'}]
        """
        q = """
            WITH $name_mapping as name_mapping 
            UNWIND name_mapping as single_rename
            MATCH (class :Class) WHERE id(class) = single_rename[0]
            WITH class, class.label as old_label, single_rename
            SET class.label = single_rename[1]
            RETURN 'Class: ' + old_label + '=>' + class.label as mod
        """
        params = {'name_mapping': name_mapping}

        if self.debug:
            logging.debug(f"""
            query: {q}
            parameters: {params}
            """)

        return self.query(q, params)



    def rename_property(self, class_:str, property:str, new_property_label:str):
        q = """
        MATCH (class:Class{{label:$class_}})-[:HAS_PROPERTY]->(property:Property{{label:$property}})
        set property.label = $new_property_label
        """
        params = {
            'class_': class_,
            'property': property,
            'new_property_label': new_property_label
        }
        if self.debug:
            print(f"""
            query: {q}
            parameters: {params}
            """)
        self.query(q, params)
        #currently existing instances of nodes with label of class.label will not be renamed (the method to be called before refactoring)
        #TODO: consider adding calling apoc.refactor.rename.nodeProperty withing apoc.periodic.iterate to allow for renaming properties of instances of the class_



    ####################  TO CREATE RELATIONSHIPS  ####################

    def create_class_relationship(self, from_class: str, to_class: str, rel_name = "CLASS_RELATES_TO") -> None:
        """
        Add a relationship between the 2 specified classes, by default named "CLASS_RELATES_TO",
        provided that it doesn't already exists.
        For bulk-creation of relationships, see create_custom_rels_from_list()

        :param from_class:  Name of the 1st Class
        :param to_class:    Name of the 2nd Class
        :param rel_name:    Name of the relationship to create from the 1st to the 2nd Class
        :return:            None
        """
        cypher = f'''
                 MATCH (from:`Class` {{label: $from_class}}), (to:`Class` {{label: $to_class}})
                 MERGE (from)-[:{rel_name}]->(to)
                 '''

        self.query(cypher, {"from_class": from_class, "to_class": to_class})



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



    def create_related_classes_from_list(self, rel_list: [[str, str]]) -> [str]:
        """
        Create `Class` nodes and "CLASS_RELATES_TO" relationships among them, as specified by rel_list

        Given a list of name pairs, perform 2 operations:
        1)  Identify all the unique names, and create new nodes labeled "Class",
            each new node has one of the unique names stored in an attribute named "label"
        2)  Adds "CLASS_RELATES_TO" relationships to pairs of the newly-created nodes,
            as specified by the pairs in the elements of rel_list

        EXAMPLE:  if rel_list is  [  ["Study", "Site"],  ["Study", "Subject"]  ]
                  then 3 new `Class`-labeled nodes will be created, with "label" attributes respectively
                  valued "Study", "Site" and "Subject",
                  plus a CLASS_RELATES_TO" relationship from "Study" to "Site", and one from "Study" to "Subject"

        NOTE: if some of the Class` nodes might already exist, use create_custom_rels_from_list() instead

        :param rel_list: A list of 2-element lists, indicating a relationship among nodes of type `Class`
                         EXAMPLE:   [
                                        ["Study", "Site"],
                                        ["Study", "Subject"],
                                        ["Subject", "Race"]
                                    ]
        :return:         List of all the class names; repeated ones are taken out
        """

        # Identify all the unique names in inner elements of rel_list
        class_set = set()  # Empty set
        for rel in rel_list:
             class_set = class_set.union(rel)   # The Set Union operation will avoid duplicates

        class_list = sorted(list(class_set))            # Convert the final set back to list

        # Create the new nodes
        self.query(
            "UNWIND $classes as class CREATE (:Class {label:class})",
            {'classes': class_list}
        )

        # Create the desired relationships among them
        self.create_custom_rels_from_list(rel_list)

        return class_list



    def create_custom_mappings_from_dict(self, groupings = None) -> None:
        """
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



    ####################  TO DELETE RELATIONSHIP  ####################

    def delete_rels(self, data) -> None:
        """
        Bulk-delete the "CLASS_RELATES_TO" relationship (if present) from node pairs specified by their Neo4j IDs.
        Return status information

        :param data:    A list of dictionaries that contain the keys '_id_Class1' (FROM node) and '_id_Class2' (TO node)
                        EXAMPLE: [{'_id_Class1': 777, '_id_Class2': 123},
                                  {'_id_Class1': 111, '_id_Class2': 888}]
        :return:        None
        """
        q = """
            WITH $data as data UNWIND data as row
            MATCH (class1), (class2)
            WHERE id(class1) = row['_id_Class1'] and id(class2) = row['_id_Class2']
            OPTIONAL MATCH (class1)-[r:CLASS_RELATES_TO]->(class2)
            DELETE r
            RETURN  class1.label + '-->' + class2.label as mod
            """

        params = {'data': data}

        self.query(q, params)




    ####################  TO RETRIEVE  ####################

    def get_all_classes(self, include_id=False, sort=False) -> [dict]:
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
            RETURN class.label as Class
            '''

        if include_id:
            q += " , id(class) as _id_Class "

        if sort:
            q += " ORDER BY class.label"

        return self.query(q)



    def get_label_by_id(self, neo4j_id: int) -> str:
        """
        Return the value of the "label" attribute of the node with the given ID.
        In case of error, return "NA"

        :param neo4j_id: Neo4j internal numeric ID to identify a node
        :return:         The value of the "label" attribute
        """
        if type(neo4j_id) != int:
            return "NA"

        q = f"MATCH (n) WHERE id(n) = {neo4j_id} RETURN n.label AS label"
        result = self.query(q)

        return result[0]["label"]



    def get_class_properties(self, classes = None):
        """
        Given a string, or list of strings, locate all the `Class` nodes with a "label" attribute matching one of those values,
        and that are connected to `Property` nodes thru HAS_PROPERTY relationships.
        Collect the values of the "label" attributes of all the `Property` nodes, and
        turn them into a dictionary, indexed by the value of the "label" attribute of the `Class` nodes.

        :param classes: A string or list of strings.  EXAMPLE: ['Study', 'Site', 'Subject']
        :return:        A dictionary
        """
        if self.verbose:
            print (f"-- Getting class properties {classes}--")

        # If the passed argument was a string, turn into a 1-element list
        if type(classes) == str:
            classes = [classes]

        # Prepare a WHERE clause
        wh = (f"WHERE class.label in $classes" if classes else "")

        q = f"""
        MATCH (class:Class)-[:HAS_PROPERTY]->(property:Property)
        {wh}            
        WITH * ORDER BY class, id(property)
        WITH class, collect(property) as coll
        RETURN apoc.map.fromPairs(collect([class.label, [x in coll | x.label]])) as mp
        """
        # EXAMPLE of Cypher query constructed above:
        """
            MATCH (class:Class)-[:HAS_PROPERTY]->(property:Property)
            WHERE class.label in $classes            
            WITH * ORDER BY class, id(property)
            WITH class, collect(property) as coll
            RETURN apoc.map.fromPairs(collect([class.label, [x in coll | x.label]])) as mp
        """

        qres = self.query(q, {"classes": classes})
        if qres:
            return qres[0]['mp']
        else:
            return {}

    def get_class_short_labels(self, classes=None):
        """
        Return a dictionary with class.label as keys and
        values of non-NULL class.short_label values of the corresponding class as items
        :param classes:
        :return:
        """
        q = """
        MATCH (c:Class)
        WHERE c.label in $labels and c.short_label IS NOT NULL
        WITH collect([c.label, c.short_label]) as coll
        RETURN apoc.map.fromPairs(coll) as map
        """
        res = self.query(q, {'labels': classes})
        if res:
            return res[0]['map']
        else:
            return {}


    #########################   PANDAS-RELATED   #########################


    def get_related_classes(self) -> pd.DataFrame:
        """
        Locate all pairs of Classes (class1, class2) such that there is a "CLASS_RELATES_TO"
        from class1 to class2.
        Return their names and Neo4j ID's, as a Pandas dataframe,
        with columns named _id_Class1, Class1, _id_Class2 Class2.

        :return:    A Pandas dataframe
                    EXAMPLE: if the relationships are A->B->C and A->C, it may return
                                _id_Class1 Class1  _id_Class2 Class2
                            0         381      A         382      B
                            1         381      A         383      C
                            2         382      B         383      C
        """
        q = """
            MATCH (class1:Class)-[:CLASS_RELATES_TO]->(class2:Class)            
            RETURN id(class1) as _id_Class1, class1.label as Class1, 
                   id(class2) as _id_Class2, class2.label as Class2
            """
        if self.debug:
            logging.debug(f"""
            query: {q}      
            """)

        result = self.query(q)

        return pd.DataFrame(result)



    def load_mappings_from_df(self, df_mappings:pd.DataFrame):
        """
        The method can be used for batch-loading (Class)-[]-(Property) mapping to (Source Data Column)<-[]-(Source Data Table)
        with [:MAPS_TO_PROPERTY] relationship
        NOTE:
          - if specified Class or Property do not exist they will be created
          - if values of (Source Data Table)._domain_ = '*' then columns with specified name of all datasets will be mapped
          - if values of (Source Data Table)._domain_ and (Source Data Column)._columnname_ are None the Class and Property will just be created/merged
        EXAMPLE input data frame
        pd.DataFrame([
        {'Class':'ZZZ', 'Property':None},
        {'Class':'ZZZ', 'Property':'abc'},
        {'Class':'ZZZ', 'Property':'zyx', 'Source Data Table':'adsl', 'Source Data Column':'country'},
        {'Class':'ZZZ', 'Property':'new_property_no_mapping', 'Source Data Table':None, 'Source Data Column':'country'}
        ])
        :return:
        """
        q = """
        WITH $mappings as mappings UNWIND mappings as mapping
        WITH mapping['Class'] as class_, collect(mapping) as class_mappings
        MERGE (class:Class{label:class_})
        WITH * UNWIND class_mappings as mapping
        WITH class, mapping['Property'] as property_, collect(mapping) as class_property_mappings
        WHERE NOT property_ IS NULL
        MERGE (class)-[:HAS_PROPERTY]->(property:Property{label:property_})
        WITH * UNWIND class_property_mappings as mapping
        WITH class, property, mapping
        WHERE NOT mapping['Source Data Table'] IS NULL AND NOT mapping['Source Data Column'] IS NULL
        MATCH (sdt:`Source Data Table`)-[:HAS_COLUMN]->(sdc:`Source Data Column`)
        WHERE sdt._domain_ = mapping['Source Data Table'] AND sdc._columnname_ = mapping['Source Data Column']
        MERGE (sdc)-[:MAPS_TO_PROPERTY]->(property) 
        """
        params = {
            'mappings': df_mappings.to_dict(orient='records')
        }
        if self.debug:
            print(f"""
            query: {q}
            parameters: {params}
            """)
        self.query(q, params)
