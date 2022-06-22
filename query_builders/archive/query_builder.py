from neointerface import NeoInterface
from model_managers.model_manager import ModelManager
import pandas as pd


class QueryBuilder(NeoInterface):
    """
    To support creation of cypher queries to work with data in Neo4j

    Supports 2 modes:
      'schema_PROPERTY' - generating query for fetching data based on definitions and relationships between metadata
                 nodes(such as Class and Property)
      'schema_CLASS' - generating query for fetching data based on definitions and relationships between metadata
                 nodes(where columns are mapped to Classes and no Property nodes defined)
      'noschema' - generating query for fetching data based on all existing nodes and relationships in the database
    """
    OCLASS_MARKER = "**"
    RDFSLABEL = "rdfs:label"
    def __init__(self, mode='schema_PROPERTY', allow_optional_classes=True, *args, **kwargs):
        """
        :param mode: 'schema_PROPERTY' or 'schema_CLASS' or 'noschema'
        :param args:
        :param kwargs:
        """
        assert mode in ['schema_PROPERTY', 'schema_CLASS', 'noschema']
        self.mode = mode
        self.allow_optional_classes = allow_optional_classes
        if self.mode in ['schema_PROPERTY', 'schema_CLASS']:
            self.mm = ModelManager(*args, **kwargs)
        else:
            self.mm = None
        # self.bindings = {}
        super().__init__(*args, **kwargs)
        if self.verbose:
            print(f"---------------- {self.__class__} initialized -------------------")

    # -----------------------------------------------------------
    # ------------------------ Core Method ----------------------
    # -----------------------------------------------------------
    def generate_query_body(self, classes: list, where_map=None, allow_unrelated_subgraphs=False):
        """
        Generates query body based on self.mode
        Takes care about optional classes
        :param classes:
        :param where_map:
        :return:
        """
        if not where_map:
            where_map = {}
        # classes with a postfix ** are considered optional for matching - separating general and optional classes
        g_classes, g_opt_classes = self.split_out_optional_classes(classes)
        # separating classes that are in where clause (to go first if the query)
        wg_classes = [class_ for class_ in g_classes if class_ in where_map.keys()]

        if self.mode in ['schema_PROPERTY', 'schema_CLASS']:
            f = self.qb_generate_query_body
        else:
            f = self.qbns_generate_query_body

        if self.allow_optional_classes and g_opt_classes:
            # first part - classes from the where statement
            (wg_cypher, wg_data_dict) = ("", {})
            if wg_classes:
                wg_where_map = {k: i for k, i in where_map.items() if k in wg_classes}
                (wg_cypher, wg_data_dict) = f(classes=wg_classes, where_map=wg_where_map)

            # second part - all other non-optional classes
            (g_cypher, g_data_dict) = f(classes=[class_ for class_ in g_classes if class_ not in wg_classes],
                                        classes_for_rel=g_classes,
                                        where_map={})
            # case when all classes are present also in the where_map
            if g_cypher.strip() == "MATCH":
                g_cypher = ""

            # third part - optional classes
            o_cyphers = []
            o_data_dict = {}
            # 1 OPTIONAL MATCH statement per optional class (considering they are all allow to exist/non-exist independently)
            for oc in g_opt_classes:
                oc_where_map = ({oc: where_map[oc]} if oc in where_map.keys() else {})
                (oc_cypher, oc_data_dict) = f(classes=[oc],
                                              classes_for_rel=g_classes + [oc],
                                              where_map=oc_where_map)
                o_cyphers.append(oc_cypher)
                o_data_dict = {**o_data_dict, **oc_data_dict}
            # building query and dictionaries together
            return (
                (wg_cypher.strip() + ' \n' + 'WITH * \n' if wg_cypher else "") +
                g_cypher.strip() + ' \n' +
                ''.join(['OPTIONAL ' + c_chunk.strip() + ' \n' for c_chunk in o_cyphers]),
                {**wg_data_dict, **o_data_dict}
            )
        else:
            (wg_cypher, wg_data_dict) = ("", {})
            if wg_classes:
                wg_where_map = {k: i for k, i in where_map.items() if k in wg_classes}
                (wg_cypher, wg_data_dict) = f(classes=wg_classes, where_map=wg_where_map)

            (g_cypher, g_data_dict) = f(classes=[class_ for class_ in g_classes if class_ not in wg_classes],
                                        classes_for_rel=g_classes,
                                        where_map={})
            # case when all classes are present also in the where_map
            if g_cypher.strip() == "MATCH":
                g_cypher = ""
            return (
                (wg_cypher.strip() + ' \n' + 'WITH * \n' if wg_cypher else "") + g_cypher.strip(),
                {**wg_data_dict, **g_data_dict}
            )

    # -----------------------------------------------------------
    # -----------  General methods (regardless of mode) ---------
    # -----------------------------------------------------------
    def split_out_optional_classes(self, classes: list) -> (list, list):
        """
        Checks if class name ends with optional class marker and splits classes into usual and optional
        :param classes: list of class names
        :return: tuple of lists ([normal classes], [optional classes])
        """
        c, oc = [], []
        for class_ in classes:
            if class_.endswith(self.OCLASS_MARKER):
                oc.append(class_[:-(len(self.OCLASS_MARKER))])
            else:
                c.append(class_)
        return c, oc

    # # TODO: implement list dict of bindings instead of using lower-case label (for cases when different bindings need to be assinged to the same class
    # def append_binding(self, label):
    #     if label not in self.bindings.keys():
    #         self.bindings[label] = 'b' + str(len(self.bindings))

    def list_data_labels(self, classes: list) -> list:
        """
        Helper function, to produce a list that will be used to form the "MATCH" part of a Cypher query.

        EXAMPLE: The input list ['Class 1', '$$$'] results in ['(`class 1`:`Class 1`)', '(`$$$`:`$$$`)']
                 The lower-case versions of the class names are meant to be placeholder for Neo4j nodes,
                 in a downstream Cypher query.

        :param classes: List of strings (class names)
        :return:        A list of strings (meant to be later used to form a Cypher query)
        """
        # for label in classes:
        #     self.append_binding(label)
        # return [f"(`{self.bindings[label]}`:`{label}`)" for label in classes]
        return [f"(`{label.lower()}`:`{label}`)" for label in classes]

    def list_where_conditions_per_dict(self, mp: dict) -> ([], {}):
        """
        Given a dictionary of dictionaries (see below for example), loop thru all the entries
        and produce a list of strings, each of them suitable for inclusion in WHERE clauses in Cypher,
        together with a data-binding dictionary.

        The keys in the outer dictionary are expected to be node labels.
        The (key/values) in the inner dictionary entries are meant to be (attribute names/desired values) that
        are applicable to the node with their corresponding label.

        EXAMPLE - if the argument is:
                {
                    'CAR': {
                        'year': 2021
                    },
                    'BOAT': {
                        'make': 'Jeanneau'
                    }
                }

                then the goal is to enforce that the 'year' property for 'CAR' nodes has a value of 2021,
                and that the 'make' property for 'BOAT' nodes has a value of 'Jeanneau'.
                The above requirements are encoded in the following list of strings:

                                ["`car`.`year` = $par_1", "`boat`.`make` = $par_2"]

                in conjunction with the following data dictionary:

                                {"par_1": 2021 , "par_2": 'Jeanneau'}

                Notice that the label names are turned to lower case,
                and that backticks are used in all the label and attributes names
                (which allows for blank spaces in the final Cypher code).

        :param mp:  A dictionary of dictionaries.  EXAMPLE:
                            {
                                'SUBJECT': {
                                    'USUBJID': '01-001',
                                    'SUBJID': '001',
                                    'PATIENT GROUP': [3, 5]
                                },
                                'SEX': {
                                    'ASEX': 'Male'
                                }
                            }

                    - The keys in the outer dictionary are expected to be node labels.
                    - The (key/values) in the inner dictionary entries are meant to be (attribute names : desired values)
                      that are applicable to the node with their corresponding label.
                    - The values may be various data type, incl. strings, integers and lists.

        :return:    The pair (list_of_Cypher_strings , data_dictionary)

                    Each string in the list is of the form
                            "`label_name`.`attribute_name` = $par_N"

                            where label_name is always in lower case, and N is an integer.
                            EXAMPLE:   ["`subject`.`USUBJID` = $par_1",
                                        "`subject`.`SUBJID` = $par_2",
                                        "`subject`.`PATIENT GROUP` in $par_3",   <- NOTE: it's "in" rather than equal because $par3 is a list
                                        "`sex`.`ASEX` = $par_4"]

                    EXAMPLE of data dictionary: {"par_1": "01-001" , "par_2": "001", "par_3": [3, 5], "par_4": "Male"}
        """
        cypher_list = []
        data_dictionary = {}

        def parameter_labels(): # Generates sequential integers used in the data dictionary, such as "par_1", "par_2", etc.
            k = 1
            while True:
                yield f"par_{k}"
                k += 1

        parameter_token_stack = parameter_labels()

        # Loop over the outer dictionary
        for label, property_dict in mp.items():
            # EXAMPLE: label = 'SUBJECT' , property_dict = {'USUBJID': '01-001','SUBJID': '001', 'PATIENT GROUP': [3, 5]}

            # Loop over the inner dictionary
            for property_name, property_value in property_dict.items():
                # EXAMPLE:  property_name = 'USUBJID' , property_value = '01-001'

                # Handle ranges
                # EXAMPLE: {"Age": {"AGE": {"min": 18, "max": 65, "max_include": False, "min_include": True, "incl_null": True}}}
                if isinstance(property_value, dict) and set(property_value.keys()).intersection(
                        {"min", "max", "min_include", "max_include", "incl_null"}):

                    max_include = property_value.get("max_include", False)
                    min_include = property_value.get("min_include", False)
                    incl_null = property_value.get("incl_null") #if None then the clause is not included at all

                    t1, t2 = next(parameter_token_stack), next(parameter_token_stack)
                    data_dictionary[t1], data_dictionary[t2] = property_value.get("min"), property_value.get("max")
                    min_max_subclause_list = []
                    if data_dictionary[t1]:
                        min_max_subclause_list.append(f"${t1} {'<=' if min_include else '<'}")
                    if data_dictionary[t1] or data_dictionary[t2]:
                        min_max_subclause_list.append(f"`{label.lower()}`.`{property_name}`")
                    if data_dictionary[t2]:
                        min_max_subclause_list.append(f"{'<=' if max_include else '<'} ${t2}")

                    if incl_null is None:
                        include_nulls_list = []
                    elif incl_null:
                        include_nulls_list = [f"`{label.lower()}`.`{property_name}` IS NULL"]
                        if min_max_subclause_list:
                            include_nulls_list.append("OR")
                    else:
                        include_nulls_list = [f"`{label.lower()}`.`{property_name}` IS NOT NULL"]
                        if min_max_subclause_list:
                            include_nulls_list.append("AND")

                    full_str = " ".join(include_nulls_list + (
                            ['(' + " ".join(min_max_subclause_list) + ')'] if min_max_subclause_list else []))
                    if include_nulls_list and min_max_subclause_list:
                        full_str = f"({full_str})"
                    if full_str:
                        cypher_list.append(full_str)
                else:
                    operator = ("in" if type(
                        property_value) == list else "=")  # For list inclusions, use "in"; in all other case check for equality

                    # Extend the list of Cypher strings and their corresponding data dictionary
                    t = next(parameter_token_stack)
                    if label.startswith("!"):  # ! is NOT operator
                        cypher_list.append(f"""NOT (`{label.lower()[1:]}`.`{property_name}` {operator} ${t})""")  # The $ refers to the data binding
                    else:
                        cypher_list.append(f"`{label.lower()}`.`{property_name}` {operator} ${t}")
                    data_dictionary[t] = property_value

        return cypher_list, data_dictionary

    def generate_return(self,
                        classes: list,
                        return_disjoint: bool = False,
                        return_nodeid: bool = False,
                        return_properties: str = "*",
                        prefix_keys_with_label:bool = False,
                        ) -> (str, dict):
        """
        Helper function for get_data() and for _get_filters_helper().
        Compose and return a string suitable for the RETURN statement of a Cypher query to populate tables and menus on the dashboard.
        Used when we have a schema with Classes and Properties.

        EXAMPLES - all based on the class list ['Study', 'Site'], with various arguments:

            "RETURN apoc.map.mergeList([`study`, `site`]) as all"
            "RETURN id(`study`) as `Study`, id(`site`) as `Site`, apoc.map.mergeList([`study`, `site`]) as all"
            "RETURN collect(distinct `study`{.*}) as `Study`, collect(distinct `site`{.*}) as `Site`"
            "RETURN collect(distinct {`Study`:id(`study`)}) as `Study`, collect(distinct {`Site`:id(`site`)}) as `Site`"

        :param classes:             List of strings.  EXAMPLE: ['Study', 'Site'].  If empty, an Exception is raised.
        :param return_disjoint:     If True, the result will contain a term for each class, such as "collect(distinct `study`{.*}) as `Study`"
                                        The final goal is a Cypher query to produce a dictionary of dataframes, to populate the
                                        menus in the dashboard.
                                        EXAMPLE: "RETURN collect(distinct `study`{.*}) as `Study`, collect(distinct `site`{.*}) as `Site`"
                                    If False, the result contains the apoc.map.mergeList function, to produce data
                                        for the tables in the dashboard
                                        EXAMPLE: "RETURN apoc.map.mergeList([`study`, `site`]) as all"

        :param return_nodeid:       If True, also include in the result, just after the "RETURN " part,
                                        a string used to return the id's of all the classes.
                                        EXAMPLE: "id(`study`) as `Study`, id(`site`) as `Site`"
        :param return_properties:   A string (default value "*")  TODO: only the "*" option is currently implemented
        :param prefix_keys_with_label: If True adds a prefix (corresponding node label) to each column name of the returned data
        :param rename_rdfslabel:    If True renames all properties named rdfs:label to value of corresponding Class.short_label
        :return:                    A string
        """
        assert type(classes) == list, "ERROR in qb_generate_return(): the argument `classes` must be a list"
        assert classes != [], "ERROR in qb_generate_return(): empty lists are not an acceptable argument for `classes`"
        assert return_properties == "*" or type(return_properties) == list
        data_dictionary = {}
        all_classes = [(class_[:-len(self.OCLASS_MARKER)] if class_.endswith(self.OCLASS_MARKER) else class_)
                   for class_ in classes]
        if self.mode == 'schema_CLASS':
            short_labels = self.mm.get_class_short_labels(classes=all_classes)
            data_dictionary = {**data_dictionary, **{'rename_keys': short_labels}}
        if self.allow_optional_classes:
            classes = all_classes
        return_items = {key: [] for key in classes}
        for label in classes:
            if return_nodeid:
                item_str = f"{{`{label}`:id(`{label.lower()}`)}}"
                return_items[label].append(item_str)
            item_str = f'`{label.lower()}`{{.*}}'
            # item_str = f'apoc.map.clean({item_str},[],[NULL])'
            item_str = f'CASE WHEN {item_str} IS NULL THEN {{}} ELSE {item_str} END'
            if self.mode == 'schema_CLASS' and prefix_keys_with_label:
                item_str = f'''
                apoc.map.fromPairs(
                    [key in keys({item_str}) | 
                        [
                            "{label}." + CASE WHEN key = 'rdfs:label' THEN $rename_keys["{label}"] ELSE key END
                            , 
                            ({item_str})[key]
                        ]
                    ]
                )
                '''
            elif self.mode == 'schema_CLASS':
                item_str = f'''
                apoc.map.fromPairs([key in keys({item_str}) | [
                    CASE WHEN key = '{self.RDFSLABEL}' THEN $rename_keys['{label}'] ELSE key END, {item_str}[key]]])
                '''
            elif prefix_keys_with_label:
                item_str = f'apoc.map.fromPairs([key in keys({item_str}) | ["{label}." + key, {item_str}[key]]])'
            if return_properties != "*":
                submap_keys = "[" + ", ".join([f"'{key}'" for key in return_properties]) + "]"
                item_str = f'apoc.map.clean(apoc.map.submap({item_str}, {submap_keys}, NULL, False),[],[NULL])'
            return_items[label].append(item_str)

        if return_disjoint:  # Note: this is the branch followed when this function is used by _get_filters_helper()
            return "RETURN " + "\n, ".join([f"collect(distinct apoc.map.mergeList([{', '.join(item)}])) as `{label}`"
                                            for label, item in return_items.items()]), data_dictionary
        else:  # return_disjoint is False
            items = [item for key, item in return_items.items()]
            items_flat = [item for sublist in items for item in sublist]
            if self.debug:
                print(items_flat)
            return "RETURN apoc.map.mergeList([" + '\n, '.join(items_flat) + "]) as all", data_dictionary

    # -----------------------------------------------------------
    # ------------- Methods for mode == 'schema_PROPERTY' ----------------
    # -----------------------------------------------------------
    def qb_list_data_relationships_per_schema(self,
                                              classes: list,
                                              classes_in_pair=None,
                                              opt_rel_postix=None,
                                              allow_unrelated_subgraphs=False):
        """
        Adapted from DataProvider._generate_match_relationships_per_schema()

        From the given list of Classes, and the "CLASS_RELATES_TO" relationships among them in the database,


        It looks for `Class`-labeled nodes with "label" attributes having the values in the passed "classes" list,
        and for "CLASS_RELATES_TO" relationships among them...
        then it creates and returns a list of strings meant for later use in a Cypher query.

        EXAMPLE:    if there are 2 `Class`-labeled nodes,
                    one with a "label" attribute whose value is "car", and one "vehicle",
                    as well as a "CLASS_RELATES_TO" relationship from the car to the vehicle node...
                    then it returns the list
                            ['(`car`)-[:`HAS_VEHICLE`]->(`vehicle`)',
                             '(`car`)',
                             '(`vehicle`)']

        :param classes:         List of strings.  EXAMPLE: ['Study', 'Site', 'Subject']
        :param classes_in_pair:   None or list of strings. If the parameter is set then the specified classes must be
        present in each pair of classes in (a:Class)-[r:CLASS_RELATES_TO]->(b:Class) relationship
        :param opt_rel_postix:  Optional postfix to be added to relationship type name
        :param allow_unrelated_subgraphs:
        :return:                A (possibly empty) list of strings (meant to be later used to form a Cypher query)
        """
        relpostfix = "+ '`|`' + 'HAS_' + toUpper(coll[1]) + f'_{opt_rel_postix}'" if opt_rel_postix else ""
        # e.g. opt_rel_postix = 'POOLED' is used for Data Displays metadata

        if not allow_unrelated_subgraphs:
            q_unrel = """
            MATCH (c1:Class), (c2:Class)
            WHERE 
                c1.label in $labels and c2.label in $labels
            AND 
                id(c1)>id(c2) 
            WITH collect([c1,c2]) AS coll UNWIND coll AS item
            WITH coll, item[0] as c1, item[1] as c2
            CALL apoc.path.expandConfig(
                c1, 
                {
                    uniqueness:'RELATIONSHIP_GLOBAL', 
                    relationshipFilter:'CLASS_RELATES_TO|<SUBCLASS_OF', //TODO: to check if SUBCLASS_OF is allowed in both directions
                    labelFilter:'+Class', 
                    terminatorNodes:[c2], 
                    minLevel:1, 
                    maxLevel:50
                }
            ) YIELD path
            WITH coll, c1, c2, collect(path) AS coll_paths
            WHERE size(coll_paths)>0
            WITH coll, collect([c1,c2]) AS coll2
            WITH apoc.coll.subtract(coll,coll2) AS sub UNWIND sub AS unrelated
            RETURN unrelated[0]['label'] as c1, unrelated[1]['label'] as c2
            """
            param_unrel = {"labels": list(classes)}
            res_unrel = self.query(q_unrel, param_unrel)
            assert len(res_unrel) == 0, f"Provided classes are not all related: {classes}"

        # generating the match relationship part of the query (for that we need to subquery)
        ##optional part
        q_classes_in_pair = ("AND (a.label in $rightLabels or b.label in $rightLabels)" if classes_in_pair else "")
        dict_classes_in_pair = ({"rightLabels": classes_in_pair} if classes_in_pair else {})
        ##mandatory part
        subq0 = f"""
        //extracting from schema which classes($labels provided) are related to each other
        //if a subclass of a class is related to another class in means that the parent class is also related          
        MATCH (a:Class), (b:Class)
        WHERE a.label in $labels and b.label in $labels {q_classes_in_pair}
        AND EXISTS ( (a)-[:SUBCLASS_OF*0..50]->()-[:CLASS_RELATES_TO]->()<-[:SUBCLASS_OF*0..50]-(b))
        MATCH p = (a)-[:SUBCLASS_OF*0..50]->()-[r:CLASS_RELATES_TO]->()<-[:SUBCLASS_OF*0..50]-(b)
        WITH [a.label, b.label, r.relationship_type] as coll
        ORDER BY coll[0], coll[1]
        WITH collect(coll) as collp
        RETURN collp
        """
        collp = self.query(subq0, {**{"labels": list(classes)}, **dict_classes_in_pair})[0]['collp']

        if collp:
            subq = f"""
            WITH
            reduce(
                acc=[],
                coll in $collp |
                //acc + ['(`' + $bindings[coll[0]] + '`' + ')' + 
                acc + ['(`' + toLower(coll[0]) + '`' + ')' +
                case when size(coll) = 3 then '-[:`' + 
                    CASE WHEN coll[2] IS NULL THEN ('HAS_' + toUpper(coll[1]) {relpostfix}) ELSE coll[2] END +
                    '`]->(`' + toLower(coll[1]) + '`)'
                else '' end]    
            ) as q
            // looping over the list of lists and accumulating the query step by step 
            // WARNING! it is possible that it actually needs to loop over Cartesian product as this way not all schema relationship may be captured
            return q
            """
            # return self.query(subq, {"collp": collp, "bindings": self.bindings})[0]['q']
            return self.query(subq, {"collp": collp})[0]['q']
        else:
            return []

    def qb_generate_query_body(self,
                               classes: list,
                               classes_for_rel="*",
                               where_map=None,
                               allow_unrelated_subgraphs=False) -> (str, {}):
        """
        Helper function for get_data()
        Compose and return a string suitable for the MATCH (and optionally the WHERE) part of a Cypher string - exclusive
        of the RETURN statement.
        Used when we have a schema with Classes and Properties.

        EXAMPLE:  2 `Class` nodes ['Study', 'Site'] and a "CLASS_RELATES_TO" relationship between them,
                  will result in the following string:
                  "MATCH (`site`:`Site`), (`study`:`Study`), (`site`), (`study`)-[:`HAS_SITE`]->(`site`), (`study`)"

                  NOTE about the redundant part about having (`site`:`Site`) as well as (`site`):
                       "was just easier to implement it this way and it does not impact the result. Can be updated in the future"

        :param classes:     List of strings.  EXAMPLE: ['Study', 'Site']
        :param classes_for_rel: '*' or list of strings. if '*' then classes_for_rel considered to be equal to classes.
        classes_for_rel are used for generation of relationships via qb_list_data_relationships_per_schema.
        typical use-case with classes_for_rel != '*' - for generation of OPTIONAL MATCH chunks of query
        :param where_map:   A dictionary of dictionaries.  SEE explanation in list_where_conditions_per_dict()
        :return:            A pair consisting of a Cypher string and a data dictionary to run that Cypher query
        """
        assert (classes_for_rel == "*") or (type(classes_for_rel) == list)
        if classes_for_rel == "*":
            classes_for_rel = classes

        match_node_list = self.list_data_labels(classes)
        # EXAMPLE: ['(`study`:`Study`)', '(`site`:`Site`)', '(`subject`:`Subject`)']

        match_relationships_per_schema = \
            self.qb_list_data_relationships_per_schema(
                classes=classes_for_rel,
                # the set of classes for relationships is usually larger the one for matching nodes in the OPTIONAL MATCH statement folloing MATCH statement
                classes_in_pair=classes,
                # one of these classes must be present in each pair of CLASS_RELATES_TO to relationship
                allow_unrelated_subgraphs=allow_unrelated_subgraphs
            )
        # EXAMPLE:  ['(`site`)-[:`HAS_SUBJECT`]->(`subject`)',
        #            '(`site`)',
        #            '(`study`)-[:`HAS_SITE`]->(`site`)',
        #            '(`study`)-[:`HAS_SUBJECT`]->(`subject`)',
        #            '(`study`)',
        #            '(`subject`)']

        wh = ""
        data_dictionary = {}
        if where_map:
            (list_of_Cypher_strings, data_dictionary) = self.list_where_conditions_per_dict(where_map)
            wh = f"WHERE {' AND '.join(list_of_Cypher_strings)}"

        q = "MATCH " + "\n,".join(match_node_list + match_relationships_per_schema) + " \n" + wh
        return (q, data_dictionary)

    # -----------------------------------------------------------
    # ------------- Methods for mode == 'noschema' --------------
    # -----------------------------------------------------------
    def qbns_get_label_relationships_df(self, classes: list):
        """
        :param classes: list of labels to process
        :return: pd.DataFrame(columns=['souce_lbl', 'rel', 'target_lbl']) with all existing relationships between
        nodes having labels provided in classes list
        """
        # TODO: assess risk that sample = 100000 is not enough
        q = """
        CALL apoc.meta.data({includeLabels:$classes, sample:100000}) 
        YIELD label as souce_lbl, type, elementType, property as rel, other    
        WHERE elementType = "node" and type = "RELATIONSHIP" 
        UNWIND other as target_lbl
        WITH souce_lbl, rel, target_lbl
        WHERE target_lbl in $classes and souce_lbl in $classes
        RETURN *
        """
        params = {'classes': classes}
        if self.debug:
            print("        Query : ", q)
            print("        Query parameters: ", params)
        return pd.DataFrame(self.query(q, params))

    def qbns_list_data_relationships(self, classes: list, allow_unrelated_subgraphs=False):
        assert allow_unrelated_subgraphs == False  # TODO implement for allow_unrelated_subgraphs==False
        # for label in classes:
        #     self.append_binding(label)
        res = []
        for i, row in self.qbns_get_label_relationships_df(classes).iterrows():
            # res.append(f"(`{self.bindings[row['souce_lbl']]}`:`{row['souce_lbl']}`)-"
            #            f"[:`{row['rel']}`]->(`{self.bindings[row['target_lbl']]}`:`{row['target_lbl']}`)")
            res.append(f"(`{row['souce_lbl'].lower()}`:`{row['souce_lbl']}`)-"
                       f"[:`{row['rel']}`]->(`{row['target_lbl'].lower()}`:`{row['target_lbl']}`)")
        return res

    def qbns_generate_query_body(self,
                                 classes: list,
                                 classes_for_rel='*',
                                 where_map=None,
                                 allow_unrelated_subgraphs=False):
        assert classes_for_rel == "*" or type(classes_for_rel) == list
        if classes_for_rel == "*":
            classes_for_rel = classes

        wh = ""
        data_dictionary = {}
        if where_map:
            (list_of_Cypher_strings, data_dictionary) = self.list_where_conditions_per_dict(where_map)
            wh = f"WHERE {' AND '.join(list_of_Cypher_strings)}"

        match_node_list = self.list_data_labels(classes)
        # EXAMPLE: ['(`study`:`Study`)', '(`site`:`Site`)', '(`subject`:`Subject`)']

        match_relationships = self.qbns_list_data_relationships(classes_for_rel)
        # EXAMPLE:  ['(`site`)-[:`HAS_SUBJECT`]->(`subject`)',
        #            '(`study`)-[:`HAS_SITE`]->(`site`)']

        q = "MATCH " + "\n,".join(match_node_list + match_relationships) + " \n" + wh

        wh = ""
        data_dictionary = {}
        if where_map:
            (list_of_Cypher_strings, data_dictionary) = self.list_where_conditions_per_dict(where_map)
            wh = f"WHERE {' AND '.join(list_of_Cypher_strings)}"

        return (q, data_dictionary)
