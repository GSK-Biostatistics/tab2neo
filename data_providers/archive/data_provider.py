import re
import pandas as pd
from neointerface import NeoInterface
from query_builders.query_builder import QueryBuilder


class DataProvider(NeoInterface):
    """
    To use the data already in the database (such as the data after the transformations with ModelApplier),
    for various purposes - such as to feed the User Interface.

    Also for future data transformation, data enrichment, etc.
    """

    def __init__(self, mode='schema_PROPERTY', allow_optional_classes=True, *args, **kwargs):
        """
        :param mode:                if 'schema_PROPERTY': determines matching based on Class-Property schema \
                                    if 'schema_CLASS' - determines matching based on Class schema
                                    (where columns are mapped to Classes and no Property nodes defined)
                                    if 'noschema': determines matching based on all relationships in the database
                                    if 'noschema': NOTE: all the interrelationships should consistently exits for all classes
        :param allow_optional_classes   If True then classes in the list with postfix ** will be considered optional
        (OPTIONAL MATCH will be used to fetch data for those classes - see example test_get_data_noschema_optional_class )
        :param args:
        :param kwargs:
        """
        assert mode in ['schema_PROPERTY', 'schema_CLASS', 'noschema']
        self._mode = mode
        self.qb = QueryBuilder(mode=self._mode, allow_optional_classes=allow_optional_classes, *args, **kwargs)
        super().__init__(*args, **kwargs)
        if self.verbose:
            print(f"---------------- {self.__class__} initialized -------------------")

    @property
    def mode(self):
        return self._mode

    def set_mode(self, mode:str):
        assert mode in ['schema_PROPERTY', 'schema_CLASS', 'noschema']
        self._mode = mode
        self.qb.mode = mode

    #############################################################
    #                    DATA RETRIEVAL                         #
    #############################################################

    def get_data(self, classes: list, where_map=None, limit=20, return_q_and_dict=False):
        """
        Assembles into a Pandas dataframe the Property values from the data nodes with the specified Classes.
        Simplified version of get_data_generic()

        :param classes:         List of strings with Class names.  EXAMPLE: ['Study', 'Site', 'Subject']
        :param where_map:       Used to restrict the data (by default, no restriction.)
                                A dictionary whose keys are Classes to apply restrictions to,
                                    and whose values are dictionaries specifying conditions on Property values.
                                An implicit AND is understood among all the clauses.
                                EXAMPLE:
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
                                Notes:
                                    - The keys in the outer dictionary are expected to be node labels.
                                    - The (key/values) in the inner dictionary entries
                                      are meant to be (attribute names : desired values)
                                      that are applicable to the node with their corresponding label.
                                    - The values may be various data type, incl. strings, integers and lists.

        :param limit:           Either None or an integer.  If specified, it restricts the maximum number of rows
                                        in the returned dataframe.  Defaults to 20

        :return: pd.DataFrame   A Pandas dataframe containing all the (direct and indirect) Property values
                                of the data points from the requested Classes, plus their Neo4j IDs.
                                The Property names are prefixed with the Class names
        """
        return self.get_data_generic(
            classes=classes,
            where_map=where_map,
            allow_unrelated_subgraphs=False,
            return_nodeid=True,
            return_properties="*",
            prefix_keys_with_label=True,
            order=None,
            limit=limit,
            return_q_and_dict=return_q_and_dict
        )

    def get_data_generic(self,
                         classes: list,
                         where_map=None,
                         allow_unrelated_subgraphs: bool = False,
                         return_nodeid: bool = True,
                         return_properties: str = "*",
                         prefix_keys_with_label: bool = True,
                         order=None,
                         limit=20,
                         return_q_and_dict=False):
        """
        Assembles into a Pandas dataframe the Property values from the data nodes with the specified Classes.
        Use this instead of get_data() when you need a full set of options.

        Used when we have a schema with Classes and Properties (in self.mode == 'schema_PROPERTY')
        or on graph data as is (if self.mode == 'noschema')

        TODO: it cannot handle different classes with the same property name - it may produce wrong results;
              use prefix_keys_with_label = True in this case

        :param classes:             List of strings.  EXAMPLE: ['Study', 'Site', 'Subject']
        :param where_map:           Used to restrict the data (default, no restriction.)
                                    A dictionary of dictionaries.  SEE explanation in get_data()
        :param return_nodeid:       Boolean
        :param return_properties:   Either a list of Property names to include in the dataframe, or the string "*" (meaning, "all")
                                            EXAMPLE: ["STUDYID", "SITEID"]
        :param prefix_keys_with_label: If True adds a prefix (corresponding node label) to each column name of the returned dataframe
        :param order:               Either None (default) or a list or a string
        :param limit:               Either None or an integer.  Defaults to 20
        :return: pd.DataFrame(): nodes in path, merged with apoc.map.mergeList and converted to pd.DataFrame()
        """
        if self.verbose:
            print("-- Getting data --")
        if self.debug:
            print("classes: ", classes)
            print("where_map: ", where_map)
        assert order is None or type(order) == list or type(order) == str
        assert limit is None or type(limit) == int

        (q, data_dict) = self._get_data_generic_helper(classes=classes, where_map=where_map,
                                                       allow_unrelated_subgraphs=allow_unrelated_subgraphs,
                                                       return_nodeid=return_nodeid,
                                                       return_properties=return_properties,
                                                       prefix_keys_with_label=prefix_keys_with_label,
                                                       order=order, limit=limit)

        if self.debug:
            print("q : ", q, "\n | data_dict : ", data_dict)

        df = self.convert_qb_result_to_df(
            self.query(q, data_dict)
        )
        if self._mode in ['schema_PROPERTY', 'schema_CLASS']:
            # Re-ordering columns (in the order of classes provided in the query)
            expected_cols = []
            for class_ in classes:
                class_pure = class_
                if self.qb.allow_optional_classes and class_.endswith(self.qb.OCLASS_MARKER):
                    class_pure = class_[:(len(class_) - len(self.qb.OCLASS_MARKER))]
                for cols in self.qb.mm.get_class_properties(class_pure).values():
                    for col in cols:
                        col_name = (class_pure + "." if prefix_keys_with_label else "") + col
                        if not col_name in expected_cols:
                            expected_cols.append(col_name)
                if return_nodeid:
                    expected_cols.append(class_pure)
            if self._mode == "schema_CLASS":
                all_classes = [(class_[:-len(self.qb.OCLASS_MARKER)] if class_.endswith(self.qb.OCLASS_MARKER) else class_)
                               for class_ in classes]
                short_labels = self.qb.mm.get_class_short_labels(classes=all_classes)
                if prefix_keys_with_label:
                    expected_cols += [f"{label}.{short_label}" for label, short_label in short_labels.items() if
                                      not f"{label}.{short_label}" in expected_cols]
                else:
                    expected_cols += [short_label for short_label in short_labels.values() if not short_label in expected_cols]

            df = df[[col for col in expected_cols if col in df.columns]]
        if return_q_and_dict:
            return df, q, data_dict
        return df

    def _get_data_generic_helper(self,
                                 classes: list,
                                 where_map=None,
                                 allow_unrelated_subgraphs: bool = False,
                                 return_nodeid: bool = True,
                                 return_properties: str = "*",
                                 prefix_keys_with_label: bool = True,
                                 order=None,
                                 limit=20):
        """
        Helper function, to produce the Cypher query and data binding for get_data_generic().

        It's implemented with a Cypher query whose MAIN BODY originates from qb.generate_query_body(),
                                         and whose RETURN statement originates from qb.generate_return()

        Parameters - Same as for get_data_generic()
        :return:     The pair (Cypher query string, Cypher data binding dictionary)
        """
        if type(classes) == str:
            classes = [classes]
        if not where_map:
            where_map = {}
        (cypher, data_dict) = self.qb.generate_query_body(classes, where_map, allow_unrelated_subgraphs)

        if self.debug:
            print("In _get_data_generic_helper() :")
            print("    cypher: ", cypher)
            print("    data_dict: ", data_dict)
            print("    where_map: ", where_map)

        (q_return, data_dict2) = self.qb.generate_return(
            labels=classes,
            return_disjoint=False,
            return_nodeid=return_nodeid,
            return_properties=return_properties,
            prefix_keys_with_label=prefix_keys_with_label,
        )
        if order:
            q_return = re.sub(r'^RETURN', 'WITH', q_return)
            q = " ".join(
                [cypher,
                 q_return,
                 ] +
                ([f"ORDER BY {', '.join([f'all.`{o}`' for o in order] if type(order) == list else [f'all.`{order}`'])}"] if order else []) +
                ([f"LIMIT {str(limit)}"] if limit else []) +
                ["RETURN *"]
            )
        else:
            q = " ".join(
                [cypher,
                 q_return
                 ] +
                ([f"LIMIT {str(limit)}"] if limit else [])
            )

        # Extend the Cypher data dictionary, to also include a value for the key "labels"
        data_dict["labels"] = list(classes)
        return (q, {**data_dict, **data_dict2})

    #############################################################
    #                           FILTERS                         #
    #############################################################

    def get_filters(self, classes: list, where_map=None, return_q_and_dict=False) -> dict:
        """
        Useful to build a dropdown menu in a UI.
        Simplified version of get_filters_generic()

        :param classes:             A string or list of strings with Class names.  EXAMPLE: ['Study', 'Site', 'Subject']
        :param where_map:           See explanation in get_data()
        :param return_q_and_dict:   Optionally return query and data_dict
        :return:                    A (possibly empty) Python dictionary.  For example, see get_filters_generic()
        """
        return self.get_filters_generic(
            classes=classes,
            where_map=where_map,
            allow_unrelated_subgraphs=False,
            return_nodeid=True,
            return_properties="*",
            prefix_keys_with_label=True,
            return_q_and_dict=return_q_and_dict,
        )

    def get_filters_generic(self,
                            classes,
                            where_map=None,
                            allow_unrelated_subgraphs=False,
                            return_nodeid=True,
                            return_properties="*",
                            prefix_keys_with_label=True,
                            return_q_and_dict=False):
        """
        Useful to build a dropdown menu in a UI

        :param classes:             A string or list of strings with Class names.  EXAMPLE: ['Study', 'Site', 'Subject']
        :param where_map:           See explanation in get_data()
        :param return_nodeid:       Boolean
        :param return_properties:   Either a list of Property names to include in the dataframe, or the string "*" (meaning, "all")
                                            EXAMPLE: ["STUDYID", "SITEID"]
        :param return_q_and_dict:   Optionally return query and data_dict
        :param prefix_keys_with_label: If True adds a prefix (corresponding node label) to each column name of the returned data
        :return:                    A (possibly empty) Python dictionary
                                    EXAMPLE:
                                    {
                                        Label1: pd.DataFrame(
                                            [
                                                {prop1: value11, prop2: value21},
                                                {prop2: value21, prop2: value22},
                                                ...
                                            ]
                                        ),
                                        Label3: pd.DataFrame(
                                            [
                                                {prop3: value11},
                                                {prop3: value21},
                                                ...
                                            ]
                                        )
                                    }
        """
        results, q, data_dict = self._get_filters_helper(classes,
                                                         where_map=where_map,
                                                         allow_unrelated_subgraphs=allow_unrelated_subgraphs,
                                                         return_nodeid=return_nodeid,
                                                         return_properties=return_properties,
                                                         prefix_keys_with_label=prefix_keys_with_label)

        if return_q_and_dict:
            return {k: pd.DataFrame(i) for k, i in results.items()}, q, data_dict
        return {k: pd.DataFrame(i) for k, i in results.items()}

    def _get_filters_helper(self,
                            classes: list,
                            where_map=None,
                            allow_unrelated_subgraphs=False,
                            return_nodeid=True,
                            return_properties="*",
                            prefix_keys_with_label=True,
                            ):
        """
        Helper function for get_filters_generic()

        Look for nodes whose relationships among them conform to the "CLASS_RELATES_TO"
        relationships among `Class`-labeled nodes with "label" attributes matching the given classes.
        EXAMPLE: if classes consists of ["apple", "fruit"] then look for `apple` labeled nodes and
        `fruit` labeled nodes, with a "HAS_FRUIT" relationship between them.

        :param classes:             A string or list of strings.  EXAMPLE: ['Study', 'Site', 'Subject']
        :param where_map:           Used to restrict the data (default, no restriction.)
                                    A dictionary of dictionaries.  SEE explanation in qb_list_where_conditions_per_dict()
        :param return_nodeid:       Boolean
        :param return_properties:   Either a list of Property names to include in the dataframe, or the string "*" (meaning, "all")
                                            EXAMPLE: ["STUDYID", "SITEID"]
        :param prefix_keys_with_label: If True adds a prefix (corresponding node label) to each column name of the returned data
        """
        if self.verbose:
            print("-- Getting filters --")
        if type(classes) == str:
            # Intercept empty strings because they lead to faulty Cypher such as:  MATCH (``:``) RETURN collect(distinct ``{.*}) as ``
            assert classes.strip() != "", "ERROR in _get_filters_helper(): the argument `classes` cannot be an empty string"
            classes = [classes]

        if not where_map:
            where_map = {}

        if self.debug:
            print("classes : ", classes)
            print("where_map : ", where_map)

        (cypher, data_dict) = self.qb.generate_query_body(classes, where_map, allow_unrelated_subgraphs)
        (return_cypher, data_dict2) = self.qb.generate_return(
            labels=classes,
            return_disjoint=True,
            return_nodeid=return_nodeid,
            return_properties=return_properties,
            prefix_keys_with_label=prefix_keys_with_label,
        )
        q = " ".join([cypher, return_cypher])
        if self.debug:
            print("Inside _get_filters_helper(). q : ", q, " | data_dict : ", data_dict)

        # Extend the Cypher data dictionary, to also include a value for the key "labels"
        data_dict["labels"] = list(classes)

        query_result = self.qb.query(q, {**data_dict, **data_dict2})
        try:
            results = query_result[0]
        except KeyError:
            results = {}

        return results, q, data_dict

    #############################################################
    #                   PANDAS SUPPORT                          #
    #############################################################

    def convert_qb_result_to_df(self, neoresult, hstack=True):  # TODO: add tests
        """

        :param neoresult:
        :param hstack:  If hstack == True returns 1 dataframe (properties of all returned neo4j variables are concatenated horizontally)
                        if hstack == False returns a dictionary of dataframes (1 per returned unit)

        :return:        Either a Pandas dataframe or a dictionary of dataframes
        """

        dct_of_lists = {}
        keys = []
        for i, res in enumerate(neoresult):
            if i == 0:
                keys = res.keys()
            for prop in keys:
                if i == 0:
                    dct_of_lists[prop] = []
                dct_of_lists[prop].append(res[prop])
        if hstack:
            if len(dct_of_lists) > 0:
                return pd.concat([pd.DataFrame(lst) for lst in dct_of_lists.values()], axis=1)
            else:
                return pd.DataFrame()
        else:
            return {k: pd.DataFrame(dct_of_lists[k]) for k in keys}
