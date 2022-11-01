from collections import OrderedDict
from model_managers import ModelManager
import pandas as pd


def get_tag_label(
        label  # str or dict (if dict then format: {'short_label':<short_label>, 'label':<label>}}
):
    assert len(label) > 0
    assert isinstance(label, str) or isinstance(label, dict)
    if isinstance(label, str):
        short_label = label
    else:
        assert isinstance(label.get('label'), str) and len(label.get('label')) > 0
        if not label.get('short_label'):
            short_label = label
        else:
            assert isinstance(label.get('short_label'), str) and len(label.get('short_label')) > 0
            short_label = label['short_label']
        label = label['label']
    return short_label, label


class QueryBuilder():
    """
    QueryBuilder generates query strings
    QueryBuilder is ignorant about the database schema or existance of labels/relationships in the database
    """

    def __init__(self):
        pass

    @staticmethod
    def generate_1match(
            label  # str or dict (if dict then format: {'short_label':<short_label>, 'label':<label>}}
    ):
        short_label, label = get_tag_label(label)
        return f"(`{short_label}`:`{label}`)"

    @staticmethod
    def generate_1match_schema_check(
            self,
            label: str  # str or dict (if dict then format: {'short_label':<short_label>, 'label':<label>}}
    ):
        short_label, label = self._get_tag_label(label)
        return f"(`{short_label}`:`Class`{{label:'{label}'}})"

    @staticmethod
    def generate_1rel(
            rel: dict  # {'from':<label1>, 'to':<label2>, 'type':<type>[, 'type_tag':<type_tag>]}
    ):
        for key in ['from', 'to']:
            assert isinstance(rel.get(key), str) and len(rel.get(key)) > 0
        type_ = rel.get('type')
        if type_ is None:
            type_ = ''
        type_ext = f':`{type_}`' if type_ else ''
        type_tag = rel.get('type_tag')
        if type_tag is None:
            type_tag = rel.get('from') + '_' + type_ + '_' + rel.get('to')
        else:
            assert isinstance(rel.get('type_tag'), str) and len(rel.get('type_tag')) > 0
        res = f"(`{rel['from']}`)-[`{type_tag}`{type_ext}]->(`{rel['to']}`)"
        return res

    @staticmethod
    def generate_1rel_schema_check(
            rel: dict,  # {'from':<label1>, 'to':<label2>, 'type':<type>[, 'from_tag':str, 'to_tag':str]}
            subclass_dir=None,
            subclass_depth=50
    ):
        tag_dict = {}
        for key in ['from', 'to']:
            assert isinstance(rel.get(key), str) and len(rel.get(key)) > 0
            tag_dict[key] = rel.get(f'{key}_tag')  # from_tag or to_tag
            if tag_dict[key] is None:
                tag_dict[key] = rel.get(key)
        type_ = rel.get('type')
        if type_ is None:
            type_ = ''
        type_tag = rel.get('type_tag')
        if type_tag is None:
            type_tag = tag_dict['from'] + '_' + type_ + '_' + tag_dict['to']
        if subclass_dir == ">":
            subclass_clause1 = f"<-[:SUBCLASS_OF*0..{str(subclass_depth)}]-()"
            subclass_clause2 = f"()-[:SUBCLASS_OF*0..{str(subclass_depth)}]->"
        elif subclass_dir == "<":
            subclass_clause1 = f"-[:SUBCLASS_OF*0..{str(subclass_depth)}]->()"
            subclass_clause2 = f"()<-[:SUBCLASS_OF*0..{str(subclass_depth)}]-"
        else:
            subclass_clause1, subclass_clause2 = "", ""
        q = f"(`{tag_dict['from']}`){subclass_clause1}<-[:FROM]-(`{type_tag}`:Relationship)" \
            "-[:TO]->{subclass_clause2}(`{tag_dict['to']}`)"
        where_map = {}
        if rel.get('type'):
            where_map = {type_tag: {'type': rel.get('type')}}
        return q, where_map

    @staticmethod
    def list_where_conditions_per_dict(mp: dict) -> ([], {}):
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

        def parameter_labels():  # Generates sequential integers used in the data dictionary, such as "par_1", "par_2", etc.
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
                    incl_null = property_value.get("incl_null")  # if None then the clause is not included at all

                    t1, t2 = next(parameter_token_stack), next(parameter_token_stack)
                    data_dictionary[t1], data_dictionary[t2] = property_value.get("min"), property_value.get("max")
                    min_max_subclause_list = []
                    if data_dictionary[t1]:
                        min_max_subclause_list.append(f"${t1} {'<=' if min_include else '<'}")
                    if data_dictionary[t1] or data_dictionary[t2]:
                        min_max_subclause_list.append(f"`{label}`.`{property_name}`")
                    if data_dictionary[t2]:
                        min_max_subclause_list.append(f"{'<=' if max_include else '<'} ${t2}")

                    if incl_null is None:
                        include_nulls_list = []
                    elif incl_null:
                        include_nulls_list = [f"`{label}`.`{property_name}` IS NULL"]
                        if min_max_subclause_list:
                            include_nulls_list.append("OR")
                    else:
                        include_nulls_list = [f"`{label}`.`{property_name}` IS NOT NULL"]
                        if min_max_subclause_list:
                            include_nulls_list.append("AND")

                    full_str = " ".join(include_nulls_list + (
                        ['(' + " ".join(min_max_subclause_list) + ')'] if min_max_subclause_list else []))
                    if include_nulls_list and min_max_subclause_list:
                        full_str = f"({full_str})"
                    if full_str:
                        cypher_list.append(full_str)
                else:
                    # Handle NOT, get the key value (typically a Term) and prepend !
                    if isinstance(property_value, dict) and set(property_value.keys()).intersection({"not_in"}):
                        property_value = property_value.get("not_in")
                        label = "!" + label

                    # Handle multiple NOT IN's
                    elif isinstance(property_value, list) and all(isinstance(item, dict) for item in property_value):
                        # list of dictionaries e.g. {'rdfs:label': [{'not_in': 'DS'}, {'not_in': 'LB'}]}
                        property_value = [dct.get('not_in') for dct in property_value]
                        label = "!" + label

                    # For list inclusions, use "in"; in all other case check for equality
                    operator = ("in" if type(property_value) == list else "=")

                    # Extend the list of Cypher strings and their corresponding data dictionary
                    t = next(parameter_token_stack)
                    if label.startswith("!"):  # ! is NOT operator
                        cypher_list.append(f"""NOT (`{label[1:]}`.`{property_name}` {operator} ${t})""")  # The $ refers to the data binding
                    else:
                        cypher_list.append(f"`{label}`.`{property_name}` {operator} ${t}")

                    data_dictionary[t] = property_value

        return cypher_list, data_dictionary

    @staticmethod
    def list_where_rel_conditions_per_dict(mp: dict) -> ([], {}):
        # test_map = {
        #     'nobs': {
        #         'EXISTS': ['Ser', 'Pop', 'Asta'],
        #         'NOT EXISTS': {'exclude': ['Ser', 'Pop', 'Asta']}
        #     }
        # }
        cypher_list = []
        for label, check_dct in mp.items():
            for check, content in check_dct.items():
                assert check in ['EXISTS', 'NOT EXISTS', 'EXISTS>', 'NOT EXISTS>', 'EXISTS<', 'NOT EXISTS<']
                check_ = (check[:-1] if check[-1] in ['>', '<'] else check)
                leftdir = (check[-1] if check[-1] in ['<'] else '')
                rightdir = (check[-1] if check[-1] in ['>'] else '')
                inner_cond_list = []
                assert isinstance(content, dict)
                for operator_, lst in content.items():
                    assert operator_ in ['include', 'exclude', 'include_matched', 'exclude_matched']
                    lst_matched_str = "[" + ", ".join(['`' + item + '`' for item in lst]) + "]"
                    lst_str = "(" + " OR ".join(['x:`' + item + '`' for item in lst]) + ")"
                    if operator_ == 'include_matched':
                        inner_cond_list.append(f"x in {lst_matched_str}")
                    elif operator_ == 'exclude_matched':
                        inner_cond_list.append(f"NOT (x in {lst_matched_str})")
                    elif operator_ == 'include':
                        inner_cond_list.append(lst_str)
                    elif operator_ == 'exclude':
                        inner_cond_list.append(f"NOT {lst_str}")
                inner_cond = f"WHERE {' AND '.join(inner_cond_list)}"
                cypher_list.append(f"{check_} {{MATCH (`{label}`){leftdir}-[]-{rightdir}(x) {inner_cond}}}")
        return cypher_list, {}

    @staticmethod
    def check_connectedness(labels: list, rels: list):

        rels_ = rels.copy()
        if (len(labels) <= 1 and not rels_):
            return True
        elif labels and rels_:
            # check to see if all labels are present in rels' to/from classes
            all_rel_labels = set()
            for rel in rels_:
                all_rel_labels.update({rel.get('to'), rel.get('from')})
            for label in labels:
                if label not in all_rel_labels:
                    return False

        if rels_:
            processed_rels = []
            processed_rels.append(rels_.pop(0))
            processed_labels = {processed_rels[0].get('from'), processed_rels[0].get('to')}
            diff = 1
            while diff > 0 and rels_:
                for i, rel in enumerate(rels_):
                    if rel['from'] in processed_labels or rel['to'] in processed_labels:
                        processed_labels.update({rel['from'], rel['to']})
                        processed_rels.append(rels_.pop(i))
                        diff = 1
                        break
                    else:
                        diff = 0
            if not rels_:
                return True
        else:
            return False

    @staticmethod
    def enrich_labels_from_rels(labels: list, rels: list, oclass_marker: str):
        if rels:
            labels_from_rels = {}
            all_optional_bool = all(rel.get('optional', False) for rel in rels)
            for rel in rels:
                for key in ['from', 'to']:
                    if labels_from_rels.get(rel.get(key)) is None:
                        # if the label hasn't been found already
                        cur_opt = 2
                    elif labels_from_rels.get(rel.get(key)):
                        # the label has been found and the cur_opt is != 0
                        cur_opt = 1
                    else:
                        # the label has been found and the cur_opt is == 0
                        cur_opt = 0
                    if all_optional_bool:
                        # if all the relationships are optional, we need to have something to MATCH on normally.
                        # So we take all the 'left' or 'from' classes to be normal MATCHes.
                        # If those classes also feature in optional relationships on the 'right' or 'to' side,
                        # they become OPTIONAL MATCHes as well.
                        rel_opt = (1 if rel.get('optional') and key == 'to' else 0)
                    else:
                        rel_opt = 1 if rel.get('optional') else 0
                    labels_from_rels[rel.get(key)] = min([cur_opt, rel_opt])
            new_labels = []
            for label in labels + list(labels_from_rels.keys()):
                if labels_from_rels.get(label) == 1:
                    cur_label = label + oclass_marker
                else:
                    cur_label = label

                if label not in new_labels and label + oclass_marker not in new_labels:
                    # neither label or optional label is in the new labels, so add it
                    # we use `label + oclass_marker` here not `cur_label` as when a label has come round a second time,
                    # it might not be optional, but we still need to check to see if it already has been selected AS
                    # optional on a previous iteration
                    new_labels.append(cur_label)
                elif label in new_labels and cur_label not in new_labels:
                    # label is there, but optional is not (And we want to have an optional label),so replace label with
                    # optional label in new_labels
                    new_labels = list(map(lambda _label: _label.replace(label, cur_label), new_labels))
            return new_labels
        else:
            return labels

    @staticmethod
    def split_out_optional(labels: list, rels: list, oclass_marker: str) -> (list, list, list, list):
        """
        {
            0: [ #mandatory,
                {'label1': ['rel1', 'rel2']},
                {'label2': ['rel1', 'rel2']},
            ],
            1: [ #optional1
                ...
            ]
        }
        g_lookup = {'label': 'group_n'}
        """

        df_l_rels = pd.DataFrame(
            [
                {
                    'label': label[:-len(oclass_marker)] if label.endswith(oclass_marker) else label,
                    'optional': label.endswith(oclass_marker),
                } for label in labels
            ]
        )
        if labels:
            mand_labels = list(df_l_rels[~(df_l_rels.get('optional'))]['label'])
            df_rels = []
            df_n_to_mand = []
            for i, row in df_l_rels.iterrows():
                l_rels = []
                for rel in rels:
                    if (row['label'] in rel.get('from')) or (row['label'] in rel.get('to')):
                        l_rels.append(rel)
                df_rels.append(l_rels)
                df_n_to_mand.append(
                    len([rel for rel in l_rels if (rel.get('from') in mand_labels or rel.get('to') in mand_labels)]))
            df_l_rels['rels'] = df_rels
            df_l_rels['n_to_mand'] = df_n_to_mand
            df_l_rels = df_l_rels.sort_values(by=['optional', 'n_to_mand'], ascending=[True, False])
            g_dict = OrderedDict({0: []})
            g_lookup = {}
            g = 0
            for i, row in df_l_rels.iterrows():
                # helper list:
                label_related_to_processed = []
                for rel in row['rels']:
                    if rel.get('from') in g_lookup.keys():
                        label_related_to_processed.append(rel.get('from'))
                    elif rel.get('to') in g_lookup.keys():
                        label_related_to_processed.append(rel.get('to'))
                # processing row:
                if not row['optional']:  # if mandatory
                    g_dict[0].append({row['label']: row['rels']})
                    g_lookup[row['label']] = 0
                elif row['n_to_mand'] == 0 and label_related_to_processed:  # no relationships to mandatory labels
                    for processed_label in label_related_to_processed:
                        cur_g = g_lookup[processed_label]
                        g_dict[cur_g].append({row['label']: row['rels']})
                        g_lookup[row['label']] = cur_g
                else:
                    g += 1
                    g_dict[g] = []
                    g_dict[g].append({row['label']: row['rels']})
                    g_lookup[row['label']] = g
            g_dict_compact = []
            _processed_labels = set({})
            for key, item in g_dict.items():
                _labels = [list(dct.keys())[0] for dct in item]
                _processed_labels.update(_labels)
                _rels = []
                for dct in item:
                    for _rel in list(dct.values())[0]:
                        if _rel.get('from') in _processed_labels and _rel.get(
                                'to') in _processed_labels and _rel not in _rels:
                            _rels.append(_rel)
                g_dict_compact.append((_labels, _rels))
            return g_dict_compact
        else:
            return [(labels, rels)]

    def generate_query_body(
            self,
            labels: list,
            rels: list,  # [{'from':<label1>, 'to':<label2>, 'type':<type>}, ...]
            match="MATCH",
            where_map=None,
            where_rel_map=None
    ):
        assert match in ["MATCH", "OPTIONAL MATCH"]
        q_match = f"{match} " + ",\n".join(
            [self.generate_1match(label=label) for label in labels] +
            [self.generate_1rel(rel) for rel in rels]
        )
        q_where = ""
        q_where_list, q_where_dict = [], {}
        q_where_rel_list, q_where_rel_dict = [], {}

        if where_map:
            q_where_list, q_where_dict = self.list_where_conditions_per_dict(mp=where_map)
        if where_rel_map:
            q_where_rel_list, q_where_rel_dict = self.list_where_rel_conditions_per_dict(mp=where_rel_map)
        if where_map or where_rel_map:
            q_where = "WHERE " + " AND ".join(q_where_list + q_where_rel_list)
        return ("\n".join([q_match, q_where]), {**q_where_dict, **q_where_rel_dict})

    # #Commented out as this does not account for SUBCLASS_OF: qb.check_connectedness and dp.check_schema are used instead
    # def generate_schema_check_query_body(
    #         self,
    #         labels: list,
    #         rels: list,  # [{'from':<label1>, 'to':<label2>, 'type':<type>}, ...]
    # ):
    #     for rel in rels:
    #         for key in ['from', 'to']:
    #             assert isinstance(rel.get(key), str) and len(rel.get(key)) > 0
    #             if rel.get(key) not in labels:
    #                 labels.append(rel.get(key))
    #
    #     q_rel_list, where_map = [], {}
    #     for rel in rels:
    #         q_rel, where_map1 = self.generate_1rel_schema_check(rel)
    #         q_rel_list.append(q_rel)
    #         where_map = {**where_map, **where_map1}
    #
    #     q_match = "MATCH " + ",\n".join(
    #         [self.generate_1match_schema_check(label=label) for label in labels] +
    #         [q for q in q_rel_list]
    #     )
    #
    #     q_where_list, q_where_dict = self.list_where_conditions_per_dict(mp=where_map)
    #     q_where = ""
    #     if q_where_list:
    #         q_where = "WHERE " + " AND ".join(q_where_list)
    #     return ("\n".join([q_match, q_where]), q_where_dict)
    @staticmethod
    def gen_id_col_name(label: str, tag: str = None):
        return '_id_' + label

    @staticmethod
    def gen_uri_col_name(label: str, tag: str = None):
        return '_uri_' + label

    @staticmethod
    def generate_call(labels: list,
                      rels: list,
                      labels_to_pack: dict,
                      only_props: list) -> str:
        if labels_to_pack is None:  # is dict do below
            return ""
        assert len(labels) > 0
        assert len(only_props) == 1, f'only_props can only be one property here. It was: {only_props}'
        assert only_props[0] == 'rdfs:label', f'only_props can only "rdfs:label" here. It was: {only_props[0]}'
        return_items = []

        for label, value in labels_to_pack.items():
            if isinstance(value, list):
                assert len(value) == 1, \
                    f'The core class passed into labels_to_pack was not of length 1. Was length: {len(value)}'
                # Need to get the rel name for the apoc.path.subgraphNodes call to filter on the relationship.
                relationship_label = None
                # print(f'RELS: {rels}')
                for rel in rels:
                    if rel.get('from') == value[0] and rel.get('to') == label:
                        relationship_label = rel.get('type').replace(f"{value[0]}_", "").replace(f"_{label}", "")
                assert relationship_label is not None, f'Did not find the required relationship for generate_call()'

                item_str = f'CALL apoc.path.subgraphNodes(`{value[0]}`, {{relationshipFilter: "{relationship_label}", optional:true, minLevel: 1, maxLevel: 1}}) YIELD node AS `{label}_coll`'
            else:
                item_str = ""
            return_items.append(item_str)

        return '\n'.join(return_items)

    @staticmethod
    def generate_with(labels: list,
                      labels_to_pack: dict,
                      only_props: list) -> str:
        """
        :param labels: list of str or dict (if dict then format: {'tag':<tag>, 'label':<label>}}
        :param labels_to_pack: list of dict containing labels (keys) and their corresponding definitions (values)
        :return:
        """
        if labels_to_pack is None:  # is dict do below
            return ""
        assert len(labels) > 0
        assert len(only_props) == 1, f'only_props can only be one property here. It was: {only_props}'
        assert only_props[0] == 'rdfs:label', f'only_props can only "rdfs:label" here. It was: {only_props[0]}'
        return_items = []
        for label in labels:
            if label in labels_to_pack.keys():
                assert isinstance(labels_to_pack[label], (str, list)), \
                    f'Value in labels_to_pack is not string or list. It was: {type(labels_to_pack[label])}'
                if isinstance(labels_to_pack[label], str):
                    # If labels_to_unpack, take the dictionary of {'PREXGR1': `<=2'} as label_coll AND the label node
                    item_str = f'''
                    apoc.map.fromPairs(collect([CASE
                        WHEN `{labels_to_pack[label]}`.`Term Code` IS NOT NULL
                        THEN `{labels_to_pack[label]}`.`Term Code`
                        ELSE `{labels_to_pack[label]}`.`Short Label`
                        END, `{label}`.`rdfs:label`])) as `{label}_coll`'''
                elif isinstance(labels_to_pack[label], list):
                    item_str = f'collect(distinct `{label}_coll`.`rdfs:label`) as `{label}_coll`'
            elif label in labels_to_pack.values():
                continue
            else:
                item_str = f'`{label}`'
            return_items.append(item_str)

        return "WITH " + '\n, '.join(return_items)

    def generate_return(self,
                        labels: list,
                        labels_to_pack: dict,
                        return_disjoint: bool = False,
                        return_nodeid: bool = False,
                        return_propname: bool = True,
                        return_termorder: bool = False,
                        return_class_uris: bool = False,
                        only_props: list = None,
                        ) -> str:
        """
        :param labels: list of str or dict (if dict then format: {'tag':<tag>, 'label':<label>}}
        :param return_nodeid:
        :return:
        """
        assert len(labels) > 0
        return_items = {}
        for label in labels:
            if labels_to_pack is not None and label in labels_to_pack.values():
                # Do not want a return statement for definitions
                # print(f'{label} skipped over for query return')
                continue
            return_items[label] = []
            assert isinstance(label, str) or isinstance(label, dict)
            if isinstance(label, str):
                assert len(label) > 0
                tag = label
            if isinstance(label, dict):
                for key in ['tag', 'label']:
                    assert isinstance(label.get(key), str)
                    assert len(label.get(key)) > 0
                tag = label['tag']
            if return_nodeid:
                id_col_name = self.gen_id_col_name(label, tag)
                item_str = f"{{`{id_col_name}`:id(`{label}`)}}"
                return_items[label].append(item_str)
            if return_termorder:
                item_str = f"""CASE 
                                    WHEN `{label}`.Order IS NULL THEN {{}} 
                                    ELSE {{`{label + 'N' if "".join(labels).isupper() else label + ' (N)'}`:`{label}`.Order}} 
                            END"""
                return_items[label].append(item_str)
            if return_class_uris:
                uri_col_name = self.gen_uri_col_name(label)
                item_str = f"CASE WHEN `{label}`.uri IS NULL THEN {{}} ELSE {{`{uri_col_name}`:`{label}`.uri}} END"
                return_items[label].append(item_str)

            if only_props:
                if labels_to_pack is not None and label in labels_to_pack.keys():
                    item_str = f'{{`{label}`: `{label}_coll`}}'
                    # print(f'ITEM STRING DEF: {item_str}')
                else:
                    item_str = f'apoc.map.submap(`{tag}`, $only_props, NULL, False)'
            else:
                item_str = f'`{tag}`{{.*}}'

            item_str = f'CASE WHEN {item_str} IS NULL THEN {{}} ELSE {item_str} END'
            if return_propname:
                plus_key_clase = ' + "." + key'
            else:
                plus_key_clase = ''

            if not (labels_to_pack is not None and label in labels_to_pack.keys() and not isinstance(labels_to_pack[label], list)):
                item_str = f'apoc.map.fromPairs([key in keys({item_str}) | ["{label}"{plus_key_clase}, {item_str}[key]]])'
            return_items[label].append(item_str)
        if return_disjoint:
            return "RETURN " + "\n, ".join([f"collect(distinct apoc.map.mergeList([{', '.join(item)}])) as `{label}`"
                                            for label, item in return_items.items()])
        else:  # return_disjoint is False
            items = [item for key, item in return_items.items()]
            items_flat = [item for sublist in items for item in sublist]
            return "RETURN apoc.map.mergeList([" + '\n, '.join(items_flat) + "]) as all"
