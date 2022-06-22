from data_providers import DataProvider
from model_managers import ModelManager
import logging
import pandas as pd


class ADaMDataProvider(DataProvider):
    """
    A wrapper-class over DataProvider to extract specifically ADaM data
    """

    RDFSLABEL = "rdfs:label"

    def __init__(self, check_for_refarctored=True, *args, **kwargs):
        self.check_for_refarctored = check_for_refarctored
        self.mm = ModelManager(*args, **kwargs)
        super().__init__(*args, **kwargs)

    def get_data_adam(self, standard: str, domain: str, required_classes_column: str, study=None, where_map=None, user_role=None):
        assert where_map is None or isinstance(where_map, dict)
        if not where_map:
            where_map = {}
        assert study is None or isinstance(study, str)
        if study:
            where_map = {**where_map, **{
                'Study': {'rdfs:label': study}}}
        # TODO: add assert statements to check prerequisites - e.g. extraction model contains all required nodes and properties

        meta = self.neo_get_meta(standard=standard, table=domain, requied_classes_column=required_classes_column)
        if self.debug:
            print("meta", meta)
        if meta:
            classes = (['Study'] if 'Study' not in meta[0]['classes'] else []) + meta[0]['classes']
            if user_role:
                classes, no_access = self.neo_validate_access(classes, user_role=user_role)
                print(
                    f"WARNING: the following classes were excluded as the user_role {user_role} access is restricted: {no_access}")

            if meta[0]['labels_to_pack']:
                labels_to_pack = meta[0]['labels_to_pack']
                if isinstance(labels_to_pack, dict):
                    assert all(True if label in classes else False for label in labels_to_pack.keys()), \
                        f'All labels in labels_to_pack must be in classes. labels_to_pack: {labels_to_pack.keys()}, classes: {classes}'
            else:
                labels_to_pack = None

            if meta[0]['req_classes']:
                classes = [(class_ + self.OCLASS_MARKER if (class_ not in meta[0]['req_classes']) and (class_ != 'Domain Abbreviation') else class_) for
                           class_ in classes]
            if self.debug:
                print(f"Getting classes: {classes}")


            assert labels_to_pack is None or isinstance(labels_to_pack, (dict, list)), f'labels_to_pack should be None, dictionary or list. It was {type(labels_to_pack)}'
            rels = meta[0]['rels'] + meta[0]['def_rels']
            for rel in rels:
                if rel.get('to') + '**' in classes:
                    rel['optional'] = 'True'

            df = self.get_data_generic(labels=classes,
                                       rels=rels,
                                       labels_to_pack=labels_to_pack,
                                       where_map=where_map,
                                       infer_rels=True,
                                       return_nodeid=False,
                                       return_propname=False,
                                       use_shortlabel=True,
                                       only_props=[self.RDFSLABEL],
                                       limit=None)

            df = self.recode_df(meta=meta, df=df)

            # renaming and re-ordering columns (according to metadata):
            meta_rename_dct = meta[0]['rename_dct']

            # remove any class rename mappings where there are term rename mappings available
            meta_rename_dct = {key: val for key, val in meta_rename_dct.items()
                               if val not in meta[0]['rename_terms'].values()}

            meta_rename_dct.update(meta[0]['rename_terms'])

            rename_dct = {}
            for key, item in meta_rename_dct.items():
                if item not in rename_dct.values():  # to avoid 2 columns with the same name
                    if key != item:
                        rename_dct[key] = item
            print(f"RENAME DICT: {rename_dct}")
            df = df.rename(rename_dct, axis=1)

            return df

    def neo_get_meta(self, standard: str, table: str, requied_classes_column: str):

        q1 = f"""
        MATCH (dt:`Data Table`{{_domain_:$table}})-[:HAS_COLUMN]->(dc:`Data Column`), (dc)-[:MAPS_TO_CLASS]->(class:Class)<-[:TO]-(rel:Relationship)-[:FROM]->(fromclass:Class)
        WHERE
        EXISTS(
        (dc)<-[:MAPS_TO_COLUMN]-(rel)-[:TO]->(class)
        )
        CALL {{
            WITH *
            OPTIONAL MATCH val_path = (class)<-[r1:MAPS_TO_CLASS]-(dc)-[:MAPS_TO_VALUE]->(term:Term)
            WHERE r1.how IS NOT NULL
            RETURN val_path, term.`Term Code` as term_1_name
        }}
        CALL {{
            WITH *
            OPTIONAL MATCH (class)<-[r2:MAPS_TO_CLASS]-(dc)-[:MAPS_TO_WHERE_VALUE]->(term_where_value:Term)
            OPTIONAL MATCH recode_path = (dc)-[r:MAP_RECODE]->(term_map_recode:Term)
            RETURN CASE WHEN length(recode_path) <> 0 IS NOT NULL
                        THEN term_map_recode.`Codelist Code`
                        ELSE term_where_value.`Term Code`
                        END as term_2_name
        }}
        OPTIONAL MATCH (dc)-[r:MAPS_TO_CLASS]->(class)
        OPTIONAL MATCH def_path = (dc)-[:MAPS_TO_WHERE]->(definition:Class)
        WITH *
        ORDER BY dt, dc.Order, dc
        RETURN
        collect(CASE WHEN length(def_path) <> 0
                THEN {{from: class.label, to: definition.label, type: "Definition", definition: class.short_label}}
                ELSE NULL END) as def_rels,
        collect({{from: fromclass.label, to: class.label, type: rel.relationship_type, short_label: class.short_label}}) as rels,
        collect(distinct definition.label) + collect(distinct class.label) as classes,
        apoc.map.fromPairs(collect(CASE WHEN length(val_path) <> 0 THEN [class.label, r.how]
                                        ELSE NULL END)) as maps_how,
        apoc.map.fromPairs(collect(CASE WHEN length(def_path) <> 0 THEN [class.label, definition.label]
                                        WHEN length(val_path) <> 0 THEN [class.label, [fromclass.label]]
                                        ELSE NULL END)) as labels_to_pack,
        apoc.coll.toSet(collect(CASE WHEN tostring(dc.`{requied_classes_column}`) <> 'NaN' THEN class.label ELSE NULL END))
            as req_classes,          
        apoc.map.fromPairs(
            [y in   
                [x in collect(distinct {{class:class, dc:dc, r:r}}) WHERE NOT x['r'] IS NULL] | //filtering for classes with existing Column MAPS_TO_CLASS relationship
                [
                    //y['class'].label + '.' + y['class'].short_label,    //to be used as key of the dict
                    y['class'].short_label,    //to be used as key of the dict
                    y['dc']._columnname_                               // to be used as value of the dict
                ]
            ]
        ) as rename_dct,
        apoc.map.fromPairs(collect([CASE WHEN term_1_name IS NOT NULL THEN term_1_name ELSE term_2_name END, dc.`_columnname_`])) as rename_terms,
        apoc.map.fromPairs(collect([dc._columnname_, dc.Order])) as order_dct,
        dt['SortOrder'] as sorting
        """

        q2 = """
        MATCH (dt:`Data Table`{_domain_:$table})-[:HAS_COLUMN]->(dc:`Data Column`)
        , (dc)-[:MAPS_TO_CLASS]->(class:Class)<-[:TO]-(rel:Relationship)-[:FROM]->(fromclass:Class)
        WHERE
        EXISTS(
        (dc)<-[:MAPS_TO_COLUMN]-()
        )
        WITH *
        MATCH
        (dc)-[r:MAPS_TO_CLASS]->(class), (dc)-[:MAPS_TO_VALUE]->(term:Term)<-[:HAS_CONTROLLED_TERM]-(class)
        WHERE r.how IS NOT NULL
        WITH collect(distinct term.`rdfs:label`) as terms, collect(distinct term.`Term Code`) as short_terms, class
        RETURN apoc.map.mergeList([apoc.map.fromPairs(collect([class.label, terms])),
                                   apoc.map.fromPairs(collect([class.short_label, short_terms]))]) as values_to_pack
        """

        q3 = """
        MATCH (dt:`Data Table`{_domain_:$table})-[:HAS_COLUMN]->(dc:`Data Column`)
        , (dc)-[:MAPS_TO_CLASS]->(class:Class)<-[:TO]-(rel:Relationship)-[:FROM]->(fromclass:Class)
        WHERE
        EXISTS(
        (dc)<-[:MAPS_TO_COLUMN]-()
        )
        WITH *
        MATCH
        (dc)-[r:MAPS_TO_CLASS]->(class), (dc)-[r_recode:MAP_RECODE]->(term:Term)<-[:HAS_CONTROLLED_TERM]-(class)
        WITH apoc.map.fromPairs(collect([term.`rdfs:label`, r_recode.to])) as recode_maps, class
        RETURN apoc.map.fromPairs(collect([class.label, recode_maps])) as recode_how
        """
        params = {'standard': standard, 'table': table}
        if self.debug:
            logging.debug(f"""
                               query: {q1}
                               parameters: {params}
                           """)
        res1 = self.query(q1, params)
        if self.debug:
            logging.debug(f"""
                               query: {q2}
                               parameters: {params}
                           """)
        res2 = self.query(q2, params)
        if self.debug:
            logging.debug(f"""
                               query: {q3}
                               parameters: {params}
                           """)
        res3 = self.query(q3, params)

        res1[0].update(res2[0])  # add values_to_pack to meta
        res1[0]['maps_how'].update(res3[0]['recode_how'])

        return res1

    def neo_validate_classes_to_extract(self, classes: list) -> ([], []):
        """
        :param classes: list of classes to validate (nodes with labels Class and property label must exist)
        :return: [], [] - list of valid classes and list if invalid classes
        Identifies non-valid classes with count==0 or no count property (at least 1 instance got extracted during reshaping)
        """
        q = """
        MATCH (n:Class) 
        WHERE n.count = 0 OR NOT EXISTS (n.count)
        RETURN DISTINCT n.label as class
        """
        pre_non_valid = [res['class'] for res in self.query(q, {'classes': classes})]
        non_valid = []
        for class_ in pre_non_valid:
            if class_ in classes:
                non_valid.append(class_)

        valid = [class_ for class_ in classes if class_ not in non_valid]
        return valid, non_valid

    def neo_validate_access(self, classes: list, user_role=None) -> ([], []):
        """
        :param classes: list of classes to validate (nodes with labels Class and property label must exist)
        :param user_role: when None no access restrictions are accounted for, otherwise the classes with
        (:`User Role`{name:$user_role})-[:RESTRICTED_ACCESS]->(:Class) would be considered invalid
        :return:
        Identifies non-valid classes to which the user_role(if provided does not have access to)
        """
        no_access = []
        if user_role:
            q = """
                MATCH (role:`User Role`{name:$user_role})
                OPTIONAL MATCH (role)-[:ACCESS_RESTRICTED]->(c:Class)
                RETURN DISTINCT role.name as role, c.label as class
                """
            pre_no_access = [res['class'] for res in self.query(q, {'user_role': user_role})]
            if pre_no_access == []:
                raise Exception(f"User Role {user_role} does not exist")
            for class_ in pre_no_access:
                if class_ in classes:
                    no_access.append(class_)
        has_access = [class_ for class_ in classes if class_ not in no_access]
        return has_access, no_access

    def recode_df(self, meta: dict, df: pd.DataFrame):

        labels_to_pack = meta[0]['labels_to_pack']
        values_to_pack = meta[0]['values_to_pack']
        maps_how = meta[0]['maps_how']

        for label, value in labels_to_pack.items():  # Find all labels (cols) that need to be recoded in some way

            assert isinstance(value, (str, list)), \
                f'Labels to pack had an unexpected value of type: {type(value)}. It must be a list or a string!'

            return_label_list, _, _, _ = self.mm.translate_to_shortlabel(labels=[label], rels=[], where_map={},
                                                                         labels_to_pack=None)
            assert len(return_label_list) == 1
            short_label = return_label_list[0].get('short_label')

            if isinstance(value, list):  # Case where list in df cell that needs to be unpacked into separate columns.
                recode_map = maps_how.get(label)
                new_columns = values_to_pack.get(label)
                short_new_columns = values_to_pack.get(short_label)

                def f1(x):
                    new_row = []
                    if recode_map == 'Convert to flag Y/N':
                        new_value_true = "Y"
                        new_value_false = "N"
                    elif recode_map in 'Convert to flag Y/Blank':
                        new_value_true = "Y"
                        new_value_false = ""
                    else:
                        new_value_true = None
                        new_value_false = None
                    assert new_value_true is not None and new_value_false is not None, \
                        f'recode map was not found for {label}'

                    for new_col in new_columns:
                        if not isinstance(x, list):  # no values to unpack, so leave new cells empty
                            return [""]*len(new_columns)
                        elif new_col in x:
                            new_row.append(new_value_true)
                        else:
                            new_row.append(new_value_false)
                    return new_row

                df[short_new_columns] = df.apply(lambda x: f1(x[short_label]), axis=1, result_type='expand')
                if short_label not in short_new_columns:  # the new column has the same name as the old col, so don't drop as it gets replaced anyway above
                    df.drop(labels=short_label, axis=1, inplace=True)  # remove the column as its unneeded now

            else:
                # It is a string. So case where dict is in the df cell.
                # Can unpack to separate cols without mapping, or the same col with mapping.
                # Depends on if that label is in maps_how.
                if label in maps_how.keys():
                    # Case with single column needs to map its values
                    # EOS group is an example of this case.
                    recode_map = maps_how.get(label)

                    def f2(x):
                        assert len(x) <= 1, f'Expected a dict of length 0 or 1, got length of: {len(x)}'
                        if len(x) == 1:
                            new_value = list(x.values())[0]
                        else:
                            new_value = ""  # no value found, dict in cell was empty
                        try:
                            return recode_map[new_value]
                        except KeyError:
                            # new value does not need recoding
                            return new_value

                    df[short_label] = df[short_label].apply(f2)

                else:  # case where values are present as dict {column_label: value} and need to be unpacked.
                    # expand dict in df cells to seperate columns, then drop the original column
                    df = df.drop(short_label, axis=1).assign(**pd.DataFrame(df[short_label].values.tolist()))
        return df
