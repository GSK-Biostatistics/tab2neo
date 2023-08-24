import json
from abc import abstractmethod
from itertools import chain, combinations
from logger.logger import logger
from model_managers import ModelManager

from derivation_method.utils import get_arrows_json_cypher, merge_dicts_on_node_keys
from derivation_method.action import AssignLabel, BranchSave, BranchLoad, CallAPI, RunScript, BuildUri, LinkStat, BranchCombine, Link, RunCypher, Action
from derivation_method.method import Method


class SuperMethod(Method):
    def __init__(self, method, interface, action_info, action_dict=None):
        super().__init__()
        self.interface = interface
        self.method = method
        self.name = method.name
        self.study = method.study
        self.action_info = action_info
        self.action_id = action_info.get("id")
        self.type = action_info.get('type')
        self.method_node_id = action_info.get("node_id")
        self.params = {"node_id": self.method_node_id, "parent_id": self.study}
        self.meta = action_dict if action_dict else self.fetch_metadata()
        self.actions = Method.retrieve_actions(self)

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def build_retrieve_actions_params(self):
        return {"method_id": self.meta.get("id"), "parent_id": self.method.study + "_" + self.method.name}

    def fetch_metadata(self):
        q = """
        MATCH (m:Method)
        WHERE id(m) = $method_node_id
        RETURN m
        """
        params = {"method_node_id": self.method_node_id}
        res = self.interface.query(q, params)[0]
        res["m"]["node_id"] = self.method_node_id
        assert res["m"], f"Method node from SuperMethod action {self.action_info} not found"
        return res["m"]

    @abstractmethod
    def retrieve_actions(self):
        pass

    def handle_supermethod(self, action_dict, previous_actions=None):
        pass

    def apply(self, df=None, **kwargs):
        logger.info(f"Performing {str(self)}, {self.action_id}")
        logger.info(f"\tTotal # of Actions: {len(self.actions)}")

        if df.empty:
            logger.info(f"\tBypassing {self.action_id}, {len(df)} records in data")
            return df

        for i, action in enumerate(self.actions):
            logger.info(f"Applying {action}: #{i + 1} of {len(self.actions)} of ({self.name})")
            if action.type == 'call_api':
                df = action.apply(df=df, **kwargs)
            else:
                df = action.apply(df=df)
        return df

    def rollback(self, detached_nodes_dct):
        logger.info(f"\tTotal # of Actions: {len(self.actions)}")
        for i, action in enumerate(self.actions):
            logger.info(f"Rolling back Action: #{i + 1} of {len(self.actions)}")
            detached_nodes_dct = action.rollback(detached_nodes_dct)
        return detached_nodes_dct
        

class SubjectLevelLinkSuperMethod(SuperMethod):

    param_class = {
        'label': 'Parameter',
        'short_label': 'PARAM'
    }
    aval_class = {
        'label': 'Analysis Value',
        'short_label': 'AVAL'
    }
    aval_c_class = {
        'label': 'Analysis Value (C)',
        'short_label': 'AVALC'
    }
    record_class = {
        'label': 'Record',
        'short_label': 'RECORD'
    }
    subject_class = {
        'label': 'Subject',
        'short_label': 'USUBJID'
    }
    param_term = None

    def __init__(self, method, interface, action_info, previous_actions, action_dict=None):
        super().__init__(method=method, interface=interface, action_info=action_info, action_dict=action_dict)
        self.previous_actions = previous_actions
        if method.schema_available:
            self.actions = self.retrieve_actions()            

    def param_term_exists(self, label: str) -> bool:
        """
        Check to see if a Parameter term exists for a certain Analysis Value.
        """

        q = '''
        MATCH (param_cls:Class {label: $param_label})-[:HAS_CONTROLLED_TERM]->(term:Term)
        WHERE term.`rdfs:label` = $subject_level_class_label
        RETURN term
        '''
        params = {
            "subject_level_class_label": label,
            "param_label": self.param_class.get('label')
        }
        res = self.interface.query(q, params)
        if res:
            return True
        else:
            return False

    def fetch_metadata(self):
        q = """
        MATCH (action)-[subject_level_rel:SUBJECT_LEVEL]->(class:Class)
        WHERE id(action) = $node_id
        OPTIONAL MATCH (action)-[:TERM]->(term:Term)
        RETURN action{.*} as m, class, term
        """
        res = self.interface.query(q, self.params)
        assert res, f"Could not find metadata for {self.action_info}."
        res = res[0]
        res["m"]["node_id"] = self.method_node_id
        assert res["m"], f"Method node for SubjectLevelLink action {self.action_info} not found"

        if res.get('term') is None:
            self.param_term = res['class'].get('label')
        else:
            self.param_term = res['term'].get('rdfs:label')

        if not self.param_term_exists(self.param_term):
            logger.error(f'Could not find Parameter term with rdfs:label {self.param_term}')

        return res

    def retrieve_actions(self):
        """
        Further translating the metadata into a list of other method actions.
        Any actions types that implement the rollback() method require a "node_id" to be included in its action_dict
        :return: list of action objects
        """
        actions = []
        cls = self.meta.get('class')
        term = self.meta.get('term')

        aval_class_to_use = self.aval_class.get('label') if cls.get('data_type') in ['int', 'float'] else self.aval_c_class.get('label')
        aval_class_sh_to_use = self.aval_class.get('short_label') if cls.get('data_type') in ['int', 'float'] else self.aval_c_class.get('short_label')
        # ToDo When ingesting set `data_type` property on SDTM classes

        actions.extend([
            AssignLabel({
                "id": self.meta["m"]["id"] + "_assign_class",
                "node_id": self.method_node_id,
                "type": "assign_class",
                "on_class_short_label": cls.get('short_label'),
                "on_class": cls.get('label'),
                "assign_short_label": aval_class_sh_to_use,
                "assign_label": aval_class_to_use
            }, self, dont_fetch=True),
            BuildUri({
                "id": self.meta["m"]["id"] + "_build_uri",
                "node_id": self.method_node_id,
                "type": "build_uri",
                "prefix": f"Subject_level_{self.param_term}_", #TODO: put param in uri using uri_bys (when param values avail. if df)
                "uri_fors": [self.record_class['short_label']],
                "uri_fors_long": [self.record_class['label']],
                "uri_bys": [
                    self.subject_class['short_label'], 
                    # self.param_class['short_label'],
                    ], 
                "uri_bys_long": [
                    self.subject_class['label'], 
                    # self.param_class['label']
                    ],
                "uri_labels": [],
                "uri_labels_long": []
            }, self, dont_fetch=True),
            Link({
                "id": self.meta["m"]["id"] + "_record_link",
                "node_id": self.method_node_id,
                "type": "link",
                "relationship_type": self.record_class.get('label'),
                "from_class": self.subject_class['label'], 
                "from_short_label": self.subject_class['short_label'],
                "to_class": self.record_class.get('label'),
                "to_short_label":self.record_class.get('short_label'),
                "to_class_property": 'rdfs:label',
                "use_uri": True
            }, self, dont_fetch=True),
            Link({
                "id": self.meta["m"]["id"] + "_param_link",
                "node_id": self.method_node_id,
                "type": "link",
                "relationship_type": self.param_class.get('label'),
                "from_class": self.record_class['label'],
                "from_short_label": self.record_class['short_label'],
                "to_class": self.param_class.get('label'),
                "to_short_label":self.param_class.get('short_label'),
                "to_class_property": 'rdfs:label',
                "to_value": cls.get('label') if term is None else term.get('rdfs:label'),
                "merge": True
            }, self, dont_fetch=True),
            Link({
                "id": self.meta["m"]["id"] + "_record_aval_link",
                "node_id": self.method_node_id,
                "type": "link",
                "relationship_type": aval_class_to_use,
                "from_class": self.record_class.get('label'),
                "from_short_label": self.record_class.get('short_label'),
                "to_class": aval_class_to_use,
                "to_short_label": aval_class_sh_to_use                
            }, self, dont_fetch=True),
            # Link({
            #     "id": self.meta["m"]["id"] + "_param_aval_link",
            #     "node_id": self.method_node_id,
            #     "type": "link",
            #     "relationship_type": aval_class_to_use,
            #     "from_class": self.param_class.get('label'),
            #     "from_short_label": self.param_class.get('short_label'),
            #     "to_class": aval_class_to_use,
            #     "to_short_label": aval_class_sh_to_use,                
            # }, self, dont_fetch=True)
        ])
        return actions

    def retrieve_json(self) -> dict:
        logger.info(f"Building json: {self.action_id}")

        nodes = [{
                "id": self.action_id,
                "labels": ["Method"],
                "properties": {
                    "type": 'subject_level_link',
                    "id": self.action_id
                }
            }]
        
        cls = self.meta.get('class')
        term = self.meta.get('term')

        nodes.append({
                "id": cls.get('label'),
                "labels": ["Class"],
                "properties": {
                    "label": cls.get('label'),
                }
            })
        relationships = [
            {"id": f"{cls.get('label')}_SUBJECT_LEVEL_rel", "toId": cls.get('label'), "fromId": self.action_id, "type": "SUBJECT_LEVEL"}
        ]

        if term:
            nodes.append(
            {
                "id": term.get('Term Code') + term.get('Codelist Code'),
                "labels": ["Term"],
                "properties": {
                    "Term Code": term.get('Term Code'),
                    "Codelist Code": term.get('Codelist Code'),
                }
            }
            )
            relationships.append({"id": f"{term.get('Term Code') + term.get('Codelist Code')}_TERM_rel", "toId": term.get('Term Code') + term.get('Codelist Code'), "fromId": self.action_id, "type": "TERM"})

        return {"nodes": nodes, "relationships": relationships}    
        
        
class DecodeSuperMethod(SuperMethod):
    def __init__(self, method, interface, action_info, previous_actions, action_dict=None):
        super().__init__(method=method, interface=interface, action_info=action_info, action_dict=action_dict)
        self.previous_actions = previous_actions
        if method.schema_available:
            self.actions = self.retrieve_actions()
            # self.validate_decode_meta()

    def fetch_metadata(self):
        q = """
        MATCH (from_class:Class)<-[:FROM_CLASS]-(action)-[:TO_CLASS]->(to_class:Class)
        WHERE id(action) = $node_id
        MATCH (from_class)-[:HAS_CONTROLLED_TERM]->(from_class_term:Term)
        ,(to_class)-[:HAS_CONTROLLED_TERM]->(to_class_term:Term)
        ,(from_class_term)-[:SAME_AS]->(to_class_term)
        ,(from_class)<-[:FROM|TO]-(r:Relationship)-[:FROM|TO]->(to_class)
        WITH action, from_class, r, to_class, collect([from_class_term.`rdfs:label`, to_class_term.`rdfs:label`]) as term_pairs
        LIMIT 1
        RETURN
        action{.*} as m,
        from_class.label as from_class_label,
        from_class.short_label as from_class_short_label,
        to_class.label as to_class_label,
        to_class.short_label as to_class_short_label,
        r.relationship_type as relationship_type,
        term_pairs
        """
        res = self.interface.query(q, self.params)
        assert res, f"Could not find metadata for {self.action_info}. Please check that there are SAME_AS rels between at least one pair of terms for this actions classes!"
        res = res[0]
        res["m"]["node_id"] = self.method_node_id
        assert res["m"], f"Method node from Decode action {self.action_info} not found"
        return res

    def retrieve_actions(self):
        """
        Further translating the metadata into a list of other method actions.
        Any actions types that implement the rollback() method require a "node_id" to be included in its action_dict
        :return: list of action objects
        """
        actions = [
            CallAPI({
                "id": self.meta["m"]["id"] + "_run_script",
                "node_id": self.method_node_id,
                "type": "call_api",
                "script": "remap_term_values",
                "lang": "python",
                "package": "basic_df_ops",
                "params": json.dumps(
                    {
                        "original_col": self.meta['from_class_short_label'],
                        "new_col": self.meta['to_class_short_label'],
                        "term_pairs": self.meta['term_pairs'],
                        "remove_unmapped_rows": True if self.meta["m"].get("remove_unmapped_rows") is None or self.meta["m"].get("remove_unmapped_rows")=='true' else False
                    }
                ),
                "github_repo": "gsk-tech/cldnb",  # TODO: update with open-source (future) repository 
                "github_branch": "main",
                "repo_scripts_path": "src/utils",
            }, self, dont_fetch=True),
            Link({
                "id": self.meta["m"]["id"] + "_link",
                "node_id": self.method_node_id,
                "type": "link",
                "relationship_type": self.meta['relationship_type'],
                "from_class": self.meta['from_class_label'],
                "from_short_label": self.meta['from_class_short_label'],
                "to_class": self.meta['to_class_label'],
                "to_short_label": self.meta['to_class_short_label'],
                "to_class_property": 'rdfs:label',
                "merge": True,
            }, self, dont_fetch=True)
        ]
        return actions

    def retrieve_json(self):
        q1 = """
        CALL apoc.create.vNode(["Method"], {id: $method_id, type: 'decode', remove_unmapped_rows: $remove_unmapped_rows}) YIELD node as method
        WITH method
        MATCH (from_class:Class) WHERE from_class.label = $from_class
        WITH method, from_class
        MATCH (to_class:Class) WHERE to_class.label = $to_class
        CALL apoc.create.vNode(["Class"], apoc.map.submap(from_class, ['label'])) YIELD node as from_class_
        CALL apoc.create.vNode(["Class"], apoc.map.submap(to_class, ['label'])) YIELD node as to_class_
        CALL apoc.create.vRelationship(method, "FROM_CLASS", {}, from_class_) YIELD rel as from_rel
        CALL apoc.create.vRelationship(method, "TO_CLASS", {}, to_class_) YIELD rel as to_rel
        WITH [[method, from_rel, from_class_], [method, to_rel, to_class_]] as coll
        UNWIND coll as item
        WITH item[0] as x, item[1] as r, item[2] as y
        """

        params = {
            'from_class': self.meta.get('from_class_label'),
            'to_class': self.meta.get('to_class_label'),
            'method_id': self.action_id,
            'remove_unmapped_rows': self.meta['m'].get('remove_unmapped_rows') if self.meta['m'].get('remove_unmapped_rows') is not None else 'true'
        }

        res1 = get_arrows_json_cypher(
            neo=self.interface,
            q=q1,
            params=params,
            hide_labels=Action.hide_labels,
            hide_props=Action.hide_props
        )
        res1 = res1[0]

        return res1


class ApplyStatSuperMethod(SuperMethod):
    built_terms_for_distinct_values_flag = False
    def __init__(self, method, interface, action_info, previous_actions, action_dict=None):
        super().__init__(method=method, interface=interface, action_info=action_info, action_dict=action_dict)
        self.previous_actions = previous_actions
        self.update_schema_init_npct_class()
        if method.schema_available:
            self.actions = self.retrieve_actions()
            self.validate_apply_stat_meta()

    def fetch_metadata(self):
        q = """
               MATCH (action)
                WHERE id(action) = $node_id
                OPTIONAL MATCH (action)-[:Result]->(result:Class)  
                OPTIONAL MATCH (result)<-[:TO]-(result_rel:Relationship)<-[:SOURCE_RELATIONSHIP]-(:Method{type:'get_data'})-[:NEXT*1..50]->(action)
                WITH action, result, collect(result_rel) as coll       
                WITH action, result, coll[0] as result_rel
                WITH action, 
                    collect(distinct coalesce(result_rel.short_label, result.short_label)) as results,
                    collect(distinct result.label) as results_long
                OPTIONAL MATCH (action)-[dim_r:Dimension]->(dimension:Class),
                    (dimension)<-[:TO|FROM]-(dimension_rel:Relationship)<-[:SOURCE_RELATIONSHIP]-(:Method{type:'get_data'})-[:NEXT*1..50]->(action)                                                     
                OPTIONAL MATCH (action)-[:Statistic]->(statistic:Class)
                WITH *
                ORDER BY dimension.short_label, statistic.short_label
                WITH action, results, results_long,
                    collect(distinct {
                        dimension: coalesce(dimension.short_label, dimension_rel.short_label), 
                        required: dim_r.required,
                        denominator: dim_r.denominator,
                        all_ct: dim_r.all_ct
                    }) as dimensions,
                    collect(distinct {
                        dimension: dimension.label, 
                        required: dim_r.required,
                        denominator: dim_r.denominator
                    }) as dimensions_long,                   
                    collect(distinct statistic.short_label) as statistics,
                    collect(distinct statistic.label) as statistics_long                     
                RETURN 
                    action{.*} as m, 
                    results, 
                    results_long,
                    dimensions, 
                    dimensions_long,
                    statistics,
                    statistics_long
        """

        res = self.interface.query(q, self.params)
        assert res, f"Method node from action {self.meta} not found"
        res = res[0]
        res["m"]["node_id"] = self.method_node_id
        assert res["m"], f"Method node from ApplyStat action {self.action_info} not found"

        if res.get('dimensions_long', False):
            for class_dct in res.get('dimensions_long'):
                self.build_terms_for_distinct_values(class_dct)

        additional_ct_classes = []
        for dim in res.get('dimensions', []):
            # TODO: Validation eg cant be all_ct and denominator?
            if dim.get('all_ct') == 'true':
                additional_ct_classes.append(dim.get('dimension'))

        if additional_ct_classes:
            q = """
            UNWIND $ct_classes as class_short
            MATCH (c:Class)-[:HAS_CONTROLLED_TERM]->(t:Term)
            WHERE c.short_label = class_short
            RETURN c.short_label as short_label, collect([id(t), t.`rdfs:label`]) as ct
            """
            res1 = self.interface.query(q, {'ct_classes': additional_ct_classes})
            res["m"]["additional_ct"] = res1

        return res

    def build_terms_for_distinct_values(self, class_dct):
        """
        Creates (or merges) term nodes for each value in a class if at least one of those values is not unique.
        E.G. Class Status has two values of 'Attended' and 'Not Attended' but a subject has both Attended and
        Not Attended status' for a single visit. There will be two Attended nodes where we only want one. So we create a
        term node for Attended and Not Attended in order to have distinct values for a class.
        """
        uri_map = {
            key: item
            for key, item in ModelManager.URI_MAP.items()
            if key in ["Term"]
        }

        class_label = class_dct.get('dimension')
        term_provenance = f"apply_stat:{self.params.get('parent_id')}_{self.method.name}"

        q1 = f"""
        MATCH (n:`{class_label}`)
        WHERE toString(n.`rdfs:label`) <> 'NaN'
        // instead maybe need to change NaN properties to 'CLD_NAN' as multiple nodes with NaN are considered distinct by cypher
        // this will let use this as a term later
        // SET n.`rdfs:label` = CASE 
        //    WHEN toString(n.`rdfs:label`) = 'NaN' THEN 'CLD_NAN' 
        //    ELSE n.`rdfs:label` 
        //    END
        WITH DISTINCT n.`rdfs:label` as rdfs, collect(n) as coll_n
        WITH rdfs, coll_n
        WHERE size(coll_n)>1
        RETURN rdfs, size(coll_n) as count
        """
        q2 = f"""
        MATCH (n:`{class_label}`)
        WHERE toString(n.`rdfs:label`) <> 'NaN'
        WITH DISTINCT n.`rdfs:label` as rdfs, collect(n) as coll_n
        MATCH (class:Class{{label: $class}})
        WITH class, rdfs, $class as class_label, class.short_label as class_short_label, coll_n
        MERGE (class)-[:HAS_CONTROLLED_TERM]->(term:Term{{`rdfs:label`: rdfs}})
        ON CREATE
            SET 
                term._status_ = 'CREATED',
                term.`provenance` = '{term_provenance}',
                term.`Codelist Code` = class_short_label, 
                term.`Term Code` = toString(rdfs),
                term:`{class_label}`
        WITH CASE WHEN term._status_ = 'CREATED' THEN TRUE ELSE FALSE END as term_created_status, term, coll_n
        REMOVE term._status_
        WITH *
        CALL {{
                WITH coll_n, term
                UNWIND coll_n as n
                MERGE (n)-[:Term]->(term)
                RETURN collect(n) as n
                }}
        RETURN term, term_created_status
        """
        params = {'class': class_label}
        res = self.interface.query(q1, params)
        if res:
            logger.info(f'\tTerms are required for Dimension Class {class_dct} as it has multiple nodes with the same rdfs:label')
            self.built_terms_for_distinct_values_flag = True
            res = self.interface.query(q2, params)
            for term_created in res:
                if term_created.get('term_created_status'):
                    logger.warning(f'\t\tCreated new term - {term_created.get("term")}')
                else:
                    logger.debug(f'\t\tMerged term - {term_created.get("term")}')

            logger.info(f'\tGenerating uri\'s for new terms...')
            where_clause = f"WHERE '{class_label}' in labels(x)"
            uri_map['Term']['where'] = where_clause
            self.interface.rdf_generate_uri(uri_map)
            logger.info(f'\tDone')

    def retrieve_actions(self):
        """
        Further translating the metadata into a list of other method actions.
        Any actions types that implement the rollback() method require a "node_id" to be included in its action_dict
        :return: list of action objects
        """
        actions = []
        if self.built_terms_for_distinct_values_flag:
            actions.append(
                RunCypher({
                    # replace value node ids with corresponding term node ids in _id_class_label column
                    "id": self.meta["m"]["id"] + "_run_cypher",
                    "type": "run_cypher",
                    "query":
                        """
                        UNWIND $data as row 
                        MATCH (node)-[:Term]->(term:Term)<-[:HAS_CONTROLLED_TERM]-(class:Class) WHERE id(node) in [class in $class_labels | row['_id_' + class.short_label]]
                        WITH row, apoc.map.fromPairs(collect(['_id_' + class.short_label, id(term)])) as upd_map
                        RETURN apoc.map.merge(row, upd_map) as new_row
                        """,
                    "params": json.dumps({
                        "class_labels": [
                            {'short_label': dct_short['dimension'], 'long_label': dct_long['dimension']}
                            for dct_short, dct_long in zip(self.meta.get("dimensions"), self.meta.get("dimensions_long"))
                        ]
                    }),
                    "include_data": "true",
                    "update_df": "true",
                }, self, dont_fetch=True)
            )

        additional_ct = self.meta['m'].get('additional_ct', False)
        if additional_ct:
            actions.append(
                CallAPI({
                    "id": self.meta["m"]["id"] + "_run_script_ct_cartesian_product",
                    "type": "call_api",
                    "node_id": self.method_node_id,
                    "script": 'ct_cartesian_product',
                    "lang": "python",
                    "package": "basic_df_ops",
                    "params": json.dumps(
                        {
                            "dimensions": [dim_dict.get('dimension') for dim_dict in self.meta.get("dimensions")],
                            "dimension_ct": additional_ct
                        },
                    ),
                    "github_repo": "gsk-tech/cldnb",  # TODO: update with open-source (future) repository
                    "github_branch": "main",
                    "repo_scripts_path": "src/utils",
                }, self, dont_fetch=True),
            )

        actions.append(
            BranchSave({
                "id": self.meta["m"]["id"] + "_branch1",
                "type": "branch_save"  # saves the current state of self.df into self.branch_load_dfs dict
            }, self, dont_fetch=True)
            )

        # creating combinations of dimensions to loop over
        dimension_combinations = self.all_dimension_combinations(self.meta.get("dimensions"))
        dimension_combinations_long = self.all_dimension_combinations(self.meta.get("dimensions_long"))

        for i, dimensions in enumerate(dimension_combinations):
            dimensions_long = dimension_combinations_long[i]
            if i > 0:
                actions.extend([
                    BranchLoad({
                        "id": self.meta["m"]["id"] + "_branch1",
                        "type": "branch_load"  # loads a saved state of self.df from self.branch_load_dfs dict
                    }, self, dont_fetch=True)
                ])
            actions.extend([
                CallAPI({
                    "id": self.meta["m"]["id"] + "_run_script_" + str(i),
                    "node_id": self.method_node_id,
                    "type": "call_api",
                    "script": self.meta["m"]["script"],
                    "lang": "python",
                    "package": "basic_df_ops",
                    "params": json.dumps(
                        {
                            "value_cols": self.meta["results"],
                            "by": [y for x in
                                   [["_id_" + by["dimension"], by["dimension"]] for by in
                                    dimensions]
                                   for y in x
                                   ],
                            "agg": [x for x in self.meta["statistics"]]
                        },
                    ),
                    "github_repo": "gsk-tech/cldnb", #TODO: update with open-source (future) repository
                    "github_branch": "main",
                    "repo_scripts_path": "src/utils",
                }, self, dont_fetch=True),
                BuildUri({
                    "id": self.meta["m"]["id"] + "_build_uri" + "_" + str(i),
                    "type": "build_uri",
                    "prefix": ",".join(self.meta["results"]),
                    "uri_fors": self.meta["statistics"],
                    "uri_fors_long": self.meta["statistics_long"],
                    "uri_bys": [by["dimension"] for by in dimensions],
                    "uri_bys_long": [by["dimension"] for by in dimensions_long],
                }, self, dont_fetch=True),
                LinkStat({
                    "id": self.meta["m"]["id"] + "_link_stat" + "_" + str(i),
                    "node_id": self.method_node_id,
                    "type": "link_stat",
                    "dimensions": dimensions,
                    "dimensions_long": dimensions_long,
                    "statistics": self.meta.get("statistics"),
                    "statistics_long": self.meta.get("statistics_long"),
                    "results": self.meta.get("results"),
                    "results_long": self.meta.get("results_long"),
                }, self, dont_fetch=True),
            ])
            # Processing percentages (if denominator is provided)
            valid_statistic_for_pct = ["n", "n_distinct"]
            statistic_for_pct = [stat for stat in self.meta["statistics"] if stat in valid_statistic_for_pct]
            if statistic_for_pct and 0 < len([x for x in dimensions if x['denominator'] == 'true']) < len(dimensions):
                denom_dimensions = [by for by in dimensions if by.get("denominator") == "true"]
                denom_dimensions_long = [by for by in dimensions_long if by.get("denominator") == "true"]
                assert len(statistic_for_pct) == 1, \
                    f"Cannot calculate percentages having >1 statistic from {valid_statistic_for_pct} in metadata"
                statistic_for_pct = statistic_for_pct[0]
                statistic_for_pct_long = self.meta["statistics_long"][self.meta["statistics"].index(statistic_for_pct)]
                actions.extend([
                    BranchSave({  # saving numerators
                        "id": self.meta["m"]["id"] + "_numerator_" + str(i),
                        "type": "branch_save"  # loads a saved state of self.df from self.branch_load_dfs dict
                    }, self, dont_fetch=True),
                    BranchLoad({  # loading back data from get_data
                        "id": self.meta["m"]["id"] + "_branch1",
                        "type": "branch_load"  # loads a saved state of self.df from self.branch_load_dfs dict
                    }, self, dont_fetch=True),
                    CallAPI({  # computing denominators
                        "id": self.meta["m"]["id"] + "_run_script_denom_" + str(i),
                        "node_id": self.method_node_id,
                        "type": "call_api",
                        "script": self.meta["m"]["script"],
                        "lang": "python",
                        "package": "basic_df_ops",
                        "params": json.dumps(
                            {
                                "value_cols": self.meta["results"],
                                "by": [y for x in
                                       [["_id_" + by["dimension"], by["dimension"]] for by in denom_dimensions]
                                       for y in x
                                       ],
                                "agg": self.meta["statistics"]
                            }
                        ),
                        "github_repo": "gsk-tech/cldnb",  # TODO: update with open-source (future) repository
                        "github_branch": "main",
                        "repo_scripts_path": "src/utils",
                    }, self, dont_fetch=True),
                    BuildUri({
                        "id": self.meta["m"]["id"] + "_build_denom_uri_" + str(i),
                        "type": "build_uri",
                        "prefix": ",".join(self.meta["results"]),
                        "uri_fors": self.meta["statistics"],
                        "uri_fors_long": self.meta["statistics_long"],
                        "uri_bys": [by["dimension"] for by in denom_dimensions],
                        "uri_bys_long": [by["dimension"] for by in denom_dimensions_long],
                    }, self, dont_fetch=True),
                    LinkStat({  # saving denominators as n_distinct(or n)
                        "id": self.meta["m"]["id"] + "_link_denom_counts_" + str(i),
                        "node_id": self.method_node_id,
                        "type": "link_stat",
                        "dimensions": denom_dimensions,
                        "dimensions_long": denom_dimensions_long,
                        "statistics": self.meta.get("statistics"),
                        "statistics_long": self.meta.get("statistics_long"),
                        "results": self.meta.get("results"),
                        "results_long": self.meta.get("results_long"),
                    }, self, dont_fetch=True),
                    CallAPI({
                        "id": self.meta["m"]["id"] + "_run_script_rename_denom_" + str(i),
                        "node_id": self.method_node_id,
                        "type": "call_api",
                        "script": "rename_columns",
                        "lang": "python",
                        "package": "basic_df_ops",
                        "params": json.dumps(
                            {
                                "rename_dict": {
                                    statistic_for_pct: "denominator",
                                    "_id_" + statistic_for_pct: "_id_denominator",
                                    "_uri_" + statistic_for_pct: "_uri_denominator"
                                }
                            }
                        ),
                        "github_repo": "gsk-tech/cldnb",  # TODO: update with open-source (future) repository
                        "github_branch": "main",
                        "repo_scripts_path": "src/utils",
                    }, self, dont_fetch=True),
                    BranchSave({
                        "id": self.meta["m"]["id"] + "_denominator_" + str(i),
                        "type": "branch_save"  # loads a saved state of self.df from self.branch_load_dfs dict
                    }, self, dont_fetch=True),
                    BranchCombine({
                        "id": self.meta["m"]["id"] + "_branch_combine" + str(i),
                        "type": "branch_combine",
                        "branches": [
                            self.meta["m"]["id"] + "_numerator_" + str(i),
                            self.meta["m"]["id"] + "_denominator_" + str(i)
                        ]
                    }, self, dont_fetch=True),
                    CallAPI({
                        "id": self.meta["m"]["id"] + "_run_script_devide_" + str(i),
                        "node_id": self.method_node_id,
                        "type": "call_api",
                        "script": "divide",
                        "lang": "python",
                        "package": "basic_df_ops",
                        "params": json.dumps(
                            {
                                "values": [statistic_for_pct, "denominator"],
                                "out_col": "npct"
                            }
                        ),
                        "github_repo": "gsk-tech/cldnb",  # TODO: update with open-source (future) repository
                        "github_branch": "main",
                        "repo_scripts_path": "src/utils",
                    }, self, dont_fetch=True),
                    CallAPI({
                        "id": self.meta["m"]["id"] + "_run_script_multiply_" + str(i),
                        "node_id": self.method_node_id,
                        "type": "call_api",
                        "script": "multiply",
                        "lang": "python",
                        "package": "basic_df_ops",
                        "params": json.dumps({
                            "values": ["npct", "&100"],
                            "out_col": "npct",
                            "decimal_places": self.meta["m"].get('percentage_dp') if self.meta["m"].get('percentage_dp')
                            else 0}),
                        "github_repo": "gsk-tech/cldnb",  # TODO: update with open-source (future) repository
                        "github_branch": "main",
                        "repo_scripts_path": "src/utils",
                    }, self, dont_fetch=True),
                    BuildUri({
                        "id": self.meta["m"]["id"] + "_build_uri_for_pct_" + str(i),
                        "type": "build_uri",
                        "prefix": ",".join(self.meta["results"]) + f"({','.join(self.meta['statistics'])})",
                        "uri_fors": ["npct"],
                        "uri_fors_long": ["Number of observations (Percent)"],
                        "uri_bys": [by["dimension"] for by in dimensions] +
                                   [by["dimension"] for by in dimensions if by.get("denominator") == "true"],
                        "uri_bys_long": [by["dimension"] for by in dimensions_long] +
                                        [by["dimension"] for by in dimensions_long if by.get("denominator") == "true"],
                    }, self, dont_fetch=True),
                    LinkStat({
                        "id": self.meta["m"]["id"] + "_link_pct_" + str(i),
                        "node_id": self.method_node_id,
                        "type": "link_stat",
                        "dimensions": dimensions,
                        "dimensions_long": dimensions_long,
                        "statistics": ["npct"],
                        "statistics_long": ["Number of observations (Percent)"],
                        "results": self.meta.get("results"),
                        "results_long": self.meta.get("results_long"),
                    }, self, dont_fetch=True),
                    Link({
                        "id": self.meta["m"]["id"] + "_link_pct_numerator_" + str(i),
                        "node_id": self.method_node_id,
                        "type": "link",
                        "relationship_type": "Numerator of",
                        "from_class": statistic_for_pct_long,
                        "to_class": "Number of observations (Percent)",
                        "from_short_label": statistic_for_pct,
                        "to_short_label": "npct",
                        "merge": False,
                    }, self, dont_fetch=True),
                    Link({
                        "id": self.meta["m"]["id"] + "_link_pct_denominator_" + str(i),
                        "node_id": self.method_node_id,
                        "type": "link",
                        "relationship_type": "Denominator of",
                        "from_class": statistic_for_pct_long,
                        "to_class": "Number of observations (Percent)",
                        "from_short_label": statistic_for_pct,
                        "to_short_label": "npct",
                        "from_column": "denominator",
                        "merge": False,
                    }, self, dont_fetch=True),
                ])
        self.update_schema_stat_to_dimensions()
        return actions

    def validate_apply_stat_meta(self):
        # check that all filter classes must be also required dimensions in apply_stat
        filter_classes = []
        for previous_action in self.previous_actions:
            if previous_action["type"] == "get_data" and previous_action.filter_dict is not None:
                filter_classes += list(previous_action.meta["where_map"].keys())
        for filter_class in filter_classes:
            if filter_class not in [
                dim['dimension']
                for dim in self.meta.get("dimensions_long")
                if dim["required"] == "true"
            ]:
                raise TypeError(
                    f"""`{filter_class}` is not in required dimensions of {self.params}: {self.meta.get('m').get('id')}. 
                    All filter classes must be also required dimensions in apply_stat for statistics node to 
                    have accurate uri
                    """)
        # check that all get_data relationships to classes which are not `denominator:true` as optional
        dimensions_long = [dim["dimension"] for dim in self.meta.get('dimensions_long')]
        denom_dimensions_long = [dim["dimension"] for dim in self.meta.get('dimensions_long')
                                 if dim['denominator'] == 'true']
        if denom_dimensions_long:
            for previous_action in self.previous_actions:
                if previous_action["type"] == "get_data":
                    for rel in previous_action.meta["source_rels"]:
                        if (
                                (rel["to"] in dimensions_long and rel["to"] not in denom_dimensions_long) or
                                (rel["from"] in dimensions_long and rel["from"] not in denom_dimensions_long)
                        ) and not str(rel.get("optional")).lower() == "true":
                            raise TypeError(
                                f"""
                        Relationship {rel} must be `optional:true` in get_data 
                        since one of the classes {rel["to"]}/{rel[
                                    "from"]} is not denominator Dimension (`denominator:true`) 
                        """
                            )

    @staticmethod
    def all_dimension_combinations(dimensions: list):
        dimensions_req = [dim for dim in dimensions if dim.get('required') == 'true']
        dimensions_notreq = [dim for dim in dimensions if dim.get('required') != 'true']

        def all_subsets(ss):
            return list(chain(*map(lambda x: combinations(ss, x), range(0, len(ss) + 1))))

        return [dimensions_req + list(comb) for comb in all_subsets(dimensions_notreq)]

    def update_schema_stat_to_dimensions(self):
        q = """
        MATCH (c:Class), (stat:Class)
        WHERE c.label in $dimensions and stat.label in $statistics
        MERGE (c)<-[:FROM]-(:Relationship{relationship_type:stat.label})-[:TO]->(stat)
        """
        params = {
            'dimensions': [x['dimension'] for x in self.meta.get("dimensions_long")],
            'statistics': self.meta.get("statistics_long"),
        }
        self.interface.query(q, params)

    def update_schema_init_npct_class(self):
        q = """
        MERGE (c:Class{label:'Number of observations (Percent)'})
        SET c.short_label = 'npct'
        SET c.derived = 'true'
        SET c.is_stat = 'true'
        SET c.is_visible = 'false'
        WITH c
        MATCH (d:Class) WHERE d.label in $dimensions
        MERGE (d)<-[:FROM]-(:Relationship{relationship_type:'Number of observations (Percent)'})-[:TO]->(c)
        """
        params = {
            'dimensions': [x['dimension'] for x in self.meta.get("dimensions_long")]
        }
        self.interface.query(q, params)

    def retrieve_json(self):
        logger.info(f"Building json: {self.action_id}")

        q1 = """
                CALL apoc.create.vNode(["Method"], 
                    CASE
                        WHEN $percentage_dp IS NOT NULL THEN 
                        {id: $method_id, type: 'apply_stat', script: 'group_by', lang: 'python', package: 'basic_df_ops', 
                        percentage_dp: $percentage_dp}
                        ELSE
                        {id: $method_id, type: 'apply_stat', script: 'group_by', lang: 'python', package: 'basic_df_ops'}
                        END
                    ) YIELD node as method
                WITH method, $dimensions as dims, $req_dims as req_dims, $denom_dims as denom_dims
                WITH method, $dimensions as dims 
                MATCH (x:Class) WHERE x.label IN dims
                WITH x, method, 
                    CASE WHEN x.label IN $req_dims THEN 'true' ELSE NULL END as is_required, 
                    CASE WHEN x.label IN $denom_dims THEN 'true' ELSE NULL END as is_denominator,
                    CASE WHEN x.label IN $all_ct_dims THEN 'true' ELSE NULL END as should_all_ct
                CALL apoc.create.vNode(["Class"], apoc.map.submap(x, ['label'])) YIELD node as x_
                CALL apoc.create.vRelationship(method, "Dimension", {
                    denominator: is_denominator,
                    required: is_required,
                    `all_ct`: should_all_ct
                }, x_) YIELD rel
                WITH [[method, rel, x_]] as coll
                UNWIND coll as item
                WITH item[0] as x, item[1] as r, item[2] as y
                """

        q2 = """
                CALL apoc.create.vNode(["Method"], {id: $method_id, type: 'apply_stat', script: 'group_by', lang: 'python',
                                            package: 'basic_df_ops'}
                                            ) YIELD node as method
                WITH method, $statistics as stats
                MATCH (x:Class) WHERE x.label IN stats
                CALL apoc.create.vNode(["Class"], apoc.map.submap(x, ['label'])) YIELD node as x_
                CALL apoc.create.vRelationship(method, "Statistic", {}, x_) YIELD rel
                WITH [[method, rel, x_]] as coll
                UNWIND coll as item
                WITH item[0] as x, item[1] as r, item[2] as y
                """

        q3 = """
                CALL apoc.create.vNode(["Method"], {id: $method_id, type: 'apply_stat', script: 'group_by', lang: 'python',
                                            package: 'basic_df_ops'}
                                            ) YIELD node as method
                WITH method, $result as res
                MATCH (x:Class) WHERE x.label = res
                CALL apoc.create.vNode(["Class"], apoc.map.submap(x, ['label'])) YIELD node as x_
                CALL apoc.create.vRelationship(method, "Result", {}, x_) YIELD rel
                WITH [[method, rel, x_]] as coll
                UNWIND coll as item
                WITH item[0] as x, item[1] as r, item[2] as y
                """
        assert len(self.meta.get('results_long')) == 1, \
            f"Expected result to be a list of len 1, it was: {self.meta.get('results_long')}"
        result = self.meta.get('results_long')[0]

        dimensions = []
        dimensions_required = []
        dimensions_denominator = []
        all_ct_dims = []

        for dim in self.meta.get('dimensions_long'):
            if dim.get('dimension'):
                dimensions.append(dim.get('dimension'))
            if dim.get('required'):
                dimensions_required.append(dim.get('dimension'))
            if dim.get('denominator'):
                dimensions_denominator.append(dim.get('dimension'))
            if dim.get('all_ct'):
                dimensions_denominator.append(dim.get('dimension'))

        params = {
            'dimensions': dimensions,
            'statistics': self.meta.get('statistics_long'),
            'result': result,
            'req_dims': dimensions_required,
            'denom_dims': dimensions_denominator,
            'all_ct_dims': all_ct_dims,
            'percentage_dp': self.meta.get('m').get('percentage_dp') if dimensions_denominator else None,
            'method_id': self.action_id
        }

        res1 = get_arrows_json_cypher(
            neo=self.interface,
            q=q1,
            params=params,
            hide_labels=Action.hide_labels,
            hide_props=Action.hide_props
        )
        res1 = res1[0]

        res2 = get_arrows_json_cypher(
            neo=self.interface,
            q=q2,
            params=params,
            hide_labels=Action.hide_labels,
            hide_props=Action.hide_props
        )
        res2 = res2[0]

        res3 = get_arrows_json_cypher(
            neo=self.interface,
            q=q3,
            params=params,
            hide_labels=Action.hide_labels,
            hide_props=Action.hide_props
        )
        res3 = res3[0]

        output_res = {
            'nodes': [],
            'relationships': [],
            'style': {}
        }
        current_nodes = {}
        filter_node_dict = {
            'Class': 'label',
            'Term': 'rdfs:label',
            'Method': 'id',
            # 'Relationship': 'relationship_type'
        }

        # go through each res and compile them (without duplicates)
        for res in [res1, res2, res3]:
            # Cleanup null v-relationship values
            for rel in res['relationships']:
                clean_props = {}
                rel_properties = rel.get('properties')
                for key, value in rel_properties.items():
                    if value is not None:
                        clean_props[key] = value
                rel['properties'] = clean_props

            node_indexes_to_skip = []
            for node_index, node in enumerate(res['nodes']):
                for label in filter_node_dict.keys():
                    if label in node['labels']:
                        prop_filter = filter_node_dict[label]  # label or id
                        unique_node_key = self.get_unique_key(node, label, prop_filter)
                        # eg Analysis Value Class, apply_stat Method
                        # need to check for nodes that could already be present and 'merge' them
                        if unique_node_key in current_nodes.keys():
                            # find all rels to/from this node, then point them to the preexisting node
                            for rel in res['relationships']:
                                if rel['fromId'] == node['id']:
                                    # setting the id in the rel to the preexisting node
                                    rel['fromId'] = current_nodes[unique_node_key]
                                elif rel['toId'] == node['id']:
                                    # setting the id in the rel to the preexisting node
                                    rel['toId'] = current_nodes[unique_node_key]
                            # remove that node from the json
                            node_indexes_to_skip.append(node_index)

                        else:  # otherwise add that node to the current nodes to check against later
                            current_nodes[unique_node_key] = node['id']

            # add all the nodes and relationships to the output_res
            output_res['nodes'].extend([node for i, node in enumerate(res['nodes']) if i not in node_indexes_to_skip])
            output_res['relationships'].extend(res['relationships'])

        return output_res

    @staticmethod
    def get_unique_key(node, label, prop_filter):
        key = node['properties'][prop_filter] + ' ' + label
        return key
