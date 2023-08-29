import json
import logging
import time
import os
import re
from functools import reduce
from operator import add
from pathlib import Path
from typing import List

from neointerface import NeoInterface

from data_providers import DataProvider
from derivation_method.action import GetData, CallAPI, RunScript, Link, AssignLabel, BuildUri
from derivation_method.method import Method
from derivation_method.super_method import SuperMethod, ApplyStatSuperMethod, DecodeSuperMethod, SubjectLevelLinkSuperMethod
from derivation_method.utils import simplify_arrows_json
from logger.logger import logger
from model_managers import ModelManager
from derivation_method.utils import visualise_json, topological_sort

RDFSLABEL = ModelManager.RDFSLABEL


def derivation_method_factory(interface,
                              data="online",
                              study=None,
                              name=None,
                              overwrite_db=False,
                              schema_available=True,
                              check_schema=False):
    if isinstance(data, dict):
        return DictDerivationMethod(content=data,
                                    name=name,
                                    interface=interface,
                                    study=study,
                                    overwrite_db=overwrite_db,
                                    schema_available=schema_available,
                                    check_schema=check_schema)
    elif isinstance(data, Path) or isinstance(data, str) and data.endswith(".json") or data.endswith(".ttl"):
        path = Path(data)
        if path.suffix == ".json":
            with path.open() as file:
                return DictDerivationMethod(content=json.load(file),
                                            interface=interface,
                                            study=study,
                                            name=path.name.replace(path.suffix, ""),
                                            overwrite_db=overwrite_db,
                                            schema_available=schema_available,
                                            check_schema=check_schema)
        elif path.suffix == ".ttl":
            with path.open() as file:
                return RdfDerivationMethod(content=file.read(),
                                           interface=interface,
                                           study=study,
                                           name=path.name.replace(path.suffix, ""),
                                           overwrite_db=overwrite_db,
                                           schema_available=schema_available)
        else:
            if 'archive' not in path.name:
                raise TypeError(f"Unknown File Type {path.suffix} for {path.name}")

    else:
        return OnlineDerivationMethod(name=data, interface=interface, study=study)


class DerivationMethod(Method):  # common variables
    """
    Metadata for Derivations
    """

    def __init__(self, data, interface: NeoInterface = None, name=None, study=None, schema_available=None):
        super().__init__()
        self.interface = interface if interface is not None else DataProvider(rdf=True)
        self.schema_available = True if schema_available is None else schema_available
        self.batch = False
        self.data = data
        self.name = name
        self._db_id = None
        self._actions = None
        if study is None:
            res = self.interface.query(f"""
            MATCH (s)
            WHERE s:Study or s:`Study Pool`
            WITH apoc.text.join(labels(s),'|') as lbl, collect(s) as coll
            WITH apoc.map.fromPairs(collect([lbl, coll])) as mp
            WITH coalesce(mp['Study Pool'], mp['Study']) as coll
            UNWIND coll as study
            RETURN study.`{RDFSLABEL}` as STUDYID 
            ORDER BY STUDYID""")
            assert len(
                res) == 1, "Provide 'study' id at init of DerivationMethod, as there are 0 or >1 studies in the database"
            if res:
                study = res[0]['STUDYID']
        self.study = study

    @property
    def exists_in_db(self) -> bool:
        q = """
            MATCH (m:Method{id:$method_id, parent_id:$study_id})
            RETURN id(m)
            """
        params = {'method_id': self.name, 'study_id': self.study}
        return len(self.interface.query(q, params)) > 0

    @property
    def db_id(self):
        if self._db_id is None:
            q = """
                MATCH (m:Method{id:$method_id})
                WHERE m.parent_id IS NULL or (m.parent_id) = $study_id
                RETURN id(m)
                """        
            params = {'method_id': self.name, 'study_id': self.study}
            self._db_id = self.interface.query(q, params)[0]["id(m)"]
        return self._db_id

    @property
    def actions(self):
        raise NotImplementedError

    @property
    def applied(self):
        if self.name == "online":
            return False
        q = """
            MATCH (m:Method{id:$method_id, parent_id:$study_id})-[r:METHOD_ACTION]-(action:Method),
            (action)-[:APPLIED]->(chg:Changes)
            RETURN distinct chg.applied_datetime
            """
        params = {'method_id': self.name, 'study_id': self.study}
        res = self.interface.query(q, params)
        return len(res) > 0

    def enrich_existing_relationship_nodes(self):
        # Enriching existing Relationship nodes so that load_method_json_inline merges correctly
        q = """
        MATCH (r:Relationship)-[:FROM]->(from), (r)-[:TO]->(to)
        SET r.`FROM.Class.label` = from.label
        SET r.`TO.Class.label` = to.label
        """
        self.interface.query(q)

    def load(self):
        logger.info(f"Loading {self.name}")

    def load_decision(self, overwrite_db) -> bool:
        if self.exists_in_db:
            if overwrite_db:
                return True
            else:
                _ = self.db_id
                logger.info(
                    f"Derivation {self.name} already exists in neo4j (node id: {self.db_id}), method will not be reloaded. "
                    f"To overwrite the version already in the database, set overwrite_db=True")
                return False
        else:
            return True

    def delete_method(self, method_id='*'):
        assert isinstance(method_id, str), \
            f"method_id should be str or None, received {type(method_id)}. Provide id or '*' to delete all methods"
        params = {'method_id': method_id}
        wh = ("" if method_id == '*' else "WHERE m.id = $method_id")
        actions = [
            f"""
            MATCH (m:Method)
            {wh}
            OPTIONAL MATCH path = (m)-[r:METHOD_ACTION|NEXT*1..10]->(x:`Method`)
            OPTIONAL MATCH path2 = (x)-[r2]->(y)
            //OPTIONAL MATCH (m)-[r*1..10]->(x:`Method`)-[r2]->(y)     
            DETACH DELETE r2, x
            """,
            f"""
            MATCH (m:Method)
            {wh}
            OPTIONAL MATCH (m)-[r]-()
            DELETE r
            DELETE m
            """
        ]
        for q in actions:
            self.query(q, params)

    def post_load_enrichment(self):
        new_method_node_ids = self.get_new_method_node_ids()  # result like [{'id(m)': 211325}]
        for qres in new_method_node_ids:
            self.set_nodes_parent_id(method_node_id=qres['id(m)'])

        # NOTE: here we regenerate uri to include parent_id(study id) in the uri this becomes an instance of
        # the selected method specifically for the study(or pooled analysis) - i.e. database
        if self.schema_available:  # limit URI generation to the newly loaded method nodes
            self.interface.rdf_generate_uri({key: item
                                             for key, item in ModelManager.URI_MAP.items()
                                             if key in ["Method"]
                                             })
            self.explicit_inputs_of_method()
            self.explicit_outputs_of_method()

        else:  # need to generate URIs on all nodes loaded with the method
            self.interface.rdf_generate_uri(ModelManager.URI_MAP)

        if not self.batch:
            _ = self.db_id  # get db_id

    def build_retrieve_actions_params(self):
        return {"method_id": self.name, "parent_id": self.study}

    def handle_supermethod(self, action_info, previous_actions=None):
        previous_actions = previous_actions if previous_actions is not None else []
        if action_info["type"] == "apply_stat":
            return ApplyStatSuperMethod(method=self, interface=self.interface, action_info=action_info, previous_actions=previous_actions)
        if action_info["type"] == "decode":
            return DecodeSuperMethod(method=self, interface=self.interface, action_info=action_info, previous_actions=previous_actions)
        if action_info["type"] == "subject_level_link":
            return SubjectLevelLinkSuperMethod(method=self, interface=self.interface, action_info=action_info, previous_actions=previous_actions)
        return SuperMethod(method=self, interface=self.interface, action_info=action_info)

    def check_method_actions_order(self):
        for i, action in enumerate(self.actions):
            if isinstance(action, GetData):
                if i == len(self.actions) - 1 or isinstance(self.actions[i + 1], GetData):
                    raise TypeError(f"get_data cannot be last or followed by get_data Method Action (index {i})")
            
    def get_new_method_node_ids(self):
        q = """
        MATCH (m:Method{id:$method_id})
        WHERE m.parent_id IS NULL
        RETURN id(m)
        """
        params = {"method_id": self.name}
        return self.interface.query(q, params)

    def set_nodes_parent_id(self, method_node_id):
        q = """
        MATCH (m:Method)
        WHERE id(m) = $method_node_id
        SET m.parent_id = $parent_id
        WITH *
        MATCH (m)-[:METHOD_ACTION*1..2]->(child:Method)
        RETURN id(child), m.id
        """
        params = {"method_node_id": method_node_id, "parent_id": self.study}
        results = self.interface.query(q, params)
        for qres in results:
            self.interface.query(q, {"method_node_id": qres["id(child)"],
                                     "parent_id": "_".join([self.study, qres["m.id"]])})

    def apply_generator(self, set_branch_dict: dict = None, limit: int = 0, apply_changes: bool = True):
        if set_branch_dict is None:
            set_branch_dict = {}
        if not self.applied:
            self.check_method_actions_order()
            logger.info(f"Applying {self.name} for Study {self.study}")
            logger.info(f"\tTotal # of Actions: {len(self.actions)}")
            df = None
            for i, action in enumerate(self.actions):
                logger.info(f"Applying {action}: #{i + 1} of {len(self.actions)} of ({self.name})")
                start_time = time.time()

                if isinstance(action, GetData):
                    df = action.apply(df, limit=limit)

                elif isinstance(action, CallAPI) and (action.meta.get('github_repo') in set_branch_dict):
                    # when a CallAPI action has a github_repo that is found in set_branch_dict, we use that branch for
                    # the API call
                    repo = action.meta.get('github_repo')
                    df = action.apply(df, github_branch=set_branch_dict[repo])

                elif isinstance(action, (Link, AssignLabel)):
                    if apply_changes:
                        df = action.apply(df)
                    else:
                        logger.info(f'\tSkipping action as applying in limited mode')
                        continue
                else:
                    df = action.apply(df)
                logger.debug(f"\t\tCompleted in: {(time.time() - start_time):.2f}' seconds")
                if df is None:
                    logger.warning(f'Action {action} returned a None df!')
                yield df

    def apply(self, overwrite_derived_data=None, set_branch_dict: dict = None, limit: int = 0, apply_changes=True):
        """
        :param set_branch_dict: Dictionary of form {'repo': 'branch', ...} to allow the CallAPI action to use branches
        that are not main
        :param overwrite_derived_data: Bool
        :param limit: int, number of rows to return where 0 is unlimited
        :param set_branch_dict: dict, a dictionary of {'github_branch': branch, 'github_token': token, 'github_baseurl': baseurl} to be passed to call_api actions if required
        :param apply_changes: Bool
        :return:
        """
        if set_branch_dict is None:
            set_branch_dict = {}
        if self.applied:
            if overwrite_derived_data:
                OnlineDerivationMethod(name=self.name, interface=self.interface).rollback()
            else:
                logger.warning(
                    f"Method {self.name} has been derived, to reapply, either rollback or set overwrite_derivation_data=True"
                    f"Continuing...")
                return logger.info(f"Continuing...")
        return list(self.apply_generator(set_branch_dict, limit, apply_changes))[-1]

    def apply_limited(self, overwrite_derived_data=None, set_branch_dict: dict = None, limit: int = 10):
        logger.info(f'Running apply in limited mode with limit of {limit} data rows')
        return self.apply(overwrite_derived_data, set_branch_dict, limit, apply_changes=False)

    def explicit_inputs_of_method(self):
        params = {'method_id': self.name}
        wh = ("" if self.batch else "WHERE m.id = $method_id")
        term_clause = """
            OPTIONAL MATCH (class)-[:HAS_CONTROLLED_TERM]->(term:Term),
            (term)<-[:ON_VALUE]-(:Method)<-[:NEXT]-(action)
            """
        q = f"""
        MATCH (m:Method)
        {wh}
        WITH *
        MATCH p=(m)-[:METHOD_ACTION*1..50]->(action:Method) //TODO: check that deep trees >10 never exist 
        WHERE action.type = 'get_data'
        OPTIONAL MATCH (action)-[r:SOURCE_CLASS]->(class:Class)
        {term_clause}
        WITH m, action, collect(CASE WHEN term IS NULL THEN class else term END) as coll1
        OPTIONAL MATCH (action)-[r2:SOURCE_RELATIONSHIP]->(rel:Relationship)-[:TO|:FROM]->(class)
        {term_clause}
        WITH m, coll1, collect(CASE WHEN term IS NULL THEN rel else term END) as coll2
        WITH m, coll1+coll2 as coll
        UNWIND coll as input
        MERGE (m)-[:METHOD_INPUT]->(input)
        """
        self.interface.query(q, params)

    def explicit_outputs_of_method(self):
        params = {'method_id': self.name}
        wh = ("" if self.batch else "WHERE m.id = $method_id")
        q = f"""
        MATCH (m:Method)
        {wh}   
        WITH *
        MATCH p=(m)-[:METHOD_ACTION*1..10]->(action:Method) //TODO: check that deep trees >10 never exist 
        WHERE action.type in ['assign_class']
          OR
          (action.type in ['link'] and (
              EXISTS ( (action)-[:TO_VALUE]->() )
                OR
              EXISTS ( (action)-[:FROM_VALUE]->() )
          ))
          OR
          (action.type in ['link'] and
            EXISTS ( (action)-[:LINK]->() )
          )
        OPTIONAL MATCH (action)-[dir_value]->(term:Term)
        ,(term)<-[:HAS_CONTROLLED_TERM]-(class:Class)
        WHERE type(dir_value) in ["TO_VALUE", "FROM_VALUE"]
        OPTIONAL MATCH (action)-[:LINK]->(rel1:Relationship)-[dir_class]->(class)                
        WHERE type(dir_class) = apoc.text.split(type(dir_value), "_")[0]
        OPTIONAL MATCH (action)-[assign_class:CLASS]->(a_class:Class)
        OPTIONAL MATCH (action)-[rl:LINK]->(rel2:Relationship)
        OPTIONAL MATCH (rel2)-[r2class:TO|FROM]->(class2)
        WHERE type(r2class) = CASE WHEN rl.how in ['merge_from', 'create_from', 'merge_from_on_uri'] THEN 'FROM' ELSE 'TO' END
        WITH DISTINCT 
        m, 
        a_class, 
        CASE WHEN term IS NULL THEN class else term END as output_1,
        rel2 as output_2
        WITH m, collect(output_1) + collect(output_2) + collect(distinct a_class) as coll
        UNWIND coll as output       
        MERGE (m)-[:METHOD_OUTPUT]->(output)               
        """
        self.interface.query(q, params)

    def fetch_metadata(self):
        pass

    def _get_derivation_node_id(self):
        raise NotImplementedError

    def merge_action_json(self, name, action_json_list: list, derivation_method_json: dict = None):
        """
        Merge actions from action_json_list into provided derivation_method_json OR create a new derivation_method_json if not provided.
        :param name: Str name of derivation
        :param action_json_list: List of jsons representing actions
        :param derivation_method_json: json representing a derivation method
        """

        def get_last_action_node_id(method_json):
            """Use topological sorting to find the last action node id from the NEXT rels of the method json"""
            next_rels = []
            for rel in method_json['relationships']:
                if rel.get('type') == 'NEXT':
                    next_rels.append((rel.get('fromId'), rel.get('toId')))
            action_node_ids, _ = topological_sort(next_rels)
            return action_node_ids[-1]
        

        def get_unique_key(node, label, prop_filter):
            if label == 'Method':
                key = f'{node[prop_filter]} {label}'
            else:
                key = f'{node["properties"][prop_filter]} {label}'
            return key


        def rel_key(id_, type_):
            return f'{id_}{type_}'


        if derivation_method_json is None:
            derivation_method_json = {
                "nodes":
                    [
                        {
                            "id": "core0",
                            "position": {},
                            "caption": "",
                            "labels": ["Method"],
                            "properties": {"id": name}
                        }
                    ],
                "relationships": [],
                'style': {}
            }
            previous_method_node_id = None
        else:
            previous_method_node_id = get_last_action_node_id(derivation_method_json)

        filter_node_dict = {
            'Class': 'label',
            'Term': 'Term Code',
            'Method': 'id',
            'Relationship': 'unique_key'  # get set as a temp property below
        }

        # set up some tracking vars for merging nodes
        node_map = {}  # {node_id: node, ...}
        rel_map = {}  # {fromId + type: rel, ...}
        current_nodes = {}  # {'Analysis Value Class': 'n95'} ie {property: id}
        
        # compile a map of node ids
        for node in derivation_method_json['nodes']:
            node_map[node.get('id')] = node

        for rel in derivation_method_json['relationships']:
            # compile a map of relationships
            _key = rel_key(id_=rel.get('fromId'), type_=rel.get('type'))
            rel_map[_key] = rel

        for action_json in action_json_list:
            
            node_indexes_to_skip = []

            for node in action_json['nodes']:
                node_map[node.get('id')] = node

            for rel in action_json['relationships']:
                # compile a map of relationships
                _key = rel_key(id_=rel.get('fromId'), type_=rel.get('type'))
                rel_map[_key] = rel

            for node_index, node in enumerate(action_json['nodes']):
                if 'Method' in node['labels']:

                    # With branch_combine, can get duplicate methods. Skip over them here as they get added to
                    # current_nodes further down
                    if get_unique_key(node, 'Method', 'id') not in current_nodes.keys():

                        # add the METHOD_ACTION relationship from the core/super method node
                        derivation_method_json['relationships'].append(
                            {
                                'id': f'ma_rel_{node["id"]}',
                                'fromId': self._get_derivation_node_id(),
                                'toId': node['id'],
                                'type': 'METHOD_ACTION',
                                'properties': {},
                                'style': {}
                            }
                        )

                        # add the NEXT relationship if this is not the first method node
                        if previous_method_node_id is not None:
                            # check to make sure there isn't already a next from previous json creation.
                            # E.g. get_data and filter
                            if not any(rel for rel in action_json['relationships']
                                        if
                                        (rel.get('fromId') == previous_method_node_id) and (rel.get('type') == 'NEXT')):

                                derivation_method_json['relationships'].append(
                                    {
                                        'id': f'next_rel_{node["id"]}',
                                        'fromId': previous_method_node_id,
                                        'toId': node['id'],
                                        'type': 'NEXT',
                                        'properties': {},
                                        'style': {}
                                    }
                                )

                        previous_method_node_id = node['id']

                # need to add a property 'unique_key' to each rel node in order to test for uniqueness later.
                if 'Relationship' in node.get('labels'):
                    # fetching the node TO and FROM the relationship node to use their labels for a unique key
                    from_key = rel_key(id_=node.get('id'), type_='FROM')
                    to_key = rel_key(id_=node.get('id'), type_='TO')
                    from_rel = rel_map.get(from_key)
                    to_rel = rel_map.get(to_key)
                    from_node = node_map.get(from_rel.get('toId'))
                    to_node = node_map.get(to_rel.get('toId'))

                    # 'from_node_label + rel_node_type + to_node_label' as unique key
                    _key = f'{from_node.get("properties").get("label")}' \
                        f'{node.get("properties").get("relationship_type")}' \
                        f'{to_node.get("properties").get("label")}'

                    node['properties']['unique_key'] = _key

                for label in filter_node_dict.keys():
                    if label in node['labels']:
                        prop_filter = filter_node_dict[label]  # label, relationship_type
                        unique_node_key = get_unique_key(node, label, prop_filter)
                        # eg Analysis Value Class, Analysis Value Relationship
                        # need to check for nodes that could already be present and 'merge' them
                        if unique_node_key in current_nodes.keys():
                            # find all rels to/from this node, then point them to the preexisting node
                            for rel in action_json['relationships']:
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

                # remove the temp unique key added above
                if 'Relationship' in node.get('labels'):
                    del node['properties']['unique_key']

            # add all the nodes and relationships to the derivation method
            derivation_method_json['nodes'].extend([node for i, node in enumerate(action_json['nodes'])
                                                    if i not in node_indexes_to_skip])
            derivation_method_json['relationships'].extend(action_json['relationships'])

            # some cases of an empty rel, ie {}, being in the relationships list. Remove them here. Also remove duplicates.
            unique_rels = []
            unique_rel_keys = set()
            for rel in derivation_method_json['relationships']:
                if rel:
                    _key = rel.get('fromId') + rel.get('type') + rel.get(
                        'toId')  # expecting only one unique rel between two nodes
                    if _key not in unique_rel_keys:
                        unique_rel_keys.add(_key)
                        unique_rels.append(rel)
            derivation_method_json['relationships'] = unique_rels

        return derivation_method_json

    def build_derivation_method_json(self, name=None, actions=None, json_str=True):
        if not name:
            name = self.name
        if not actions:
            self._actions = None
            actions = self.actions

        logger.info(f"Compiling json: {name}")

        action_json_list = [action.retrieve_json() for action in actions]
        derivation_method_json = self.merge_action_json(name, action_json_list)
        if json_str:
            return json.dumps(visualise_json(derivation_method_json), indent=4)
        else:
            return derivation_method_json


class OnlineDerivationMethod(DerivationMethod):
    def __init__(self, name, interface=None, study=None, schema_available=None):
        super().__init__(data=name, name=name, interface=interface, study=study, schema_available=schema_available)
        self.interface.create_index(label='Method', key='parent_id')
        self.batch = self.name == "online"
        self.available_methods = self._available_methods()
        self.applied_methods = self._applied_methods()

    @property
    def actions(self):
        if not self.exists_in_db:
            logger.error(f"Method {self.name} does not exist in database for study {self.study}. "
                         f"Unable to retrieve Actions. First load the method")
        if self._actions is None:
            self._actions = self.retrieve_actions()
        return self._actions

    def _delete_orphaned_nodes(self, detached_nodes_dicts: list):
        node_ids = list(set(reduce(add, [detached_node["detached_nodes"] for detached_node in detached_nodes_dicts])))

        q = f"""
        MATCH (n)
        WHERE id(n) in $node_ids AND NOT n:Term
        OPTIONAL MATCH (n)-[r2]-()
        WHERE NOT type(r2) = "IS_A"
        WITH size(collect(type(r2))) as rel_count, n as n, id(n) as node_id, labels(n) as labels
        WHERE rel_count = 0
        DETACH DELETE n
        RETURN distinct node_id, labels
        """
        params = {"node_ids": node_ids}
        deleted_nodes = self.interface.query(q, params)
        for labels in set([tuple(node["labels"]) for node in deleted_nodes]):
            logger.debug(
                f"\t\tDeleted {len([node for node in deleted_nodes if labels == tuple(node['labels'])])} with Label {labels}")

        q2 = f"""
        MATCH (n)
        WHERE id(n) in $node_ids
        RETURN id(n) as node_id, labels(n) as labels
        """
        remaining_nodes = self.interface.query(q2, params)
        for labels in set([tuple(node["labels"]) for node in remaining_nodes]):
            logger.debug(
                f"\t\tNOT Deleted {len([node for node in remaining_nodes if labels == tuple(node['labels'])])} with label {labels}")

        if len(remaining_nodes) + len(deleted_nodes) != len(node_ids):
            error_msg = f"Number of remaining nodes ({len(remaining_nodes)} + {len(deleted_nodes)} is not equal to {len(node_ids)})"
            logger.error(error_msg)

    def _available_methods(self) -> list:
        q = f"""
            MATCH (m:Method {{parent_id:$study_id}})
            WHERE m.type IS NULL AND NOT (m)<-[:METHOD_ACTION]-() 
            RETURN m.id as method_id
            """
        params = {'study_id': self.study}
        res = self.interface.query(q, params)
        if not res:
            logger.warning(f"There are no methods in database for study {self.study}.")
        return [method.get("method_id") for method in res]

    def _applied_methods(self) -> list:
        q = f"""
            MATCH (m:Method {{parent_id:$study_id}})-[r:METHOD_ACTION]->(action:Method), (action)-[:APPLIED]->(chg:Changes)
            RETURN distinct m.id as method_id
            """
        params = {'study_id': self.study}
        res = self.interface.query(q, params)
        # if not res:
        #     logger.warning(f"There are no applied methods in database for study {self.study}.")
        #     logger.warning(f"The following methods have been loaded but not applied:")
        #     logger.warning(f"{self.available_methods}")
        return [method.get("method_id") for method in res]

    def _get_derivation_node_id(self):
        """All core id nodes in OnlineDerivationMethod will be `core0`"""
        return 'core0'

    def rollback(self):
        if self.applied:
            detached_nodes_dct = []
            # TODO: check if action.applied_metadata_node_id is None for relevant actions
            logger.info(f"{self.name}: Starting Rollback")
            logger.info(f"\tTotal # of Actions: {len(self.actions)}")
            for i, action in enumerate(self.actions):
                logger.info(f"Rolling back Action: #{i + 1} of {len(self.actions)}")
                detached_nodes_dct = action.rollback(detached_nodes_dct)
            if len(detached_nodes_dct) > 0:
                self._delete_orphaned_nodes(detached_nodes_dct)
            logger.info(f"\t\t Completed Rollback!")
        else:
            logger.error(f"Method {self.name} has not been applied for study {self.study}. Nothing to rollback.")

    def delete(self):
        logger.info(f"""Deleting: {self.name if not self.batch else 'ALL derivation methods'}  from the database""")
        if self.batch:
            for method in self.applied_methods:
                OnlineDerivationMethod(name=method, interface=self.interface).rollback()
        elif self.name in self.applied_methods:
            self.rollback()
        params = {"method_id": self.name, "study_id": self.study}
        wh = ("" if self.batch else "WHERE m.id = $method_id and m.parent_id = $study_id")
        actions = [
            f"""
            MATCH (m:Method)
            {wh}
            OPTIONAL MATCH path = (m)-[r:METHOD_ACTION|NEXT*1..10]->(x:`Method`)
            OPTIONAL MATCH path2 = (x)-[r2]->(y)
            //OPTIONAL MATCH (m)-[r*1..10]->(x:`Method`)-[r2]->(y)     
            DETACH DELETE r2, x
            """,
            f"""
            MATCH (m:Method)
            {wh}
            OPTIONAL MATCH (m)-[r]-()
            DELETE r
            DELETE m
            """
        ]
        for q in actions:
            self.interface.query(q, params)

    def explicit_prerequisites_of_method(self):
        """
        Identifies dependenices between Methods and explicitly stores as METHOD_PREREQ relationship
        Caveat:
            only works for -[:METHOD_INPUT]->(r:Relationship)
            for derived Classes we don't have -[:METHOD_INPUT]->(:Class) and should avoid in the future
        :return:
        """
        params = {} if self.batch else {'method_id': self.name}
        wh = "WHERE m.type IS NULL AND NOT (m)<-[:METHOD_ACTION]-()"
        wh += ("" if self.batch else "AND m.id = $method_id")
        q = f"""
        MATCH (m:Method)
        {wh} 
        MATCH (m)-[:METHOD_INPUT]->(r:Relationship)-[:FROM|TO]->(c:Class)
        WHERE NOT (m)-[:METHOD_INPUT]->(c)
        OPTIONAL MATCH (c)-[:HAS_CONTROLLED_TERM]->(t:Term)<-[:METHOD_INPUT]-(m)
        WHERE EXISTS ( ()-[:METHOD_OUTPUT]->(t) )
        WITH m, coalesce(t, r) as input
        MATCH (prereq:Method)
        MATCH (prereq:Method)-[:METHOD_OUTPUT]->()<-[:FROM|TO*0..1]-(input)
        WHERE (prereq.type IS NULL AND NOT (prereq)<-[:METHOD_ACTION]-())
        WITH DISTINCT m, collect(DISTINCT prereq) as coll
        UNWIND coll as prereq
        MERGE (m)-[:METHOD_PREREQ]->(prereq)
        RETURN * 
        """
        self.interface.query(q, params)

    def resolve_methods_order(self, error_cutoff: int = 1000):
        """
        :param error_cutoff:
        :return:
        """

        def get_batch(resolved: list = None):
            if not resolved:
                resolved = []
            q = """
            MATCH (m:Method)
            WHERE m.type IS NULL AND NOT (m)<-[:METHOD_ACTION]-() AND NOT m.id in $resolved
            OPTIONAL MATCH (m)-[:METHOD_PREREQ]->(prereq)
            WHERE NOT prereq.id in $resolved AND NOT prereq = m
            WITH m, collect(prereq) as coll
            WHERE size(coll) = 0
            RETURN m.id as method, id(m) as db_id
            """
            params = {'resolved': resolved}
            res = self.interface.query(q, params)
            return [x['method'] for x in res]

        resolved = []
        p_resolved = None
        i = 0
        while resolved != p_resolved and i < error_cutoff:
            p_resolved = resolved.copy()
            batch = get_batch(resolved=resolved)
            resolved += batch
            i += 1

        return resolved


class DictDerivationMethod(DerivationMethod):
    """
    A DerivationMethod that has been loaded from json external to the database.

    :param content: Dictionary of json representing the DerivationMethod. Contains two keys 'nodes' and 'relationships'
    where each value is a list of dictionaries representing nodes and relationships respectively.
    :param name: The name of the DerivationMethod
    :param interface: A neointerface object for interfacing with the database
    :param study: The name of the study this DerivationMethod is a part of
    :param overwrite_db: Bool representing the decision of overwriting a derivation of the same name in the database on load
    :param check_schema: Bool representing the decision to check on load if a derivation's schema is present in the database
    """


    def __init__(self, content, name=None, interface=None, study=None, overwrite_db=False,
                 schema_available=None, check_schema:bool=False):
        super().__init__(data=content,
                         name=self.get_derivation_name(content) if name is None else name,
                         interface=interface,
                         study=study,
                         schema_available=schema_available)
        
        self.predicted_output_class_info = None
        self.content = content
        self.load(overwrite_db=overwrite_db, check_schema=check_schema)

    @property
    def actions(self):
        if self._actions is None:
            self._actions = self.retrieve_actions()
        return self._actions

    def get_derivation_name(self, content):
        next_rels = reduce(add, [[rel["fromId"], rel["toId"]] for rel in content["relationships"] if
                                 rel["type"] == "NEXT"])

        for node in content["nodes"]:
            if "Method" in node["labels"] and list(node["properties"].keys()) == ["id"] and node["id"] not in next_rels:
                self.name = node["properties"]["id"]
                return self.name
        logger.warning(f"Cannot identify derivation method name from JSON dictionary.")
        raise KeyError

    def _get_derivation_node_id(self):
        next_rels = reduce(add, [[rel["fromId"], rel["toId"]] for rel in self.content["relationships"] if
                                 rel["type"] == "NEXT"], [])

        for node in self.content["nodes"]:
            if "Method" in node["labels"] and list(node["properties"].keys()) == ["id"] and node["id"] not in next_rels:
                return node["id"]
        logger.warning(f"Cannot identify derivation method id from JSON dictionary.")
        raise KeyError

    def load(self, overwrite_db:bool, check_schema: bool = False):
        if self.load_decision(overwrite_db):
            if check_schema:
                self.validate_method_schema()
            if self.exists_in_db:
                OnlineDerivationMethod(name=self.name, interface=self.interface, study=self.study).delete()
            self._load()

    def _load(self):
        super().load()
        self.enrich_existing_relationship_nodes()
        if self.schema_available:
            self.validate_method_dict(self.content["nodes"], self.content["relationships"])
        self.content, merge_on = ModelManager.arrows_dict_uri_dict_enrich(
            dct=simplify_arrows_json(self.content),
            uri_map=ModelManager.URI_MAP)
        self.interface.load_arrows_dict(self.content, merge_on, always_create=['Method'])
        self.post_load_enrichment()

        self._actions = None  # reset action metadata in memory. Next time self.actions is called, it will fetch metadata from database.

        return self.content

    def get_action_type_ids(self, type: str) -> List[int]:
        # Assuming that all links have id of format 'linkx' where x is an integer
        id_integers = [0]
        for node in self.content['nodes']:
            if "properties" in node:
                if node['properties'].get('type') == type:
                    id_int = node['properties']['id'].replace(type, '')
                    id_integers.append(id_int)
        return id_integers

    @property
    def predict_output_classes(self) -> dict:
        if self.predicted_output_class_info is None:
            self.predicted_output_class_info = self._predict_output_classes()
        return self.predicted_output_class_info

    def _predict_output_classes(self) -> dict:
        """
        Predict the output classes of current derivation method (from schema/run_script/assign_class info)
        Runs a limited self.apply() in order to get the most up to date changes node for any run_script actions.
        :return: dict of format {'ASSIGN_CLASSES': [], 'CLASSES AFTER RUNSCRIPT': ['AAGE', 'USUBJID'], 'NEW RUNSCRIPT CLASSES': ['AGEGR'], 'PREDICTED CLASSES': ['AGEGR']}
        """

        self.apply_limited(overwrite_derived_data=False)

        logger.info(f'\tPredicting output classes {self.name}')

        filepath = os.path.dirname(__file__)
        with open(os.path.join(filepath, 'predict_output_classes.cql')) as cypherfile:
            query = cypherfile.read()
        params = {
            "method_id": self.name,
            "study_id": self.study
        }
        res = self.interface.query(query, params)[0]
        
        for key in ['ASSIGN_CLASSES', 'CLASSES AFTER RUNSCRIPT', 'NEW RUNSCRIPT CLASSES', 'PREDICTED CLASSES']:
            if res[key] is None:
                res[key] = []
        
        if not res['PREDICTED CLASSES']:
            logger.debug(f'Query \n {query}')
            logger.debug(f':params {params}')
            logger.warn(f'Predicted 0 output classes for {self.name}')
        else:
            logger.info(f'Predicted classes: {res["PREDICTED CLASSES"]}')
            logger.debug(f'predict_output_classes result: {res}')

        return res

    def _predict_links(self) -> tuple:
        # generate info about predicted links for this derivation method
        logger.info(f'\tPredicting links actions for {self.name}')

        filepath = os.path.dirname(__file__)
        filename = 'predict_links.cql'
        with open(os.path.join(filepath, filename)) as cypherfile:
            query = cypherfile.read()
        params = self.predict_output_classes

        res = self.interface.query(query, params)
        if not res:
            logger.debug(f'Query \n {query}')
            logger.debug(f'Params {params}')
            logger.warn(f'Predicted 0 links for {self.name}')
            return []
        else:
            res = res[0]['PREDICTED_LINKS']

        new_links = []
        subject_level_classes = []

        for predicted_link in res:
            cls = predicted_link.get('C_PRIMARY')
            from_classes = predicted_link.get('PREDICTED_FROM_CLASSES', [])
            to_classes = predicted_link.get('PREDICTED_TO_CLASSES', [])

            logger.debug(f'\t\tPrimary Class: {cls}')
            logger.debug(f'\t\tPredicted from classes: {from_classes}')
            logger.debug(f'\t\tPredicted from classes: {to_classes}')

            if cls.get('aval_repr', False):
                logger.debug(f'\t\tPrimary Class: {cls} is a Subject Level Class')
                subject_level_classes.append(cls)

            for from_class in from_classes:
                new_links.append(
                    {
                        "from_class": from_class.get('label'),
                        "from_short_label": from_class.get('short_label'),
                        "to_class": cls.get('label'),
                        "to_short_label": cls.get('short_label'),
                        "relationship_type": cls.get('label')  # todo this should be rel type from schema
                    }
                )
            for to_class in to_classes:
                new_links.append(
                    {
                        "from_class": cls.get('label'),
                        "from_short_label": cls.get('short_label'),
                        "to_class": to_class.get('label'),
                        "to_short_label": to_class.get('short_label'),
                        "relationship_type": to_class.get('label')  # todo this should be rel type from schema
                    }
                )

        return new_links, subject_level_classes

    def _generate_link_actions(self) -> list:
        """
        Create Link action objects and then convert them to json.
        """
        logger.info(f'Generating link actions for {self.name}')

        base_link_id = max(self.get_action_type_ids(type='link'))

        predicted_links, subject_level_classes = self._predict_links()

        if not predicted_links:
            return []
        new_link_jsons = []

        for i, link in enumerate(predicted_links, start=1):
            new_link_jsons.append(
                Link(
                    action_dict={
                        "id": f"link{base_link_id+i}",
                        "type": "link",
                        "relationship_type": link.get('relationship_type'),
                        "from_class": link.get('from_class'),
                        "from_short_label": link.get('from_short_label'),
                        "to_class": link.get('to_class'),
                        "to_short_label": link.get('to_short_label'),
                        "to_class_property": 'rdfs:label',
                        "link_how": 'merge'  # link.get('link_how')
                    },
                    interface=self.interface,
                    dont_fetch=True
                ).retrieve_json()
            )

        for i, cls in enumerate(subject_level_classes, start=1):
            new_link_jsons.append(
                SubjectLevelLinkSuperMethod(
                    method=self,
                    interface=self.interface,
                    previous_actions=self.actions,
                    action_info={"type": 'subject_level_link', "id": f"subject_level_link{i}"},
                    action_dict={
                        "m": {
                            "type": 'subject_level_link',
                            "id": f"subject_level_link{i}"
                        },
                        "class": cls,
                        "term": None
                    }
                ).retrieve_json()
            )

        return new_link_jsons

    def merge_predicted_links(self):
        """
        Generate predicted links and merge them with the current method json held in self.content
        Will run a limited (10 row) self.apply() to generate changes nodes for any run_script actions.
        :param load_to_db: Bool that decided if new method json should be loaded into the database
        """

        link_json_list = self._generate_link_actions()
        if link_json_list:
            self.content = self.merge_action_json(
                name=self.name, action_json_list=link_json_list, derivation_method_json=self.content)
            self.load(overwrite_db=True)
            self._actions = None  # so a future call to self.actions will pull new actions from database
        else:
            logger.warn(f'Could not predict links for {self.name}')

    def fetch_get_data_classes(self) -> List[str]:
        """
        Return a list of classes found in GetData actions of this derivation method
        :return: list of class strings
        """
        get_data_classes = []
        for action in self.actions:
            if isinstance(action, GetData):
                get_data_classes += action.meta["source_classes"]
                get_data_classes += [rel.get('from') for rel in action.meta["source_rels"]]
                get_data_classes += [rel.get('to') for rel in action.meta["source_rels"]]
        return get_data_classes

    def _fetch_uri_meta_from_db(self) -> dict:
        predicted_output_classes = self.predict_output_classes
        output_classes = predicted_output_classes.get('PREDICTED CLASSES')  # list of short labels

        q = """
        MATCH (c:Class) 
        WHERE (c.short_label in $output_classes) AND (c.classes_for_uri IS NOT NULL)
        WITH c, [x in split(c.classes_for_uri, "|") | trim(x)] as short_cls_uri
        CALL apoc.cypher.run(
            "MATCH (c_uri:Class) WHERE c_uri.short_label in $short_cls_uri RETURN c_uri",
            {short_cls_uri: short_cls_uri}
        ) YIELD value
        WITH c, apoc.map.fromPairs([['label', value.c_uri.label], ['short_label', value.c_uri.short_label]]) as uri_map
        RETURN c as class, collect(uri_map) as uri_labels
        """
        params = {"output_classes": output_classes}
        res = self.interface.query(q, params)
        # list of class dictionaries e.g. 
        # [
        #   {
        #       'class': {'short_label': 'AAGE', 'label': 'Analysis Age', 'uri': 'neo4j://graph.schema#Class/Analysis+Age', 'derived': 'true', 'classes_for_uri': 'USUBJID|AVISIT'},
        #      'uri_labels': [{'label': 'Subject', 'short_label': 'USUBJID'}, {}, ...]
        #   },
        #   {}, ...
        # ]
        if not res:
            logger.warn(f'Could not find uri metadata for classes: {output_classes}')
        return res

    def _create_build_uri_actions(self) -> list:
        """
        Build a list of json dictionaries that represent build_uri actions based on `classes_for_uri` property on schema classes
        """
        uri_meta = self._fetch_uri_meta_from_db()
        base_uri_id = max(self.get_action_type_ids(type='build_uri'))
        build_uri_actions = []
        for i, item in enumerate(uri_meta, start=1):
            cls = item.get('class')
            uri_labels = [uri.get('label') for uri in item.get('uri_labels')]
            uri_short_labels = [uri.get('short_label') for uri in item.get('uri_labels')]

            build_uri_actions.append(
                BuildUri(
                    action_dict={
                        "id": f"build_uri{base_uri_id+i}",
                        "type": "build_uri",
                        "prefix": "",
                        "prefix": "",
                        "uri_fors": [cls.get("short_label")],
                        "uri_fors_long": [cls.get("label")],
                        "uri_bys": uri_short_labels,
                        "uri_bys_long": uri_labels,
                        "uri_labels": [],
                        "uri_labels_long": []
                    },
                    interface=self.interface,
                    dont_fetch=True
                ).retrieve_json()
            )
        return build_uri_actions

    def merge_build_uri_from_schema(self):
        """
        Add BuildUri actions to the derivation method for classes that have property `classes_for_uri` in the schema.
        Expects `classes_for_uri` property to have string of pipe `|` separated values.
        Will run a limited (10 row) self.apply() to generate changes nodes for any run_script actions.
        :param load_to_db: Bool that decided if new method json should be loaded into the database
        """

        build_uri_list = self._create_build_uri_actions()
        if build_uri_list:
            self.content = self.merge_action_json(
                name=self.name, action_json_list=build_uri_list, derivation_method_json=self.content)
            self.load(overwrite_db=True)
            # ToDo if in the future we run all merge new action functions in one go, 
            # we should only load to db after all predict functions have been run.
        else:
            logger.warn(f'Could not create any BuildUri actions for {self.name}')

    def validate_method_schema(self, method_json: dict = None):
        """
        Raise a KeyError if any schema present in the method_json is not present in the attached Neo4j database.
        :param method_json: json dictionary representing a derivation method. If None then take self.content as method_json.
        """

        logger.info(f'Checking schema is present in database')

        if method_json is None:
            method_json = self.content

        node_dct = {}
        # set up lookup dict for relationship node rels and term rels
        rel_dct_from = {}  # {fromId: {FROM: rel, TO: rel}, ...}  TO and FROM rels that come from a rel_node
        rel_dct_to = {}  # {toId: rel, ...}  rel coming from a class to a term node

        for node in method_json.get('nodes'):
            node_dct[node.get('id')] = node

        for rel in method_json.get('relationships'):
                # set up some relationships maps for later
                if rel.get('type') in ['FROM', 'TO']:
                    # no rel_node should have two FROM (or TO) rels, so unique keys
                    if rel.get('fromId') not in rel_dct_from:
                        rel_dct_from[rel.get('fromId')] = {rel.get('type'): rel}
                    else:
                        rel_dct_from[rel.get('fromId')][rel.get('type')] = rel
                elif rel.get('type') == 'HAS_CONTROLLED_TERM':
                    # no two classes should have the same term, so unique keys
                    rel_dct_to[rel.get('toId')] = rel

        missing_schema_nodes = self._check_method_schema(method_json, node_dct, rel_dct_from, rel_dct_to)

        if missing_schema_nodes.get('Classes') or missing_schema_nodes.get('Terms') or missing_schema_nodes.get(
                'Relationships') or missing_schema_nodes.get('Term Relationships'):
            missing_classes = missing_schema_nodes.get('Classes') if missing_schema_nodes.get('Classes') else ''
            missing_terms = missing_schema_nodes.get('Terms') if missing_schema_nodes.get('Terms') else ''
            missing_rels = missing_schema_nodes.get('Relationships') if missing_schema_nodes.get(
                'Relationships') else ''
            missing_term_rels = missing_schema_nodes.get('Term Relationships') if missing_schema_nodes.get(
                'Term Relationships') else ''
            logger.error(f'Some of the schema required for {self.name} could not be found in the connected database!\n {missing_classes=}\n {missing_rels=}\n {missing_terms=}\n {missing_term_rels=}')
            raise KeyError

    
    def _check_method_schema(self, json, node_dct, rel_dct_from, rel_dct_to) -> dict:
        """
        Return a dictionary of any schema from the derivation method that is not present in the attached Neo4j database.
        """
        
        missing_nodes = {
            'Terms': [],
            'Classes': [],
            'Relationships': [],
            'Term Relationships': []
        }
        decode_node_dct = {}
        decode_class_dct = {}

        for node in json.get('nodes'):

            if 'decode' == node.get('properties').get('type'):
                decode_node_dct[node.get('id')] = node

            elif 'Class' in node.get('labels'):
                missing_nodes['Classes'].append(
                    {
                        'label': node.get('properties').get('label')
                        }
                    )

            elif 'Term' in node.get('labels'):
                has_controlled_term_rel = rel_dct_to[node.get('id')]
                term_class_id = has_controlled_term_rel.get('fromId')
                class_label = node_dct[term_class_id].get('properties').get('label')

                term_dct = {
                    'Class Label': class_label,
                    'Codelist Code': node.get('properties').get('Codelist Code'),
                    'Term Code': node.get('properties').get('Term Code')
                }
                if node.get('labels'):
                    if 'Study Specific Term' not in node.get('labels'):  # allow non-existence of Study-Specific Terms
                        missing_nodes['Terms'].append(term_dct)

            elif 'Relationship' in node.get('labels'):
                from_rel_class_id = rel_dct_from[node.get('id')].get('FROM').get('toId')
                to_rel_class_id = rel_dct_from[node.get('id')].get('TO').get('toId')

                from_class_label = node_dct[from_rel_class_id].get('properties').get('label')
                to_class_label = node_dct[to_rel_class_id].get('properties').get('label')

                rel_type = node.get('properties').get('relationship_type')
                rel_dct = {'from': from_class_label, 'to': to_class_label, 'relationship_type': rel_type}

                missing_nodes['Relationships'].append(rel_dct)

        for rel in json.get('relationships'):
            if rel.get('fromId') in decode_node_dct:
                try:
                    decode_class_dct[rel.get('fromId')]
                except KeyError:
                    decode_class_dct[rel.get('fromId')] = {}
                # there is a decode method, so get its from and to class labels
                if rel.get('type') == 'FROM_CLASS':
                    decode_from_class_label = node_dct.get(rel.get('toId')).get('properties').get('label')
                    decode_class_dct[rel.get('fromId')]['FROM_CLASS'] = decode_from_class_label
                if rel.get('type') == 'TO_CLASS':
                    decode_to_class_label = node_dct.get(rel.get('toId')).get('properties').get('label')
                    decode_class_dct[rel.get('fromId')]['TO_CLASS'] = decode_to_class_label

        if missing_nodes['Classes']:
            # check to see if classes are in the database.
            missing_nodes['Classes'] = self._check_classes_against_db(missing_nodes['Classes'])

        if missing_nodes['Relationships']:
            # check to see if relationships are in the database.
            missing_nodes['Relationships'] = self._check_rels_against_db(missing_nodes['Relationships'])

        if missing_nodes['Terms']:
            # check to see if terms are in the database.
            missing_nodes['Terms'] = self._check_terms_against_db(missing_nodes['Terms'])

        if decode_class_dct:
            # check to see if SAME_AS rels exist between these classes
            missing_nodes['Term Relationships'] = self._check_term_schema_against_db(decode_class_dct)

        return missing_nodes

    def _check_term_schema_against_db(self, decode_class_dct):

        missing_class_pairs = []
        for _, classes in decode_class_dct.items():
            q = '''
                MATCH (from_class:Class), (to_class:Class)
                WHERE from_class.label = $FROM_CLASS AND to_class.label = $TO_CLASS
                MATCH (from_class)-[:HAS_CONTROLLED_TERM]->(from_class_term:Term)
                ,(to_class)-[:HAS_CONTROLLED_TERM]->(to_class_term:Term)
                ,(from_class_term)-[r_sa:SAME_AS]->(to_class_term)
                RETURN r_sa
                '''
            params = classes
            res = self.interface.query(q, params)
            if not res:
                missing_class_pairs.append([classes.get('FROM_CLASS'), classes.get('TO_CLASS')])
            return missing_class_pairs

    def _check_rels_against_db(self, rel_dcts):
        # check to see if a list of relationship exist in the database
        missing_rels = []
        for rel in rel_dcts:
            q = '''
            MATCH path = (c_from:Class)<-[:FROM]-(rel:Relationship)-[:TO]->(c_to:Class)
            WHERE c_from.label = $from AND c_to.label = $to AND rel.relationship_type = $relationship_type
            RETURN path
            '''
            params = rel
            res = self.interface.query(q, params)
            if not res:
                missing_rels.append(rel)
        return missing_rels

    def _check_classes_against_db(self, class_dicts):
        # check to see if a list of classes exist in the database
        missing_classes = []
        for cls in class_dicts:
            q = '''
            MATCH (c:Class)
            WHERE c.label = $label
            RETURN c
            '''
            params = cls
            res = self.interface.query(q, params)
            if not res:
                missing_classes.append(cls)
        return missing_classes

    def _check_terms_against_db(self, term_dcts):
        # check to see if a list of terms exist in the database
        missing_terms = []
        for term in term_dcts:
            q = '''
            MATCH path = (c:Class)-[:HAS_CONTROLLED_TERM]->(t:Term)
            WHERE c.label = $`Class Label` AND t.`Term Code` = $`Term Code`
            RETURN path
            '''
            params = term
            res = self.interface.query(q, params)
            if not res:
                missing_terms.append(term)
        return missing_terms

    def validate_method_dict(self, nodes, relationships):

        assert "nodes" in self.content.keys(), f"{self.name}: {self.content}"
        assert "relationships" in self.content.keys(), f"{self.name}: {self.content}"

        method_ids = []
        methods_by_type = {}
        rels = {}
        classes = {}
        terms = {}

        # collecting nodes in dicts
        for node in nodes:
            assert "labels" in node.keys(), f"{self.name}: {node}"
            assert "properties" in node.keys(), f"{self.name}: {node}"
            assert "id" in node.keys(), f"{self.name}: {node}"
            if "Method" in node["labels"]:
                assert "id" in node["properties"].keys(), f"Method does not have an id: {self.name}: {node}"
                method_ids.append(node["properties"]['id'])
                if "type" in node["properties"]:
                    node_type = node["properties"]["type"]
                else:
                    node_type = ""
                if node_type not in methods_by_type.keys():
                    methods_by_type[node_type] = []
                methods_by_type[node_type].append({
                    "node": node,
                    "fromRels": self._validate_method_dict_get_rels(node["id"], "fromId", relationships),
                    "toRels": self._validate_method_dict_get_rels(node["id"], "toId", relationships)
                })
            if "Relationship" in node["labels"]:
                rels[node["id"]] = {
                    "node": node,
                    "fromRels": self._validate_method_dict_get_rels(node["id"], "fromId", relationships),
                    "toRels": self._validate_method_dict_get_rels(node["id"], "toId", relationships)
                }
            if "Class" in node["labels"]:
                classes[node["id"]] = {
                    "node": node,
                    "fromRels": self._validate_method_dict_get_rels(node["id"], "fromId", relationships),
                    "toRels": self._validate_method_dict_get_rels(node["id"], "toId", relationships)
                }
            if "Term" in node["labels"]:
                terms[node["id"]] = {
                    "node": node,
                    "fromRels": self._validate_method_dict_get_rels(node["id"], "fromId", relationships),
                    "toRels": self._validate_method_dict_get_rels(node["id"], "toId", relationships)
                }

        assert "" in methods_by_type.keys(), f"{self.name} Method must have a core Method node with no type assigned, {self.content}"

        method_ids_seen = set()
        assert len(method_ids) == len(set(method_ids)), \
            f"Found duplicate method ids: {[x for x in method_ids if x in method_ids_seen or method_ids_seen.add(x)]}"

        core_method_found = False
        for m in methods_by_type[""]:
            if m["node"]["properties"]["id"] == self.name:
                core_method_found = True
            for r in m["fromRels"] + m["toRels"]:
                assert r["type"] in ["NEXT",
                                     "METHOD_ACTION"], f"{self.name} Unexpected relationship {r} with Method {m}"
        assert core_method_found, f"Core method (with id {self.name}) not found"
        get_data_actions = ["get_data"]
        if not any([m in methods_by_type.keys() for m in get_data_actions]):
            logging.warning(
                f"{self.name} Method should have at least one get_data_action {get_data_actions}: {self.content}")
        else:
            for m in methods_by_type["get_data"]:
                for r in m["fromRels"]:
                    assert r["type"] in ["NEXT", "SOURCE_CLASS", "SOURCE_RELATIONSHIP"], \
                        f"{self.name} Unexpected relationship {r} from Method {m}"
                    if r["type"] == "SOURCE_RELATIONSHIP":
                        assert r["toId"] in rels.keys(), \
                            f"{self.name} Node with label Relationship is expected at SOURCE_RELATIONSHIP: {r}"
                    if r["type"] == "SOURCE_CLASS":
                        assert r["toId"] in classes.keys(), \
                            f"{self.name} Node with label Class is expected at SOURCE_CLASS: {r}"
                for r in m["toRels"]:
                    assert r["type"] in ["NEXT",
                                         "METHOD_ACTION"], f"{self.name} Unexpected relationship {r} to Method {m}"

        filter_ms = methods_by_type.get("filter")
        if not filter_ms:
            filter_ms = []
        for m in filter_ms:
            for r in m["fromRels"]:
                assert r["type"] in ["NEXT", "ON", "ON_VALUE", "FILTER_RELATIONSHIP", "ONLY_RELATED_TO"], \
                    f"{self.name} Unexpected relationship {r} from Method {m}"
                if r["type"] == "ON":
                    assert r["toId"] in classes.keys(), \
                        f"{self.name} Node with label Class is expected at ON: {r}"
                if r["type"] == "ON_VALUE":
                    assert r["toId"] in terms.keys(), \
                        f"{self.name} Node with label Term is expected at ON_VALUE: {r}"
            for r in m["toRels"]:
                assert r["type"] in ["NEXT",
                                     "METHOD_ACTION"], f"{self.name} Unexpected relationship {r} to Method {m}"

        write_data_actions = ["link", "assign_class", "apply_stat", "decode", "subject_level_link"]
        if not any([m in methods_by_type.keys() for m in write_data_actions]):
            logging.warning(
                f"{self.name} Method should have at least one write_data_action {write_data_actions}, it had: {self._fetch_action_names_from_content()}")
        else:
            link_ms = methods_by_type.get("link")
            if not link_ms:
                link_ms = []
            for m in link_ms:
                for r in m["fromRels"]:
                    assert r["type"] in ["NEXT", "LINK", "TO_VALUE", "FROM_VALUE"], \
                        f"{self.name} Unexpected relationship {r} from Method {m}"
                    if r["type"] == "LINK":
                        assert r["toId"] in rels.keys(), \
                            f"{self.name} Node with label Relationship is expected at LINK: {r}"
                    if r["type"] == "TO_VALUE":
                        assert r["toId"] in terms.keys(), \
                            f"{self.name} Node with label Term is expected at TO_VALUE: {r}"
                    if r["type"] == "FROM_VALUE":
                        assert r["toId"] in terms.keys(), \
                            f"{self.name} Node with label Term is expected at FROM_VALUE: {r}"
                for r in m["toRels"]:
                    assert r["type"] in ["NEXT",
                                         "METHOD_ACTION"], f"{self.name} Unexpected relationship {r} to Method {m}"
            ac_ms = methods_by_type.get("assign_class")
            if not ac_ms:
                ac_ms = []
            for m in ac_ms:
                for r in m["fromRels"]:
                    assert r["type"] in ["NEXT", "ON", "CLASS"], \
                        f"{self.name} Unexpected relationship {r} from Method {m}"
                    if r["type"] in ["ON", "CLASS"]:
                        assert r["toId"] in classes.keys(), \
                            f"{self.name} Node with label Class is expected on ON/CLASS: {r}"
                for r in m["toRels"]:
                    assert r["type"] in ["NEXT",
                                         "METHOD_ACTION"], f"{self.name} Unexpected relationship {r} to Method {m}"

        # Validating Relationships
        for rel in rels.values():
            rel_TO_rels = [x for x in rel["fromRels"] if x["type"] == "TO"]
            rel_FROM_rels = [x for x in rel["fromRels"] if x["type"] == "FROM"]
            assert len(
                rel_TO_rels) == 1, f"{self.name} Relationship must have exactly 1 :TO class: {rel}: {rel_TO_rels}"
            assert len(
                rel_FROM_rels) == 1, f"{self.name} Relationship must have exactly 1 :FROM class: {rel}: {rel_TO_rels}"
            assert rel_TO_rels[0]["toId"] in classes.keys(), \
                f"{self.name} Relationship must TO-point to a Class: {rel}: {rel_TO_rels[0]}"
            assert rel_FROM_rels[0]["toId"] in classes.keys(), \
                f"{self.name} Relationship must FROM-point to a Class: {rel}: {rel_FROM_rels[0]}"

        # # Validating apply_stat
        # if "apply_stat" in methods_by_type.keys():
        #     for apply_stat_method in methods_by_type['apply_stat']:
        #         as_rels_by_type = {}
        #         for rel in rels:
        #             if rel["fromId"] == apply_stat_method['node']['id']:
        #                 if rel["type"] not in as_rels_by_type.keys():
        #                     as_rels_by_type[rel["type"]] = []
        #                 as_rels_by_type[rel["type"]].append(rel)
        #         assert as_rels_by_type.get("Result"), \
        #             f"{id} apply_stat Method must specify at least 1 Result: {apply_stat_method}"
        #         assert as_rels_by_type.get("Statistic"), \
        #             f"{id} apply_stat Method must specify at least 1 Statistic: {apply_stat_method}"

        return True

    @staticmethod
    def _validate_method_dict_get_rels(nodeid: str, key: str, rels: dict):
        res = []
        for r in rels:
            if r.get(key) == nodeid:
                res.append(r)
        return res

    def _fetch_action_names_from_content(self):
        """Go through self.content and retrieve action types for methods (actions)"""
        return [node['properties'].get('type') for node in self.content.get('nodes', list()) if ('Method' in node.get('labels')) and (node['properties'].get('type', False))]


class SnippetDerivationMethod(DictDerivationMethod):
    """
    Subclass of DictDerivationMethod which instead of getting passed json directly, takes in a list of actions and
    subsequently generates its own json.
    """
    def __init__(self, name: str, actions: list, interface: NeoInterface = None, study=None, overwrite_db=False,
                 schema_available=None):
        content = self.build_derivation_method_json(name, actions, json_str=False)
        super().__init__(content=content,
                         name=name,
                         interface=interface,
                         study=study,
                         overwrite_db=overwrite_db,
                         schema_available=schema_available)


class RdfDerivationMethod(DerivationMethod):
    def __init__(self, content, name=None, interface=None, study=None, overwrite_db=False, schema_available=None):
        super().__init__(data=content, name=name, interface=interface, study=study, schema_available=schema_available)
        self.content = content
        if self.load_decision(overwrite_db):
            if self.exists_in_db:
                OnlineDerivationMethod(name=self.name, interface=self.interface, study=self.study).delete()
            self.load()

    @property
    def actions(self):
        if self._actions is None:
            self._actions = self.retrieve_actions()
        return self._actions

    def load(self):
        super().load()
        self.interface.rdf_generate_uri(ModelManager.URI_MAP)
        res = self.interface.rdf_import_subgraph_inline(self.content)
        self.post_load_enrichment()
        return res
