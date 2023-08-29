import json
import logging
import os
import re
from abc import abstractmethod, ABC

import pandas as pd
import numpy as np
import requests
from datetime import date as DatetimeDate

from cryptography.fernet import Fernet

from logger.logger import logger
from model_managers import ModelManager
from query_builders import QueryBuilder
from derivation_method.utils import get_arrows_json_cypher, merge_dicts_on_node_keys, add_warn_log_if_column_missing, extract_classes_from_query
from neointerface.neointerface import NeoInterface

RDFSLABEL = ModelManager.RDFSLABEL


class Action:
    """
    Method Action Types
    """

    hide_labels = ["Resource", "Variable", "Dataset"]
    hide_props = {"Resource": ["uri"],
                  "Class": ["CoreClass", "count", "create", "from_domains", 'Origin', 'Codelist', 'Predecessor',
                            'Variable', 'Significant Digits', 'Decoded Variable', 'Label', 'n_with_same_name',
                            'Last modified date', 'Format', 'relationship_type', 'Core', 'Order', 'Comment',
                            'dataElementName', 'CDISC Notes', 'Dataset', 'Method',
                            'dataElementLabel', 'Role', 'Data Type', 'Length', 'n_with_same_label',
                            'Key Variables', 'Sort Order', 'Description', 'Repeating', 'Reference Data', 'Class',
                            'Purpose', 'Structure'],
                  "Relationship": ["TO.Class.label", "FROM.Class.label", 'Origin', 'Codelist', 'Predecessor',
                                   'Variable', 'Significant Digits', 'Decoded Variable', 'Label',
                                   'n_with_same_name', 'Last modified date', 'Format',
                                   'Core', 'Order', 'Comment', 'dataElementName', 'CDISC Notes',
                                   'Dataset', 'Method', 'dataElementLabel', 'Role', 'Data Type',
                                   'Length', 'n_with_same_label'],
                  "Term": ['Order', 'Synonyms', 'NCI Codelist Code', 'include', 'Extensible', 'Definition',
                           'Term', 'define_yn', 'Name', 'NCI Term Code', 'Decode_dtype', 'Last modified date',
                           'Decoded Value', 'ID', 'Scored', 'NCI Preferred Term']}

    def __init__(self, action_dict, method=None, interface: NeoInterface = None, dont_fetch=False):
        self.dct = action_dict
        self.action_node_id = action_dict.get("node_id")
        self.action_id = action_dict.get("id")
        self.type = action_dict.get('type')
        self.method = method
        self.interface = method.interface if method else interface
        self.meta = action_dict if dont_fetch else None
        self.fetch_metadata(self.interface)
        self._applied_changes_node_id = None
        self.applied = None

    def __getitem__(self, item):
        return self.dct[item]

    def __repr__(self):
        return f"{self.__class__.__name__} Action"

    @abstractmethod
    def _fetch_metadata(self, interface):
        pass

    def fetch_metadata(self, interface):
        if self.meta is None:
            return self._fetch_metadata(interface)

    @staticmethod
    def _rename_short_label_acc_to_rel(interface, method_id: str, parent_id: str, short_label: str):
        q = """
        MATCH path=(m:Method{id:$method_id, parent_id:$parent_id})-[:METHOD_ACTION]->(action:`Method`{type:'get_data'}),
        (action)-[:SOURCE_RELATIONSHIP]->(rel:Relationship)-[:TO]->(c:Class{short_label: $short_label})
        RETURN rel{.*} as rel
        ORDER BY rel['short_label'] //to ensure the first one is with non-missing short_label(if any)
        """
        params = {"method_id": method_id, "parent_id": parent_id, "short_label": short_label}
        res = interface.query(q, params)
        rtrn = short_label
        if res:
            sl_set = set([r.get("rel").get("short_label") for r in res if r.get("rel").get("short_label", False)])
            assert len(sl_set) <= 1, \
                f"Unexpected Relationships with different short_label in data query: {sl_set} from ({res})"
            if len(sl_set) == 1:
                sl = res[0].get("rel").get("short_label")
            else:
                sl = None
            if sl:
                rtrn = sl
        return rtrn

    @staticmethod
    def _create_isa_relationship(interface, classes: list):
        for class_ in classes:
            q = f"""        
            MATCH (c:Class), (instance) 
            WHERE instance:`{class_}` AND c.label = $class
            MERGE (instance)-[:IS_A]->(c)
            WITH * 
            MATCH (c)-[:HAS_CONTROLLED_TERM]->(term:Term)
            WHERE term.`{RDFSLABEL}` = instance.`{RDFSLABEL}`
            MERGE (instance)-[:Term]->(term)
            """
            interface.query(q, {'class': class_})

    def apply(self, df=None):
        logger.info(f"Performing {str(self)}, {self.action_id}")

    def rollback(self, detached_nodes_dct):
        return detached_nodes_dct

    def retrieve_json(self):
        """
        Compiles and returns arrows.app compatible json dict for an action.
        :return: json dict
        """
        raise NotImplementedError(f'{repr(self)} has not implemented the retrieve_json() method')


class AppliesChanges(Action, ABC):

    @property
    def applied_changes_node_id(self) -> list:
        if self._applied_changes_node_id is None:
            self._applied_changes_node_id = self._fetch_applied_changes_node_id()
        return self._applied_changes_node_id

    def _fetch_applied_changes_node_id(self) -> list:
        # get node_id for node storing applied metadata: (action)-[:APPLIED]-(changes)
        q = f"""
            MATCH (action)-[applied:APPLIED]->(changes:Changes)
            WHERE id(action) = $node_id and changes.action_id = $action_id
            return id(changes) as node_id
            """
        params = {"node_id": self.action_node_id, "action_id": self.action_id}
        res = self.method.interface.query(q, params)
        node_id = [node["node_id"] for node in res]

        # apply_stat may have >1 changes node
        if len(res) > 1:
            self.applied = True
            if self != "LinkStat Action":
                logger.warn(
                    f"Derivation method {self.method.name} action {self.action_id} has multiple 'Changes' nodes ({node_id})"
                    f"e.g. MATCH (m:Method)-[r:APPLIED]-(c:Changes) WHERE id(c) in [{node_id}] RETURN m,r,c"
                    f"Attempting to roll back all applications of {self.method.name}")
        elif len(res) == 0:
            self.applied = False
        self.applied = True
        return node_id

    def create_changes_node(self, action_node_id, set_props: dict):
        q = """
        MATCH (action:Method)
        WHERE id(action) = $node_id 
        CALL apoc.merge.node(['Changes'], $set_props) YIELD node as changes
        MERGE (action)-[:APPLIED]->(changes)
        SET changes.date = datetime.realtime()
        RETURN id(changes) as node_id
        """

        params = {"node_id": action_node_id,
                  "set_props": set_props}

        res = self.method.interface.query(q, params)[0]
        logger.debug(f"Node ID {res['node_id']} created to store {self} metadata for {self.action_id}")

    def _delete_applied_metadata_node(self, node_id):
        q = f"MATCH (n) WHERE id(n)=$node_id DETACH DELETE n"
        params = {"node_id": node_id}
        self.method.interface.query(q, params)


class GetData(Action):
    def __init__(self, action_dict, filter_dict, method=None, interface=None, dont_fetch=False):
        self.filter_dict = filter_dict
        super().__init__(action_dict=action_dict, method=method, interface=interface, dont_fetch=dont_fetch)

    def _fetch_metadata(self, interface):
        params = {"node_id": self.action_node_id}
        q = """
        MATCH (action)
        WHERE id(action) = $node_id
        OPTIONAL MATCH (action)-[r:SOURCE_CLASS]->(class:Class)
        OPTIONAL MATCH (action)-[r2:SOURCE_RELATIONSHIP]->(rel:Relationship),
        (rel)-[:FROM]->(from:Class),
        (rel)-[:TO]->(to:Class)
        WITH 
            action, 
            collect(class.label)  as source_classes,
            collect(class.label + case when (r.optional is null or toLower(r.optional) = "false") then '' else '**' end) as get_classes, 
            collect({from: from.label, to: to.label, type: rel.relationship_type, short_label: rel.short_label, optional: r2.optional}) as source_rels
        WITH *, 
            [x in source_classes WHERE NOT x IS NULL] as source_classes,                    
            [x in get_classes WHERE NOT x IS NULL] as get_classes,
            [x in source_rels WHERE NOT ((x['from'] IS NULL) OR (x['to'] IS NULL))] as source_rels                    
        RETURN action.type as type, source_classes, source_rels, get_classes, action.allow_unrelated_subgraphs as allow_unrelated_subgraphs
                    """
        res = interface.query(q, params)
        assert res, f"Could not find the required metadata for {params}: {self.dct}"
        self.meta = res[0]

        if self.filter_dict is not None:
           
            q=f"""
                MATCH (action:Method)-[onr:ON]->(class:Class)
                WHERE id(action) = $node_id //AND class.label in $source_classes
                OPTIONAL MATCH (action)-[:ON_VALUE]->(term:Term)<-[:HAS_CONTROLLED_TERM]-(class)
                WITH class, term, apoc.map.fromPairs(collect(
                    [
                        '{RDFSLABEL}'
                            ,
                        CASE WHEN size(keys(onr)) = 0 THEN
                            term.`{RDFSLABEL}`
                        ELSE
                            //converting str to bool if req.
                            apoc.map.fromPairs([key in keys(onr) |
                                [key,
                                    CASE WHEN key in ['min', 'max'] THEN
                                        CASE WHEN apoc.meta.cypher.type(onr[key]) = 'STRING' THEN //convertion to Float/Integer
                                            CASE WHEN apoc.text.regexGroups(onr[key], '^(\d+\.\d+)|(\d+e\-\d+)$') THEN
                                                toFloat(onr[key])
                                            ELSE
                                                CASE WHEN apoc.text.regexGroups(onr[key], '^(\d+)|(\d+e\d+)$') THEN
                                                    toInteger(onr[key])
                                                ELSE
                                                    onr[key] //keeping string
                                                END
                                            END
                                        ELSE
                                            onr[key]
                                        END
                                    ELSE
                                        CASE WHEN key = 'not_in' THEN
                                            term.`rdfs:label`
                                            //term.`Term Code`
                                        ELSE
                                            CASE toLower(onr[key])
                                                WHEN 'true' THEN True
                                                WHEN 'false' THEN False
                                            ELSE onr[key]
                                            END
                                        END
                                    END]
                            ])
                        END
                    ]
                )) as mp
                WITH class.label as label, apoc.map.values(mp, ['{RDFSLABEL}']) as value
                WITH label, apoc.coll.flatten(collect(value)) as values
                WITH label, apoc.map.fromPairs(collect(['{RDFSLABEL}',
                    CASE WHEN size(values) = 1 THEN values[0] ELSE values END])) as rdmap
                WITH apoc.map.fromPairs(collect([label, rdmap])) as where_map
                RETURN where_map"""

            params = {"node_id": self.filter_dict["node_id"],
                      "source_classes": self.meta["source_classes"]}
            res2 = interface.query(q, params)

            # Filter FILTER_RELATIONSHIP
            q = """
            MATCH (action:Method)-[:FILTER_RELATIONSHIP]->(class_rel:Class), (action)-[:ONLY_RELATED_TO]->(only_rel_to:Class)
            WHERE id(action) = $node_id 
            WITH action, class_rel, collect(only_rel_to) as coll_only_rel_to
            WITH action, apoc.map.fromPairs(
                collect([class_rel.label, {`NOT EXISTS`: {exclude: [x in coll_only_rel_to | x.label] + ['Class']}}])
            ) as where_rel_map
            RETURN where_rel_map
            """
            params = {"node_id": self.filter_dict["node_id"]}
            res3 = interface.query(q, params)

            assert res2 or res3, f"Could not find the required metadata for {params}: {self.dct}"
            if not res2:
                res2 = [{}]
            if not res3:
                res3 = [{}]
            self.meta = {**self.meta, **res2[0], **res3[0]}

    def apply(self, df=None, limit: int = 0):
        """
        :param limit: int, number of rows to return where 0 is unlimited
        """
        super().apply(df)
        self.df = df
        dp = self.method.interface
        assert "get_classes" in self.meta, self.meta
        infer_rels = False
        allow_unrelated_subgraphs = (True if self.meta.get("allow_unrelated_subgraphs") == "true" else False)
        if not self.meta.get("source_rels") and len(self.meta.get("get_classes")) > 1 and not allow_unrelated_subgraphs:
            infer_rels = True
        source_data = dp.get_data_generic(
            labels=self.meta.get("get_classes"),
            rels=self.meta.get("source_rels"),
            allow_unrelated_subgraphs=allow_unrelated_subgraphs,
            infer_rels=infer_rels,
            where_map=self.meta.get("where_map"),
            where_rel_map=self.meta.get("where_rel_map"),
            use_shortlabel=True,
            use_rel_labels=True,
            return_nodeid=True,
            return_propname=False,
            only_props=RDFSLABEL,
            limit=limit
        )  # TODO: to tackle DataProvider and subclasses
        logger.info(f"{len(source_data)} records in source data")
        logger.debug(f"Columns: {source_data.columns.to_list()}")
        logger.debug(f"DataFrame: \n{source_data.to_string(max_rows=10)}")
        missing_col = add_warn_log_if_column_missing(source_data.columns.to_list())
        if len(missing_col) > 0:
            logger.warning(f"Columns missing: {missing_col}")
        logger.debug(f"{source_data.to_string(max_rows=10)}")
        self.df = source_data  # but for now we assign it to source_data
        if len(source_data) == 0:
            logger.error(f"No data found for:"
                         f"{self.meta.get('get_classes')} "
                         f"{self.meta.get('source_rels')}"
                         f"{self.meta.get('where_map')}"
                         f" {self.meta.get('where_rel_map')}")
            raise RuntimeError(f"{self} returned 0 rows")
        return source_data

    def retrieve_json(self):
        logger.info(f"Building json: {self.action_id}")

        keep_labels = ["Class", "Relationship", "Term", "Study Specific Term", "Method"]
        keep_props = {"Class": ["label"],
                      "Relationship": ["relationship_type"],
                      "Study Specific Term": ["Term Code", "Codelist Code", "rdfs:label"],
                      "Term": ["Term Code", "Codelist Code"],
                      "Method": ["id", "type", "script", "lang", "package", "params", "version",
                                 "allow_unrelated_subgraphs"]}

        where_map = self.meta.get("where_map")
        where_rel_map = self.meta.get("where_rel_map")
        range_class = None
        range_props = {}
        filter_rel_class = None
        filter_rel_only_rel_to = []

        if where_map:
            for class_, filter_ in where_map.items():
                if isinstance(filter_.get('rdfs:label', {}), dict):
                    if filter_.get('rdfs:label', {}).get('how') == 'range':
                        range_class = class_
                        range_props = filter_.get('rdfs:label', {})
                        break  # only one range filter per derivation is allowed currently

        if where_rel_map:
            for class_, item in where_rel_map.items():
                filter_rel_class = class_
                filter_rel_only_rel_to = [cls for cls in item.get('NOT EXISTS', {}).get('exclude', []) if cls != 'Class']

        # remove short_label keys and optional keys (when value is None) from source_rels
        source_rels_no_short_labels = []
        for rel in self.meta.get("source_rels"):
            rel_copy = rel.copy()
            if 'short_label' in rel_copy:
                del rel_copy['short_label']
            if ('optional' in rel_copy) and (rel_copy.get('optional') is None):
                del rel_copy['optional']
            source_rels_no_short_labels.append(rel_copy)

        params = {
            'classes': self.meta.get("source_classes"),
            'rels': source_rels_no_short_labels,
            'terms': self.meta.get('where_map'),
            'method_id': self.action_id,
            'filter_method_id': self.action_id.replace('get_data', 'filter'),
            'true': "true",
            'allow_unrelated_subgraphs': 'true' if self.meta.get("allow_unrelated_subgraphs") else 'false'
        }

        res1_source_rels_query = """
        CALL apoc.create.vNode(["Method"], 
            CASE 
                WHEN $allow_unrelated_subgraphs = 'true' THEN {id: $method_id, type: 'get_data', 
                    allow_unrelated_subgraphs: $allow_unrelated_subgraphs} 
                ELSE {id: $method_id, type: 'get_data'} 
            END) YIELD node as method
        WITH *
        MATCH (x)<-[rf:FROM]-(r)-[rt:TO]->(y)
        WITH *
        UNWIND $rels as relpath
        WITH *
        WHERE (r:Relationship AND x:Class AND y:Class)
        AND (r.relationship_type = relpath['type'] AND x.label = relpath['from'] AND y.label = relpath['to'])
        CALL apoc.create.vRelationship(
            method,
            "SOURCE_RELATIONSHIP",
            CASE WHEN toLower(relpath['optional']) = 'true' THEN {optional: $true} ELSE {} END,
            r
            ) YIELD rel                                  
        WITH [[r,rf,x],[r,rt,y],[method, rel, r]] as coll                        
        UNWIND coll as item
        WITH item[0] as x, item[1] as r, item[2] as y
        """

        res1_source_classes_query = """
        CALL apoc.create.vNode(["Method"], 
            CASE 
                WHEN $allow_unrelated_subgraphs = 'true' THEN {id: $method_id, type: 'get_data', 
                    allow_unrelated_subgraphs: $allow_unrelated_subgraphs} 
                ELSE {id: $method_id, type: 'get_data'} 
            END) YIELD node as method
        WITH *
        MATCH (x:Class)
        WHERE x:Class AND x.label in $classes
        CALL apoc.create.vRelationship(method, "SOURCE_CLASS",{}, x) YIELD rel
        WITH [[method, rel, x]] as coll
        UNWIND coll as item
        WITH item[0] as x, item[1] as r, item[2] as y          
        """

        res2_source_rels_query = """
        CALL apoc.create.vNode(['Method'], {type:'filter', id:$filter_method_id}) YIELD node as method
        MATCH (x)<-[rf:FROM]-(r)-[rt:TO]->(y)
        WHERE r:Relationship AND x:Class AND y:Class 
        AND (
            ({from: x.label, to: y.label, type: r.relationship_type} in $rels)
            OR
            ({from: x.label, to: y.label, type: r.relationship_type, optional: $true} in $rels)
            )                 
        WITH [[r,rf,x],[r,rt,y]] as coll, method
        UNWIND coll as item
        WITH DISTINCT item[2] as y, method
        WITH y, $terms[y.label] as terms, method
        WHERE terms is not null
        WITH y, apoc.map.flatten(terms) as terms_coll, method
        WITH y, apoc.map.get(terms_coll,'rdfs:label',terms_coll['rdfs:label.not_in'],false) as term_values,
        apoc.map.get(terms_coll,'rdfs:label.not_in',null,false) as on_rel_prop, method
        UNWIND term_values as term_value
        OPTIONAL MATCH (y)-[rtt:HAS_CONTROLLED_TERM]->(t:Term)
        WHERE y.label in keys($terms) and t.`rdfs:label` = term_value
        CALL apoc.do.when(
            t IS NULL,
            '
                CALL apoc.create.vNode(
                    ["Term", "Study Specific Term"], 
                    {
                        `rdfs:label`:term_value, 
                        `Codelist Code`: y.short_label, 
                        `Term Code`: term_value
                    }
                ) YIELD node 
                CALL apoc.create.vRelationship(y, "HAS_CONTROLLED_TERM", {}, node) YIELD rel
                RETURN node as t, rel as rtt
            ',
            'RETURN t, rtt',
            {y:y, t:t, rtt: rtt, term_value:term_value}
        ) YIELD value
        WITH *, value['t'] as t, value['rtt'] as rtt, on_rel_prop
        WITH y, collect([rtt, t]) as coll, on_rel_prop, method
        WITH *, 'not_in' as not_in
        CALL apoc.create.vRelationship(method, 'ON', {}, y) YIELD rel as rel_on
                CALL apoc.do.when(
            on_rel_prop is not null,
            '
            CALL apoc.create.setRelProperty(rel_on,not_in,true) YIELD rel
            RETURN rel as rel_on2
            ',
            'RETURN rel_on',
            {rel_on:rel_on, not_in:not_in}
        ) YIELD value
        WITH *            
        UNWIND coll as pair2
        WITH *, pair2[0] as rtt, pair2[1] as t                  
        CALL apoc.create.vRelationship(method, 'ON_VALUE', {}, t) YIELD rel as rel_onvalue
        WITH coll, [[y, rtt, t],[method, rel_on, y],[method, rel_onvalue, t]] as coll_
        UNWIND coll_ as item
        WITH item[0] as x, item[1] as r, item[2] as y       
        """

        res2_source_classes_query = """      
        CALL apoc.create.vNode(['Method'], {type:'filter', id: $filter_method_id}) YIELD node as method
        MATCH (x:Class)
        WHERE x:Class AND x.label in $classes
        WITH x, $terms[x.label] as terms, method
        WHERE terms is not null
        WITH x, apoc.map.flatten(terms) as terms_coll, method
        WITH x, apoc.map.get(terms_coll,'rdfs:label',terms_coll['rdfs:label.not_in'],false) as term_values,
        apoc.map.get(terms_coll,'rdfs:label.not_in',null,false) as on_rel_prop, method
        UNWIND term_values as term_value
        OPTIONAL MATCH (x)-[rtt:HAS_CONTROLLED_TERM]->(t:Term)
        WHERE x.label in keys($terms) and t.`rdfs:label` = term_value
        CALL apoc.do.when(
            t IS NULL,
            '
                CALL apoc.create.vNode(
                    ["Term", "Study Specific Term"], 
                    {
                        `rdfs:label`:term_value, 
                        `Codelist Code`: x.short_label, 
                        `Term Code`: term_value
                    }
                ) YIELD node 
                CALL apoc.create.vRelationship(x, "HAS_CONTROLLED_TERM", {}, node) YIELD rel
                RETURN node as t, rel as rtt
            ',
            'RETURN t, rtt',
            {x:x, t:t, rtt: rtt, term_value:term_value}
        ) YIELD value
        WITH *, value['t'] as t, value['rtt'] as rtt, on_rel_prop
        WITH x, collect([rtt, t]) as coll, on_rel_prop, method
        WITH collect([x, coll]) as coll2 , on_rel_prop, method, x, coll
        WITH *, 'not_in' as not_in
        CALL apoc.create.vRelationship(method, 'ON',{} , x) YIELD rel as rel_on
        CALL apoc.do.when(
            on_rel_prop is not null,
            '
            CALL apoc.create.setRelProperty(rel_on,not_in,true) YIELD rel
            RETURN rel as rel_on2
            ',
            'RETURN rel_on',
            {rel_on:rel_on, not_in:not_in}
        ) YIELD value
        WITH *      
        UNWIND coll as pair2
        WITH *, pair2[0] as rtt, pair2[1] as t                  
        CALL apoc.create.vRelationship(method, 'ON_VALUE', {}, t) YIELD rel as rel_onvalue
        WITH coll, [[x, rtt, t],[method, rel_on, x],[method, rel_onvalue, t]] as coll_
        UNWIND coll_ as item
        WITH item[0] as x, item[1] as r, item[2] as y            
        """

        resx = None
        if self.meta.get("allow_unrelated_subgraphs") and self.meta.get('source_rels'):
            # If performing unrelated_subgraphs query with rels, ensure that classes not present in rels are included
            # in method json

            rel_classes = set()
            for rel in self.meta.get('source_rels'):
                rel_classes.add(rel.get('from'))
                rel_classes.add(rel.get('to'))

            detached_classes = []
            for class_ in self.meta.get('source_classes'):
                if class_ not in rel_classes:
                    detached_classes.append(class_)

            if detached_classes:
                resx_params = {
                    'classes': detached_classes,
                    'method_id': self.action_id,
                    'allow_unrelated_subgraphs': 'true' if self.meta.get('allow_unrelated_subgraphs') else 'false'
                }

                resx = get_arrows_json_cypher(
                    neo=self.interface,
                    q=res1_source_classes_query,
                    params=resx_params,
                    keep_labels=keep_labels,
                    keep_props=keep_props
                )
                resx = resx[0]
                resx['nodes'] = [node for node in resx.get('nodes') if 'Method' not in node['labels']]

        if self.meta.get('source_rels'):
            # if there are relationships selected, we use SOURCE_RELATIONSHIP's q and params
            res1_query = res1_source_rels_query
            res2_query = res2_source_rels_query
        else:
            # use SOURCE_CLASS q and params
            res1_query = res1_source_classes_query
            res2_query = res2_source_classes_query

        res1 = get_arrows_json_cypher(
            neo=self.interface,
            q=res1_query,
            params=params,
            keep_labels=keep_labels,
            keep_props=keep_props
        )
        res1 = res1[0]

        if resx:
            method_node_id = None

            for node in res1['nodes']:
                if 'Method' in node['labels']:
                    method_node_id = node['id']
                    break

            if method_node_id:
                for rel in resx['relationships']:
                    rel['fromId'] = method_node_id

                res1['nodes'].extend(resx['nodes'])
                res1['relationships'].extend(resx['relationships'])

        get_data_classes = [node['properties'].get('label') for node in res1['nodes'] if 'Class' in node['labels']]
        res2 = get_arrows_json_cypher(
            neo=self.interface,
            x_shift=1000,
            q=res2_query,
            params=params,
            keep_labels=keep_labels,
            keep_props=keep_props
        )

        res2 = res2[0]
        res2 = {**res2, **{'nodes': [node for node in res2['nodes'] if 'Class' not in node['labels']]}}
        res3 = get_arrows_json_cypher(
            neo=self.interface,
            x_shift=1000,
            q="""
                MATCH (c:Class {`label`:$range_class})
                CALL apoc.create.vNode(['Method'], {type:'filter', id:$method_id}) YIELD node as method
                WITH *
                CALL apoc.create.vRelationship(method, "ON", $range_props , c) YIELD rel
                WITH  [[method, rel, c]] as coll
                UNWIND coll as item
                WITH item[0] as x, item[1] as r, item[2] as y
                  """,
            params={
                'range_props': range_props,
                'range_class': range_class,
                'method_id': self.action_id.replace('get_data', 'filter')
            },
            keep_labels=keep_labels,
            keep_props=keep_props
        )
        res3 = res3[0]
        res3 = {**res3, **{'nodes': [node for node in res3['nodes'] if 'Class' not in node['labels']]}}
        res4 = get_arrows_json_cypher(
            neo=self.interface,
            x_shift=1000,
            q="""
                        MATCH (c:Class)
                        WHERE c.label = $filter_rel_class                                                 
                        CALL apoc.create.vNode(['Method'], {type:'filter', id:$method_id}) YIELD node as method
                        CALL apoc.create.vRelationship(method, "FILTER_RELATIONSHIP", {} , c) YIELD rel
                        WITH *
                        MATCH (c_to:Class) 
                        WHERE c_to.label in $filter_rel_only_rel_to                         
                        CALL apoc.create.vRelationship(method, "ONLY_RELATED_TO", {} , c_to) YIELD rel as rel_to
                        WITH [method, rel, c] as first, collect([method, rel_to, c_to]) as coll
                        WITH  [first] + coll as coll
                        UNWIND coll as item
                        WITH item[0] as x, item[1] as r, item[2] as y
                          """,
            params={
                'filter_rel_only_rel_to': filter_rel_only_rel_to,
                'filter_rel_class': filter_rel_class,
                'method_id': self.action_id.replace('get_data', 'filter')
            },
            keep_labels=keep_labels,
            keep_props=keep_props
        )
        res4 = res4[0]
        res4 = {**res4, **{'nodes': [
            node
            for node in res4['nodes']
            if 'Class' not in node['labels']
               or node['properties'].get('label') not in get_data_classes]}}

        res_nonmissing = [res for res in [res2, res3, res4] if res.get("nodes")]
        if res_nonmissing:
            filter_res = res_nonmissing[0]
            for i in range(len(res_nonmissing) - 1):
                filter_res = merge_dicts_on_node_keys(
                    filter_res,
                    res_nonmissing[i + 1],
                    {'properties': {'type': 'filter', 'id': self.action_id.replace('get_data', 'filter')}}
                )
            next_method_rel = {
                "id": "r999999998",
                "fromId": [nd['id'] for nd in res1['nodes'] if
                           'Method' in nd['labels'] and nd['properties'].get('type') == 'get_data'][0],
                "toId": [nd['id'] for nd in filter_res['nodes'] if
                         'Method' in nd['labels'] and nd['properties'].get('type') == 'filter'][0],
                "type": "NEXT",
                "properties": {}, "style": {}
            }
        else:
            next_method_rel = {}
            filter_res = res2
        res = {
            'nodes': res1['nodes'] + filter_res['nodes'],
            'relationships':
                res1['relationships'] + filter_res['relationships'] +
                ([next_method_rel] if filter_res and next_method_rel else []),
            'style': res1['style']
        }
        return res


class RunScript(AppliesChanges):

    def __init__(self, *args, script_path: str = "derivation_method.scripts", **kwargs):
        self.cols_before = []
        self.cols_after = []
        super().__init__(*args, **kwargs)

        self.script_path = script_path

    def _fetch_metadata(self, interface):
        q = """
        MATCH (m:Method)
        WHERE id(m) = $method_node_id
        RETURN m
        """
        params = {"method_node_id": self.action_node_id}
        res = interface.query(q, params)
        assert res, f"Method node from action {self.dct} not found"
        self.meta = res[0]["m"]

    def apply(self, df=None):
        super().apply(df)
        self.df = df
        self.cols_before = list(df.columns)

        params = self.meta.get('params')
        if not params:
            params = {}
        else:
            # TODO: check validity of json, also consider code injection
            logger.info(params)
            # params = json.loads(params.replace("'",'"').replace("True", "true")) #TODO: fix it another way single quotes could be in the values of dict which should not be replaced
            params = json.loads(params)

        params = {**params, **{"df": self.df}}
        params_str = ", ".join([key + f"=params['{key}']" for key, item in params.items()])
        logger.info(f"Running {self.meta.get('package')}.{self.meta.get('script')}")
        call_str = f"{self.meta.get('script')}({params_str})"
        exec(f"from {self.script_path}.{self.meta.get('package')} import *")
        modified_data = eval(call_str)

        logger.info(f"Params: {call_str}")
        logger.info(f"\n{len(modified_data)} records in modified data")

        self.cols_after = list(modified_data.columns)
        self.df = modified_data

        logger.debug(f"Columns: {modified_data.columns.to_list()}")
        logger.debug(f"DataFrame: \n{modified_data.to_string(max_rows=10)}")

        # store columns reference on changes node
        self.create_changes_node(
            self.action_node_id,
            {
                "action_id": self.action_id,
                "cols_before": self.cols_before,
                "cols_after": self.cols_after
            }
        )
        return modified_data

    def rollback(self, detached_nodes_dct):
        for node_id in self.applied_changes_node_id:
            self._delete_applied_metadata_node(node_id)
            logger.debug(f"\t{self}, {self.action_id}, applied_changes_node_id: {self.applied_changes_node_id}")
            logger.info(f'\t\t{self} rollback removed changes node with id: {node_id}')
        return detached_nodes_dct

    def retrieve_json(self):
        logger.info(f"Building json: {self.action_id}")

        method_json = {
            "nodes":
                [
                    {
                        "id": self.meta.get('id'),
                        "position": {},
                        "caption": "",
                        "labels": ["Method"],
                        "properties": {
                            "type": self.meta.get("type"),
                            "id": self.meta.get('id'),
                            "script": self.meta.get("script"),
                            "lang": self.meta.get("lang"),
                            "package": self.meta.get("package"),
                            "version": self.meta.get("version"),
                            "params": self.meta.get("params") if self.meta.get("params") else None
                            # .get("params") returns {} not None, which we need
                        }
                    }
                ],
            "relationships": [],
            'style': {}
        }
        return method_json


class RunCypher(Action):

    def _fetch_metadata(self, interface):
        q = """
        MATCH (m:Method)
        WHERE id(m) = $method_node_id
        RETURN m
        """
        params = {"method_node_id": self.action_node_id}
        res = interface.query(q, params)
        assert res, f"Method node from action {self.dct} not found"
        self.meta = res[0]["m"]

    def apply(self, df=None, interface=None):
        super().apply(df)

        query = self.meta.get('query')
        assert query, f"Method node from action {self.dct} does not contain a query"
        params = self.meta.get('params')

        if params:
            params = json.loads(params)
        else:
            params = {}

        remove_col_prefixes = params.get('remove_col_prefixes', 'true')

        if self.meta.get('include_data') == 'true':
            if df is not None:
                if df.empty:
                    logger.warning(f"RunCypher Action including data with empty df!")
                params['data'] = df.to_dict(orient='records')
            else:
                raise AttributeError('RunCypher Action unable to include data because self.df is None')

        res = self.method.interface.query(query, params, return_type='pd')
        params['action_node_id'] = self.action_node_id

        if self.meta.get('update_df') == 'true':
            if remove_col_prefixes == 'true':
                new_column_name_map = {col: col.split('.')[-1] for col in res.columns}
                res.rename(columns=new_column_name_map, inplace=True)
            self.df = res
            logger.debug(f"DataFrame: \n{res.to_string(max_rows=10)}")

        return res

    def retrieve_json(self):
        logger.info(f"Building json: {self.action_id}")

        method_json = {
            "nodes":
                [
                    {
                        "id": self.meta.get('id'),
                        "position": {},
                        "caption": "",
                        "labels": ["Method"],
                        "properties": {
                            "type": self.meta.get('type'),
                            "id": self.meta.get('id'),
                            "classes": extract_classes_from_query(self.meta.get('query', '')),  # ToDo currently is not used. Should this be removed?
                            "query": self.meta.get('query', ''),
                            "include_data": self.meta.get('include_data'),
                            "update_df": self.meta.get('update_df'),
                            'params': self.meta.get('params')
                        }
                    }
                ],
            "relationships": [],
            'style': {}
        }

        # remove any empty parameters
        method_json['nodes'][0]['properties'] = {key: value for key, value in method_json['nodes'][0]['properties'].items() if value}
        return method_json


class CallAPI(AppliesChanges):

    def __init__(self, *args, **kwargs):
        self.cols_before = []
        self.cols_after = []
        super().__init__(*args, **kwargs)

    def _fetch_metadata(self, interface):
        q = """
        MATCH (m:Method)
        WHERE id(m) = $method_node_id
        RETURN m
        """
        params = {"method_node_id": self.action_node_id}
        res = interface.query(q, params)
        assert res, f"Method node from action {self.dct} not found"
        self.meta = res[0]["m"]

    @staticmethod
    def determine_data_types(df):
        '''
        Iterates over provided df's dtypes and creates a dictionary of column type information, a list of date column
        labels and a list of datetime column labels.
        Non-object type columns are stored as their str(native_pandas_dtype) ie. {col_type: "int64"}.
        Object columns are sampled based on their first valid (non-null) value and represented as {col_type: "string"},
        col_type: 'date' or col_type: str(type(value)).
        Columns are assumed to contain values which are alike.
        :param df: pandas dataframe to be sampled
        :returns:
            dict{col_label: str(native_pandas_type), "string" or str(type(value))}
        '''

        data_types = {}

        for column_label, column_type in df.dtypes.iteritems():

            # TODO: determine index format to help catch R index conversion issues?
            pd_type = str(column_type)

            if column_type == np.object:
                # Object type could represent string, date or other
                sample_index = df[column_label].first_valid_index()
                if sample_index is None:
                    # Column contains all none values
                    pd_type = str(None)  # Nones convert to NULL on json load and then nan at api
                else:
                    sample_type = type(df[column_label].loc[sample_index])  # first non-null type
                    if sample_type == DatetimeDate:
                        pd_type = "date"
                    elif sample_type == str:
                        pd_type = "string"
                    else:
                        # Unexpected object type
                        pd_type = str(sample_type)

            data_types[column_label] = pd_type
        return data_types

    @staticmethod
    def load_json_dataframe(json_df, expected_dtypes: dict):
        '''
        Reads json dataframe, ensuring that types and dates are preserved.
        datetime conversion handled by pd.read_json(convert_dates=[]), other type conversion
        is handled by pd.read_json(dtype={})
        :param json_df: Json dataframe in "records" orientation
        :param expected_dtypes: dict of column: str(type_string)
            ie {col_1:'int64', col_2:'string', col_3: 'date'}
        :return: pd.DataFrame()
        '''

        data_type_map = {
            'string': 'object',
            'float64': np.float64,
            'int64': np.int64,
            # 'datetime64[ns]': np.datetime64,
            # 'date': DatetimeDate,
            # read json dtype property unreliable when converting date/datetimes
        }

        # Specify string type cols in dtype to retain numerical strings on read json. This is important
        # to prevent unwanted automatic string to int conversions i.e '0001234' -> 1234
        load_dtypes = {}
        date_cols = []
        datetime_cols = []

        if expected_dtypes:
            for col_label, dtype in expected_dtypes.items():
                if dtype == 'date':
                    date_cols.append(col_label)
                elif dtype == 'datetime64[ns]':
                    datetime_cols.append(col_label)
                elif dtype in data_type_map.keys():
                    load_dtypes[col_label] = data_type_map[dtype]

        df = pd.read_json(json_df, orient="records", dtype=load_dtypes, convert_dates=date_cols+datetime_cols)

        if date_cols:
            for col in date_cols:
                if col in df.columns:
                    try:
                        df[col] = df[col].dt.date
                    except AttributeError as e:
                        logger.error("Date column conversion failed!, Check api logs for more information!")
                        raise e

        # Compare df.dtypes to expected dtypes
        for source_col_label, source_col_type in df.dtypes.iteritems():
            if source_col_label not in expected_dtypes:
                logger.warning(f"Column {source_col_label} not in expected dtypes and subject to automatic conversions")
                continue

            if source_col_type == np.object:
                sample_index = df[source_col_label].first_valid_index()
                if sample_index is None:
                    # Column contains all Nones
                    if expected_dtypes[source_col_label] != str(type(None)):
                        logger.warning(f"Read json type missmatch for column {source_col_label}! Found: None, expected: {expected_dtypes[source_col_label]}")
                else:
                    first_valid_value_type = type(df[source_col_label].loc[sample_index])

                    if first_valid_value_type == str and expected_dtypes[source_col_label] != "string":
                        logger.warning(f"Read json type missmatch for column {source_col_label}! Found: str, expected: {expected_dtypes[source_col_label]}")

                    if first_valid_value_type == DatetimeDate and expected_dtypes[source_col_label] != 'date':
                        logger.warning(f"Read json type missmatch for column {source_col_label}! Found: {str(DatetimeDate)}, expected: {expected_dtypes[source_col_label]}")
            else:
                if str(source_col_type) != expected_dtypes[source_col_label]:
                    logger.warning(f"Read json type missmatch for column {source_col_label}! Found: {str(source_col_type)}, expected: {expected_dtypes[source_col_label]}")

        return df

    # TODO: add option for use yes/no dtypes cdisc?
    @staticmethod
    def update_exp_dtypes_cdisc(df_columns: list, expected_dtypes: dict):
        '''
        Samples column labels for cdisc DT or DTM naming conventions and assigns them
        a type in expected dtypes as required.
        :param df_columns: list of column lables
        :param expected_dtypes: dict of column: str(type_string)
            ie {col_1:'int64', col_2:'string', col_3: 'date'}
        :return: expected_dtypes
        '''

        for col in df_columns:
            if col not in expected_dtypes and col.endswith("DT") and not col.startswith("_id_"):
                expected_dtypes[col] = "date"
                logger.debug(f"update_exp_dtypes_cdisc added column {col} of type 'date'")
            if col not in expected_dtypes and col.endswith("DTM") and not col.startswith("_id_"):
                expected_dtypes[col] = "datetime64[ns]"
                logger.debug(f"update_exp_dtypes_cdisc added column {col} of type 'datetime64[ns]'")
        return expected_dtypes

    def apply(
            self,
            df=None,
            github_base_url: str = os.getenv('GITHUB_BASE_URL'),
            github_branch: str = 'main',
            github_token: str = os.environ.get('GIT_TOKEN')
    ):
        super().apply(df)
        self.df = df
        self.cols_before = list(df.columns)

        params = self.meta.get("params")
        if not params:
            params = {}
        else:
            logger.info(params)
            params = json.loads(params)

        # no_kw_1 used for parameters that get passed without a keyword. E.g. datasets into admiral functions.
        # We convert the dataframe to json format here.
        params = {**{"no_kw_1": self.df.to_json(orient="records"), **params}}

        expected_data_types = self.determine_data_types(self.df)

        force_columns = self.meta.get("force_columns")
        if force_columns:
            logger.info(f"Appending force columns: {self.meta.get('force_columns')}")
            # format ['key,type', 'key,type']
            for dict_pair in force_columns:
                try:
                    s_key, s_value = dict_pair.split(',')
                except AttributeError:
                    logger.warning(f'Failed to load force column! Unexpected format: {dict_pair}')
                    continue
                except ValueError:
                    logger.warning(f'Failed to load force column! Unexpected format: {dict_pair}')
                    continue

                # Force column values expected to correlate with determine_data_types format
                if s_key in expected_data_types.keys():
                    existing_type = expected_data_types.pop(s_key)
                    logger.warning(f'Forcing existing column: {s_key} of type: {existing_type} to be type: {s_value}')

                expected_data_types[s_key] = s_value

        # TODO: review if needed here. Determine dtypes should already have
        #  found existing columns and their types
        expected_data_types = self.update_exp_dtypes_cdisc(df, expected_data_types)

        logger.info(f"Expected data types: {expected_data_types}")

        # All parameters that can be passed to the API:
        # func: str
        # language: Optional[str] = 'python'
        # language_version: Optional[str] = None
        # package: Optional[str] = None
        # package_version: Optional[str] = None
        # github_repo: Optional[str] = None
        # repo_scripts_path: Optional[str] = None
        # github_token: Optional[str] = None
        # params: dict
        # supply_full_eval_traceback: bool = False     To get full traceback on any eval() errors
        # expected_data_types: Dict{str: str}     Key:column label, value: col_type_str

        # any action.get() that cannot find the parameter returns None.
        api_packet = {
            "func": self.meta.get("script"),
            "language": self.meta.get("lang"),
            "package": self.meta.get("package"),
            "params": params,
            "supply_full_eval_traceback": True,

            "expected_data_types": expected_data_types,

            'github_base_url': github_base_url,

            "github_repo": self.meta.get("github_repo"),

            "github_branch": github_branch,

            "repo_scripts_path": self.meta.get("repo_scripts_path"),

            "github_token": github_token
        }

        # check if func and params both have values that are not None
        assert all([api_packet[i] for i in ["func", "params"]])

        # REQUIRES THE TWO ENVIRONMENT VARIABLES BELOW
        api_url = os.environ.get("CLD_API_HOST")
        api_auth = os.getenv('CLD_API_AUTH')

        if "items" not in api_url:
            api_url = api_url+"items/" if api_url.endswith('/') else api_url+"/items/"
        else:
            api_url = api_url if api_url.endswith('/') else api_url+"/"

        logger.info(f"calling api at {api_url} with function: {self.meta.get('script')}")
        if self.meta.get("github_repo"):
            logger.info(
                f"source code from: {github_base_url}/{self.meta.get('github_repo')}/"
                f"{self.meta.get('repo_scripts_path')}"
            )
            logger.info(f'Using branch {github_branch}')

        if 'domino' in api_url:
            json_packet = {'data': api_packet}

            # send a post request to the api endpoint. the json parameter is passed the api_packet dictionary
            resp = requests.post(url=api_url, auth=(api_auth, api_auth), json=json_packet, verify=False)
        else:
            json_packet = api_packet

            # send a post request to the api endpoint. the json parameter is passed the api_packet dictionary
            resp = requests.post(url=api_url, json=json_packet)

        logger.info(f"API response status code: {resp.status_code}, content type: {resp.headers['Content-Type']}")
        # check the api response status code is 200
        assert resp.status_code == 200, f'Status code {resp.status_code}'

        # if using domino API function_return is passed via 'result'
        json_return = resp.json()
        if 'result' in json_return:
            json_return = json_return.get('result')

        modified_data = json_return.get('function_return')
        api_cols = json_return.get('return_cols')   # List of df cols after api call module

        # display the api logs
        api_logs = json_return.get('logs')
        logger.info(f'API logs \n{api_logs}')

        # If the API runs into an error, it will return None
        assert modified_data is not None, f"API ran into an expected error. Check logs above!"

        if api_cols:
            expected_data_types = self.update_exp_dtypes_cdisc(api_cols, expected_data_types)
        else:
            logger.warning("New df columns not returned from api!")

        self.df = self.load_json_dataframe(modified_data, expected_data_types)
        self.cols_after = list(self.df.columns)

        logger.info(f"{len(self.df)} records in modified data")
        logger.debug(f"DataFrame: \n{self.df.to_string(max_rows=10)}")

        # make a request to the cldgitapi for commit id

        # only do this if the repo exists
        if self.meta.get("github_repo") is not None:
            token = Fernet(os.environ.get('CLDGITAPI_ENCRYPTION_KEY')).encrypt(github_token.encode())
            headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
            gitapi_url = os.getenv('CLD_GIT_API_HOST')
            endpoint = 'get_commit'

            if self.meta.get('lang', '').lower() in ['py', 'python']:
                extension = 'py'
            elif self.meta.get('lang', '').lower() == 'r':
                extension = 'R'
            else:
                raise KeyError(f"Expected property 'lang' to be 'python', 'Python', 'py', 'r' or 'R'. It was {self.meta.get('lang', '')}")

            file_path = f'{self.meta.get("repo_scripts_path")}/{self.meta.get("package")}.{extension}'

            json_ = { 'repo': self.meta.get("github_repo"), 'branch': github_branch, 'base_url': github_base_url, 'file_path': file_path}

            # print to input logs
            logger.info(f'calling gitapi at {gitapi_url} to request latest commit_id for file {file_path} in repo {self.meta.get("github_repo")}')

            # api call 
            commit_resp = requests.get(url=f'{gitapi_url}{endpoint}/', params={'token': token}, headers=headers, json=json_)
            response_content = commit_resp.json()
            try:
                assert commit_resp.status_code == 200, f'Status code {commit_resp.status_code}'
            except AssertionError as err:
                logger.error(response_content.get('detail'))
                raise err
            else:
                commit_id = response_content.get('commit_id')

            # print output to logs and check response code
            logger.info(f"Retrieved commit_id: {commit_id}")

            # Save reference to df columns
            self.create_changes_node(
                self.action_node_id,
                {
                    "action_id": self.action_id,
                    "cols_before": self.cols_before,
                    "cols_after": self.cols_after,
                    "commit_id":commit_id
                }
            )

        else:
            self.create_changes_node(
                self.action_node_id,
                {
                    "action_id": self.action_id,
                    "cols_before": self.cols_before,
                    "cols_after": self.cols_after
                }
            )

        return self.df

    def rollback(self, detached_nodes_dct):
        for node_id in self.applied_changes_node_id:
            self._delete_applied_metadata_node(node_id)
            logger.debug(f"\t{self}, {self.action_id}, applied_changes_node_id: {self.applied_changes_node_id}")
            logger.info(f'\t\t{self} rollback removed changes node with id: {node_id}')
        return detached_nodes_dct

    def retrieve_json(self):
        method_json = {
            "nodes":
                [
                    {
                        "id": self.meta.get('id'),
                        "position": {},
                        "caption": "",
                        "labels": ["Method"],
                        "properties": {
                            "type": self.meta.get("type"),
                            "id": self.meta.get('id'),
                            "script": self.meta.get("script"),
                            "lang": self.meta.get("lang"),
                            "package": self.meta.get("package"),
                            "version": self.meta.get("version"),
                            "github_repo": self.meta.get("github_repo"),
                            "repo_scripts_path": self.meta.get("repo_scripts_path"),
                            "params": self.meta.get("params") if self.meta.get("params") else None
                            # .get("params") returns {} not None, which we need
                        }
                    }
                ],
            "relationships": [],
            'style': {}
        }
        return method_json


class AssignLabel(AppliesChanges):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _fetch_metadata(self, interface):
        q = """
        MATCH                                 
        (action)-[:ON]->(on:Class),
        (action)-[:CLASS]->(assign:Class)
        WHERE id(action) = $node_id
        OPTIONAL MATCH p=(assign)-[rel]->(on)
        OPTIONAL MATCH (on)<-[:TO]-(source_rel:Relationship),
        (source_rel)<-[:SOURCE_RELATIONSHIP]-(:Method)<-[:METHOD_ACTION*0..10]-(root:Method),
        (root)<-[:METHOD_ACTION*0..10]->(action) //if short_label is renamed accoring to source_rel short_label as per DataProvider.get_data_generic(use_rel_labels=True)
        RETURN 
            action.type as type, 
            assign.label as assign_label, 
            assign.short_label as assign_short_label,
            on.label as on_class,
            on.short_label as on_class_short_label,
            id(assign) as from, 
            type(rel) as relationship_type, 
            id(on) as to
                        """
        params = {"node_id": self.action_node_id}
        res = self.method.interface.query(q, params)
        assert res, f"Could not find the required metadata for {params}: {self.dct}"
        res = res[0]
        # #renaming on_class_short_label if it should be renamed accoring to short_label on Relationship:
        res = {**res,
               **{"on_class_short_label":
                   self._rename_short_label_acc_to_rel(
                       interface,
                       method_id=self.method.name,
                       parent_id=self.method.study,
                       short_label=res["on_class_short_label"],
                   )}}
        self.meta = res

    def apply(self, df=None):
        super().apply(df)
        logger.info(f"{self}: `{self.meta.get('assign_label')}` on `{self.meta.get('on_class')}`")
        self.df = df

        if self.df.empty:
            logger.info(f"\tBypassing {self.action_id}, {len(self.df)} records in data")
            self.applied = False
            return self.df

        q = f"""
        MATCH (on:`{self.meta['on_class']}`)
        WHERE id(on) in $node_ids
        SET on:`{self.meta['assign_label']}`
        RETURN count(*)
        """
        nodeid_colname = QueryBuilder.gen_id_col_name(self.meta["on_class_short_label"])
        params = {"node_ids": list(self.df[nodeid_colname])}
        res = self.method.interface.query(q, params)
        self._create_isa_relationship(self.method.interface, classes=self.meta["assign_label"])
        logger.info(f"\t{res[0]['count(*)']} class labels assigned")

        # add created node ids back to dataframe
        # TODO: can omit this operation if it is the last meta action?
        new_class_nodeid_colname = QueryBuilder.gen_id_col_name(self.meta.get("assign_short_label"))
        self.df[new_class_nodeid_colname] = self.df[nodeid_colname]

        # store node_ids for 'on_class' nodes, for rollback
        self.create_changes_node(
            self.action_node_id,
            {
                "action_id": self.action_id,
                "id_on": self.df[new_class_nodeid_colname].unique().tolist()
            }
        )

        self.applied = True
        return self.df

    def rollback(self, detached_nodes_dct):
        for node_id in self.applied_changes_node_id:
            logger.debug(
                f"\t{self}, {self.action_id}, applied_changes_node_id: {self.applied_changes_node_id}")
            q = f"""        
            MATCH (chg:Changes)
            WHERE id(chg) = $node_id
            RETURN size(chg.id_on) as labels_to_remove"""
            params = {"node_id": node_id}
            res = self.method.interface.query(q, params)[0]
            logger.info(
                f"\t\tRemoving {res['labels_to_remove']} labels of `{self.meta['assign_label']}` from class `{self.meta['on_class']} ")
            q = f"""
                MATCH (n:Changes) WHERE id(n) = $node_id
                WITH n.id_on as node_ids_remove_labels
                MATCH (instance)
                WHERE instance:`{self.meta["assign_label"]}` and instance:`{self.meta["on_class"]}`
                                and id(instance) in node_ids_remove_labels
                REMOVE instance:`{self.meta["assign_label"]}`
                WITH *
                OPTIONAL MATCH (instance)-[:IS_A]->(c:Class)-[:SUBCLASS_OF*0..10]->(class:Class)
                WHERE c.label = '{self.meta["assign_label"]}'
                OPTIONAL MATCH (instance)-[is_a_r:IS_A]->(class)
                DELETE is_a_r
                RETURN id(instance) as _id_labels_removed
                """
            params = {"node_id": node_id}
            res = self.method.interface.query(q, params)
            if res:
                self._delete_applied_metadata_node(node_id)
                logger.info(
                    f"\t\t{len(res)} labels successfully removed")
            else:
                logger.warning(
                    f"\t\tExpected Label `{self.meta['assign_label']}` not found on Class `{self.meta['on_class']}` ")
                self._delete_applied_metadata_node(node_id)
            self.applied = False
        return detached_nodes_dct

    def retrieve_json(self):
        logger.info(f"Building json: {self.action_id}")

        q = """
            CALL apoc.create.vNode(["Method"], {id: $method_id, type: 'assign_class'}) YIELD node as method
            WITH *
            MATCH (x:Class), (y:Class)
            WHERE (x.label = $class) AND (y.label = $class_on)
            CALL apoc.create.vNode(["Class"], apoc.map.submap(x, ['label'])) YIELD node as x_                
            CALL apoc.create.vNode(["Class"], apoc.map.submap(y, ['label'])) YIELD node as y_
            CALL apoc.create.vRelationship(method, "CLASS", {}, x_) YIELD rel as class_rel
            CALL apoc.create.vRelationship(method, "ON", {}, y_) YIELD rel as on_rel
            WITH [[method, class_rel, x_], [method, on_rel, y_]] as coll
            UNWIND coll as item
            WITH item[0] as x, item[1] as r, item[2] as y
            """
        params = {
            'class': self.meta.get('assign_label'),
            'class_on': self.meta.get('on_class'),
            'method_id': self.action_id
        }

        res = get_arrows_json_cypher(
            neo=self.interface,
            q=q,
            params=params,
            hide_labels=self.hide_labels,
            hide_props=self.hide_props
        )

        res = res[0]

        return res


class Link(AppliesChanges):
    def __init__(self, *args, **kwargs):
        self.df = None
        self.node_ids_dct = None
        self.linked_node_pairs = None
        self.rel_type = None
        super().__init__(*args, **kwargs)

    @staticmethod
    def _set_parent_class_labels(interface, class_: str):
        # getting subclasses to set
        q = """
        MATCH path = (c:Class)-[:SUBCLASS_OF*1..50]->(c2:Class)
        WHERE 
            c.label = $class
        AND 
            NOT EXISTS ( (c2)-[:SUBCLASS_OF]->(:Class) )
        RETURN [n in nodes(path)[1..] | n.label] as parent_classes
        """
        params = {'class': class_}
        res = interface.query(q, params)

        if res:
            # setting parent classes
            for pc in res[0]["parent_classes"]:
                q2 = f"""            
                MATCH (x:`{class_}`)
                SET x:`{pc}`
                """
                interface.query(q2)

    def _fetch_metadata(self, interface):
        q = f"""
        MATCH                                 
        (action)-[to_rel:LINK]->(rel:Relationship),
        (rel)-[:FROM]->(from:Class),
        (rel)-[:TO]->(to:Class)
        WHERE id(action) = $node_id                      
        OPTIONAL MATCH (action)-[tov_rel:TO_VALUE]->(to_term)<-[:HAS_CONTROLLED_TERM]-(to)  //for TO_VALUE
        OPTIONAL MATCH (action)-[fv_rel:FROM_VALUE]->(from_term)<-[:HAS_CONTROLLED_TERM]-(from)  //for TO_VALUE    
        OPTIONAL MATCH (to)-[:HAS_CONTROLLED_TERM]->(to_value_ct:Term)  //for checking compliance with ct when to_class_property
        OPTIONAL MATCH (from)-[:HAS_CONTROLLED_TERM]->(from_value_ct:Term)  //for checking compliance with ct when from_class_property
        OPTIONAL MATCH (from)-[:HAS_CONTROLLED_TERM]->(from_value_ct:Term)
        RETURN action.type as type, action.id as action_id, rel.relationship_type as relationship_type, 
        from.label as from_class, to.label as to_class, 
        CASE WHEN to_rel['how'] in ['merge', 'merge_to', 'create', 'create_to', 'merge_on_uri'] OR tov_rel IS NOT NULL THEN 
            '{RDFSLABEL}' 
        ELSE 
            NULL
        END as to_class_property,
        CASE WHEN to_rel['how'] in ['merge_from', 'create_from', 'merge_from_on_uri'] OR fv_rel IS NOT NULL THEN 
            '{RDFSLABEL}' 
        ELSE 
            NULL
        END as from_class_property,
        CASE WHEN to_rel['how'] in ['merge', 'merge_to', 'merge_from', 'merge_on_uri', 'merge_from_on_uri'] OR tov_rel IS NOT NULL THEN
            True
        ELSE
            False
        END as merge,
        CASE WHEN to_rel['how'] in ['merge_on_uri', 'merge_from_on_uri'] THEN
            True
        ELSE
            False
        END as use_uri,           
        to_term.`{RDFSLABEL}` as to_value,
        from_term.`{RDFSLABEL}` as from_value,
        collect(DISTINCT to_value_ct.`{RDFSLABEL}`) as to_class_property_ct,
        to.short_label as to_short_label,
        collect(DISTINCT from_value_ct.`{RDFSLABEL}`) as from_class_property_ct,
        from.short_label as from_short_label,
        to_rel.from_column as from_column, //if the column with FROM node_ids/values has been renamed in self.df then the altered name can be used
        to_rel.to_column as to_column, //if the column with TO node_ids/values has been renamed in self.df then the altered name can be used
        to_rel.how as link_how
        """
        params = {"node_id": self.action_node_id}
        res = self.method.interface.query(q, params)
        assert res, f"Could not find the required metadata for {params}: {self.dct}"
        res = res[0]
        # #renaming to_short_label if it should be renamed according to short_label on Relationship:
        for key in ["to_short_label", "from_short_label"]:
            res = {**res, **{key: self._rename_short_label_acc_to_rel(
                interface,
                method_id=self.method.name,
                parent_id=self.method.study,
                short_label=res[key]
            )}}
        self.meta = res

    def apply(self, df=None):
        super().apply(df)
        self.df = df

        if self.df.empty:
            logger.info(f"\tBypassing {self.action_id}, {len(self.df)} records in data")
            self.applied = False
            return self.df

        # add a warning if more than one identical uri exists in self.df
        if self.meta.get('use_uri'):
            it=None
            if self.meta.get('link_how')=='merge_on_uri': it='to'
            elif self.meta.get('link_how')=='merge_from_on_uri': it='from'
            if it is not None:    
                if "_uri_"+self.meta.get(f'{it}_short_label') in list(self.df.columns):
                    uri_col = "_uri_"+self.meta.get(f'{it}_short_label')     
                    if len(self.df[uri_col])>1 and self.df[uri_col].squeeze().is_unique is False:
                        logger.warning(f"More than 1 identical uri exists in column {uri_col}")     

        if "from_class" not in self.meta.keys():
            raise ValueError("from_class not found")
        if "to_class" not in self.meta.keys():
            raise ValueError("to_class not found")
        logger.info(f"\t{self}: `{self.meta.get('from_class')}` to `{self.meta.get('to_class')}`")

        node_ids_dct = {}
        nan_node_ids = []
        for it in ["from", "to"]: 
            node_ids_dct, merge_nan = self._generate_node_ids_for_linking(it=it, df=df, node_ids_dct=node_ids_dct,
                                                                          use_uri=self.meta.get('use_uri'))
            if merge_nan:
                res = self._property_cleanup(it, node_ids_dct)
                nan_node_ids = res[0].get('nan_node_ids')
                logger.debug(f"Link data created {len(nan_node_ids)} nodes with NULL properties during nan cleanup")

        if len(self.node_ids_dct) == 2:
            q = f"""
            UNWIND $node_ids as nodeid_pair
            MATCH (from), (to)
            WHERE id(from) = nodeid_pair[0] and id(to) = nodeid_pair[1]
            MERGE (from)-[new_rel_id:`{self.rel_type}`]->(to) 
            RETURN id(from) as id_from, id(to) as id_to, id(new_rel_id) as new_rel_id
            """
            params = {"node_ids": [list(tpl) for tpl in zip(self.node_ids_dct["from"], self.node_ids_dct["to"])]}
            res = self.method.interface.query(q, params)

            # nothing to be written in the self.df as nodeids are already there
        elif self.meta.get('from_value') is None:
            q = f"""
            MERGE (to:`{self.meta['to_class']}`{{`{self.meta['to_class_property']}`:$to_value}})
            WITH *
            UNWIND $node_ids as nodeid
            MATCH (from)
            WHERE id(from) = nodeid
            MERGE (from)-[new_rel_id:`{self.rel_type}`]->(to)
            RETURN id(from) as id_from, id(to) as id_to, id(new_rel_id) as new_rel_id
            """
            params = {"node_ids": self.node_ids_dct["from"],
                      "to_value": self.meta["to_value"]}
            res = self.method.interface.query(q, params)
            self._set_parent_class_labels(self.method.interface, class_=self.meta["to_class"])

            nodeid_colname = QueryBuilder.gen_id_col_name(self.meta.get("to_short_label"))
            # writing the created node ids back to dataframe (if further processing is needed)
            self.df[nodeid_colname] = [r["id_to"] for r in res]
        elif self.meta.get("to_value") is None:
            q = f"""
            MERGE (from:`{self.meta['from_class']}`{{`{self.meta['from_class_property']}`:$from_value}})
            WITH *
            UNWIND $node_ids as nodeid
            MATCH (to)
            WHERE id(to) = nodeid
            MERGE (from)-[new_rel_id:`{self.rel_type}`]->(to)
            RETURN id(from) as id_from, id(to) as id_to, id(new_rel_id) as new_rel_id
            """
            params = {"node_ids": self.node_ids_dct["to"],
                      "from_value": self.meta["from_value"]}
            res = self.method.interface.query(q, params)
            self._set_parent_class_labels(self.method.interface, class_=self.meta["from_class"])
            nodeid_colname = QueryBuilder.gen_id_col_name(self.meta.get("from_short_label"))
            # writing the created node ids back to dataframe (if further processing is needed)
            self.df[nodeid_colname] = [r["id_from"] for r in res]
        else:
            assert self.meta.get("from_value") and self.meta.get("to_value")
            q = f"""
            MERGE   (from:`{self.meta['from_class']}`{{`{self.meta['from_class_property']}`:$from_value}})
            MERGE   (to:`{self.meta['to_class']}`{{`{self.meta['to_class_property']}`:$to_value}})
            MERGE (from)-[new_rel_id:`{self.rel_type}`]->(to)
            RETURN id(from) as id_from, id(to) as id_to, id(new_rel_id) as new_rel_id
            """
            params = {"from_value": self.meta["from_value"],
                      "to_value": self.meta["to_value"]}
            res = self.method.interface.query(q, params)
            # nothing to be written in the df TODO: check if this if this is required in the future
        self._create_isa_relationship(self.method.interface, classes=[self.meta["from_class"], self.meta[
            "to_class"]])  # TODO: identify which of the to/from actually needs isa

        if self.rel_type != 'SAME_AS':
            # we do not want to rollback SAME_AS rels so do not create Changes node to store thos changes for rollback
            created_rels = list(set([d['new_rel_id'] for d in res]))
            for d in res:
                d.pop('new_rel_id')
            self.linked_node_pairs = [dict(s) for s in set(frozenset(d.items()) for d in res)]
            logger.info(f"\t{len(self.linked_node_pairs)} links created")

            # store database IDs for nodes that were linked by this action: (action)-[APPLIED]->(changes)
            self.create_changes_node(
                self.action_node_id,
                {
                    "action_id": self.action_id,
                    "relationship_type": self.rel_type,
                    "nan_node_ids": nan_node_ids,
                    "id_rel": created_rels
                }
            )
        else:
            logger.debug(f"\tNo Changes node created as we do not rollback relationships of type {self.rel_type}")

        self.applied = True
        return self.df

    def _property_cleanup(self, it, node_ids):
        target_nodes = node_ids.get(it)
        target_property = self.meta.get(f'{it}_class_property')
        q = f"""
        MATCH (n) 
        WHERE ID(n) in $node_ids AND n.`{target_property}` = 'CLD_NAN'
        SET n.`{target_property}` = NULL
        RETURN collect(ID(n)) as nan_node_ids
        """
        res = self.method.interface.query(q, params={'node_ids': target_nodes})
        return res

    def _generate_node_ids_for_linking(self, it, df=None, node_ids_dct=None, use_uri=None):
        # it = 'from' or 'to'
        if node_ids_dct is None:
            node_ids_dct = {}
        self.df = df
        self.node_ids_dct = node_ids_dct
        merge_nan = False
        if self.meta.get(f"{it}_value"):
            pass
        elif self.meta.get(f"{it}_class_property"):
            col_name = self.meta.get(f"{it}_short_label")
            # the preceeding Method should have derived column col_name (short_label of the Class)
            if not use_uri:
                assert col_name in self.df.columns, f"{col_name} not in {self.df.columns}"
            # validating that the derived values are compliant with controlled terms for the property
            if self.meta.get(f"{it}_class_property_ct"):
                for value in set(self.df[col_name]):
                    assert value in self.meta.get(f"{it}_class_property_ct"), \
                        f"Derived value {value} is not compliant with controlled terminology: (1) Extend the CT or (2) Update the derivation"
            
            # Find nan values and replace them with cld_nan_{index} string
            load_series = self.df.get(col_name, pd.Series(data=[None]*df.shape[0], name=col_name)).copy()
            if load_series.isna().values.any():
                load_series = load_series.fillna('CLD_NAN')
                merge_nan = True

            if use_uri:
                uri_col = "_uri_" + self.meta.get(f"{it}_short_label")
                assert uri_col in self.df.columns, f"{uri_col} not in {self.df.columns}"
                uri_series = self.df[uri_col]

                self.node_ids_dct[it] = self.method.interface.load_df(
                    df=pd.concat([load_series, uri_series], axis=1),
                    label=self.meta[f"{it}_class"],
                    rename={col_name: self.meta.get(f"{it}_class_property"),
                            uri_col: 'uri'},
                    merge=self.meta.get("merge"),
                    primary_key='uri'
                )
            else:
                self.node_ids_dct[it] = self.method.interface.load_df(
                    df=load_series,
                    label=self.meta[f"{it}_class"],
                    rename={col_name: self.meta.get(f"{it}_class_property")},
                    merge=self.meta.get("merge"),
                    primary_key=self.meta.get(f"{it}_class_property")
                )

            self._set_parent_class_labels(self.method.interface, class_=self.meta[f"{it}_class"])
            nodeid_colname = QueryBuilder.gen_id_col_name(self.meta[f"{it}_short_label"])
            # writing the created node ids back to dataframe (if further processing is needed)
            self.df[nodeid_colname] = self.node_ids_dct[it]
        else:
            if self.meta.get(f"{it}_column"):
                nodeid_colname = QueryBuilder.gen_id_col_name(self.meta.get(f"{it}_column"))
            else:
                nodeid_colname = QueryBuilder.gen_id_col_name(self.meta[f"{it}_short_label"])
            # assert nodeid_colname in self.df.columns, f"Missing {nodeid_colname} node ids in the modified_data for it: {it}"
            self.node_ids_dct[it] = list(self.df[nodeid_colname])
        self.rel_type = self.meta["relationship_type"] if self.meta["relationship_type"] else \
            self.method.interface.mm.gen_default_reltype(self.meta.get("to_class"))
        return self.node_ids_dct, merge_nan

    def rollback(self, detached_nodes_dct):
        for node_id in self.applied_changes_node_id:
            logger.debug(f"\t{self}, {self.action_id}, applied_changes_node_id: {self.applied_changes_node_id}")
            # delete relationship between id_from and id_to, stored as properties on Links node
            params = {"node_id": node_id}
            q_rel = f"""
                MATCH (l:Changes) WHERE id(l) = $node_id
                WITH l.id_rel as rels
                MATCH (from)-[r]->(to)
                WHERE id(r) in rels
                WITH r, from, to
                DELETE r
                RETURN id(from) as id_from, id(to) as id_to
            """
            res_rels = self.method.interface.query(q_rel, params)
            q_nan = """
                MATCH (l:Changes) WHERE id(l) = $node_id
                WITH l.nan_node_ids AS nan_nodes
                WITH CASE WHEN nan_nodes is NULL THEN [] ELSE nan_nodes END as nan_nodes
                MATCH (n)
                WHERE ID(n) in nan_nodes AND properties(n) = {}
                DETACH DELETE n
                RETURN nan_nodes
            """
            res_nan = self.method.interface.query(q_nan, params)
            if res_rels:
                self._delete_applied_metadata_node(node_id)
                logger.info(f"\t\t{self} rollback: deleted {len(res_rels)} relationships of "
                            f"type `{self.meta['relationship_type']}` "
                            f"between `{self.meta['from_class']}` and `{self.meta['to_class']}` ")
                if res_nan:
                    nan_nodes = res_nan[0].get('nan_nodes')
                    logger.info(f"\t\t{self} rollback also deleted {len(nan_nodes)} nan nodes "
                                f"created during link action.")
                else:
                    nan_nodes = []
                detached_node_pairs = [dict(s) for s in set(frozenset(d.items()) for d in res_rels)]
                detached_nodes_dct.append({"method_name": self.method.name,
                                           "action": self,
                                           "action_id": self.action_id,
                                           "detached_nodes": list(
                                               set([value for d in detached_node_pairs for value in d.values()])),
                                           "deleted_nan_nodes": list(set(nan_nodes))})
            else:
                logger.warning(f"Expected relationships not found:"
                            f"type `{self.meta['relationship_type']}` "
                            f"between `{self.meta['from_class']}` and `{self.meta['to_class']}` "
                            f"Continuing...")
                self._delete_applied_metadata_node(node_id)
            self.applied = False
        return detached_nodes_dct

    def retrieve_json(self):
        logger.info(f"Building json: {self.action_id}")

        rel = {
            'from': self.meta.get('from_class'),
            'to': self.meta.get('to_class'),
            'type': self.meta.get('relationship_type')
        }

        if self.meta.get('to_value') and self.meta.get('from_value'):
            q = """
                CALL apoc.create.vNode(["Method"], {id: $method_id, type: 'link'}) YIELD node as method
                WITH *
                MATCH (from_term)<-[rft:HAS_CONTROLLED_TERM]-(x)<-[rf:FROM]-(r)-[rt:TO]->(y)-[rtt:HAS_CONTROLLED_TERM]->(to_term)
                WHERE r:Relationship AND x:Class AND y:Class
                AND ({from: x.label, to: y.label, type: r.relationship_type} = $rel)
                AND to_term.`rdfs:label` = $to_value_rdfs
                AND from_term.`rdfs:label` = $from_value_rdfs
                CALL apoc.create.vNode(["Class"], apoc.map.submap(x, ['label'])) YIELD node as x_                
                CALL apoc.create.vNode(["Class"], apoc.map.submap(y, ['label'])) YIELD node as y_
                CALL apoc.create.vRelationship(r, "FROM", {}, x_) YIELD rel as rf_
                CALL apoc.create.vRelationship(r, "TO", {}, y_) YIELD rel as rt_                
                CALL apoc.create.vNode(["Term"], apoc.map.submap(from_term, ['Codelist Code', 'Term Code'])) YIELD node as from_term_
                CALL apoc.create.vNode(["Term"], apoc.map.submap(to_term, ['Codelist Code', 'Term Code'])) YIELD node as to_term_
                CALL apoc.create.vRelationship(x_, "HAS_CONTROLLED_TERM", {}, from_term_) YIELD rel as rft_
                CALL apoc.create.vRelationship(y_, "HAS_CONTROLLED_TERM", {}, to_term_) YIELD rel as rtt_
                CALL apoc.create.vRelationship(method, "LINK", 
                    CASE $link_how 
                        WHEN 'merge' THEN {how: 'merge'} 
                        WHEN 'merge_from' THEN {how: 'merge_from'} 
                        WHEN 'create' THEN {how: 'create'} 
                        WHEN 'create_from' THEN {how: 'create_from'} 
                        WHEN 'merge_on_uri' THEN {how: 'merge_on_uri'}
                        WHEN 'merge_from_on_uri' THEN {how: 'merge_from_on_uri'}
                        ELSE {} END, r) YIELD rel
                CALL apoc.create.vRelationship(method, "FROM_VALUE", {}, from_term_) YIELD rel as from_val_rel
                CALL apoc.create.vRelationship(method, "TO_VALUE", {}, to_term_) YIELD rel as to_val_rel                
                WITH [[r,rf_,x_],[r,rt_,y_],[method, rel, r], [from_term_, rft_, x_], [to_term_, rtt_, y_], [method, from_val_rel, from_term_], [method, to_val_rel, to_term_]] as coll
                UNWIND coll as item
                WITH item[0] as x, item[1] as r, item[2] as y
                """
            params = {
                'link_how': self.meta.get('link_how'),
                'rel': rel,
                'to_value_rdfs': self.meta.get('to_value'),
                'from_value_rdfs': self.meta.get('from_value'),
                'method_id': self.action_id
            }
        elif self.meta.get('from_value'):
            q = """
                CALL apoc.create.vNode(["Method"], {id: $method_id, type: 'link'}) YIELD node as method
                WITH *
                MATCH (from_term)<-[rft:HAS_CONTROLLED_TERM]-(x)<-[rf:FROM]-(r)-[rt:TO]->(y)
                WHERE r:Relationship AND x:Class AND y:Class
                AND ({from: x.label, to: y.label, type: r.relationship_type} = $rel)
                AND from_term.`rdfs:label` = $from_value_rdfs
                CALL apoc.create.vNode(["Class"], apoc.map.submap(x, ['label'])) YIELD node as x_                
                CALL apoc.create.vNode(["Class"], apoc.map.submap(y, ['label'])) YIELD node as y_
                CALL apoc.create.vRelationship(r, "FROM", {}, x_) YIELD rel as rf_
                CALL apoc.create.vRelationship(r, "TO", {}, y_) YIELD rel as rt_
                CALL apoc.create.vNode(["Term"], apoc.map.submap(from_term, ['Codelist Code', 'Term Code'])) YIELD node as from_term_
                CALL apoc.create.vRelationship(x_, "HAS_CONTROLLED_TERM", {}, from_term_) YIELD rel as rft_                
                CALL apoc.create.vRelationship(method, "LINK", 
                    CASE $link_how 
                        WHEN 'merge' THEN {how: 'merge'} 
                        WHEN 'merge_from' THEN {how: 'merge_from'} 
                        WHEN 'create' THEN {how: 'create'} 
                        WHEN 'create_from' THEN {how: 'create_from'} 
                        WHEN 'merge_on_uri' THEN {how: 'merge_on_uri'}
                        WHEN 'merge_from_on_uri' THEN {how: 'merge_from_on_uri'}
                        ELSE {} END, r) YIELD rel
                CALL apoc.create.vRelationship(method, "FROM_VALUE", {}, from_term_) YIELD rel as from_val_rel
                WITH [[r,rf_,x_],[r,rt_,y_],[method, rel, r], [from_term_, rft_, x_], [method, from_val_rel, from_term_]] as coll
                UNWIND coll as item
                WITH item[0] as x, item[1] as r, item[2] as y
                """
            params = {
                'link_how': self.meta.get('link_how'),
                'rel': rel,
                'from_value_rdfs': self.meta.get('from_value'),
                'method_id': self.action_id
            }
        elif self.meta.get('to_value'):
            q = """
                CALL apoc.create.vNode(["Method"], {id: $method_id, type: 'link'}) YIELD node as method
                WITH *
                MATCH (x)<-[rf:FROM]-(r)-[rt:TO]->(y)-[rtt:HAS_CONTROLLED_TERM]->(to_term)
                WHERE r:Relationship AND x:Class AND y:Class
                AND ({from: x.label, to: y.label, type: r.relationship_type} = $rel)
                AND to_term.`rdfs:label` = $to_value_rdfs
                CALL apoc.create.vNode(["Class"], apoc.map.submap(x, ['label'])) YIELD node as x_                
                CALL apoc.create.vNode(["Class"], apoc.map.submap(y, ['label'])) YIELD node as y_
                CALL apoc.create.vRelationship(r, "FROM", {}, x_) YIELD rel as rf_
                CALL apoc.create.vRelationship(r, "TO", {}, y_) YIELD rel as rt_
                CALL apoc.create.vNode(["Term"], apoc.map.submap(to_term, ['Codelist Code', 'Term Code'])) YIELD node as to_term_
                CALL apoc.create.vRelationship(y_, "HAS_CONTROLLED_TERM", {}, to_term_) YIELD rel as rtt_        
                CALL apoc.create.vRelationship(method, "LINK", 
                    CASE $link_how 
                        WHEN 'merge' THEN {how: 'merge'} 
                        WHEN 'merge_from' THEN {how: 'merge_from'} 
                        WHEN 'create' THEN {how: 'create'} 
                        WHEN 'create_from' THEN {how: 'create_from'} 
                        WHEN 'merge_on_uri' THEN {how: 'merge_on_uri'}
                        WHEN 'merge_from_on_uri' THEN {how: 'merge_from_on_uri'}
                        ELSE {} END, r) YIELD rel
                CALL apoc.create.vRelationship(method, "TO_VALUE", {}, to_term_) YIELD rel as to_val_rel
                WITH [[r,rf_,x_],[r,rt_,y_],[method, rel, r], [to_term_, rtt_, y_], [method, to_val_rel, to_term_]] as coll
                UNWIND coll as item
                WITH item[0] as x, item[1] as r, item[2] as y
                """
            params = {
                'link_how': self.meta.get('link_how'),
                'rel': rel,
                'to_value_rdfs': self.meta.get('to_value'),
                'method_id': self.action_id
            }
        else:
            q = """
                CALL apoc.create.vNode(["Method"], {id: $method_id, type: 'link'}) YIELD node as method
                WITH *
                MATCH (x)<-[rf:FROM]-(r)-[rt:TO]->(y)
                WHERE r:Relationship AND x:Class AND y:Class 
                AND ({from: x.label, to: y.label, type: r.relationship_type} = $rel)
                CALL apoc.create.vNode(["Class"], apoc.map.submap(x, ['label'])) YIELD node as x_                
                CALL apoc.create.vNode(["Class"], apoc.map.submap(y, ['label'])) YIELD node as y_
                CALL apoc.create.vRelationship(r, "FROM", {}, x_) YIELD rel as rf_
                CALL apoc.create.vRelationship(r, "TO", {}, y_) YIELD rel as rt_
                CALL apoc.create.vRelationship(method, "LINK", 
                    CASE $link_how 
                        WHEN 'merge' THEN {how: 'merge'} 
                        WHEN 'merge_from' THEN {how: 'merge_from'} 
                        WHEN 'create' THEN {how: 'create'} 
                        WHEN 'create_from' THEN {how: 'create_from'} 
                        WHEN 'merge_on_uri' THEN {how: 'merge_on_uri'}
                        WHEN 'merge_from_on_uri' THEN {how: 'merge_from_on_uri'}
                        ELSE {} END, r) YIELD rel
                WITH [[r,rf_,x_],[r,rt_,y_],[method, rel, r]] as coll
                UNWIND coll as item
                WITH item[0] as x, item[1] as r, item[2] as y
                """
            params = {
                'link_how': self.meta.get('link_how'),
                'rel': rel,
                'method_id': self.action_id
            }

        res = get_arrows_json_cypher(
            neo=self.interface,
            q=q,
            params=params,
            hide_labels=self.hide_labels,
            hide_props=self.hide_props
        )
        res = res[0]

        return res


class LinkStat(AppliesChanges):
    def __init__(self, action_dict, method, dont_fetch=True):
        super().__init__(action_dict, method, dont_fetch=dont_fetch)

    def _fetch_metadata(self, interface):
        pass

    def apply(self, df=None):
        super().apply(df)
        self.df = df
        # results, results_long, dimensions, dimensions_long, statistics, statistics_long
        logger.info(f"""
        Linking statistic {self.meta.get('statistics')} 
        for `{self.meta.get('results_long')}` 
        with `{[dim.get('dimension') for dim in self.meta.get('dimensions_long')]}`
        """)
        assert len(
            self.method.meta.get("results_long")) == 1, f"link_stat supports only 1 result at the moment: {self.meta}"
        q = """
            WITH apoc.map.mergeList([
            apoc.map.fromPairs(apoc.coll.zip($statistics_long, $statistics)),
            apoc.map.fromPairs(apoc.coll.zip($results_long, $results)),
            apoc.map.fromPairs(apoc.coll.zip($dimensions_long, $dimensions))
        ]) as lbl_map //short_label property cannot be used as sometimes short_label comes from Relationship                                     
        MATCH (c:Class) WHERE c.label in $statistics_long                                        
        MATCH (res_c:Class) WHERE res_c.label in $results_long
        MATCH (dim_c:Class) WHERE dim_c.label in $dimensions_long                    
        UNWIND $data as row
        CALL apoc.merge.node(
            ['Resource', c.label], 
            {uri:row['_uri_' + lbl_map[c.label]]}, 
            {`rdfs:label`: row[lbl_map[c.label]]}, 
            {}
        ) YIELD node
        CALL apoc.merge.relationship(res_c, c.label, {}, {}, node, {}) YIELD rel as rel_to_class
        MATCH (dim)
        WHERE id(dim) = row['_id_' + lbl_map[dim_c.label]]        
        CALL apoc.merge.relationship(dim, c.label, {}, {}, node, {}) YIELD rel
        WITH row, apoc.map.fromPairs(collect(['_id_' + c.short_label, id(node)])) as nodeid_map
        RETURN apoc.map.merge(row, nodeid_map) as res
        """
        params = {
            'data': self.df.to_dict(orient='records'),
            'statistics': self.meta.get("statistics"),
            'statistics_long': self.meta.get("statistics_long"),
            'results': self.meta.get("results"),
            'results_long': self.meta.get("results_long"),
            'dimensions': [dim["dimension"] for dim in self.meta.get("dimensions")],
            'dimensions_long': [dim["dimension"] for dim in self.meta.get("dimensions_long")]
            # 'dimensions': [dim["dimension"] for dim in self.method.dct.get("dimensions")],
            # 'dimensions_long': [dim["dimension"] for dim in self.method.dct.get("dimensions_long")]
        }
        res = self.method.interface.query(q, params)

        # assert len(res) == self.df.shape[0], f"Could not link statistics correctly {params}"
        if not res:
            logger.error(f"Could not link statistics correctly {params}: no matching node ids for dimension nodes")
            raise RuntimeError()
        elif len(res) != self.df.shape[0]:
            logger.warning(
                f"Some statistics links have not been created{params}: potentially some of the dimension values are missing at some records")
        if res:
            self.df = pd.DataFrame([x['res'] for x in res])
            self._create_isa_relationship(self.method.interface, classes=self.meta["statistics_long"])
        # store URIs in dedicated node, for rollback
        created_uris = []
        for stat in self.meta.get("statistics"):
            created_uris.extend(df[f"_uri_{stat}"].unique())

        self.create_changes_node(
            self.action_node_id,
            {
                "action_id": self.action_id,
                "created_uris": created_uris
            }
        )
        self.applied = True
        return self.df

    def rollback(self, detached_nodes_dct):
        for node_id in self.applied_changes_node_id:
            logger.debug(f"\t{self}, {self.action_id}, applied_changes_node_id: {self.applied_changes_node_id}")
            q = f"""
                MATCH (n:Changes) where id(n)= $node_id
                WITH n.created_uris as uris_to_rollback
                MATCH (statistics) 
                WHERE statistics.uri in uris_to_rollback
                WITH statistics, id(statistics) as id_stats
                DETACH DELETE statistics
                RETURN id_stats
            """
            params = {"node_id": node_id}
            res = self.method.interface.query(q, params)
            if res:
                self._delete_applied_metadata_node(node_id)
                logger.info(f"\tdeleted {len(res)} statistics nodes")
            else:
                logger.warning(f"\t\tExpected statistics nodes CANNOT be found for {self.method.name}!")
                self._delete_applied_metadata_node(node_id)
                logger.info(f"\tContinuing...")
            self.applied = False
        return detached_nodes_dct


class BuildUri(Action):

    def _fetch_metadata(self, interface):
        q = """
        MATCH (action)
        WHERE id(action) = $node_id
        OPTIONAL MATCH (action)-[:URI_FOR]->(uri_for:Class)                
        OPTIONAL MATCH (action)-[:URI_BY]->(uri_by:Class)
        OPTIONAL MATCH (action)-[:URI_LABEL]->(uri_label:Class)
        WITH action, 
            apoc.coll.sortMaps(collect(DISTINCT uri_for), 'label') as uri_fors, 
            apoc.coll.sortMaps(collect(DISTINCT uri_by), 'label') as uri_bys,
            apoc.coll.sortMaps(collect(DISTINCT uri_label), 'label') as uri_labels
        RETURN 
           action{.*} as m,
           action.prefix as prefix,
           action.store_on_existing_nodes as store_on_existing_nodes,
           [x in uri_fors | x.short_label] as uri_fors,
           [x in uri_fors | x.label] as uri_fors_long,
           [x in uri_bys | x.short_label] as uri_bys,
           [x in uri_bys | x.label] as uri_bys_long,
           [x in uri_labels | x.short_label] as uri_labels,
           [x in uri_labels | x.label] as uri_labels_long
        """
        params = {"node_id": self.action_node_id}
        res = self.method.interface.query(q, params)
        assert res, f"Could not find the required metadata for {params}: {self.dct}"
        self.meta = res[0]

    def apply(self, df=None):
        super().apply(df)

        assert self.meta.get('uri_fors_long') and (self.meta.get('uri_bys_long') or self.meta.get('uri_labels_long')), \
            f"Cannot perform build uri action without a FOR and at least 1 BY or 1 LABEL!"
        logger.info(f"Building uri `{self.meta.get('uri_fors_long')}` with `{self.meta.get('uri_bys_long')} and uri labels {self.meta.get('uri_labels_long')}`")
        self.df = df
        prefix = self.meta.get("prefix")
        if not prefix:
            prefix = ""
        for col in self.meta.get("uri_bys"):
            assert col in self.df.columns, f'required BY column {col} not found in df columns {self.df.columns}'
        # uri_fors, uri_fors_long, uri_bys, uri_bys_long, uri_labels, uri_labels_long
        for i, f in enumerate(self.meta.get("uri_fors")):
            self.df["_uri_" + f] = self.df[[by for by in self.meta.get("uri_bys")]].apply(
                lambda row: prefix + "_" + f + "_by_" + "/".join([
                    key + ":" + str(item) for key, item in row.items()]), axis=1)
            if self.meta.get("uri_labels"):
                self.df["_uri_" + f] = self.df["_uri_" + f].map(lambda x: x + "_label_" + "/".join(self.meta.get("uri_labels")))

            if self.meta.get("store_on_existing_nodes") == "true":
                uri_property_name = "_uri_" + f
                id_property_name = "_id_" + f
                df = self.df[[uri_property_name, id_property_name]]
                
                q = f"""
                UNWIND $data as row
                MATCH (node)
                WHERE id(node) = row['{id_property_name}']
                SET node.uri = row['{uri_property_name}']
                """    
                params = {"data": df.to_dict(orient='records')}
                self.method.interface.query(q, params)
        return self.df

    def retrieve_json(self):

        def get_unique_key(node, label, prop_filter):
            key = node['properties'][prop_filter] + ' ' + label
            return key

        q1 = '''
        CALL apoc.create.vNode(["Method"], 
            CASE 
                WHEN $prefix <> '' THEN {id: $method_id, type: 'build_uri', prefix: $prefix} 
                ELSE {id: $method_id, type: 'build_uri'} 
            END) YIELD node as method
        MATCH (c:Class) WHERE c.label in $for_labels 
        CALL apoc.create.vNode(["Class"], apoc.map.submap(c, ['label', 'label'])) YIELD node as node
        CALL apoc.create.vRelationship(method, "URI_FOR", {}, node) YIELD rel as rel
        WITH [[method, rel, node]] as coll
        UNWIND coll as item
        WITH item[0] as x, item[1] as r, item[2] as y
        '''

        q2 = '''
        CALL apoc.create.vNode(["Method"], 
            CASE 
                WHEN $prefix <> '' THEN {id: $method_id, type: 'build_uri', prefix: $prefix} 
                ELSE {id: $method_id, type: 'build_uri'} 
            END) YIELD node as method
        MATCH (c:Class) WHERE c.label in $by_labels 
        CALL apoc.create.vNode(["Class"], apoc.map.submap(c, ['label', 'label'])) YIELD node as node
        CALL apoc.create.vRelationship(method, "URI_BY", {}, node) YIELD rel as rel
        WITH [[method, rel, node]] as coll
        UNWIND coll as item
        WITH item[0] as x, item[1] as r, item[2] as y
        '''

        q3 = '''
        CALL apoc.create.vNode(["Method"], 
            CASE 
                WHEN $prefix <> '' THEN {id: $method_id, type: 'build_uri', prefix: $prefix} 
                ELSE {id: $method_id, type: 'build_uri'} 
            END) YIELD node as method
        MATCH (c:Class) WHERE c.label in $uri_labels 
        CALL apoc.create.vNode(["Class"], apoc.map.submap(c, ['label', 'label'])) YIELD node as node
        CALL apoc.create.vRelationship(method, "URI_LABEL", {}, node) YIELD rel as rel
        WITH [[method, rel, node]] as coll
        UNWIND coll as item
        WITH item[0] as x, item[1] as r, item[2] as y
        '''

        params = {
            'for_labels': self.meta.get('uri_fors_long'),
            'by_labels': self.meta.get('uri_bys_long'),
            'uri_labels': self.meta.get('uri_labels_long'),
            'prefix': self.meta.get('prefix'),
            'method_id': self.action_id
        }

        res1 = get_arrows_json_cypher(
            neo=self.interface,
            q=q1,
            params=params,
            hide_labels=self.hide_labels,
            hide_props=self.hide_props
        )
        res1 = res1[0]

        res2 = get_arrows_json_cypher(
            neo=self.interface,
            q=q2,
            params=params,
            hide_labels=self.hide_labels,
            hide_props=self.hide_props
        )
        res2 = res2[0]

        res3 = get_arrows_json_cypher(
            neo=self.interface,
            q=q3,
            params=params,
            hide_labels=self.hide_labels,
            hide_props=self.hide_props
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
            node_indexes_to_skip = []
            for node_index, node in enumerate(res['nodes']):
                for label in filter_node_dict.keys():
                    if label in node['labels']:
                        prop_filter = filter_node_dict[label]  # label or id
                        unique_node_key = get_unique_key(node, label, prop_filter)
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


class BranchAction(Action):

    def __init__(self, action_dict, method, dont_fetch=True):
        super().__init__(action_dict, method, dont_fetch=dont_fetch)

    def _fetch_metadata(self, interface):
        pass


class BranchSave(BranchAction):

    def apply(self, df=None):
        super().apply(df)
        self.df = df
        if self.method.branch_dfs is None:
            self.method.branch_dfs = {}
        assert isinstance(self.method.branch_dfs, dict), f'Expected the method attribute branch_dfs to be of type dict. It was of type {type(self.method.branch_dfs)}'
        self.method.branch_dfs[self.meta.get("id")] = self.df.copy()
        logger.info(f"Saved data as: {self.meta.get('id')}")
        return self.df


class BranchLoad(BranchAction):

    def apply(self, df=None):
        super().apply(df)
        assert isinstance(self.method.branch_dfs, dict), f'Expected the method attribute branch_dfs to be of type dict. It was of type {type(self.method.branch_dfs)}'
        assert self.meta.get("id") in self.method.branch_dfs, f'Expected the method attribute branch_dfs to contain the key {self.meta.get("id")}, however it has keys {self.method.branch_dfs.keys()}'
        self.df = self.method.branch_dfs[self.meta.get("id")].copy()
        logger.info(f"Loaded data from: {self.meta.get('id')}")
        return self.df


class BranchCombine(BranchAction):
    def __init__(self, action_dict, method, dont_fetch=False):
        super().__init__(action_dict, method, dont_fetch=dont_fetch)

    def _fetch_metadata(self, interface):
        q = """
        MATCH (action)-[:METHOD_BRANCH]->(branch:Method)
        WHERE id(action) = $node_id                       
        RETURN collect(branch.id) as branches                            
        """
        params = {"node_id": self.action_node_id}
        res = self.method.interface.query(q, params)
        assert res, f"Could not find the required metadata for {params}: {self.dct}"
        self.meta = res[0]

    def apply(self, df=None):
        super().apply(df)
        assert all(id in self.method.branch_dfs for id in self.meta.get("branches")), f'Expected the method attribute branch_dfs to contain the key {self.meta.get("id")}, however it has keys {self.method.branch_dfs.keys()}'
        dfs = [self.method.branch_dfs[id].copy() for id in self.meta.get("branches")]
        df = dfs[0]
        for i in range(1, len(dfs)):
            by = [col for col in df.columns if col in dfs[i].columns]
            df = pd.merge(df, dfs[i], on=by)
        self.df = df.copy()
        logger.info(f"Combined data from: {self.meta.get('branches')}")
        return self.df
