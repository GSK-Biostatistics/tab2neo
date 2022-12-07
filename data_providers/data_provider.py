import logging
import pandas as pd
from neointerface import NeoInterface
from logger.logger import logger
from model_managers import ModelManager
from query_builders.query_builder import QueryBuilder

logger.setLevel(logging.INFO)


class DataProvider(NeoInterface):
    """
    Version 2
    To use the data already in the database (such as the data after the transformations with ModelApplier v2),
    for various purposes - such as to feed the User Interface.
    """
    OCLASS_MARKER = '**'

    def __init__(self, *args, **kwargs):
        self.qb = QueryBuilder()
        self.mm = ModelManager(*args, **kwargs)
        super().__init__(*args, **kwargs)
        if self.verbose:
            print(f"---------------- {self.__class__} initialized -------------------")

    def check_schema(self, labels: list, rels: list):
        for label in labels:
            q = "MATCH " + self.qb.generate_1match_schema_check(label=label) + " RETURN *"
            res = self.query(q)
            assert res, f"Class {label} not found in the schema"
        for rel in rels:
            q1, params1 = self.qb.generate_1rel_schema_check(rel, subclass_dir=">")
            q1 = "MATCH " + q1 + " RETURN * LIMIT 1"
            res1 = self.query(q1, params1)
            q2, params2 = self.qb.generate_1rel_schema_check(rel, subclass_dir="<")
            q2 = "MATCH " + q2 + " RETURN * LIMIT 1"
            res2 = self.query(q2, params2)
            assert res1 or res2, f"Relationship {rel} not found in the schema"

    def get_data(self, labels: list, rels: list = None, where_map=None, return_nodeid=False):
        """
        A typical get_data_generic call with nodes with multiple parameters and repeating property names btw labels
        :return: pd.DataFrame
        """
        self.get_data_generic(
            labels=labels,
            rels=rels,
            infer_rels=False,
            where_map=where_map,
            allow_unrelated_subgraphs=False,
            use_shortlabel=False,
            return_nodeid=return_nodeid,
            return_propname=True,
            check_schema=False,
            limit=None,
            return_q_and_dict=False)

    def get_data_cld(self,
                     labels: list,
                     rels: list = None,
                     where_map=None,
                     where_rel_map=None,
                     return_nodeid=True,
                     return_termorder=False,
                     return_class_uris=False):
        """
        A typical get_data_generic call for CLD project:
            Class-Relationship schema is used and shortlabel is used as the
            1 property per class named rdfs:label
        output df column names
        :return: pd.DataFrame, str, dict
        """
        return self.get_data_generic(
            labels=labels,
            rels=rels,
            infer_rels=True,
            where_map=where_map,
            where_rel_map=where_rel_map,
            allow_unrelated_subgraphs=False,
            use_shortlabel=True,
            return_nodeid=return_nodeid,
            return_propname=False,
            return_termorder=return_termorder,
            return_class_uris=return_class_uris,
            only_props=["rdfs:label"],
            check_schema=False,
            limit=None,
            return_q_and_dict=True)

    def get_data_generic(
            self,
            labels: list,
            rels: list = None,  # [{'from':<label1>, 'to':<label2>, 'type':<type>}, ...]
            labels_to_pack=None,  # {'label1': 'label1_def', 'label2': 'label2_def', 'label3': [term1, term2, ...], ...}
            infer_rels=False,
            where_map=None,
            where_rel_map=None,
            allow_unrelated_subgraphs: bool = False,
            use_shortlabel: bool = False,
            use_rel_labels: bool = True,
            # use_rel_labels only active when use_shortlabel == True; the short_label of the [:TO] class is updated with the one in Relationship.short_label
            return_nodeid: bool = True,
            return_propname: bool = True,
            return_termorder: bool = False,
            only_props: list = None,  # to return only specified properties. If None - return all
            return_disjoint: bool = False,  # to return dict with Class as key and distinct values as values set to True
            return_class_uris: bool = False,  # to return dict with Class as key and distinct values as values set to True
            check_schema=False,
            limit=None,
            return_q_and_dict=False
    ):
        if only_props:
            if isinstance(only_props, str):
                only_props = [only_props]
        assert len(labels) > 0 or len(rels) > 0
        if rels:
            for rel in rels:
                assert isinstance(rel, dict)
                for key in ['from', 'to']:
                    assert isinstance(rel.get(key), str) and len(rel.get(key)) > 0
        # #making sure all labels from the rels are in labels
        labels = self.qb.enrich_labels_from_rels(labels=labels, rels=rels, oclass_marker=self.OCLASS_MARKER)
        # #creating a list of labels without an optional match sign (**)
        labels_clean = []
        for label in labels:
            assert isinstance(label, str)
            if label.endswith(self.OCLASS_MARKER):
                labels_clean.append(label[:-len(self.OCLASS_MARKER)])
            else:
                labels_clean.append(label)
        # #empty where_map if None
        if not where_map:
            where_map = {}
        if not where_rel_map:
            where_rel_map = {}
        # #infer rels from the schema if rels are not provided and infer_rels parameter is True

        if not rels:
            if infer_rels:
                rels = self.mm.infer_rels(labels=labels, oclass_marker=self.OCLASS_MARKER)
            else:
                rels = []
        # #filling the rel.type with the default relationship type (HAS_ ...)
        rels = self.mm.gen_default_reltypes_list(rels)
        # #checking that the requested Class and Relationship nodes exist in the schema
        if check_schema:
            self.check_schema(labels=labels_clean, rels=rels)
        # #checking that there are not unrelated subgraphse if allow_unrelated_subgraphs set to True
        if not allow_unrelated_subgraphs:
            assert self.qb.check_connectedness(labels=labels_clean, rels=rels), \
                f"Diconnected subgraphs found in {(labels_clean, rels)}"
        # #building queries to run against the database
        q_res, q, params = None, None, None
        q_body_list = []
        if only_props:
            params = {'only_props': only_props}
        else:
            params = {}
        # #if any labels are marked for optional match - separately generating a query for each MATCH statement
        for i, (_labels, _rels) in enumerate(
                self.qb.split_out_optional(labels=labels, rels=rels, oclass_marker=self.OCLASS_MARKER)):
            # schema check for each subquery:
            if not allow_unrelated_subgraphs:
                assert self.qb.check_connectedness(labels=_labels, rels=_rels), \
                    f"Diconnected subgraphs found in {(_labels, _rels)}"

            # building sub-query
            _where_map = {k: i for k, i in where_map.items() if k in _labels}
            _where_rel_map = {k: i for k, i in where_rel_map.items() if k in _labels}
            # TODO: validate the insides of where_rel_map only contain labels in _labels

            if use_shortlabel:
                _labels, _rels, _labels_to_pack, _where_map, _where_rel_map = self.mm.translate_to_shortlabel(
                    _labels,
                    _rels,
                    labels_to_pack,
                    _where_map,
                    _where_rel_map,
                    use_rel_labels=use_rel_labels
                )
            # ------------------------------ QUERY BUILD ------------------------------------#
            (_q_body, _params) = self.qb.generate_query_body(
                labels=_labels,
                rels=_rels,
                match=("MATCH" if i == 0 else "OPTIONAL MATCH"),
                where_map=_where_map,
                where_rel_map=_where_rel_map
            )
            q_body_list.append(_q_body)
            params = {**params, **_params}

        q_body = "\n".join(q_body_list)
        if use_shortlabel:
            translated = self.mm.translate_to_shortlabel(labels_clean, rels, labels_to_pack, {},
                                                         use_rel_labels=use_rel_labels)
            labels_clean = [dct['short_label'] for dct in translated[0]]
            rels = translated[1]
            labels_to_pack = translated[2]

        # print(f'Labels to Pack (get_data_generic): {labels_to_pack}')

        q_call = "\n" + self.qb.generate_call(
            labels=labels_clean,
            rels=rels,
            labels_to_pack=labels_to_pack,
            only_props=only_props
        )

        q_with = "\n" + self.qb.generate_with(
            labels=labels_clean,
            labels_to_pack=labels_to_pack,
            only_props=only_props
        )

        q_return = "\n" + self.qb.generate_return(
            labels=labels_clean,
            labels_to_pack=labels_to_pack,
            return_nodeid=return_nodeid,
            return_propname=return_propname,
            return_termorder=return_termorder,
            return_class_uris=return_class_uris,
            only_props=only_props,
            return_disjoint=return_disjoint
        )
        q_limit = (f" LIMIT {str(limit)}" if limit else "")
        q = q_body + q_call + q_with + q_return + q_limit
        # ------------------------------ QUERY RUN ------------------------------------#
        if self.verbose:
            logger.info(f"QUERY: {q}")
            logger.info(f"PARAMS: {params}")
        q_res = self.query(q, params)
        if return_disjoint:
            res = q_res
        else:
            res = pd.DataFrame([r['all'] for r in q_res])
        # TODO: rename rdfs:label to short_label of each class
        if return_q_and_dict:
            return res, q, params
        else:
            return res
