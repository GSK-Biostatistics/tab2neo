import pytest
from query_builders.query_builder import QueryBuilder


# Provide QueryBuilder object that can be used by the various tests that need it
# (inside, it includes a database connection)
@pytest.fixture(scope="module")
def qbr():
    qbr = QueryBuilder(mode='noschema')
    qbr.clean_slate()
    yield qbr


def test_qbns_get_label_relationships_df(qbr):
    qbr.clean_slate()
    my_classes = ['Apple', 'Orange', 'Fruit']
    qbr.query("CREATE (a:Apple)-[:IS_A]->(f:Fruit), (o:Orange)-[:IS_A]->(f)")
    result = qbr.qbns_get_label_relationships_df(classes=my_classes).to_dict(orient='records')
    expected_result = [{'souce_lbl': 'Apple', 'rel': 'IS_A', 'target_lbl': 'Fruit'},
                       {'souce_lbl': 'Orange', 'rel': 'IS_A', 'target_lbl': 'Fruit'}]
    assert result == expected_result

def test_qbns_get_label_relationships_df2(qbr):
    qbr.clean_slate()
    ia = qbr.create_node_by_label_and_dict('Apple', {'type':1})
    io = qbr.create_node_by_label_and_dict('Orange', {'type': 3})
    if_ = qbr.create_node_by_label_and_dict('Fruit', {'n_types':9999})
    iarel = qbr.create_node_by_label_and_dict('AdditioanlInfo', {'desc': 'blah-blah'})
    qbr.link_nodes_by_ids(ia,if_,'IS_A')
    qbr.link_nodes_by_ids(io, if_, 'IS_A')
    qbr.link_nodes_by_ids(ia, iarel, 'IS_A')
    result = qbr.qbns_get_label_relationships_df(['Apple', 'Orange', 'Fruit', 'AdditioanlInfo']).to_dict(orient='records')
    expected_result = [{'souce_lbl': 'Apple', 'rel': 'IS_A', 'target_lbl': 'AdditioanlInfo'},
                       {'souce_lbl': 'Apple', 'rel': 'IS_A', 'target_lbl': 'Fruit'},
                       {'souce_lbl': 'Orange', 'rel': 'IS_A', 'target_lbl': 'Fruit'}]
    assert result == expected_result


def test_qbns_generate_query_body(qbr):
    qbr.clean_slate()
    my_classes = ['Apple', 'Orange', 'Fruit']
    qbr.query("CREATE (a:Apple)-[:IS_A]->(f:Fruit), (o:Orange)-[:IS_A]->(f)")
    cypher, cypher_dict = qbr.qbns_generate_query_body(classes=my_classes)
    expected_cypher = "MATCH (`apple`:`Apple`),(`orange`:`Orange`),(`fruit`:`Fruit`),(`apple`:`Apple`)-[:`IS_A`]->(`fruit`:`Fruit`),(`orange`:`Orange`)-[:`IS_A`]->(`fruit`:`Fruit`)"
    assert cypher.replace("\n", "").strip() == expected_cypher
    assert cypher_dict == {}

    cypher2, cypher_dict2 = qbr.qbns_generate_query_body(classes = my_classes,
                                                           where_map = {'Apple': {'label': 'NON-EXISTING VALUE'}})
    assert cypher_dict2 == {'par_1': 'NON-EXISTING VALUE'}
