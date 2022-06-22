import pytest
from query_builders import query_builder


# Provide QueryBuilder object that can be used by the various tests that need it
# (inside, it includes a database connection)
@pytest.fixture(scope="module")
def qbr():
    qbr = query_builder.QueryBuilder(mode='schema_PROPERTY')
    qbr.clean_slate()
    yield qbr


def test_qb_generate_query_body(qbr):
    # Test out some pathological cases for starters (non-existent `Class` nodes)
    (q, data_dict) = qbr.qb_generate_query_body([""])
    assert q.strip() == "MATCH (``:``)"
    assert data_dict == {}

    (q, data_dict) = qbr.qb_generate_query_body(["cl"])
    assert q.strip() == "MATCH (`cl`:`cl`)"
    assert data_dict == {}

    (q, data_dict) = qbr.qb_generate_query_body(["cl1", "cl2"])
    assert q.replace("\n", "").strip() == "MATCH (`cl1`:`cl1`),(`cl2`:`cl2`)"
    assert data_dict == {}

    # A simple but non-trivial run, with 2 `Class` nodes and a "CLASS_RELATES_TO" relationship between them
    qbr.clean_slate()
    my_classes = qbr.mm.create_related_classes_from_list([['Study', 'Site']])
    (q, data_dict) = qbr.qb_generate_query_body(my_classes, allow_unrelated_subgraphs=True)
    crop_q = q.replace("\n", "").strip()
    expected_q = "MATCH (`site`:`Site`),(`study`:`Study`),(`study`)-[:`HAS_SITE`]->(`site`)"
    assert crop_q == expected_q
    assert data_dict == {}

    # Now, prepare the data as was done in the 1st example in testqb_list_data_relationships_per_schema()
    qbr.clean_slate()
    my_classes = ['Study', 'Site', 'Subject', 'Parameter Category', 'Parameter']
    qbr.query(
        "UNWIND $classes as class CREATE (c1:Class{label:class})",
        {'classes': my_classes}
    )
    qbr.mm.create_custom_rels_from_list([['Study', 'Site'],
                                         ['Study', 'Subject'],
                                         ['Site', 'Subject'],
                                         ['Parameter Category', 'Parameter']])
    (q, data_dict) = qbr.qb_generate_query_body(my_classes, allow_unrelated_subgraphs=True)
    """
    Note: the internal variable match_node_list will be:
          ['(`study`:`Study`)', '(`site`:`Site`)', '(`subject`:`Subject`)', '(`parameter category`:`Parameter Category`)', '(`parameter`:`Parameter`)']
          and match_relationships_per_schema will be:
          ['(`parameter`)', 
          '(`parameter category`)-[:`HAS_PARAMETER`]->(`parameter`)', 
          '(`parameter category`)', 
          '(`site`)-[:`HAS_SUBJECT`]->(`subject`)', 
          '(`site`)', 
          '(`study`)-[:`HAS_SITE`]->(`site`)', 
          '(`study`)-[:`HAS_SUBJECT`]->(`subject`)', 
          '(`study`)', 
          '(`subject`)']
    """
    crop_q = q.replace("\n", "").strip()
    expected_q = "MATCH (`study`:`Study`),(`site`:`Site`),(`subject`:`Subject`)," \
               "(`parameter category`:`Parameter Category`),(`parameter`:`Parameter`)," \
               "(`parameter category`)-[:`HAS_PARAMETER`]->(`parameter`)," \
               "(`site`)-[:`HAS_SUBJECT`]->(`subject`),(`study`)-[:`HAS_SITE`]->(`site`),(`study`)-[:`HAS_SUBJECT`]->(`subject`)"
    # print(crop_q)
    # print(expected_q)
    assert crop_q == expected_q
    assert data_dict == {}

    # Repeat the last run, but this time with a mapping for the Cypher clause
    where_map = {
        'SITE': {
            'location': 'Miami'
        },
        'SUBJECT': {
            'user_id': 123
        }
    }

    (q, data_dict) = qbr.qb_generate_query_body(my_classes, where_map=where_map, allow_unrelated_subgraphs=True)

    expected_q += " WHERE `site`.`location` = $par_1 AND `subject`.`user_id` = $par_2"  # Same as before, but with an extra part
    assert q.replace("\n", "").strip() == expected_q
    assert data_dict == {'par_1': 'Miami', 'par_2': 123}


def test_generate_query_body_optional_match(qbr):
    qbr_o = query_builder.QueryBuilder(allow_optional_classes=True)
    qbr_o.clean_slate()
    qbr_o.mm.create_related_classes_from_list([
        ['Study', 'Site'],
        ['Site', 'Subject'],
        ['Study', 'Subject'],
        ['Site', 'SiteName']
    ])
    my_classes = ['Study', 'Site', 'SiteName**', 'Subject**']
    (q, data_dict) = qbr_o.generate_query_body(my_classes)
    q_crop = q.replace("\n", "").strip().replace(chr(10), '')

    q_expected = " ".join([
        "MATCH (`study`:`Study`),(`site`:`Site`),(`study`)-[:`HAS_SITE`]->(`site`)",
        "OPTIONAL MATCH (`sitename`:`SiteName`),(`site`)-[:`HAS_SITENAME`]->(`sitename`)",
        "OPTIONAL MATCH (`subject`:`Subject`),(`site`)-[:`HAS_SUBJECT`]->(`subject`),(`study`)-[:`HAS_SUBJECT`]->(`subject`)"
    ])
    print(q_crop)
    print(q_expected)

    assert q_crop == q_expected
    assert data_dict == {}

def test_qb_list_data_relationships_per_schema(qbr):
    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    qbr.clean_slate()
    my_classes = qbr.mm.create_related_classes_from_list([['Study', 'Site'],
                                                          ['Study', 'Subject'],
                                                          ['Site', 'Subject'],
                                                          ['Parameter Category', 'Parameter']])
    extra_classes = qbr.mm.create_related_classes_from_list([['Some Other Class 1', 'Some Other Class 2']])

    #allow_unrelated_subgraphs=True
    generated_list = qbr.qb_list_data_relationships_per_schema(my_classes, allow_unrelated_subgraphs=True)
    expected_list = ['(`parameter category`)-[:`HAS_PARAMETER`]->(`parameter`)',
                     '(`site`)-[:`HAS_SUBJECT`]->(`subject`)',
                     '(`study`)-[:`HAS_SITE`]->(`site`)',
                     '(`study`)-[:`HAS_SUBJECT`]->(`subject`)',
                     ]
    # print(generated_list)
    # print(expected_list)
    assert generated_list == expected_list

    #allow_unrelated_subgraphs = False
    generated_list2 = []
    try:
        generated_list2 = qbr.qb_list_data_relationships_per_schema(my_classes, allow_unrelated_subgraphs=False)
    except Exception as e:
        expected_exception = "Provided classes are not all related: ['Parameter', 'Parameter Category', 'Site', 'Study', 'Subject']"
        assert str(e) == expected_exception
    assert len(generated_list2) == 0

    #subclass and custom relationship type
    qbr.query("""
    MATCH (c:Class{label:'Parameter'})<-[r:CLASS_RELATES_TO]->(:Class)
    SET r.relationship_type = "INCLUDES"
    MERGE (c)<-[:SUBCLASS_OF]-(c2:Class{label:'Lab Parameter'})
    """)
    generated_list3 = qbr.qb_list_data_relationships_per_schema(
        ['Lab Parameter', 'Parameter Category', 'Site', 'Study', 'Subject'],
        allow_unrelated_subgraphs=True
    )
    expected_list3 = ['(`parameter category`)-[:`INCLUDES`]->(`lab parameter`)',
                     '(`site`)-[:`HAS_SUBJECT`]->(`subject`)',
                     '(`study`)-[:`HAS_SITE`]->(`site`)',
                     '(`study`)-[:`HAS_SUBJECT`]->(`subject`)',
                     ]
    assert generated_list3 == expected_list3


    # Try out an empty list
    generated_list = qbr.qb_list_data_relationships_per_schema([], allow_unrelated_subgraphs = True)
    assert generated_list == []

    # A simple case with just 1 class, and no "CLASS_RELATES_TO" relationships
    my_classes = ['color']
    qbr.clean_slate()
    qbr.query(
        "UNWIND $classes as class CREATE (c1:Class{label:class})",
        {'classes': my_classes}
    )
    generated_list = qbr.qb_list_data_relationships_per_schema(my_classes, allow_unrelated_subgraphs = True)
    expected_list = [] # this case the nodes with label 'color' will be matched with qb.list_data_labels - here an empty list is expected
    assert generated_list == expected_list

    # A slightly more complex case, with 2 class, and a "CLASS_RELATES_TO" relationship between them
    my_classes = ['car', 'vehicle']
    qbr.clean_slate()
    qbr.query(
        "UNWIND $classes as class CREATE (c1:Class{label:class})",
        {'classes': my_classes}
    )

    qbr.mm.create_custom_rels_from_list([['car', 'vehicle']])
    generated_list = qbr.qb_list_data_relationships_per_schema(my_classes, allow_unrelated_subgraphs = True)
    expected_list = ['(`car`)-[:`HAS_VEHICLE`]->(`vehicle`)']
    assert generated_list == expected_list
