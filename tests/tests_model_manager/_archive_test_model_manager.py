import pytest
from model_managers import model_manager
from utils.utils import compare_unordered_lists, compare_recordsets
import pandas as pd


# Provide ModelManager object that can be used by the various tests that need it
# (inside, it includes a database connection)
@pytest.fixture(scope="module")
def mm():
    mm = model_manager.ModelManager(apoc=True)
    yield mm



def test_create_class(mm):
    mm.clean_slate()

    mm.create_class("My First Class")
    result = mm.get_nodes()
    assert result == [{'label': 'My First Class'}]

    mm.create_class(["A", "B"])
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'My First Class'}, {'label': 'A'}, {'label': 'B'}])

    mm.create_class("A")     # Merge is True by default; so, nothing gets created since class "A" already exists
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'My First Class'}, {'label': 'A'}, {'label': 'B'}])

    mm.create_class("A", merge=False)     # A 2nd class named "A" will get created
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'My First Class'}, {'label': 'A'}, {'label': 'A'}, {'label': 'B'}])

    mm.create_class(["B", "X"], merge=True) # Only class "X" gets created, because "B" already exists
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'My First Class'}, {'label': 'A'}, {'label': 'A'}, {'label': 'B'}, {'label': 'X'}])



def test_create_custom_rels_from_list(mm):
    mm.clean_slate()

    mm.create_class(["Study", "Site", "Subject"])   # Note that "Race" is missing

    rels = [
                ["Study", "Site"],
                ["Study", "Subject"],
                ["Subject", "Race"],
                ["Subject", None]
           ]

    mm.create_custom_rels_from_list(rels, create_if_absent = False)
    q = '''
        MATCH (:Class {label:$l1})-[r:CLASS_RELATES_TO]-(:Class {label:$l2})
        RETURN count(r) AS cnt
        '''

    result = mm.query(q, {"l1": "Study", "l2": "Site"})
    assert result[0]["cnt"] == 1

    result = mm.query(q, {"l1": "Study", "l2": "Subject"})
    assert result[0]["cnt"] == 1

    result = mm.query(q, {"l1": "Subject", "l2": "Race"})
    assert result[0]["cnt"] == 0        # Race was not included, because no "Race" Class was present

    mm.create_custom_rels_from_list(rels, create_if_absent = True)
    result = mm.query(q, {"l1": "Study", "l2": "Site"})
    assert result[0]["cnt"] == 1

    result = mm.query(q, {"l1": "Study", "l2": "Subject"})
    assert result[0]["cnt"] == 1

    result = mm.query(q, {"l1": "Subject", "l2": "Race"})
    assert result[0]["cnt"] == 1        # This time it gets created



def test_get_all_classes(mm):
    mm.clean_slate()

    class_list = ["G", "S", "K"]
    mm.create_class(class_list)

    sorted = mm.get_all_classes(sort=True)
    assert sorted == [{'Class': 'G'}, {'Class': 'K'}, {'Class': 'S'}]

    unsorted = mm.get_all_classes(sort=False)
    assert compare_recordsets(unsorted, [{'Class': 'G'}, {'Class': 'K'}, {'Class': 'S'}])

    long = mm.get_all_classes(include_id=True, sort=True)
    for entry in long:
        assert type(entry["_id_Class"]) == int
        assert entry["Class"] in class_list
        


def test_rename_classes_by_ids(mm):
    mm.clean_slate()

    # Create 3 classes
    mm.create_class(["A", "B", "C"])
    q = '''
        MATCH(n)
        RETURN id(n) AS node_id, n.label AS node_label
        '''
    result_list = mm.query(q)
    # EXAMPLE: [{'node_id': 311, 'node_label': 'A'}, {'node_id': 312, 'node_label': 'B'}, {'node_id': 313, 'node_label': 'C'}]

    # Rename the 3 classes just created
    mapping = [[d['node_id'], d['node_label'] + " new"]
                        for d in result_list]
    # EXAMPLE: [[341, 'A new'], [342, 'B new'], [343, 'C new']]

    info = mm.rename_classes_by_ids(mapping)
    assert compare_recordsets(info, [{'mod': 'Class: A=>A new'}, {'mod': 'Class: B=>B new'}, {'mod': 'Class: C=>C new'}])

    for n in mapping:
        # Verify that there's exactly 1 node with the given Neo4j ID's and the new names in the "label" attribute
        (node_id, new_name) = n
        q = '''
            MATCH(n :Class {label: $name}) WHERE id(n) = $node_id
            RETURN count(n) as number_of_nodes
            '''
        params = {"name": new_name, "node_id": node_id}
        result = mm.query(q, params)
        assert result == [{'number_of_nodes': 1}]



def test_get_label_by_id(mm):
    mm.clean_slate()

    result = mm.create_class("A")
    A_id = result[0][0]['neo4j_id']
    assert mm.get_label_by_id(A_id) == "A"

    assert mm.get_label_by_id("Not an int") == "NA"
    assert mm.get_label_by_id(None) == "NA"


def test_create_property(mm):
    mm.clean_slate()

    mm.create_property("C", "P", merge=True)

    q = '''
        MATCH (n:Class {label:"C"})-[r:HAS_PROPERTY]->(p:Property {label:"P"})
        RETURN COUNT(r) AS rel_count
        '''
    result = mm.query(q)
    assert result[0]["rel_count"] == 1



def test_delete_rels(mm):
    mm.clean_slate()

    result = mm.create_class(["A", "B", "C"])
    A_id = result[0][0]['neo4j_id']
    B_id = result[1][0]['neo4j_id']
    C_id = result[2][0]['neo4j_id']

    rels = [
                ["A", "B"],
                ["A", "C"],
                ["B", "C"]
           ]
    mm.create_custom_rels_from_list(rels, create_if_absent = False)

    q = "MATCH (:Class)-[r:CLASS_RELATES_TO]->(:Class) RETURN COUNT(r) AS num_rel"

    result = mm.query(q)
    assert result[0]["num_rel"] == 3   # 3 "CLASS_RELATES_TO" relationships found


    data = [{'_id_Class1': A_id, '_id_Class2': B_id},
            {'_id_Class1': A_id, '_id_Class2': C_id},
            {'_id_Class1': B_id, '_id_Class2': C_id}]

    mm.delete_rels(data)

    result = mm.query(q)
    assert result[0]["num_rel"] == 0    # The relationships are now gone



def test_get_related_classes(mm):
    mm.create_class(["A", "B", "C", "Lone Wolf"])
    mm.create_class_relationship("A", "B")
    mm.create_class_relationship("B", "C")
    mm.create_class_relationship("A", "C")
    # Note: nothing connects to Class "Lone Wolf"

    df_result = mm.get_related_classes()
    print(df_result)

    q = '''
        MATCH (a {label:"A"}), (b {label:"B"}), (c {label:"C"})
        RETURN [id(a), id(b), id(c)] AS id_triplet
        '''
    query_result = mm.query(q)
    id_A, id_B, id_C = query_result[0]["id_triplet"]
    #print(id_A, id_B, id_C)

    df_expected = pd.DataFrame(
                 [  [id_A,"A", id_B,"B"] ,
                    [id_A,"A", id_C,"C"] ,
                    [id_B,"B", id_C,"C"]
                 ] ,
                 columns=["_id_Class1","Class1", "_id_Class2","Class2"])
    #print(df_expected)

    # Compare the dataframes regardless of row order,
    # since get_related_classes() guarantees column order but not row order
    df_result_sorted = df_result.sort_values(by=df_result.columns.tolist()).reset_index(drop=True)
    df_expected_sorted = df_expected.sort_values(by=df_result.columns.tolist()).reset_index(drop=True)
    #print(df_result_sorted)
    assert df_expected_sorted.equals(df_result_sorted)



def test_get_class_properties(mm):

    # Start by requesting a non-existent `Class` node
    result = mm.get_class_properties("I_dont_exist_sorry")
    assert result == {}

    # Test on one `Class` node NOT attached to any `Property` nodes
    q = """ 
        CREATE (:Class {label: "car"})
        """
    mm.query(q)
    result = mm.get_class_properties("car")
    assert result == {}

    # Attach a `Property` node to the `Class` node
    q = """ 
        MATCH (n:Class {label: "car"}) MERGE (n)-[:HAS_PROPERTY]->(p:Property {label: "color"})
        """
    mm.query(q)

    result = mm.get_class_properties("car")
    assert result == {'car': ['color']}

    # Attach a 2nd `Property` node to the `Class` node
    q = """
        MATCH (n:Class {label: "car"}) MERGE (n)-[:HAS_PROPERTY]->(p:Property {label: "make"})
        """
    mm.query(q)

    result = mm.get_class_properties("car")
    assert result == {'car': ['color', 'make']}

    # Attach a 3rd `Property` node to the `Class` node
    q = """
        MATCH (n:Class {label: "car"}) MERGE (n)-[:HAS_PROPERTY]->(p:Property {label: "year"})
        """
    mm.query(q)

    result = mm.get_class_properties("car")
    assert result == {'car': ['color', 'make', 'year']}

    # Now also request a 2nd, non-existing, `Class` node
    result = mm.get_class_properties(["car", "i_dont_exist"])
    assert result == {'car': ['color', 'make', 'year']}

    # Create a different `Class` node, NOT attached to any `Property` nodes
    q = """ 
        CREATE (:Class {label: "boat"})
        """
    mm.query(q)
    result = mm.get_class_properties(["car", "boat"])
    print(result)
    assert result == {'car': ['color', 'make', 'year']}
    # TODO: is the above result what we really want???  Or should it be: {'car': ['color', 'make', 'year'], 'boat': []}

    # Attach a `Property` node to the new ("boat") `Class` node
    q = """ 
        MATCH (n:Class {label: "boat"}) MERGE (n)-[:HAS_PROPERTY]->(p:Property {label: "number_masts"})
        """
    mm.query(q)

    result = mm.get_class_properties(["car", "boat"])
    assert result == {'car': ['color', 'make', 'year'], 'boat': ['number_masts']}

    # Attach another `Property` node to the new ("boat") `Class` node
    q = """ 
        MATCH (n:Class {label: "boat"}) MERGE (n)-[:HAS_PROPERTY]->(p:Property {label: "draft"})
        """
    mm.query(q)

    result = mm.get_class_properties(["car", "boat"])
    assert result == {'car': ['color', 'make', 'year'], 'boat': ['number_masts', 'draft']}

    # TODO: test the scenario of a `Property` node linked to
    #       by multiple distinct `Class` nodes.  Alexey said: "I think it can be a valid scenario in the future"


def test_load_mappings_from_df_1(mm):
    test_df = pd.DataFrame([
        {'Class':'ZZZ', 'Property':None},
        {'Class':'ZZZ', 'Property':'abc'},
        {'Class':'ZZZ', 'Property':'zyx', 'Source Data Table':'adsl', 'Source Data Column':'country'},
        {'Class':'ZZZ', 'Property':'new_property_no_mapping', 'Source Data Table':None, 'Source Data Column':'country'}
        ])
    mm.clean_slate()
    mm.query("""
    CREATE (adsl:`Source Data Table`{_domain_:'adsl'})-[:HAS_COLUMN]->(:`Source Data Column`{_columnname_:'country'})
    CREATE (adsl)-[:HAS_COLUMN]->(:`Source Data Column`{_columnname_:'dummy'})
    """)
    mm.load_mappings_from_df(test_df)
    result = mm.query(
        """
        MATCH (adsl:`Source Data Table`{_domain_:'adsl'})-[:HAS_COLUMN]->(country:`Source Data Column`{_columnname_:'country'}),
        (adsl)-[:HAS_COLUMN]->(:`Source Data Column`{_columnname_:'dummy'}), 
        (country)-[:MAPS_TO_PROPERTY]->(zyx:Property{label:'zyx'}),
        (zyx)<-[:HAS_PROPERTY]-(class:Class{label:'ZZZ'}),
        (class)-[:HAS_PROPERTY]->(:Property{label:'abc'}),
        (class)-[:HAS_PROPERTY]->(:Property{label:'new_property_no_mapping'})
        RETURN *
        """
    )
    assert len(result) == 1, "Check test database that the expected mapping was created"