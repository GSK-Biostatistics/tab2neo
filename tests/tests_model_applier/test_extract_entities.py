import pytest
from model_appliers import model_applier
import neointerface
from utils.utils import compare_recordsets


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = neointerface.NeoInterface()
    yield neo_obj


#################################   Tests for PART 1 of  extract_class_entities()  #################################


def test_extract_class_entities_part_1_A(db):
    """
    MAIN FOCUS: _extract_class_entities_part_1()
    Simple scenarios
    """

    # Completely clear the database
    db.clean_slate()

    # Create minimalist test data conforming to the structure expected by the _extract_class_entities_part_1() method
    # See image "State prior to call to refactor_all/Sample data for chain from classes to tables.PNG"
    q1 = """ 
        CREATE 
           (:Class {label: "car"})-[:HAS_PROPERTY]->(p:Property {label: "color"}),
           (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col:`Source Data Column` {_columnname_: "car_color"}),
           (sdt)<-[:HAS_TABLE]-(:`Source Data Folder`), 
           (col)-[:MAPS_TO_PROPERTY]->(p)
        """
    db.query(q1)

    mod_a = model_applier.ModelApplier()

    mod_a.debug = True
    res1 = mod_a._extract_class_entities_part_1()
    assert res1 == [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}]

    # Now add data to create a 2nd path also conforming to the expected structure, with the same `Class` and `Source Data Table` end points
    #   see "Chain from classes to tables 2.PNG"

    q2 = """
        MATCH (cl:Class {label: "car"}),
              (src:`Source Data Table` {_domain_: "Automotive"})
        MERGE
            (cl)-[:HAS_PROPERTY]->(p:Property {label: "make"})<-[:MAPS_TO_PROPERTY]-(col:`Source Data Column` {_columnname_: "car_make"})<-[:HAS_COLUMN]-(src)
        """
    db.query(q2)

    res2 = mod_a._extract_class_entities_part_1()
    assert res2 == [
        {'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color'], ['car_make', 'make']], 'lbl': ['car']}]


def test_extract_class_entities_part_1_B(db):
    """
    MAIN FOCUS: _extract_class_entities_part_1()
    More complex scenarios
    """

    # Completely clear the database
    db.clean_slate()

    # The following test data demonstrate the concept of reconciling broader classes ("vehicle")
    #       with more narrow tables ("Automotive" and "Aerospace")
    #       See "Chain from classes to tables 4.PNG"
    q1 = """ 
        CREATE 
           (:Class {label: "vehicle"})-[:HAS_PROPERTY]->(p:Property {label: "color"}),
           (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col1:`Source Data Column` {_columnname_: "car_color"}),
           (sdt)-[:HAS_COLUMN]->(col2:`Source Data Column` {_columnname_: "plane_color"}),
           (sdt)<-[:HAS_TABLE]-(:`Source Data Folder`), 
           (col1)-[:MAPS_TO_PROPERTY]->(p),
           (col2)-[:MAPS_TO_PROPERTY]->(p)
        """
    db.query(q1)

    mod_a = model_applier.ModelApplier()

    res1 = mod_a._extract_class_entities_part_1()
    expected1 = [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color'], ['plane_color', 'color']],
                  'lbl': ['vehicle']}]

    assert compare_recordsets(res1, expected1)


def test_extract_class_entities_part_1_C(db):
    """
    MAIN FOCUS: _extract_class_entities_part_1()
    """

    # Completely clear the database
    db.clean_slate()

    # Create minimalist test data as done at the beginning of test_extract_class_entities_part_1_A()
    # See image "Sample data for chain from classes to tables.PNG"
    q1 = """ 
        CREATE 
           (:Class {label: "car", create: True})-[:HAS_PROPERTY]->(p:Property {label: "color"}),
           (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col:`Source Data Column` {_columnname_: "car_color"}),
           (sdt)<-[:HAS_TABLE]-(:`Source Data Folder`), 
           (col)-[:MAPS_TO_PROPERTY]->(p)
        """
    db.query(q1)

    mod_a = model_applier.ModelApplier()  # The passed argument indicates a list of labels that must always be created

    res1 = mod_a._extract_class_entities_part_1()
    expected1 = [{'mode': 'create', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}]
    # print(res1)
    # print(expected1)
    assert res1 == expected1
    # Note that it's now 'apoc.create.node' rather than 'apoc.merge.node'

    # Repeat with a new ModelApplier, removing create:True from the 'car' Class
    db.query("""MATCH (c:Class {label: "car"}) REMOVE c.create""")

    res2 = mod_a._extract_class_entities_part_1()
    expected2 = [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}]
    # print(res2)
    # print(expected2)
    assert res2 == expected2
    # It's back to 'apoc.merge.node'


def test_extract_class_entities_part_1_C_where_map(db):
    """
    MAIN FOCUS: _extract_class_entities_part_1()
    """

    # Completely clear the database
    db.clean_slate()

    # Create minimalist test data as done at the beginning of test_extract_class_entities_part_1_A()
    # See image "Sample data for chain from classes to tables.PNG"
    q1 = """ 
            CREATE 
               (car_class:Class {label: "car", create: True})-[:HAS_PROPERTY]->(p:Property {label: "color"}),
               (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col:`Source Data Column` {_columnname_: "car_color"}),
               (sdt)<-[:HAS_TABLE]-(sdf:`Source Data Folder`{_folder_: "f1"}), 
               (col)-[:MAPS_TO_PROPERTY]->(p),
               (car_class)-[:HAS_PROPERTY]->(p2:Property {label: "type"}),
               (sdt2:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col21:`Source Data Column` {_columnname_: "car_color"}),
               (sdt2)<-[:HAS_TABLE]-(sdf2:`Source Data Folder`{_folder_: "f2"}), 
               (sdt2)-[:HAS_COLUMN]->(col22:`Source Data Column` {_columnname_: "car_type"}),
               (col21)-[:MAPS_TO_PROPERTY]->(p),
               (col22)-[:MAPS_TO_PROPERTY]->(p2)
            """
    db.query(q1)

    mod_a = model_applier.ModelApplier()
    res1 = mod_a._extract_class_entities_part_1()
    print(res1)
    expected_res1 = [
        {'mode': 'create', 'domain': 'Automotive', 'coll': [['car_color', 'color'], ['car_type', 'type']],
         'lbl': ['car']},
        {'mode': 'create', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}
    ]
    assert compare_recordsets(res1, expected_res1)

    res2 = mod_a._extract_class_entities_part_1(where_map={'Source Data Folder': {'_folder_': "f1"}})
    expected_res2 = [
        {'mode': 'create', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}
    ]
    assert res2 == expected_res2


def test_extract_class_entities_part_1_select_single(db):
    """
    MAIN FOCUS: _extract_class_entities_part_1()
    Specifically, test selecting a single domain or class for refactoring
    """

    # Completely clear the database
    db.clean_slate()

    # Create minimalist test data with two domains and two classes
    # See image "Sample data for chain from classes to tables.PNG"
    q1 = """ 
            CREATE 
            (c1:Class {label: "car"})-[:HAS_PROPERTY]->(p1:Property {label: "color"}),
            (sdt1:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col1:`Source Data Column` {_columnname_: "car_color"}),
            (sdt1)<-[:HAS_TABLE]-(:`Source Data Folder`), 
            (col1)-[:MAPS_TO_PROPERTY]->(p1),
            (c2:Class {label: "plane"})-[:HAS_PROPERTY]->(p2:Property {label: "color"}),
            (sdt2:`Source Data Table` {_domain_: "Aerospace"})-[:HAS_COLUMN]->(col2:`Source Data Column` {_columnname_: "plane_color"}),
            (sdt2)<-[:HAS_TABLE]-(:`Source Data Folder`), 
            (col2)-[:MAPS_TO_PROPERTY]->(p2)
            """
    db.query(q1)

    mod_a = model_applier.ModelApplier()

    # test extracting a single domain
    res1 = mod_a._extract_class_entities_part_1(where_map={'Source Data Table': {'_domain_': ['Automotive']}})
    expected_res1 = [
        {'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}
    ]
    assert compare_recordsets(res1, expected_res1)

    # test extracting a single class
    res2 = mod_a._extract_class_entities_part_1(where_map={'Class': {'label': ['plane']}})
    expected_res2 = [
        {'mode': 'merge', 'domain': 'Aerospace', 'coll': [['plane_color', 'color']], 'lbl': ['plane']}
    ]
    assert compare_recordsets(res2, expected_res2)


def test_extract_class_entities_part_1_select_multiple(db):
    """
    MAIN FOCUS: _extract_class_entities_part_1()
    Specifically, test selecting multiple domains or classes for refactoring
    """

    # Completely clear the database
    db.clean_slate()

    # Create minimalist test data with three domains and three classes
    # See image "Sample data for chain from classes to tables.PNG"
    q1 = """ 
        CREATE 
        (c1:Class {label: "car"})-[:HAS_PROPERTY]->(p1:Property {label: "color"}),
        (sdt1:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col1:`Source Data Column` {_columnname_: "car_color"}),
        (sdt1)<-[:HAS_TABLE]-(:`Source Data Folder`), 
        (col1)-[:MAPS_TO_PROPERTY]->(p1),
        (c2:Class {label: "plane"})-[:HAS_PROPERTY]->(p2:Property {label: "color"}),
        (sdt2:`Source Data Table` {_domain_: "Aerospace"})-[:HAS_COLUMN]->(col2:`Source Data Column` {_columnname_: "plane_color"}),
        (sdt2)<-[:HAS_TABLE]-(:`Source Data Folder`), 
        (col2)-[:MAPS_TO_PROPERTY]->(p2),
        (c3:Class {label: "boat"})-[:HAS_PROPERTY]->(p3:Property {label: "color"}),
        (sdt3:`Source Data Table` {_domain_: "Marine"})-[:HAS_COLUMN]->(col3:`Source Data Column` {_columnname_: "boat_color"}),
        (sdt3)<-[:HAS_TABLE]-(:`Source Data Folder`), 
        (col3)-[:MAPS_TO_PROPERTY]->(p3) 
            """
    db.query(q1)

    mod_a = model_applier.ModelApplier()

    # test extracting 2 domains
    res1 = mod_a._extract_class_entities_part_1(where_map={'Source Data Table': {'_domain_': ['Automotive', 'Marine']}})
    print(res1)
    expected_res1 = [
        {'mode': 'merge', 'domain': 'Marine', 'coll': [['boat_color', 'color']], 'lbl': ['boat']},
        {'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}
    ]
    assert compare_recordsets(res1, expected_res1)

    # test extracting a single class
    res2 = mod_a._extract_class_entities_part_1(where_map={'Class': {'label': ['plane', 'boat']}})
    print(res2)
    expected_res2 = [
        {'mode': 'merge', 'domain': 'Aerospace', 'coll': [['plane_color', 'color']], 'lbl': ['plane']},
        {'mode': 'merge', 'domain': 'Marine', 'coll': [['boat_color', 'color']], 'lbl': ['boat']}
    ]
    assert compare_recordsets(res2, expected_res2)


#################################   Tests for PART 2   #################################

def test_extract_class_entities_part_2_A(db):
    """
    MAIN FOCUS: _extract_class_entities_part_2()

    A minimalist run
    """

    # Completely clear the database
    db.clean_slate()

    # Create minimalist test data.  See image "extract_class_entities/BEFORE _extract_class_entities_part_2.png"
    q1 = """ 
        CREATE 
           (:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"})          
        """
    db.query(q1)

    # Use the list produced by test_extract_class_entities_part_1_A()
    res = [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}]

    mod_a = model_applier.ModelApplier()

    mod_a._extract_class_entities_part_2(res)

    # Verify that the expected node and relationship got created.
    # See the image "extract_class_entities/AFTER _extract_class_entities_part_2.png"
    cypher = "MATCH (n:car {color:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1


def test_extract_class_entities_part_2_B(db):
    """
    MAIN FOCUS: _extract_class_entities_part_2()

    Similar to test_extract_class_entities_part_2_A, but with zero overlap
    between the properties of the `Source Data Row` node
    and the zero-th elements in the dictionary value for "coll".
    """

    # Completely clear the database
    db.clean_slate()

    # Create minimalist test data.  See image "extract_class_entities/BEFORE _extract_class_entities_part_2.png"
    q1 = """ 
        CREATE 
           (:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"})          
        """
    db.query(q1)

    # Use the list produced by test_extract_class_entities_part_1_A()
    res = [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color_NO_OVERLAP_ANYMORE', 'color']], 'lbl': ['car']}]

    mod_a = model_applier.ModelApplier()

    mod_a._extract_class_entities_part_2(res)

    # Verify that the node and relationship that emerged out of test_extract_class_entities_part_2_A() this time does NOT get created
    cypher = "MATCH (n:car {car_color:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0  # The lack of overlap sabotaged the process of adding new nodes/relationships


def test_extract_class_entities_part_2_C(db):
    """
    MAIN FOCUS: _extract_class_entities_part_2()

    Similar to test_extract_class_entities_part_2_A, but with a more complex scenario with multiple `Source Data Row` nodes.
    We're also changing the new label from "car" to "Color" (in anticipation of adding more attributes in later tests.)
    See "extract_class_entities/test_extract_class_entities_part_2_C.png"
    """

    # Completely clear the database
    db.clean_slate()

    # Create test data with a `Source Data Row` node that is a hub for multiple `Source Data Row` nodes
    # See "extract_class_entities/test_extract_class_entities_part_2_C.png"
    q1 = """ 
        CREATE 
           (n:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"}),
           (n)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue"})  
        """
    db.query(q1)

    # Use the list produced by test_extract_class_entities_part_1_A()
    res = [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'COLOR']], 'lbl': ['Color']}]

    mod_a = model_applier.ModelApplier()

    mod_a._extract_class_entities_part_2(res)

    # Verify that the expected nodes and relationships got created.
    # See "extract_class_entities/test_extract_class_entities_part_2_C.png"
    cypher = "MATCH (n:Color {COLOR:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (n:Color {COLOR:'blue'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1


def test_extract_class_entities_part_2_D(db):
    """
    MAIN FOCUS: _extract_class_entities_part_2()

    Similar to test_extract_class_entities_part_2_C, but with a 2nd attribute in one of the `Source Data Row` nodes.
    See "extract_class_entities/test_extract_class_entities_part_2_D.png"
    """

    # Completely clear the database
    db.clean_slate()

    # Create test data with a `Source Data Row` node that is a hub for multiple `Source Data Row` nodes
    # One of the `Source Data Row` nodes has a 2nd attribute, "car_make"
    # See "extract_class_entities/test_extract_class_entities_part_2_D.png"
    q1 = """ 
        CREATE 
           (n:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"}),
           (n)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue", car_make: "Toyota"})  
        """
    db.query(q1)

    # Now we're also including 'car_make' among the values of 'coll'
    res = [{'mode': 'merge',
            'domain': 'Automotive',
            'coll': [['car_color', 'color'], ['car_make', 'make']],
            'lbl': ['Color']}]

    mod_a = model_applier.ModelApplier()

    mod_a._extract_class_entities_part_2(res)

    # Verify that the expected nodes and relationships got created.
    # See "extract_class_entities/test_extract_class_entities_part_2_D.png"
    cypher = "MATCH (n:Color {color:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = '''
            MATCH (n:Color {color:'blue', make:'Toyota'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue', car_make:'Toyota'}) 
            RETURN rel
            '''
    result = db.query(cypher)
    assert len(result) == 1


def test_extract_class_entities_part_2_E(db):
    """
    MAIN FOCUS: _extract_class_entities_part_2()

    Similar to test_extract_class_entities_part_2_D, but with a 2nd dictionary in the parameter list.
    See "extract_class_entities/test_extract_class_entities_part_2_E.png"
    """

    # Completely clear the database
    db.clean_slate()

    # Create test data with a `Source Data Row` node that is a hub for multiple `Source Data Row` nodes
    # One of the `Source Data Row` nodes has a 2nd attribute, "car_make"
    # See "extract_class_entities/test_extract_class_entities_part_2_E.png"
    q1 = """ 
        CREATE 
           (n:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"}),
           (n)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue", car_make: "Toyota"})  
        """
    db.query(q1)

    # Now we have a 2-entry list
    res = [{'mode': 'merge',
            'domain': 'Automotive',
            'coll': [['car_color', 'color']],
            'lbl': ['Color']}
        ,
           {'mode': 'merge',
            'domain': 'Automotive',
            'coll': [['car_make', 'make']],
            'lbl': ['Manufacturer']}
           ]

    mod_a = model_applier.ModelApplier()
    mod_a.debug=True
    mod_a._extract_class_entities_part_2(res)

    # Verify that the expected nodes and relationships got created.
    # See "extract_class_entities/test_extract_class_entities_part_2_E.png"
    cypher = "MATCH (n:Color {color:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = '''
            MATCH (n:Color {color:'blue'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue', car_make:'Toyota'})
            RETURN rel
            '''
    result = db.query(cypher)
    assert len(result) == 1

    cypher = '''
            MATCH (n:Manufacturer {make:'Toyota'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue', car_make:'Toyota'})
            RETURN rel
            '''
    result = db.query(cypher)
    assert len(result) == 1


def test_extract_class_entities_part_2_F(db):
    """
    MAIN FOCUS: _extract_class_entities_part_2()

    Similar to test_extract_class_entities_part_2_E, but with a 2nd domain ("Maritime") in addition to "Automotive".
    These 2 domain have a label in common ("Color") but differ in the remaining labels ("Manufacturer" for "Automotive",
        vs. "BoatType" for "Maritime")
    See "extract_class_entities/test_extract_class_entities_part_2_F.png"
    """

    # Completely clear the database
    db.clean_slate()

    # Create test data, similar to what done in test_extract_class_entities_part_2_E,
    # but with a 2nd domain ("Maritime") in addition to "Automotive"
    # See "extract_class_entities/test_extract_class_entities_part_2_F.png"
    q1 = """ 
        CREATE 
            (a:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"}),
            (a)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue", car_make: "Toyota"}),
            (m:`Source Data Table` {_domain_: "Maritime"})-[:HAS_DATA]->(:`Source Data Row` {boat_color: "rainbow"}),
            (m)-[:HAS_DATA]->(:`Source Data Row` {boat_color: "red", boat_type: "ketch"}) 
        """
    db.query(q1)

    # Now we have a 4-entry list (2 for each of the 2 domains)
    res = [{'mode': 'merge',
            'domain': 'Automotive',
            'coll': [['car_color', 'color']],
            'lbl': ['Color']}
        ,
           {'mode': 'merge',
            'domain': 'Automotive',
            'coll': [['car_make', 'make']],
            'lbl': ['Manufacturer']}
        ,
           {'mode': 'merge',
            'domain': 'Maritime',
            'coll': [['boat_color', 'color']],
            'lbl': ['Color']}
        ,
           {'mode': 'merge',
            'domain': 'Maritime',
            'coll': [['boat_type', 'type']],
            'lbl': ['BoatType']}
           ]

    mod_a = model_applier.ModelApplier()

    mod_a._extract_class_entities_part_2(res)

    # Verify that the expected nodes and relationships got created.
    # See "extract_class_entities/test_extract_class_entities_part_2_F.png"
    cypher = "MATCH (n:Color {color:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = '''
            MATCH (n:Color {color:'blue'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue', car_make:'Toyota'}) 
            RETURN rel
            '''
    result = db.query(cypher)
    assert len(result) == 1

    cypher = '''
            MATCH (n:Manufacturer {make:'Toyota'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue', car_make:'Toyota'}) 
            RETURN rel
            '''
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (n:Color {color:'rainbow'})-[rel:FROM_DATA]->(m:`Source Data Row` {boat_color:'rainbow'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = '''
            MATCH (n:Color {color:'red'})-[rel:FROM_DATA]->(m:`Source Data Row` {boat_color:'red', boat_type:'ketch'}) 
            RETURN rel
            '''
    result = db.query(cypher)
    assert len(result) == 1

    cypher = '''
            MATCH (n:BoatType {type:'ketch'})-[rel:FROM_DATA]->(m:`Source Data Row` {boat_color:'red', boat_type:'ketch'}) 
            RETURN rel
            '''
    result = db.query(cypher)
    assert len(result) == 1


# TODO:  Test "apoc.create.node"


#################################   Integration tests for PARTS 1 and 2 COMBINED  #################################

def test_extract_class_entities_A(db):
    """
    MAIN FOCUS: extract_class_entities() ,
                which is the combination of the _extract_class_entities_part_1 and _extract_class_entities_part_2,
                tested above.
    1 Property, 1 Source Data Column, 1 Source Data Row
    See the image "extract_class_entities/test_extract_class_entities_A.png"
    """

    # Completely clear the database
    db.clean_slate()

    # Create minimalist test data conforming to the structure expected by the _extract_class_entities_part_1() method,
    #       plus minimalist `Source Data Row` data about
    # A combination of test_extract_class_entities_part_1_A() and test_extract_class_entities_part_2_A()
    q1 = """ 
        CREATE 
           (:Class {label: "car"})-[:HAS_PROPERTY]->(p:Property {label: "color"}),
           (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col:`Source Data Column` {_columnname_: "car_color"}),
           (sdt)<-[:HAS_TABLE]-(:`Source Data Folder`), 
           (col)-[:MAPS_TO_PROPERTY]->(p),
           (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"})
        """
    db.query(q1)

    mod_a = model_applier.ModelApplier()

    mod_a.extract_class_entities()
    # Note: the internal parameters are [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}]
    #       as determined in test_extract_class_entities_part_1_A()

    # Verify that the expected node and relationship got created.
    # See the image "extract_class_entities/test_extract_class_entities_A.png"
    cypher = "MATCH (n:car {color:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1


def test_extract_class_entities_B(db):
    """
    MAIN FOCUS: extract_class_entities() ,
                which is the combination of the _extract_class_entities_part_1 and _extract_class_entities_part_2,
                tested above.
    Like test_extract_class_entities_A() but with a 2nd Source Data Row
    1 Property, 1 Source Data Column, 2 Source Data Rows
    See the image "extract_class_entities/test_extract_class_entities_B.png"
    """

    db.clean_slate()  # Completely clear the database

    q1 = """ 
        CREATE 
           (:Class {label: "car"})-[:HAS_PROPERTY]->(p:Property {label: "color"}),
           (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col:`Source Data Column` {_columnname_: "car_color"}),
           (sdt)<-[:HAS_TABLE]-(:`Source Data Folder`), 
           (col)-[:MAPS_TO_PROPERTY]->(p),
           (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"}),
           (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue"})
        """
    db.query(q1)

    mod_a = model_applier.ModelApplier()

    mod_a.extract_class_entities()
    # Note: the internal parameters are [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}]

    # Verify that the expected nodes and relationship got created.
    # See the image "extract_class_entities/test_extract_class_entities_B.png"
    cypher = "MATCH (n:car {color:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (n:car {color:'blue'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1


def test_extract_class_entities_C(db):
    """
    MAIN FOCUS: extract_class_entities() ,
                which is the combination of the _extract_class_entities_part_1 and _extract_class_entities_part_2,
                tested above.
    Like test_extract_class_entities_B() but with a 3rd Source Data Row, of equal value to another one
    1 Property, 1 Source Data Column, 3 Source Data Rows (two of them equal)
    See the image "extract_class_entities/test_extract_class_entities_C.png"
    """

    db.clean_slate()  # Completely clear the database

    q1 = """ 
        CREATE 
           (:Class {label: "car"})-[:HAS_PROPERTY]->(p:Property {label: "color"}),
           (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col:`Source Data Column` {_columnname_: "car_color"}),
           (sdt)<-[:HAS_TABLE]-(:`Source Data Folder`), 
           (col)-[:MAPS_TO_PROPERTY]->(p),
           (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"}),
           (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue"}),
           (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue"})
        """
    db.query(q1)

    mod_a = model_applier.ModelApplier()

    mod_a.extract_class_entities()
    # Note: the internal parameters are [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}]

    # Verify that the expected nodes and relationship got created.
    # See the image "extract_class_entities/test_extract_class_entities_C.png"
    cypher = "MATCH (n:car {color:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (n:car {color:'blue'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 2


def test_extract_class_entities_D(db):
    """
    MAIN FOCUS: extract_class_entities() ,
                which is the combination of the _extract_class_entities_part_1 and _extract_class_entities_part_2,
                tested above.
    Like test_extract_class_entities_C() but with a more Source Data Rows, and a 2nd Property/Source Data Column
    2 Property, 2 Source Data Column, 7 Source Data Rows (with a variety of unique/repeating values, both individually and in pairs)
    See the image "extract_class_entities/test_extract_class_entities_D.png"
    """

    db.clean_slate()  # Completely clear the database

    q1 = """ 
        CREATE 
            (c:Class {label: "car"})-[:HAS_PROPERTY]->(p1:Property {label: "color"}),
            (c)-[:HAS_PROPERTY]->(p2:Property {label: "make"}),
            (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col1:`Source Data Column` {_columnname_: "car_color"}),
            (sdt)<-[:HAS_TABLE]-(:`Source Data Folder`), 
            (col1)-[:MAPS_TO_PROPERTY]->(p1),
            (sdt)-[:HAS_COLUMN]->(col2:`Source Data Column` {_columnname_: "car_make"}),
            (col2)-[:MAPS_TO_PROPERTY]->(p2),
            (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "white", car_make: "Toyota"}),
            (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "white", car_make: "Honda"}),
            (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "white", car_make: "Lamborghini"}),
            (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue", car_make: "Toyota"}),
            (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue", car_make: "Toyota"}),
            (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "blue", car_make: "Honda"}),
            (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "red", car_make: "Porsche"})
        """
    db.query(q1)

    mod_a = model_applier.ModelApplier()

    mod_a.extract_class_entities()
    # Note: the internal parameters are [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', 'color']], 'lbl': ['car']}]

    # Verify that the expected nodes and relationship got created.
    # See the image "extract_class_entities/test_extract_class_entities_D.png"
    cypher = "MATCH (n:car {color: 'white', make: 'Honda'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white', car_make: 'Honda'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (n:car {color: 'white', make: 'Lamborghini'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white', car_make: 'Lamborghini'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (n:car {color: 'white', make: 'Toyota'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white', car_make: 'Toyota'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (n:car {color: 'blue', make: 'Toyota'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue', car_make: 'Toyota'}) RETURN rel"
    result = db.query(cypher)
    assert len(
        result) == 2  # This is different from the other ones, because there are 2 relationships converging to the "car" node

    cypher = "MATCH (n:car {color: 'blue', make: 'Honda'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'blue', car_make: 'Honda'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (n:car {color: 'red', make: 'Porsche'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'red', car_make: 'Porsche'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1


# TODO (perhaps) : more extract_class_entities() testing in scenarios with multiple domains.
#                  Testing the entire refactor_all(), i.e.  extract_class_entities() followed by link_classes()
#                  Testing of the reading various file formats (reserved the unused "test_data_transform_1.py" to that purpose)
#                  Testing the entire data-transformation chain from reading a file to the very end (link_classes)