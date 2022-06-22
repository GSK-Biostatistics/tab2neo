import pytest
from model_appliers import model_applier
from utils.utils import compare_recordsets


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = model_applier.ModelApplier(mode = "schema_CLASS")
    yield neo_obj

def test_extract_class_entities_mode_maps_to_class(db):
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
           (c:Class {label: "color"}),
           (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col:`Source Data Column` {_columnname_: "car_color"}),
           (sdt)<-[:HAS_TABLE]-(:`Source Data Folder`), 
           (col)-[:MAPS_TO_CLASS]->(c)
        """
    db.query(q1)

    res1 = db._extract_class_entities_part_1()
    expected1 = [{'mode': 'merge', 'domain': 'Automotive', 'coll': [['car_color', db.RDFSLABEL]], 'lbl': ['color']}]
    # print(res1)
    # print(expected1)
    assert res1 == expected1


def test_extract_class_entities_part_2_mode_maps_to_class(db):
    db.clean_slate()

    # create metadata
    q1 = """ 
                CREATE 
                   (c:Class {label: "color"}),
                   (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_COLUMN]->(col:`Source Data Column` {_columnname_: "car_color"}),
                   (sdt)<-[:HAS_TABLE]-(:`Source Data Folder`), 
                   (col)-[:MAPS_TO_CLASS]->(c)
                """
    db.query(q1)
    part1_res = db._extract_class_entities_part_1()

    #create data
    q1 = """ 
        CREATE 
           (:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"})          
        """
    db.query(q1)

    #running extraction
    db._extract_class_entities_part_2(part1_res)

    # Verify that the expected node and relationship got created.
    # See the image "extract_class_entities/AFTER _extract_class_entities_part_2.png"
    cypher = "MATCH (n:color {`rdfs:label`:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1