import pytest
from model_appliers import model_applier


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = model_applier.ModelApplier()
    yield neo_obj


def test_label_entities(db):
    # Completely clear the database
    db.clean_slate()
    db.query("CREATE (c:Category{name:'Blood Pressure'})-[:HAS_OBSERVATION]->(o:Observation{value:120})")
    db.label_entities(
        class_='Observation',
        add_label={'Class':'Category', 'Property':'name'}
    )
    result = db.query("MATCH (dp:`Blood Pressure`:Observation) RETURN *")
    assert len(result)>0

def test_label_entities_cypher(db):
    # Completely clear the database
    db.clean_slate()
    db.query("""
    CREATE (a)-[:TEST]->(b{`to use`:'XYZ'})
    CREATE (b)-[:TEST]->(c{x:1})
    """)
    db.label_entities(
        class_='a',
        add_label={'Class':'b', 'Property':'to use'}, #even if no labels assigned if b returned from cond_cypher it works
        cond_cypher="""
        MATCH (a)-[]->(b)
        WHERE EXISTS( (b)-[]->({x:$for_where_value}) )
        RETURN a, b
        """,
        cond_cypher_dict={'for_where_value': 1}
    )
    result = db.query("MATCH (dp:`XYZ`) RETURN *")
    assert len(result)>0

def test_label_entities_label_itself(db):
    # Completely clear the database
    db.clean_slate()
    db.query("""
    CREATE (:`Adverse Event`{id:1, severity:'Severe'})
    CREATE (:`Adverse Event`{id:2, severity:'Mild'})
    """)
    db.label_entities(
        class_='Adverse Event',
        add_label={'Class': 'Adverse Event', 'Property': 'severity'},
        cond_cypher="MATCH (`adverse event`:`Adverse Event`) RETURN `adverse event`"
    )
    result = db.query("MATCH (x) WHERE x.severity in labels(x) RETURN x.severity ORDER BY x.severity")
    assert result == [{'x.severity': 'Mild'}, {'x.severity': 'Severe'}]

