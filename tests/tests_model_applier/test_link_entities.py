import pytest
from model_appliers import model_applier
import neointerface


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = neointerface.NeoInterface()
    yield neo_obj


def test_link_classes_1(db):
    """
    MAIN FOCUS: link_classes()

    Test the following sequence:
        1) clear dbase
        2) create 2 new nodes with label "Class" and an attribute called "label"
        3) create 3 nodes with particular labels and relationships among them
        4) run link_classes()
        5) asserts that a new relationship got created as expected
    """

    # Completely clear the database
    db.clean_slate()

    # Create 2 new nodes with label "Class" and an attribute called "label"
    id1 = db.create_node_by_label_and_dict("Class", {"label": "apple"})
    id2 = db.create_node_by_label_and_dict("Class", {"label": "fruit"})
    id3 = db.create_node_by_label_and_dict("Relationship", {})
    db.link_nodes_by_ids(id3, id1, "FROM")
    db.link_nodes_by_ids(id3, id2, "TO")

    left = db.create_node_by_label_and_dict("apple")
    right = db.create_node_by_label_and_dict("fruit")
    sdr = db.create_node_by_label_and_dict("Source Data Row")
    db.link_nodes_by_ids(left, sdr, "FROM_DATA")
    db.link_nodes_by_ids(right, sdr, "FROM_DATA")

    mod_a = model_applier.ModelApplier()
    mod_a.link_classes()

    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    # print(result)
    assert len(result) == 1


def test_link_classes_2(db):
    """
    A larger scenario from that of test_link_classes_1
    """

    # Completely clear the database
    db.clean_slate()

    # Set up the metadata
    q0 = """
            CREATE 
                   (a:Class {label: "apple"})<-[:FROM]-(:Relationship)-[:TO]->(f:Class {label: "fruit"}),
                   (:Class {label: "fuji_apple"})<-[:FROM]-(:Relationship)-[:TO]->(a),
                   (:Class {label: "pear"})<-[:FROM]-(:Relationship)-[:TO]->(f)
            """
    db.query(q0)

    # Set up the data
    q1 = "CREATE (:apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)"
    db.query(q1)

    q2 = "CREATE (:pear)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)"
    db.query(q2)

    q3 = "CREATE (:fuji_apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:apple)"
    db.query(q3)

    # Run link_classes()
    mod_a = model_applier.ModelApplier()
    mod_a.link_classes()

    # Verify that the expected relationships got created
    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:pear)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:fuji_apple)-[rel:HAS_APPLE]->(r:apple) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    """
    TO DO:
    maybe also try using the same apple and Source Data Row from q1:
    MATCH (a:apple)-[:FROM_DATA]->(sdr:Source Data Row)
    CREATE (:fuji_apple)-[:FROM_DATA]->(sdr)<-[:FROM_DATA]-(a)
    and see how this works
    """


def test_link_classes_3(db):
    """
    MAIN FOCUS: link_classes()

    Set up miniature databases similar to that of test_link_classes_1 (defined more concisely),
    but always "sabotage" them in ways to foil the creation of a node relationship by link_classes
    """

    """
    First, repeat the test of test_link_classes_1, in a more concise way
    """
    # Completely clear the database
    db.clean_slate()
    # Set up the metadata and data
    q = """ CREATE 
                   (:Class {label: "apple"})<-[:FROM]-(:Relationship)-[:TO]->(:Class {label: "fruit"}),
                   (:apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)
        """
    db.query(q)
    # Run link_classes()
    mod_a = model_applier.ModelApplier()
    mod_a.link_classes()
    # Verify that the expected relationship got created
    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    """
    First, start again from scratch, but with various variations that torpedoes the application of link_classes()
    """

    # In this round, alter the label of one of the "Class" nodes
    db.clean_slate()
    q = """ CREATE 
                   (:Altered_Class {label: "apple"})<-[:FROM]-(:Relationship)-[:TO]->(:Class {label: "fruit"}),
                   (:apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)
        """
    db.query(q)
    mod_a = model_applier.ModelApplier()
    mod_a.link_classes()
    # Verify that NO relationship got created
    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0

    # In this round, alter a label in one of the "Class" nodes
    db.clean_slate()
    q = """ CREATE 
                   (:Class {label: "ALTERED_label"})<-[:FROM]-(:Relationship)-[:TO]->(:Class {label: "fruit"}),
                   (:apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)
        """
    db.query(q)
    mod_a = model_applier.ModelApplier()
    mod_a.link_classes()
    # Verify that NO relationship got created
    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0

    # In this round, alter the name of the relationship between the "Class" nodes
    db.clean_slate()
    q = """ CREATE 
                   (:Class {label: "apple"})-[:ALTERED_RELATIONSHIP]->(:Class {label: "fruit"}),
                   (:apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)
        """
    db.query(q)
    mod_a = model_applier.ModelApplier()
    mod_a.link_classes()
    # Verify that NO relationship got created
    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0

    # In this round, alter the label of one of the data nodes
    db.clean_slate()
    q = """ CREATE 
                   (:Class {label: "apple"})<-[:FROM]-(:Relationship)-[:TO]->(:Class {label: "fruit"}),
                   (:ALTERED_apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)
        """
    db.query(q)
    mod_a = model_applier.ModelApplier()
    mod_a.link_classes()
    # Verify that NO relationship got created
    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0

    # In this round, alter the name of the relationship between the data nodes
    db.clean_slate()
    q = """ CREATE 
                   (:Class {label: "apple"})<-[:FROM]-(:Relationship)-[:TO]->(:Class {label: "fruit"}),
                   (:apple)-[:FROM_DATA]->(:`ALTERED_Source Data Row`)<-[:FROM_DATA]-(:fruit)
        """
    db.query(q)
    mod_a = model_applier.ModelApplier()
    mod_a.link_classes()
    # Verify that NO relationship got created
    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0

    # In this round, alter the label of the "Source Data Row" node
    db.clean_slate()
    q = """ CREATE 
                   (:Class {label: "apple"})<-[:FROM]-(:Relationship)-[:TO]->(:Class {label: "fruit"}),
                   (:apple)-[:FROM_DATA]->(:`ALTERED Source Data Row`)<-[:FROM_DATA]-(:fruit)
        """
    db.query(q)
    mod_a = model_applier.ModelApplier()
    mod_a.link_classes()
    # Verify that NO relationship got created
    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0


def test_link_classes_select_1(db):
    """
    A scenario to test linking a single class or domain
    """

    # Completely clear the database
    db.clean_slate()

    # Set up the metadata
    q0 = """
            CREATE 
            (a:Class {label: "apple", from_domains: "fruit"})<-[:FROM]-(:Relationship)-[:TO]->(f:Class {label: "fruit", from_domains: "fruit"}),
            (fa:Class {label: "fuji_apple", from_domains: "fruit"})<-[:FROM]-(:Relationship)-[:TO]->(a),
            (p:Class {label: "pear", from_domains: "fruit"})<-[:FROM]-(:Relationship)-[:TO]->(f),
            (ft:Class {label: "fruit_tree", from_domains: "trees"})<-[:FROM]-(:Relationship)-[:TO]->(f),
            (l:Class {label: "leaves", from_domains: "trees"})<-[:TO]-(:Relationship)-[:FROM]->(ft),
            (ft)<-[:FROM]-(:Relationship)-[:TO]->(p),
            (ft)<-[:FROM]-(:Relationship)-[:TO]->(a),
            (fa)<-[:FROM]-(:Relationship)-[:TO]->(ft)
            """
    db.query(q0)

    # Set up the data
    q1 = "CREATE (:apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)"
    db.query(q1)

    q2 = "CREATE (:pear)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)"
    db.query(q2)

    q3 = "CREATE (:fuji_apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:apple)"
    db.query(q3)

    q4 = "CREATE (:fruit_tree)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:leaves)"
    db.query(q4)

    # Run link_classes() for a single class
    mod_a = model_applier.ModelApplier()
    # mod_a.link_classes()
    mod_a.link_classes(classes=["fuji_apple"])

    # Verify the expected relationships got created
    cypher = "MATCH (l:fuji_apple)-[rel:HAS_APPLE]->(r:apple) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0  # this relationship shouldn't have been created

    cypher = "MATCH (l:pear)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0  # this relationship shouldn't have been created

    cypher = "MATCH (l:fruit_tree)-[rel:HAS_LEAVES]->(r:leaves) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0  # this relationship shouldn't have been created

    # Now let's link pear
    mod_a.link_classes(classes=["pear"])

    cypher = "MATCH (l:fuji_apple)-[rel:HAS_APPLE]->(r:apple) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0  # this relationship shouldn't have been created

    cypher = "MATCH (l:pear)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:fruit_tree)-[rel:HAS_LEAVES]->(r:leaves) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0  # this relationship shouldn't have been created

    # now let's try a domain, we should see 1 new relationship (apple->HAS_FRUIT->fruit)
    mod_a.link_classes(domains=["fruit"])

    cypher = "MATCH (l:fuji_apple)-[rel:HAS_APPLE]->(r:apple) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:pear)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:fruit_tree)-[rel:HAS_LEAVES]->(r:leaves) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0  # this relationship shouldn't have been created

    # now let's link the domain "trees" - all Source Data Row nodes should now be linked
    mod_a.link_classes(domains=["trees"])

    cypher = "MATCH (l:fuji_apple)-[rel:HAS_APPLE]->(r:apple) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:pear)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:fruit_tree)-[rel:HAS_LEAVES]->(r:leaves) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1


def test_link_classes_select_2(db):
    """
    A scenario to test linking multiple selected classes and domains
    """

    # Completely clear the database
    db.clean_slate()

    # Set up the metadata
    q0 = """
            CREATE 
            (a:Class {label: "apple", from_domains: "fruit"})<-[:FROM]-(:Relationship)-[:TO]->(f:Class {label: "fruit", from_domains: "fruit"}),
            (fa:Class {label: "fuji_apple", from_domains: "fruit"})<-[:FROM]-(:Relationship)-[:TO]->(a),
            (p:Class {label: "pear", from_domains: "fruit"})<-[:FROM]-(:Relationship)-[:TO]->(f),
            (ft:Class {label: "fruit_tree", from_domains: "trees"})<-[:FROM]-(:Relationship)-[:TO]->(f),
            (l:Class {label: "leaves", from_domains: "trees"})<-[:TO]-(:Relationship)-[:FROM]->(ft),
            (ft)<-[:FROM]-(:Relationship)-[:TO]->(p),
            (ft)<-[:FROM]-(:Relationship)-[:TO]->(a),
            (fa)<-[:FROM]-(:Relationship)-[:TO]->(ft)
            """
    db.query(q0)

    # Set up the data
    q1 = "CREATE (:apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)"
    db.query(q1)

    q2 = "CREATE (:pear)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:fruit)"
    db.query(q2)

    q3 = "CREATE (:fuji_apple)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:apple)"
    db.query(q3)

    q4 = "CREATE (:fruit_tree)-[:FROM_DATA]->(:`Source Data Row`)<-[:FROM_DATA]-(:leaves)"
    db.query(q4)

    # Run link_classes() for a two classes
    mod_a = model_applier.ModelApplier()

    mod_a.link_classes(classes=["fuji_apple", "leaves"])

    # Verify the expected relationships got created
    cypher = "MATCH (l:fuji_apple)-[rel:HAS_APPLE]->(r:apple) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0  # this relationship shouldn't have been created

    cypher = "MATCH (l:pear)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 0  # this relationship shouldn't have been created

    cypher = "MATCH (l:fruit_tree)-[rel:HAS_LEAVES]->(r:leaves) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    # Now let's link remaining nodes via domains parameter
    mod_a.link_classes(domains=["trees", "fruit"])

    cypher = "MATCH (l:fuji_apple)-[rel:HAS_APPLE]->(r:apple) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:pear)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1

    cypher = "MATCH (l:fruit_tree)-[rel:HAS_LEAVES]->(r:leaves) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1
