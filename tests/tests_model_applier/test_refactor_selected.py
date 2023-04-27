import pytest
from model_appliers import model_applier
import neointerface


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = neointerface.NeoInterface()
    yield neo_obj

def prep_data(db):
    # Set up the metadata
    # first create the nodes
    q1 = """ 
            UNWIND [{_id:19220, properties:{AEDECOD:"Injection site reaction", _folder_:"data/dummy_study", AESTDY:4, AEBODSYS:"General disorders and administration site conditions", _domain_:"AE", USUBJID:999, STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19221, properties:{AEDECOD:"Injection site reaction", _folder_:"data/dummy_study", AESTDY:29, AEBODSYS:"General disorders and administration site conditions", _domain_:"AE", USUBJID:999, STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19222, properties:{AEDECOD:"Nasopharyngitis", _folder_:"data/dummy_study", AESTDY:43, AEBODSYS:"Infections and infestations", USUBJID:999, _domain_:"AE", STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19223, properties:{AEDECOD:"Injection site reaction", _folder_:"data/dummy_study", AESTDY:85, AEBODSYS:"General disorders and administration site conditions", _domain_:"AE", USUBJID:999, STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19224, properties:{AEDECOD:"Injection site reaction", _folder_:"data/dummy_study", AESTDY:113, AEBODSYS:"General disorders and administration site conditions", _domain_:"AE", USUBJID:999, STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19225, properties:{AEDECOD:"Injection site reaction", _folder_:"data/dummy_study", AESTDY:141, AEBODSYS:"General disorders and administration site conditions", _domain_:"AE", USUBJID:999, STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19226, properties:{AEDECOD:"Nasopharyngitis", _folder_:"data/dummy_study", AESTDY:162, AEBODSYS:"Infections and infestations", USUBJID:999, _domain_:"AE", STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19227, properties:{AEDECOD:"Injection site reaction", _folder_:"data/dummy_study", AESTDY:169, AEBODSYS:"General disorders and administration site conditions", _domain_:"AE", USUBJID:999, STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19228, properties:{AEDECOD:"Injection site reaction", _folder_:"data/dummy_study", AESTDY:197, AEBODSYS:"General disorders and administration site conditions", _domain_:"AE", USUBJID:999, STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19229, properties:{AEDECOD:"Injection site reaction", _folder_:"data/dummy_study", AESTDY:225, AEBODSYS:"General disorders and administration site conditions", _domain_:"AE", USUBJID:999, STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19230, properties:{AEDECOD:"symptom", _folder_:"data/dummy_study", AESTDY:97, AEBODSYS:"Infections and infestations", USUBJID:9999, _domain_:"AE", STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19231, properties:{AEDECOD:"symptom2", _folder_:"data/dummy_study", AESTDY:118, AEBODSYS:"Infections and infestations", _domain_:"AE", USUBJID:9999, STUDYID:"dummy_study0", _filename_:"mae.xlsx"}}, 
                    {_id:19239, properties:{_folder_:"data/dummy_study", SEX:"F", _domain_:"DM", USUBJID:999, SITEID:99, ARM:"100 mg- PARALLEL", STUDYID:"dummy_study0", AGE:109, _filename_:"mdm.xlsx"}}, 
                    {_id:19240, properties:{_folder_:"data/dummy_study", SEX:"F", _domain_:"DM", USUBJID:9999, SITEID:99, ARM:"Placebo - PARALLEL", STUDYID:"dummy_study0", AGE:66, _filename_:"mdm.xlsx"}}] AS row
            CREATE (n:`UIL`{`UIID`: row._id}) SET n += row.properties SET n:`Source Data Row`;
            """
    db.query(q1)

    q2 = """
            UNWIND [{_id:19232, properties:{_folder_:"data/dummy_study"}}] AS row
            CREATE (n:`UIL`{`UIID`: row._id}) SET n += row.properties SET n:`Source Data Folder`;
            """
    db.query(q2)

    q3 = """
            UNWIND [{_id:19234, properties:{Order:1, _folder_:"data/dummy_study", _columnname_:"STUDYID", _domain_:"AE",uri:"neo4j://graph.schema#Source%20Data%20Column/AE/STUDYID", Core:"Required"}}, 
                    {_id:19235, properties:{Order:3, _folder_:"data/dummy_study", _columnname_:"USUBJID", _domain_:"AE", uri:"neo4j://graph.schema#Source%20Data%20Column/AE/USUBJID", Core:"Required"}}, 
                    {_id:19236, properties:{Order:22, _folder_:"data/dummy_study", _columnname_:"AEBODSYS", _domain_:"AE", uri:"neo4j://graph.schema#Source%20Data%20Column/AE/AEBODSYS", Core:"Expected"}}, 
                    {_id:19237, properties:{Order:13, _folder_:"data/dummy_study", _columnname_:"AEDECOD", _domain_:"AE", uri:"neo4j://graph.schema#Source%20Data%20Column/AE/AEDECOD", Core:"Required"}}, 
                    {_id:19238, properties:{Order:48, _folder_:"data/dummy_study", _columnname_:"AESTDY", _domain_:"AE", uri:"neo4j://graph.schema#Source%20Data%20Column/AE/AESTDY", Core:"Permissible"}}, 
                    {_id:19242, properties:{Order:1, _folder_:"data/dummy_study", _columnname_:"STUDYID", _domain_:"DM", uri:"neo4j://graph.schema#Source%20Data%20Column/DM/STUDYID", Core:"Required"}}, 
                    {_id:19243, properties:{Order:13, _folder_:"data/dummy_study", _columnname_:"SITEID", _domain_:"DM", uri:"neo4j://graph.schema#Source%20Data%20Column/DM/SITEID", Core:"Required"}}, 
                    {_id:19244, properties:{Order:3, _folder_:"data/dummy_study", _columnname_:"USUBJID", _domain_:"DM", uri:"neo4j://graph.schema#Source%20Data%20Column/DM/USUBJID", Core:"Required"}}, 
                    {_id:19245, properties:{Order:17, _folder_:"data/dummy_study", _columnname_:"AGE", _domain_:"DM", uri:"neo4j://graph.schema#Source%20Data%20Column/DM/AGE", Core:"Expected"}}, 
                    {_id:19246, properties:{Order:19, _folder_:"data/dummy_study", _columnname_:"SEX", _domain_:"DM", uri:"neo4j://graph.schema#Source%20Data%20Column/DM/SEX", Core:"Required"}}, 
                    {_id:19247, properties:{Order:23, _folder_:"data/dummy_study", _columnname_:"ARM", _domain_:"DM", uri:"neo4j://graph.schema#Source%20Data%20Column/DM/ARM", Core:"Required"}}] AS row
            CREATE (n:`UIL`{`UIID`: row._id}) SET n += row.properties SET n:`Source Data Column`;
            """
    db.query(q3)

    q4 = """
            UNWIND [{_id:19249, properties:{CoreClass:false, label:"Dictionary-Derived Term", from_domains:"AE", uri:"neo4j://graph.schema#Class/Dictionary-Derived%20Term"}}, 
                    {_id:19251, properties:{CoreClass:false, from_domains:"AE", label:"Body System or Organ Class Code", uri:"neo4j://graph.schema#Class/Body%20System%20or%20Organ%20Class%20Code"}}, 
                    {_id:19252, properties:{CoreClass:false, from_domains:"DM", label:"Description of Planned Arm", uri:"neo4j://graph.schema#Class/Description%20of%20Planned%20Arm"}}, 
                    {_id:19256, properties:{CoreClass:true, label:"Adverse Events", from_domains:"AE", uri:"neo4j://graph.schema#Class/Adverse%20Events"}}, 
                    {_id:19257, properties:{CoreClass:false, label:"Sex", from_domains:"DM", uri:"neo4j://graph.schema#Class/Sex"}}, 
                    {_id:19261, properties:{CoreClass:false, from_domains:"AE", label:"Study Day of Start of Observation", uri:"neo4j://graph.schema#Class/Study%20Day%20of%20Start%20of%20Observation"}}, 
                    {_id:19263, properties:{CoreClass:true, label:"Demographics", from_domains:"DM", uri:"neo4j://graph.schema#Class/Demographics"}}, 
                    {_id:19265, properties:{CoreClass:false, label:"Age", from_domains:"DM", uri:"neo4j://graph.schema#Class/Age"}}, 
                    {_id:19266, properties:{CoreClass:false, label:"Preferred Term Code", from_domains:"AE", uri:"neo4j://graph.schema#Class/Preferred%20Term%20Code"}}, 
                    {_id:19267, properties:{CoreClass:false, label:"Subject", from_domains:"AE, DM", uri:"neo4j://graph.schema#Class/Subject"}}, 
                    {_id:19271, properties:{CoreClass:false, label:"Study Site Identifier", from_domains:"DM", uri:"neo4j://graph.schema#Class/Study%20Site%20Identifier"}}, 
                    {_id:19272, properties:{CoreClass:false, from_domains:"AE", label:"Body System or Organ Class", uri:"neo4j://graph.schema#Class/Body%20System%20or%20Organ%20Class"}}, 
                    {_id:19273, properties:{CoreClass:false, label:"Study", from_domains:"AE, DM", uri:"neo4j://graph.schema#Class/Study"}}] AS row
            CREATE (n:`UIL`{`UIID`: row._id}) SET n += row.properties SET n:Class; 
            """
    db.query(q4)

    q5 = """
            UNWIND [{_id:19233, properties:{_folder_:"data/dummy_study", SortOrder:"USUBJID,STUDYID", _domain_:"AE", uri:"neo4j://graph.schema#Source%20Data%20Table/AE", _filename_:"mae.xlsx"}}, 
                    {_id:19241, properties:{_folder_:"data/dummy_study", SortOrder:"STUDYID,USUBJID", _domain_:"DM", uri:"neo4j://graph.schema#Source%20Data%20Table/DM", _filename_:"mdm.xlsx"}}] AS row
            CREATE (n:`UIL`{`UIID`: row._id}) SET n += row.properties SET n:`Source Data Table`;
            """
    db.query(q5)

    q6 = """
            UNWIND [{_id:19248, properties:{`Class.label`:"Description of Planned Arm", label:"ARM", uri:"neo4j://graph.schema#Property/Description%20of%20Planned%20Arm/ARM"}}, 
                    {_id:19250, properties:{`Class.label`:"Study Day of Start of Observation", label:"--STDY", uri:"neo4j://graph.schema#Property/Study%20Day%20of%20Start%20of%20Observation/--STDY"}}, 
                    {_id:19253, properties:{`Class.label`:"Demographics", label:"USUBJID", uri:"neo4j://graph.schema#Property/Demographics/USUBJID"}}, 
                    {_id:19254, properties:{`Class.label`:"Subject", label:"USUBJID", uri:"neo4j://graph.schema#Property/Subject/USUBJID"}}, 
                    {_id:19255, properties:{`Class.label`:"Sex", label:"SEX", uri:"neo4j://graph.schema#Property/Sex/SEX"}}, 
                    {_id:19258, properties:{`Class.label`:"Adverse Events", label:"USUBJID", uri:"neo4j://graph.schema#Property/Adverse%20Events/USUBJID"}}, 
                    {_id:19259, properties:{`Class.label`:"Age", label:"AGE", uri:"neo4j://graph.schema#Property/Age/AGE"}}, 
                    {_id:19260, properties:{`Class.label`:"Study Site Identifier", label:"SITEID", uri:"neo4j://graph.schema#Property/Study%20Site%20Identifier/SITEID"}}, 
                    {_id:19262, properties:{`Class.label`:"Adverse Events", label:"STUDYID", uri:"neo4j://graph.schema#Property/Adverse%20Events/STUDYID"}}, 
                    {_id:19264, properties:{`Class.label`:"Demographics", label:"STUDYID", uri:"neo4j://graph.schema#Property/Demographics/STUDYID"}}, 
                    {_id:19268, properties:{`Class.label`:"Body System or Organ Class", label:"--BODSYS", uri:"neo4j://graph.schema#Property/Body%20System%20or%20Organ%20Class/--BODSYS"}}, 
                    {_id:19269, properties:{`Class.label`:"Dictionary-Derived Term", label:"--DECOD", uri:"neo4j://graph.schema#Property/Dictionary-Derived%20Term/--DECOD"}}, 
                    {_id:19270, properties:{`Class.label`:"Study", label:"STUDYID", uri:"neo4j://graph.schema#Property/Study/STUDYID"}}] AS row
            CREATE (n:`UIL`{`UIID`: row._id}) SET n += row.properties SET n:Property;
            """
    db.query(q6)

    # create relationships between the nodes
    q7 = """
            UNWIND [{start: {_id:19256}, end: {_id:19249}, properties:{}}, 
                    {start: {_id:19256}, end: {_id:19272}, properties:{}}, 
                    {start: {_id:19272}, end: {_id:19249}, properties:{}}, 
                    {start: {_id:19267}, end: {_id:19263}, properties:{}}, 
                    {start: {_id:19263}, end: {_id:19271}, properties:{}}, 
                    {start: {_id:19266}, end: {_id:19249}, properties:{}}, 
                    {start: {_id:19256}, end: {_id:19261}, properties:{}}, 
                    {start: {_id:19267}, end: {_id:19256}, properties:{}}, 
                    {start: {_id:19251}, end: {_id:19272}, properties:{}}, 
                    {start: {_id:19263}, end: {_id:19252}, properties:{}}, 
                    {start: {_id:19263}, end: {_id:19265}, properties:{}}, 
                    {start: {_id:19256}, end: {_id:19273}, properties:{}}, 
                    {start: {_id:19263}, end: {_id:19273}, properties:{}}, 
                    {start: {_id:19267}, end: {_id:19271}, properties:{}}, 
                    {start: {_id:19263}, end: {_id:19257}, properties:{}}, 
                    {start: {_id:19267}, end: {_id:19257}, properties:{}}, 
                    {start: {_id:19267}, end: {_id:19265}, properties:{}}, 
                    {start: {_id:19267}, end: {_id:19273}, properties:{}}, 
                    {start: {_id:19267}, end: {_id:19252}, properties:{}}] AS row
            MATCH   (start:`UIL`{`UIID`: row.start._id})
            MATCH   (end:`UIL`{`UIID`: row.end._id})
            CREATE  (start)-[r:CLASS_RELATES_TO]->(end) SET r += row.properties;
            """
    db.query(q7)

    q8 = """
            UNWIND [{start: {_id:19263}, end: {_id:19264}, properties:{}}, 
                    {start: {_id:19257}, end: {_id:19255}, properties:{}}, 
                    {start: {_id:19249}, end: {_id:19269}, properties:{}}, 
                    {start: {_id:19272}, end: {_id:19268}, properties:{}}, 
                    {start: {_id:19265}, end: {_id:19259}, properties:{}}, 
                    {start: {_id:19256}, end: {_id:19258}, properties:{}}, 
                    {start: {_id:19271}, end: {_id:19260}, properties:{}}, 
                    {start: {_id:19256}, end: {_id:19262}, properties:{}}, 
                    {start: {_id:19252}, end: {_id:19248}, properties:{}}, 
                    {start: {_id:19261}, end: {_id:19250}, properties:{}}, 
                    {start: {_id:19273}, end: {_id:19270}, properties:{}}, 
                    {start: {_id:19267}, end: {_id:19254}, properties:{}}, 
                    {start: {_id:19263}, end: {_id:19253}, properties:{}}] AS row
            MATCH   (start:`UIL`{`UIID`: row.start._id})
            MATCH   (end:`UIL`{`UIID`: row.end._id})
            CREATE  (start)-[r:HAS_PROPERTY]->(end) SET r += row.properties; """
    db.query(q8)

    q9 = """
            UNWIND [{start: {_id:19232}, end: {_id:19233}, properties:{}}, 
                    {start: {_id:19232}, end: {_id:19241}, properties:{}}] AS row
            MATCH   (start:`UIL`{`UIID`: row.start._id})
            MATCH   (end:`UIL`{`UIID`: row.end._id})
            CREATE  (start)-[r:HAS_TABLE]->(end) SET r += row.properties;
            """
    db.query(q9)

    q10 = """
            UNWIND [{start: {_id:19233}, end: {_id:19234}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19235}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19236}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19237}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19238}, properties:{}}, 
                    {start: {_id:19241}, end: {_id:19242}, properties:{}}, 
                    {start: {_id:19241}, end: {_id:19243}, properties:{}}, 
                    {start: {_id:19241}, end: {_id:19244}, properties:{}}, 
                    {start: {_id:19241}, end: {_id:19245}, properties:{}}, 
                    {start: {_id:19241}, end: {_id:19246}, properties:{}}, 
                    {start: {_id:19241}, end: {_id:19247}, properties:{}}] AS row
            MATCH   (start:`UIL`{`UIID`: row.start._id})
            MATCH   (end:`UIL`{`UIID`: row.end._id})
            CREATE  (start)-[r:HAS_COLUMN]->(end) SET r += row.properties;
            """
    db.query(q10)

    q11 = """
            UNWIND [{start: {_id:19233}, end: {_id:19220}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19221}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19222}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19223}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19224}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19225}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19226}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19227}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19228}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19229}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19230}, properties:{}}, 
                    {start: {_id:19233}, end: {_id:19231}, properties:{}}, 
                    {start: {_id:19241}, end: {_id:19239}, properties:{}}, 
                    {start: {_id:19241}, end: {_id:19240}, properties:{}}] AS row
            MATCH   (start:`UIL`{`UIID`: row.start._id})
            MATCH   (end:`UIL`{`UIID`: row.end._id})
            CREATE  (start)-[r:HAS_DATA]->(end) SET r += row.properties;
            """
    db.query(q11)

    q12 = """
            UNWIND [{start: {_id:19245}, end: {_id:19259}, properties:{}}, 
                    {start: {_id:19246}, end: {_id:19255}, properties:{}}, 
                    {start: {_id:19237}, end: {_id:19269}, properties:{}}, 
                    {start: {_id:19243}, end: {_id:19260}, properties:{}}, 
                    {start: {_id:19242}, end: {_id:19264}, properties:{}}, 
                    {start: {_id:19234}, end: {_id:19270}, properties:{}}, 
                    {start: {_id:19244}, end: {_id:19254}, properties:{}}, 
                    {start: {_id:19244}, end: {_id:19253}, properties:{}}, 
                    {start: {_id:19242}, end: {_id:19270}, properties:{}}, 
                    {start: {_id:19236}, end: {_id:19268}, properties:{}}, 
                    {start: {_id:19235}, end: {_id:19254}, properties:{}}, 
                    {start: {_id:19238}, end: {_id:19250}, properties:{}}, 
                    {start: {_id:19234}, end: {_id:19262}, properties:{}}, 
                    {start: {_id:19235}, end: {_id:19258}, properties:{}}, 
                    {start: {_id:19247}, end: {_id:19248}, properties:{}}] AS row
            MATCH   (start:`UIL`{`UIID`: row.start._id})
            MATCH   (end:`UIL`{`UIID`: row.end._id})
            CREATE (start)-[r:MAPS_TO_PROPERTY]->(end) SET r += row.properties;
            """
    db.query(q12)

def test_refactor_selected(db):
    """
    A scenario to test linking selected classes and domains.
    1. create data for 2 domains - AE and DM.
    2. refactor_selected(classes=["Subject"]
    3. refactor_selected(domains=["DM"]
    4. refactor_selected(domains=["AE"]
    """

    # Completely clear the database
    db.clean_slate()
    prep_data(db)

    mod_a = model_applier.ModelApplier(mode="schema_PROPERTY")

    # Verify Subject nodes do not already exist
    cypher = "MATCH (n:Subject) RETURN n"
    result = db.query(cypher)
    assert len(result) == 0

    # Run refactor_selected for Subject class
    mod_a.refactor_selected(classes=["Subject"])
    # Verify the expected nodes got created
    cypher = "MATCH (n:Subject) RETURN n"
    result = db.query(cypher)
    assert len(result) == 2

    # Verify the expected relationships got created
    cypher = "MATCH (n:Subject)-[rel:FROM_DATA]->() RETURN rel"
    result = db.query(cypher)
    assert len(result) == 14

    # Now refactor DM domain (not AE)
    mod_a.refactor_selected(domains=["DM"])

    # Verify nodes haven't been created for AE domain, other than for Subject class
    cypher = """MATCH p=(n:`Source Data Table`)-[r1:HAS_DATA]-(n2:`Source Data Row`)<-[r2:FROM_DATA]-(n3)
                WHERE n._filename_ = 'mae.xlsx'
                RETURN DISTINCT n3"""
    result = db.query(cypher)
    assert len(result) == 2  # only 2 data nodes should exist for Subject (refactored above)

    # Verify nodes have been created for DM domain
    cypher = """MATCH p=(n:`Source Data Table`)-[r1:HAS_DATA]-(n2:`Source Data Row`)<-[r2:FROM_DATA]-(n3)
                WHERE n._filename_ = 'mdm.xlsx'
                RETURN DISTINCT n3"""
    result = db.query(cypher)
    assert len(result) == 11  # all 11 data nodes should now exist for DM

    # Now refactor for the remaining domain; AE
    mod_a.refactor_selected(domains=["AE"])

    # Verify missing nodes have now been created for AE domain
    cypher = """MATCH p=(n:`Source Data Table`)-[r1:HAS_DATA]-(n2:`Source Data Row`)<-[r2:FROM_DATA]-(n3)
                WHERE n._filename_ = 'mae.xlsx'
                RETURN DISTINCT n3"""
    result = db.query(cypher)
    assert len(result) == 23  # all 23 data nodes should now exist for AE

    # Finally verify all expected relationships were created
    cypher = """MATCH(n)-[r]-(n1) WHERE n._filename_ = 'mae.xlsx' RETURN DISTINCT r"""
    result = db.query(cypher)
    assert len(result) == 90

    cypher = """MATCH(n)-[r]-(n1) WHERE n._filename_ = 'mdm.xlsx' RETURN DISTINCT r"""
    result = db.query(cypher)
    assert len(result) == 23


def test_refactor_selected_2classes_domains(db):
    """
    A scenario to test linking selected classes and domains (list >1).
    """
    mod_a = model_applier.ModelApplier(mode="schema_PROPERTY")

    # 1. refactor_selected(classes=["Subject", "Sex])
    db.clean_slate()
    prep_data(db)
    test_classes = ["Subject", "Sex"]
    mod_a.refactor_selected(classes=test_classes)
    # checking test_classes were created:
    q1 = f"""
        MATCH (node) 
        WHERE size(apoc.coll.intersection(labels(node), $classes)) > 0
        RETURN node
        """
    qres1 = db.query(q1, {'classes': test_classes})
    assert len(qres1) == 3

    # checking other than test_classes were not created:
    q2 = f"""
        MATCH (c:Class)
        WHERE NOT c.label in $classes 
        WITH collect(c.label) as other_classes 
        MATCH (node) 
        WHERE size(apoc.coll.intersection(labels(node), other_classes)) > 0
        RETURN node
        """
    qres2 = db.query(q2, {'classes': test_classes})
    assert len(qres2) == 0

    # 2. refactor_selected(domains=["DM", "AE"]
    db.clean_slate()
    prep_data(db)
    test_domains = ["DM", "AE"]
    mod_a.refactor_selected(domains=test_domains)

    q3 = """
    MATCH p=(n:`Source Data Table`)-[r1:HAS_DATA]-(n2:`Source Data Row`)
    WHERE n._filename_ = 'mae.xlsx'    
    OPTIONAL MATCH (n2)<-[r2:FROM_DATA]-(n3)
    RETURN count(n2) as rows, count(r2) as rels
    """
    qres3 = db.query(q3)
    assert qres3[0]['rows'] == qres3[0]['rels'] and qres3[0]['rows'] > 0
