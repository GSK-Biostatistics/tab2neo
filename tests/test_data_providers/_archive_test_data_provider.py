import json
import os
filepath = os.path.dirname(__file__)
import pytest
from data_providers import data_provider
import pandas as pd
from utils.utils import compare_unordered_lists


# Provide a DataProvider object (which contains a database connection)
# that can be used by the various tests that need it
@pytest.fixture(scope="module")
def dp():
    dp = data_provider.DataProvider(verbose=False)
    yield dp



######################   TESTS FOR DATA RETRIEVAL   ######################


def test_get_data(dp):

    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['Subject', 'Treatment']])
    assert my_classes == ['Subject', 'Treatment']

    dp.qb.mm.create_property('Subject', 'USUBJID')
    # This time, we have 2 Properties for Treatment
    dp.qb.mm.create_property('Treatment', 'TRT01A')
    dp.qb.mm.create_property('Treatment', 'TRT01AN', merge=False)
    cypher = "CREATE (:Subject {USUBJID:'1234'})-[:`HAS_TREATMENT`]->(:Treatment {TRT01A:'Placebo', TRT01AN:2.0})"
    dp.query(cypher)

    df = dp.get_data(my_classes)
    # Note: the new defaults are now return_nodeid=True, and prefix_keys_with_label=True

    # It returns a dataframe such as (where 23 is the Neo4j of the `Subject` node, and 24 that of the `Treatment` node):
    '''
      Subject.USUBJID  Subject Treatment.TRT01A  Treatment.TRT01AN  Treatment
    0            1234       23          Placebo                2.0         24
    '''
    assert len(df) == 1
    result = dp.get_nodes("Subject", return_nodeid=True)
    subject0_id = result[0]["neo4j_id"]
    result = dp.get_nodes("Treatment", return_nodeid=True)
    treatment0_id = result[0]["neo4j_id"]

    expected_df = pd.DataFrame(  [["1234", "Placebo", 2.0, subject0_id, treatment0_id]],
                        columns = ["Subject.USUBJID", "Treatment.TRT01A", "Treatment.TRT01AN", "Subject", "Treatment"])

    assert df.sort_index(axis=1).equals(expected_df.sort_index(axis=1))     # Compare regardless of column order


    # Add a 2nd data point, with the same schema
    cypher = "CREATE (:Subject {USUBJID:'9876'})-[:`HAS_TREATMENT`]->(:Treatment {TRT01A:'Active', TRT01AN:1.0})"
    dp.query(cypher)
    df = dp.get_data(my_classes)
    # Dataframe such as (the numbers in the Subject and Treatment columns will vary):
    '''
      Subject.USUBJID  Subject Treatment.TRT01A  Treatment.TRT01AN  Treatment
    0            1234       23          Placebo                2.0         24
    1            9876       25           Active                1.0         26
    '''
    assert len(df) == 2
    result = dp.get_nodes("Subject", return_nodeid=True, properties_condition={"USUBJID": "9876"})
    subject1_id = result[0]["neo4j_id"]

    result = dp.get_nodes("Treatment", return_nodeid=True, properties_condition={"TRT01A": "Active"})
    treatment1_id = result[0]["neo4j_id"]

    expected_df.loc[1] = ["9876", "Active", 1.0, subject1_id, treatment1_id]
    assert df.sort_index(axis=1).equals(expected_df.sort_index(axis=1))     # Compare regardless of column order


    # Now retrieve the same data, but with conditions

    df = dp.get_data(my_classes, where_map = {'Treatment': {'TRT01A': 'Active'}})
    '''
      Subject.USUBJID  Subject Treatment.TRT01A  Treatment.TRT01AN  Treatment
    0            9876       25           Active                1.0         26
    '''
    assert len(df) == 1

    expected_df = pd.DataFrame(  [["9876", subject1_id, "Active", 1.0, treatment1_id]],
                        columns = ["Subject.USUBJID", "Subject", "Treatment.TRT01A", "Treatment.TRT01AN", "Treatment"])
                                                             # Use the IDs from the 2nd row of the previous dataset

    assert df.sort_index(axis=1).equals(expected_df.sort_index(axis=1))     # Compare regardless of column order


    # Add a 3rd data point, with the same schema
    cypher = "CREATE (:Subject {USUBJID:'555'})-[:`HAS_TREATMENT`]->(:Treatment {TRT01A:'Active', TRT01AN:1.0})"
    dp.query(cypher)
    df = dp.get_data(my_classes)
    # Dataframe such as (the numbers in the Subject and Treatment columns will vary):
    '''
      Subject.USUBJID  Subject Treatment.TRT01A  Treatment.TRT01AN  Treatment
    0            1234      122          Placebo                2.0        123
    1            9876      124           Active                1.0        125
    2             555      126           Active                1.0        127
    '''
    # Retrieve the same data with a double condition that leads back to the same row from the last data fetch
    df = dp.get_data(my_classes, where_map = {'Treatment': {'TRT01A': 'Active'}, 'Subject': {'USUBJID': '9876'}})
    assert len(df) == 1
    assert df.sort_index(axis=1).equals(expected_df.sort_index(axis=1))     # Compare regardless of column order



def test_get_data_generic_prefix_label(dp):

    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['Appl e', 'fruit']])
    dp.query("MATCH (c:Class) MERGE (c)-[:HAS_PROPERTY]->(:Property{label:'label'})")

    # Now expand the database just created above
    cypher = "CREATE (a:`Appl e` {label:'app le'})-[:HAS_FRUIT]->(f:`fruit` {label:'fruit'}) RETURN id(a) as a_id, id(f) as f_id"
    dp.debug=True
    nodeids = dp.query(cypher)

    result = dp.get_data_generic(my_classes, prefix_keys_with_label=True, return_nodeid=False)
    print('result:', result)
    df = pd.DataFrame({
        'Appl e.label': ['app le'],
        'fruit.label': ['fruit']})
    assert df.equals(result)



def test_get_data_generic_prefix_label_and_nodeid(dp):

    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['Appl e', 'fruit']])
    dp.query("MATCH (c:Class) MERGE (c)-[:HAS_PROPERTY]->(:Property{label:'label'})")

    # Now expand the database just created above
    cypher = "CREATE (a:`Appl e` {label:'app le'})-[:HAS_FRUIT]->(f:`fruit` {label:'fruit'}) RETURN id(a) as a_id, id(f) as f_id"
    dp.debug=True
    nodeids = dp.query(cypher)

    result = dp.get_data_generic(my_classes)
    print('result:', result)
    df = pd.DataFrame({
        'Appl e.label': ['app le'],
        'Appl e': [nodeids[0]['a_id']],
        'fruit.label': ['fruit'],
        'fruit': [nodeids[0]['f_id']]})
    assert df.equals(result)



def test_get_data_generic_helper(dp):
    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    dp.clean_slate()

    # Add 2 Class nodes, with a relationship between them
    my_classes = dp.qb.mm.create_related_classes_from_list([['apple', 'fruit']])

    (cypher, data_dict) = dp._get_data_generic_helper(my_classes, return_nodeid=False, prefix_keys_with_label=False)
    expected = '''MATCH (`apple`:`apple`)
,(`fruit`:`fruit`)
,(`apple`)-[:`HAS_FRUIT`]->(`fruit`) RETURN apoc.map.mergeList([CASE WHEN `apple`{.*} IS NULL THEN {} ELSE `apple`{.*} END
, CASE WHEN `fruit`{.*} IS NULL THEN {} ELSE `fruit`{.*} END]) as all LIMIT 20'''

    assert cypher.strip() == expected
    assert data_dict == {'labels': ['apple', 'fruit']}


    # Add 2 data nodes, with a relationship between them
    cypher = "CREATE (:apple {label:'apple'})-[:`HAS_FRUIT`]->(:fruit {label:'fruit'})"
    dp.query(cypher)

    (cypher, data_dict) = dp._get_data_generic_helper(my_classes, return_nodeid=False, prefix_keys_with_label=False)

    expected = '''MATCH (`apple`:`apple`),(`fruit`:`fruit`),(`apple`)-[:`HAS_FRUIT`]->(`fruit`) RETURN apoc.map.mergeList([CASE WHEN `apple`{.*} IS NULL THEN {} ELSE `apple`{.*} END, CASE WHEN `fruit`{.*} IS NULL THEN {} ELSE `fruit`{.*} END]) as all LIMIT 20'''

    assert cypher.replace("\n", "").strip() == expected
    assert data_dict == {'labels': ['apple', 'fruit']}



def test_get_data_generic(dp):
    # Preparing data, part 1 : create 2 `Class` nodes, with a "CLASS_RELATES_TO" relationship between them
    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['EMPLOYEE', 'DEPARTMENT']])
    df = dp.get_data(my_classes)
    assert df.empty     # Too soon in the data-building process

    # Preparing data, part 2 : attach a property node to each of the Class nodes
    dp.qb.mm.create_property('EMPLOYEE', 'Name')
    dp.qb.mm.create_property('DEPARTMENT', 'Budget')
    df = dp.get_data(my_classes)
    assert df.empty     # Still too soon in the data-building process

    # Preparing data, part 3 : add 2 data nodes (instances of the above classes), with a suitable relationship between them
    cypher = "CREATE (:EMPLOYEE {Name:'John Brown'})-[:`HAS_DEPARTMENT`]->(:DEPARTMENT {Budget:150000})"
    dp.query(cypher)

    df = dp.get_data_generic(my_classes, return_nodeid=False, prefix_keys_with_label=False)
    print(df)
    expected_df = pd.DataFrame([[150000, "John Brown"]], columns = ["Budget", "Name"])
    assert df.equals(expected_df)



def test_get_data_generic_2(dp):
    # Prepare the data, as in test_get_data()
    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['Subject', 'Treatment']])
    dp.qb.mm.create_property('Subject', 'USUBJID')
    # This time, we have 2 Properties for Treatment
    dp.qb.mm.create_property('Treatment', 'TRT01A')
    dp.qb.mm.create_property('Treatment', 'TRT01AN', merge=False)
    cypher = "CREATE (:Subject {USUBJID:'1234'})-[:`HAS_TREATMENT`]->(:Treatment {TRT01A:'Placebo', TRT01AN:2.0})"
    dp.query(cypher)

    df = dp.get_data_generic(my_classes, return_nodeid=False, prefix_keys_with_label=False)
    # Dataframe:
    '''
      USUBJID   TRT01A  TRT01AN
    0    1234  Placebo      2.0
    '''
    expected_df = pd.DataFrame([["1234", "Placebo", 2.0]], columns = ["USUBJID", "TRT01A", "TRT01AN"])
    assert df.equals(expected_df)

    # Add a 2nd data point, with the same schema
    cypher = "CREATE (:Subject {USUBJID:'9876'})-[:`HAS_TREATMENT`]->(:Treatment {TRT01A:'Active', TRT01AN:1.0})"
    dp.query(cypher)
    df = dp.get_data_generic(my_classes, return_nodeid=False, prefix_keys_with_label=False)
    # Dataframe:
    '''
      USUBJID   TRT01A  TRT01AN
    0    1234  Placebo      2.0
    1    9876   Active      1.0
    '''
    expected_df.loc[1] = ["9876", "Active", 1.0]
    assert df.equals(expected_df)

def test_get_data_generic_rename_rdfs(dp):
    dp.clean_slate()
    #loading metadata (Class/Property from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_get_data_generic_rename_rdfs_01.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

    #for testing purposes we don't reshapre the data with ModelApplier, just creating it as if it was reshaped
    cypher = """
    CREATE (subj:Subject {`rdfs:label`:'1234'})-[:`HAS_PLANNED TREATMENT PERIOD 1`]->(:`Planned Treatment Period 1` {`rdfs:label`:'Placebo'}),
    (subj)-[:HAS_SEX]->(:Sex{`rdfs:label`:'F'})
    """
    dp.query(cypher)
    # dp.debug=True
    dp.set_mode("schema_CLASS")

    #Test case #1
    df = dp.get_data_generic(['Subject', 'Planned Treatment Period 1', 'Sex'],
                             return_nodeid=False,
                             prefix_keys_with_label=False)
    expected_df = pd.DataFrame([["1234", "Placebo", "F"]], columns = ["USUBJID", "TRT01P", "SEX"])
    print(df)
    print(expected_df)
    assert df.equals(expected_df)

    #Test case #2 with prefix_keys_with_label=True
    df2 = dp.get_data_generic(['Subject', 'Planned Treatment Period 1', 'Sex'],
                             return_nodeid=False,
                             prefix_keys_with_label=True)
    expected_df2 = pd.DataFrame(
        [["1234", "Placebo", "F"]],
        columns=["Subject.USUBJID", "Planned Treatment Period 1.TRT01P", "Sex.SEX"]
    )
    # print(df2)
    # print(expected_df2)
    assert df2.equals(expected_df2)

    #Test case 3 - the Subject node will have a property MYSUBJID rather than rdfs:label
    #this should be reflected in the metadata:
    dp.query("MATCH (c:Class{label:'Subject'})-[:HAS_PROPERTY]->(prop:Property) SET prop.label = 'MYSUBJID'")
    #and data:
    dp.query("MATCH (s:Subject) SET s.MYSUBJID = s.`rdfs:label` REMOVE s.`rdfs:label`")
    #dp.debug=True
    df = dp.get_data_generic(['Subject', 'Planned Treatment Period 1', 'Sex'],
                             return_nodeid=False,
                             prefix_keys_with_label=False,
                             )
    expected_df = pd.DataFrame([["1234", "Placebo", "F"]], columns = ["MYSUBJID", "TRT01P", "SEX"])
    print(df)
    print(expected_df)
    assert df.equals(expected_df)
    dp.set_mode("schema_PROPERTY") #return to schema mode


######################   TESTS RELATED TO get_filters()   ######################


def test_get_filters_helper(dp):
    # Verify that an exception is raised in some pathological cases
    with pytest.raises(Exception):
        assert dp._get_filters_helper("")    # "ERROR in _get_filters_helper(): the argument `classes` cannot be an empty string"
        assert dp._get_filters_helper([])    # "ERROR in qb_generate_return(): empty lists are not an acceptable argument for `classes`"


    result, _, _ = dp._get_filters_helper("Non_existent_node")
    assert result == {'Non_existent_node': []}
    result, _, _ = dp._get_filters_helper(["List_with_Non_existent_node"])
    assert result == {'List_with_Non_existent_node': []}

    result, _, _ = dp._get_filters_helper(["cls1", "cls2"], return_nodeid=False, prefix_keys_with_label=False)
    assert result == {'cls1': [], 'cls2': []}

    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['apple', 'fruit']])
    result, _, _ = dp._get_filters_helper(my_classes, return_nodeid=True, prefix_keys_with_label=False)
    assert result == {'apple': [], 'fruit': []}
    # Now expand the database just created above
    cypher = "CREATE (a:`apple` {label:'apple'})-[:HAS_FRUIT]->(f:`fruit` {label:'fruit'})"
    dp.query(cypher)

    result, _, _ = dp._get_filters_helper(my_classes, return_nodeid=False, prefix_keys_with_label=False)
    """
    Note: the internal query run by _get_filters_helper is:
        MATCH (`fruit`:`fruit`), (`apple`:`apple`), (`apple`)-[:`HAS_FRUIT`]->(`fruit`), (`apple`), (`fruit`)     
        RETURN collect(distinct `fruit`{.*}) as `fruit`, collect(distinct `apple`{.*}) as `apple`
    """
    assert result == {'fruit': [{'label': 'fruit'}], 'apple': [{'label': 'apple'}]}



def test_get_filters_helper_return_nodeid(dp):

    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['apple', 'fruit']])
    cypher = "CREATE (a:`apple` {label:'apple'})-[:HAS_FRUIT]->(f:`fruit` {label:'fruit'})"
    dp.query(cypher)

    # Extract the Neo4j ID's of the newly-created `apple` and `fruit` nodes
    apple_node_data = dp.get_nodes("apple", return_nodeid=True)
    apple_id = apple_node_data[0]["neo4j_id"]   # There's only 1 node labeled "apple"

    fruit_node_data = dp.get_nodes("fruit", return_nodeid=True)
    fruit_id = fruit_node_data[0]["neo4j_id"]   # There's only 1 node labeled "fruit"

    # Repeat the last function call, but this time asking for the node IDs to be returned
    result, _, _ = dp._get_filters_helper(my_classes, return_nodeid=True, prefix_keys_with_label=False)
    """
    Note: the internal query run by _get_filters_helper is:
        MATCH (`fruit`:`fruit`), (`apple`:`apple`), (`apple`)-[:`HAS_FRUIT`]->(`fruit`), (`apple`), (`fruit`)          
        RETURN collect(distinct {`fruit`:id(`fruit`)}) as `fruit`, collect(distinct {`apple`:id(`apple`)}) as `apple`
    """
    assert result == {'fruit': [{'fruit': fruit_id, 'label': 'fruit'}], 'apple': [{'apple': apple_id, 'label': 'apple'}]}



def test_get_filters_generic_prefix_label(dp):

    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['Appl e', 'fruit']])

    # Now expand the database just created above
    cypher = "CREATE (a:`Appl e` {label:'app le'})-[:HAS_FRUIT]->(f:`fruit` {label:'fruit'})"
    dp.query(cypher)

    result = dp.get_filters_generic(my_classes, prefix_keys_with_label=True, return_nodeid = False)
    """
    Note: the internal query run by _get_filters_helper is:
        MATCH (`fruit`:`fruit`), (`appl e`:`Appl e`), (`appl e`)-[:`HAS_FRUIT`]->(`fruit`), (`appl e`), (`fruit`)     
        RETURN collect(distinct `fruit`{.*}) as `fruit`, collect(distinct `appl e`{.*}) as `appl e`
    """
    df1 = pd.DataFrame({'fruit.label': ['fruit']})    # A 1x1 table with data "fruit" and column name "label"
    df2 = pd.DataFrame({'Appl e.label': ['app le']})
    assert df1.equals(result['fruit'])
    assert df2.equals(result['Appl e'])

    #print("result : ", result)


def test_get_filters_generic(dp):

    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['apple', 'fruit']])

    # Now expand the database just created above
    cypher = "CREATE (a:`apple` {label:'apple'})-[:HAS_FRUIT]->(f:`fruit` {label:'fruit'})"
    dp.query(cypher)

    result = dp.get_filters_generic(my_classes, return_nodeid = False, prefix_keys_with_label=False)
    """
    Note: the internal query run by _get_filters_helper is:
        MATCH (`fruit`:`fruit`), (`apple`:`apple`), (`apple`)-[:`HAS_FRUIT`]->(`fruit`), (`apple`), (`fruit`)     
        RETURN collect(distinct `fruit`{.*}) as `fruit`, collect(distinct `apple`{.*}) as `apple`
    """
    df1 = pd.DataFrame({'label': ['fruit']})    # A 1x1 table with cell value "fruit" and column name "label"
    df2 = pd.DataFrame({'label': ['apple']})
    assert df1.equals(result['fruit'])
    assert df2.equals(result['apple'])



def test_get_filters(dp):

    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    dp.clean_slate()
    my_classes = dp.qb.mm.create_related_classes_from_list([['patient', 'gender']])

    # Now expand the database by adding data points of Classes 'patient' and 'gender'
    q = '''
        CREATE (:`patient` {label:'Jack'})-[:HAS_GENDER]->(:`gender` {label:'M'}),
               (:`patient` {label:'Jill'})-[:HAS_GENDER]->(:`gender` {label:'F'})
        '''
    dp.query(q)

    result = dp.get_filters_generic(my_classes)
    # A dict with 2 keys, 'patient' and 'gender', and Pandas data frames for values
    assert len(result) == 2
    expected_keys = ['patient', 'gender']
    returned_keys = list(result)    # List of all the keys in the result dict
    assert compare_unordered_lists(expected_keys, returned_keys)

    Jack_id = dp.query("MATCH (n:patient {label:'Jack'}) RETURN id(n) as nodeid")[0]["nodeid"]
    Jill_id = dp.query("MATCH (n:patient {label:'Jill'}) RETURN id(n) as nodeid")[0]["nodeid"]

    male_id =   dp.query("MATCH (n:gender {label:'M'}) RETURN id(n) as nodeid")[0]["nodeid"]
    female_id = dp.query("MATCH (n:gender {label:'F'}) RETURN id(n) as nodeid")[0]["nodeid"]

    df1 = pd.DataFrame([[Jack_id, "Jack"], [Jill_id, "Jill"]], columns=["patient", "patient.label"])    # A 2x2 dataframe
    df2 = pd.DataFrame([[male_id, "M"], [female_id, "F"]], columns=["gender", "gender.label"])    # A 2x2 dataframe
    assert df1.equals(result['patient'])
    assert df2.equals(result['gender'])




######################   TESTS FOR convert_qb_result_to_df()   ######################

def test_convert_qb_result_to_df(dp):
    # Test one form of the input
    neoresult = [{'Study': {'studyid':'ABC'}, 'Site': {'siteid':1, 'sitename':'SITE1'}},
                 {'Study': {'studyid':'ABC'}, 'Site': {'siteid':2, 'sitename':'SITE2'}}]
    result = dp.convert_qb_result_to_df(neoresult)
    '''It returns the following Pandas dataframe:
      studyid  siteid sitename
    0     ABC       1    SITE1
    1     ABC       2    SITE2
    '''
    expected_result = pd.DataFrame([["ABC", 1, "SITE1"] , ["ABC", 2, "SITE2"]],
                                   columns=['studyid', 'siteid', 'sitename'])
    assert result.equals(expected_result)

    result_dict = dp.convert_qb_result_to_df(neoresult, hstack=False)
    # It returns a dictionary with keys 'Study' and 'Site'
    assert len(result_dict) == 2
    # The value of 'Study' is the data frame
    '''
      studyid
    0     ABC
    1     ABC
    '''
    pd1 = pd.DataFrame({"studyid": ["ABC","ABC"]})
    assert result_dict['Study'].equals(pd1)
    # and the value of 'Site' is the data frame
    '''
       siteid sitename
    0       1    SITE1
    1       2    SITE2    
    '''
    pd2 = pd.DataFrame({"siteid": [1,2], "sitename": ["SITE1","SITE2"]})
    assert result_dict['Site'].equals(pd2)


    # Test an alternate form of the input
    neoresult = [{'all': {'studyid':'ABC', 'siteid':1, 'sitename':'SITE1'}},
                 {'all': {'studyid':'ABC', 'siteid':2, 'sitename':'SITE2'}}]
    result = dp.convert_qb_result_to_df(neoresult)
    # It returns the same Pandas dataframe as the first run
    assert result.equals(expected_result)

    result_dict = dp.convert_qb_result_to_df(neoresult, hstack=False)
    # It returns a dictionary with a single key 'all', whose value is the above Pandas dataframe
    assert len(result_dict) == 1
    assert result_dict['all'].equals(expected_result)


    # Tests of pathological scenarios
    result = dp.convert_qb_result_to_df([])
    assert result.empty

    result_dict = dp.convert_qb_result_to_df([], hstack=False)
    assert result_dict == {}