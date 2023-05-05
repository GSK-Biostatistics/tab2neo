# Class DataProvider(NeoInterface):
   To use the data already in the database (such as the data after the transformations with [ModelApplier](../model_appliers/README.md)),
   for various purposes - such as to feed the User Interface.

   Also for future data transformation, data enrichment, etc.

# Quick start
Given that Neo4j database contains the following metadata nodes:  

![metadata](images/data_provider_1.png)    

And the following data nodes:  

![data](images/data_provider_2.png) 

a call of 
   
    dp = DataProvider()
    dp.dp.get_data_generic(["Subject","RECORD","Sex"],infer_rels=True,return_propname=False) 
    
will return a dataframe
    


             _id_RECORD Sex  _id_Sex  _id_Subject Subject
    0               96   M       91           93    S001
    1               97   F       92           94    S002
where columns 'id.\_Sex_', 'id.\_RECORD_'and 'id.\_Treatment_' would contain unique identifiers of the corresponding nodes in the database (Neo4j node ids) and the rest of the columns will contain values of properties of the corresponding nodes.
    
# Detailed technical description
### __init__()
name | arguments| return
-----| ---------| -------
*__init__*| *args, **kwargs|

---

## DATA RETRIEVAL

### get_data()
name | arguments| return
-----| ---------| -------
*get_data*|  labels: list, rels: list = None, where_map=None, return_nodeid=False |pd.DataFrame

    Assembles into a Pandas dataframe the Property values from all the data nodes with the specified Classes.
    Simplified version of get_data_generic()

    :param labels:         List of strings with Class names.  EXAMPLE: ['Study', 'Site', 'Subject']
    :param where_map:       Used to restrict the data (by default, no restriction.)
                            A dictionary whose keys are Classes to apply restrictions to,
                                and whose values are dictionaries specifying conditions on Property values.
                            An implicit AND is understood among all the clauses.
                            EXAMPLE:
                            {
                                'SUBJECT': {
                                    'USUBJID': '01-001',
                                    'SUBJID': '001',
                                    'PATIENT GROUP': [3, 5]
                                },
                                'SEX': {
                                    'ASEX': 'Male'
                                }
                            }
                            Notes:
                                - The keys in the outer dictionary are expected to be node labels.
                                - The (key/values) in the inner dictionary entries
                                  are meant to be (attribute names : desired values)
                                  that are applicable to the node with their corresponding label.
                                - The values may be various data type, incl. strings, integers and lists.
    :param rels: A list of relationships e.g [{'from':<label1>, 'to':<label2>, 'type':<type>}, ...]
    :param return_nodeid: Boolean
    :return: pd.DataFrame   A Pandas dataframe containing all the (direct and indirect) Property values
                            of the data points from the requested Classes, plus their Neo4j IDs.
                            The Property names are prefixed with the Class names

    *EXAMPLE* - given the data in the image below,
              get_data(['Subject', 'Age']) produces:

                Subject     Age
    0            1          Placebo       
    1            2          Active       



---

---


### get_data_generic()
name | arguments| return
-----| ---------| -------
*get_data_generic*| labels: list, rels: list = None, labels_to_pack=None, infer_rels=False, where_map=None, where_rel_map=None,allow_unrelated_subgraphs: bool = False, use_shortlabel: bool = False, use_rel_labels: bool = True, return_nodeid: bool = True, return_propname: bool = True, return_termorder: bool = False, only_props: list = None, check_schema=False, limit=None, return_q_and_dict=False, pivot: bool = False| pd.DataFrame

    Assembles into a Pandas dataframe the Property values from the data nodes with the specified Classes.
    Use this instead of get_data() when you need a full set of options.

    TODO: it cannot handle different classes with the same property name - it may produce wrong results;
          use prefix_keys_with_label = True in this case

    :param labels:             List of strings.  EXAMPLE: ['Study', 'Site', 'Subject']
    :param rels: A list of relationships e.g [{'from':<label1>, 'to':<label2>, 'type':<type>}, ...]
    :param where_map:           Used to restrict the data (default, no restriction.)
                                A dictionary of dictionaries.  SEE explanation in get_data()
    :param where_rel_map 
    :param allow_unrelated_subgraphs: Boolean
    :param use_short_label: Boolean
    :param use_rel_labels: Boolean
    :param return_nodeid:       Boolean
    :param return_propname: Boolean
    :param return_termorder: Boolean
    :param only_props: list = None
    :param check_schema: Boolean
    :param limit: Either None or an integer
    :param return_q_and_dict: Boolean
    :param pivot: Boolean
    :return: pd.DataFrame(): nodes in path, merged with apoc.map.mergeList and converted to pd.DataFrame()


---
## FILTERS

### get_filters()
name | arguments| return
-----| ---------| -------
*get_filters*| classes:list, where_map=None|dict

    Useful to build a dropdown menu in a UI.
    Simplified version of get_filters_generic()

    :param classes:     A string or list of strings with Class names.  EXAMPLE: ['Study', 'Site', 'Subject']
    :param where_map:   See explanation in get_data()
    :return:            A (possibly empty) Python dictionary.  For example, see get_filters_generic()

    *EXAMPLE* - given the data in the image below,
                get_filters_generic(['patient', 'gender'])
                   
                produces a dictionary with 2 keys, 'patient' and 'gender', and Pandas data frames for values
                The 'patient' value is the dataframe:
                       patient patient.label
                    0       60          Jack
                    1       62          Jill
                The 'gender' value is the dataframe:
                       gender gender.label
                    0      61            M
                    1      63            F                


                (The numbers in the first columns are Neo4j ID's, and they will vary.)

---


![example of get_filters](images/get_filters_example.png)

Example of get_filters

---

### get_filters_generic()
name | arguments| return
-----| ---------| -------
*get_filters_generic*| classes, where_map=None, allow_unrelated_subgraphs=False, return_nodeid = False, return_properties ="*", prefix_keys_with_label=False| dict

    Useful to build a dropdown menu in a UI

    :param classes:             A string or list of strings with Class names.  EXAMPLE: ['Study', 'Site', 'Subject']
    :param where_map:           See explanation in get_data()
    :param return_nodeid:       Boolean
    :param return_properties:   Either a list of Property names to include in the dataframe, or the string "*" (meaning, "all")
                                        EXAMPLE: ["STUDYID", "SITEID"]
    :param prefix_keys_with_label: If True adds a prefix (corresponding node label) to each column name of the returned data
    :return:                    A (possibly empty) Python dictionary
                                EXAMPLE:
                                {
                                    Label1: pd.DataFrame(
                                        [
                                            {prop1: value11, prop2: value21},
                                            {prop2: value21, prop2: value22},
                                            ...
                                        ]
                                    ),
                                    Label3: pd.DataFrame(
                                        [
                                            {prop3: value11},
                                            {prop3: value21},
                                            ...
                                        ]
                                    )
                                }


---

## PANDAS SUPPORT


### convert_qb_result_to_df()
name | arguments| return
-----| ---------| -------
*convert_qb_result_to_df*| neoresult, hstack = True|
    
    :param neoresult:
    :param hstack:  If hstack == True returns 1 dataframe (properties of all returned neo4j variables are concatenated horizontally)
                    if hstack == False returns a dictionary of dataframes (1 per returned unit)

    :return:        Either a Pandas dataframe or a dictionary of dataframes




---





