"""
The script utilizes MDR graph at bolt://10.40.225.48:12002 (generated from
\\uk1salx00175.corpnet2.com\RD\gmp\data\MDR\StandardMDRSpec_3_2.xlsx)
and extended with some data from SDTM ontology https://github.com/phuse-org/rdf.cdisc.org
using the following script:
https://mygithub.gsk.com/gsk-tech/cldsdtmnb/blob/main/data_prep.py
"""
import pandas as pd
from neointerface import NeoInterface
from model_managers import ModelManager

# connecting to MDR graph
mm = ModelManager(rdf=True)
mdr_neo = NeoInterface(host="bolt://10.40.225.48:12002",
                       rdf_host='http://10.40.225.48:11002/rdf/',
                       credentials=('neo4j', 'mdr'))

def write_meta_to_graph(for_export=False, datasets = ['DM','DS','EX','AE','LB','VS','EG'], filename=None):
    """
    :param for_export: True if metadata for export is being generated (prefix in uri added and additional node (:`Data Extraction Standard`)
    :param datasets: domains for which mappings are to be created
    :param filename: filename to add on 'Data Extraction Standard' as metadata
    :return: None
    """
    mm.clean_slate()

    #getting metadata from Variables nodes of MDR to create Class and Property to map to
    q = f"""
    //Mapping to DataElements if exist, otherwise mapping to column label/name
    MATCH (v:Variables)<-[:HAS_VARIABLE]-(ds:Datasets)    
    WHERE v.Dataset in $datasets
    OPTIONAL MATCH (da:DataElement)
    WHERE da.dataElementName = v.dataElementName
    OPTIONAL MATCH (da)-[:dataElementRole]->(dar:DataElementRole)
    RETURN
      v.Dataset as _domain_,
      ds.`Sort Order` as `SortOrder`, //TODO: can keep property name with space when NeoInterface updated to account for prop names with spaces in RDF       
      v.Variable as _columnname_,
      v.Order as Order,        
      //CASE WHEN dar.label = 'Identifier Variable' AND NOT v.Dataset = 'DM' THEN        
      CASE WHEN dar.label = 'Identifier Variable' THEN
        ds.Description
      ELSE
        CASE WHEN v.dataElementLabel IS NULL THEN v.Label ELSE v.dataElementLabel END
      END as class,
      CASE WHEN v.dataElementName IS NULL THEN v.Variable ELSE v.dataElementName END as property,
      //CASE WHEN dar.label = 'Identifier Variable' AND NOT v.Dataset = 'DM' THEN True ELSE False END as CoreClass,
      CASE WHEN dar.label = 'Identifier Variable' THEN True ELSE False END as CoreClass,      
      v.Core as CoreVar
    ORDER BY _domain_, Order, _columnname_, class, property
    """
    params = {'datasets': datasets}
    data = mdr_neo.query(q, params)
    for dataset in params['datasets']:
        for key, item in {#'DOMAIN': 'Domain',
                          'USUBJID': 'Subject',
                          'STUDYID': 'Study',
                          }.items():
            data.append({'_domain_':dataset, '_columnname_':key, 'class':item, 'property':key, 'CoreClass': False, 'CoreVar': 'Required'})
    df = pd.DataFrame(data)

    #query to generate metadata nodes on NEO4J_HOST database
    q_for_export1, q_for_export2 = "", ""
    if for_export:
        q_for_export1 = f"MERGE (f:`Data Extraction Standard`{{_tag_:'MDR3_2', _filename_:$filename}}) WITH *"
        q_for_export2 = f"<-[:HAS_TABLE]-(f)"
    q2 = f"""
    {q_for_export1}
    UNWIND $data as row
    MERGE (c:Class{{label:row['class']}})
    SET c.from_domains = CASE WHEN row['_domain_'] in c.from_domains THEN c.from_domains 
        ELSE CASE WHEN c.from_domains IS NULL THEN [row['_domain_']] 
            ELSE c.from_domains + row['_domain_'] END 
        END  
    SET c.CoreClass = row['CoreClass']
    MERGE (c)-[:HAS_PROPERTY]->(p:Property{{label:row['property'], `Class.label`:row['class']}})
    MERGE (t:`Source Data Table`{{_domain_:row['_domain_']}}){q_for_export2}
    SET t.`SortOrder` = CASE WHEN row['SortOrder'] is NULL THEN t['SortOrder'] ELSE row['SortOrder'] END 
    WITH *
    MERGE (t)-[:HAS_COLUMN]->(col:`Source Data Column`{{_domain_: row['_domain_'], _columnname_:row['_columnname_']}})
    SET col.Core = row['CoreVar']
    SET col.Order = CASE WHEN row['Order'] is NULL THEN col['Order'] ELSE row['Order'] END 
    WITH *
    MERGE (col)-[:MAPS_TO_PROPERTY]->(p) 
    """
    mm.query(q2, {'data': data, 'filename': filename})

    # ------------------ LINKING-----------------------
    #(0)
    #-------- linking Core to all classes in domain--------
    q = """
    MATCH (core:Class), (c:Class)
    WHERE (core.CoreClass = True OR core.label = 'Subject') AND core<>c AND c.label <> 'Subject' AND core.from_domains[0] in c.from_domains
    MERGE (core)-[:CLASS_RELATES_TO]->(c)
    """
    mm.query(q)
    #-------- linking Subject to all CoreClass classes of other domains --------
    q = """
    MATCH (subj:Class), (core:Class)
    WHERE subj.label = 'Subject' AND core.CoreClass AND subj<>core 
    MERGE (subj)-[:CLASS_RELATES_TO]->(core)
    """
    mm.query(q)
    # -------- linking 'Demographics' to 'Date/Time of Collection' and 'Study Day of Visit/Collection/Exam' -------
    q = """
        MATCH (demo:Class), (collection:Class)
        WHERE demo.label = 'Demographics' AND collection.label in ['Date/Time of Collection', 'Study Day of Visit/Collection/Exam'] 
        MERGE (demo)-[:CLASS_RELATES_TO]->(collection)
        """
    mm.query(q)


    #(1)
    #-------- getting 'qualifies' link  --------
    q4 = """
    MATCH (x:DataElement)-[:qualifies]->(y)
    RETURN x.dataElementLabel as left, y.dataElementLabel as right
    """
    data = mdr_neo.query(q4)
    data.append({'left': 'Body System or Organ Class', 'right': 'Dictionary-Derived Term'})

    #-------- creating 'qualifies' CLASS_RELATES_TO metadata (to support ModelApplier.refactor_all) --------
    q5 = """
    UNWIND $data as row
    MATCH (left_c:Class), (right_c:Class)
    WHERE left_c.label = row['left'] and right_c.label = row['right']
    MERGE (left_c)-[:CLASS_RELATES_TO{relationship_type:'QUALIFIES'}]->(right_c)
    """
    mm.query(q5, {'data': data})


    #(2)
    #-------- getting topics -------
    q6 = """
    MATCH (de:DataElement)-[:dataElementRole]->(der:DataElementRole)
    WHERE der.label = 'Topic Variable'
    RETURN de.dataElementLabel as topic_class
    """
    _topics = mdr_neo.query(q6)
    #extending and updating topics:
    df_topics = pd.DataFrame(
        _topics + [{"topic_class": "Dictionary-Derived Term"}]
    )
    #we rather use the long name as topic than the short name
    df_topics['topic_class'] = df_topics['topic_class'].replace({
        'Short Name of Measurement, Test or Examination':'Name of Measurement, Test or Examination'})
    topics = list(df_topics['topic_class'])

    # ------- getting Result Qualifiers and fingings topics -------
    q8_2 = """
    MATCH (de:DataElement)-[:dataElementRole]->(der:DataElementRole),
    (de2:DataElement)-[:context]->(ctx:VariableGrouping)
    WHERE 
      der.label = 'Result Qualifier'   
      AND de2.dataElementLabel in $topics 
      AND ctx.contextLabel = 'Findings Observation Class Variables'
    RETURN de.dataElementLabel as rq_class, de2.dataElementLabel as topic
    """
    df_resqs = pd.DataFrame(mdr_neo.query(q8_2, {'topics':topics}))
    results = list(set(df_resqs['rq_class']))

    #------- linking Result Qualifiers to topics (Findings) -------
    q8_3 = """
    UNWIND $data as row
    MATCH (c:Class), (c_topic:Class) 
    WHERE c.label = row['rq_class'] AND c_topic.label = row['topic']
    MERGE (c_topic)-[r:CLASS_RELATES_TO]->(c)
    set r.relationship_type = 'HAS_RESULT'
    """
    mm.query(q8_3,{'data': df_resqs.to_dict(orient='records')})

    #(3)
    #linking grouping classes to topics
    q9_1 = """
    MATCH (de:DataElement)-[:dataElementRole]->(der:DataElementRole)
    WHERE der.label = 'Grouping Qualifier'
    RETURN DISTINCT de.dataElementLabel as groupping_class
    """
    grouppings = [res['groupping_class'] for res in mdr_neo.query(q9_1)]

    q9_2 = """
    MATCH (topic:Class), (gr:Class)
    WHERE topic.label in $topics and gr.label in $grouppings
    MERGE (topic)-[:CLASS_RELATES_TO{relationship_type:'IS_A'}]->(gr)
    """
    mm.query(q9_2, {'topics':topics, 'grouppings':grouppings})

    #(4)
    # category to subcategory
    mm.query("""
    MATCH (cat:Class), (scat:Class)
    WHERE cat.label = 'Category' and scat.label = 'Subcategory'
    MERGE (cat)-[:CLASS_RELATES_TO{relationship_type:'HAS_SUBCATEGORY'}]->(scat)
    """)

# --------------------------- Saving ttl ----------------------------------
def export_ttl(filename='refactoring.ttl', add_prefixes=[]):
    """
    function to export ttl file from metadata on NEO4J_HOST database
    :param filename: file to store metadata (should be same as the one used for write_meta_to_graph)
    :param add_prefixes: prefixes to add to uri (currently we use ['Metadata', 'mdr3_2'] prefixes for metadata for export
    :return: None
    """
    uri_map1 = {
                    "Data Extraction Standard": {"properties": "_tag_"},
                    "Source Data Folder": {"properties": "_folder_"},
                    "Source Data Table": {"properties": "_domain_"},
                    "Source Data Column": {"properties": ["_domain_", "_columnname_"]}
                }
    uri_map2 = {
        "Class": {"properties": "label"},
        "Property": {"properties": ["Class.label", "label"]},
    }
    mm.rdf_generate_uri(uri_map1, add_prefixes=add_prefixes)
    mm.rdf_generate_uri(uri_map2)
    rdf = mm.rdf_get_subgraph('MATCH p=()-[]-() RETURN p')
    with open(filename, "w", encoding='utf-8') as f:
        f.write(rdf)
        f.close()


write_meta_to_graph(for_export=False, datasets = ['DM','AE'])
export_ttl(filename="Map Columns to Properties_example_2domains_117106.ttl")

filename = 'export_sdtm_2domains.ttl'
write_meta_to_graph(for_export=True, datasets=['DM','AE'], filename=filename)
export_ttl(filename=filename, add_prefixes=['Metadata', 'mdr3_2'])

write_meta_to_graph(for_export=False, datasets = ['DM','DS','EX','AE','VS'])
export_ttl(filename="Map Columns to Properties_example_5domains_117106.ttl")

filename = 'export_sdtm_5domains.ttl'
write_meta_to_graph(for_export=True, datasets = ['DM','DS','EX','AE','VS'], filename=filename)
export_ttl(filename=filename, add_prefixes=['Metadata', 'mdr3_2'])

write_meta_to_graph(for_export=False, datasets = ['DM','DS','EX','AE','LB','VS','EG'])
export_ttl(filename="Map Columns to Properties_example_7domains_117106.ttl")

filename = 'export_sdtm_7domains.ttl'
write_meta_to_graph(for_export=True, datasets = ['DM','DS','EX','AE','LB','VS','EG'], filename=filename)
export_ttl(filename=filename, add_prefixes=['Metadata', 'mdr3_2'])


#TODO: custom links (e.g. Subject -> Body System, Derived term and other)