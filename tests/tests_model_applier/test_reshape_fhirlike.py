import pytest
from model_appliers import model_applier
from data_loaders.file_data_loader import FileDataLoader
from model_managers.model_manager import ModelManager
from model_appliers.model_applier import ModelApplier
from data_providers.data_provider import DataProvider
import pandas as pd


DATA_PATH = 'tests/tests_model_applier/data/test_reshape_fhirlike/' 
    
def load_fhir_ttl(interface, data_path:str):    
    with open(f'{data_path}fhir.ttl', 'r') as f:
        rdf = f.read()
        interface.rdf_import_subgraph_inline(rdf)
    #adding prefix to labels        
    interface.query("""
    call db.labels() yield label     
    where not label in ["Resource", "_GraphConfig"]
    call apoc.refactor.rename.label(label, "FHIR_" + label) yield total 
    return total
    """)   

def create_model_from_fhir(interface, data_path:str):
    #Making CLD Classes only out of some FHIR classes
    with open(f'{data_path}fhir_class_subset.txt', 'r') as f:
        lines = [''.join(c for c in line if c.isprintable()) for line in f.readlines() if not line.startswith('//')]
    #setting Classes:
    interface.query(
        """
        MATCH (c)
        WHERE (c:FHIR_Class or c:FHIR_ObjectProperty)
            AND c.label in $classes
        SET c:Class
        """,
        {'classes': lines}
    )
    #creating Relationships:
    interface.query(
        """
        MATCH (c:Class),(c2:Class)
        WHERE (c)-[:domain]->(c2) 
        OR (c)-[:domain]->()<-[:range]-(c2)
        OR c.label = "Specimen" and c2.label ends with ".specimen"
        SET c2.create=True
        MERGE (c2)<-[:FROM]-(r:Relationship{relationship_type:apoc.text.split(c.label,'\.')[-1]})-[:TO]->(c)
        """
    )

def add_clin_classes(mm:ModelManager):
    #creating Study, Site, Subject, Visit
    mm.create_class("Study")
    mm.create_class("Site")
    mm.create_class("Subject")
    mm.create_class("Arm")
    mm.create_class("Visit")
    mm.create_related_classes_from_list(rel_list=[
        ["Study", "Site", "Site"], #[from_class, to_class, rel_type]
        ["Study", "Subject", "Subject"],
        ["Site", "Subject", "Subject"],
        ["Subject", "Arm", "Arm"],
        ["Subject", "AdverseEvent", "AdverseEvent"],
        ["Subject", "MedicationAdministration", "MedicationAdministration"],
        ["Subject", "Observation", "Observation"],
        ["MedicationAdministration", "Domain", "Domain"],    
        ["Observation", "Domain", "Domain"],
        ["MedicationAdministration", "Visit", "Visit"],
        ["Observation", "Visit", "Visit"],
    ])

def map_columns(interface, data_path:str):
    with open(f'{data_path}VS_mapping.csv', 'r') as f:
        lines = [''.join(c for c in line if c.isprintable()) for line in f.readlines() if not line.startswith('//')]
    res = interface.query(
        """
        UNWIND $pairs as pair
        MATCH (x), (c:Class)
        WHERE 
        (
            (x:`Source Data Table` and x._domain_ = pair[0]) OR
            (x:`Source Data Column` and x._columnname_ = pair[0])
        )
        AND 
        c.label = pair[1]
        MERGE (x)-[:MAPS_TO_CLASS]->(c)
        WITH [coalesce(x._columnname_, x._domain_), c.label] as pair
        RETURN pair
        """,
        {"pairs": [line.split(",") for line in lines]}
    )    
    return [r["pair"] for r in res]
    

def test_reshape_fhirlike():
    fdl = FileDataLoader(rdf=True)
    mm = ModelManager(rdf=True)
    ma = ModelApplier(rdf=True, mode="schema_CLASS")
    dp = DataProvider(rdf=True)

    fdl.clean_slate()
    load_fhir_ttl(fdl, DATA_PATH)
    create_model_from_fhir(fdl, DATA_PATH)
    add_clin_classes(mm)    

    fdl.delete_nodes_by_label(delete_labels=["Source Data Row"])
    vs_df = fdl.load_file(
        folder=DATA_PATH,
        filename='VS.csv'        
    )
    vs_df = vs_df.drop(['_domain_', '_folder_', '_filename_'], axis=1)

    mapped_classes = map_columns(fdl, DATA_PATH)    
    ma.reshape_all()

    data, q, params = dp.get_data_cld(labels=list(set([pair[1] for pair in mapped_classes])))
    df = pd.DataFrame(data)
    df = df.rename({pair[1]: pair[0] for pair in mapped_classes}, axis=1)
    df = df[list(vs_df.columns)].sort_values(["STUDYID","DOMAIN","USUBJID","VISIT","VSTESTCD"], ignore_index=True)
    print(df)    
    
    pd.testing.assert_frame_equal(vs_df, df)