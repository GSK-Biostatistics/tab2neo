import pytest
from model_appliers import model_applier
from data_loaders.file_data_loader import FileDataLoader

DATA_PATH = 'tests/tests_model_applier/data/test_reshape_fhirlike/' 
# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def fdl():
    fdl = FileDataLoader(rdf=True)
    yield fdl
    
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
        interface.query(
            """
            MATCH (c)
            WHERE (c:FHIR_Class or c:FHIR_ObjectProperty)
                AND c.label in $classes
            SET c:Class
            """,
            {'classes': lines}
        )

def test_reshape_fhirlike(fdl):
    fdl.clean_slate()

    load_fhir_ttl(fdl, DATA_PATH)
    create_model_from_fhir(fdl, DATA_PATH)

    fdl.load_file(
        folder=DATA_PATH,
        filename='VS.csv'        
    )