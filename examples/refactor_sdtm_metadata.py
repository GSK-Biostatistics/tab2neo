import neointerface
from model_managers import model_manager
from model_appliers import model_applier
import time

start_time = time.time()

#neo = neointerface.NeoInterface(host = "neo4j://10.40.225.48:17687", credentials = None)
neo = neointerface.NeoInterface()

groupings = {
    "SDTM-3-2-EXCEL":{
        "Observation Class": ["Observation_Class"],
        "Domain": ["Domain_Prefix"],
        "Variable": ["Domain_Prefix", "Variable_Name", "Seq_For_Order"],
        "Variable Name": ["Variable_Name"],
        "Variable Label": ["Variable_Label"],
        "Variable Role": ["Role"],
    }
}

class_relationships = [
    ["Observation Class", "Domain"],
    ["Domain", "Variable"],
    ["Domain", "Variable Name"],
    ["Variable", "Variable Name"],
    ["Variable", "Variable Label"],
    ["Variable", "Variable Role"]
]


neo.clean_slate(keep_labels=["Message", "Source Data Row", "Source Data Table", "Source Data Folder", "Source Data Column"])

#mm = model_managers.ModelManager(host = "neo4j://10.40.225.48:17687", credentials = None)
mm = model_manager.ModelManager()
mm.create_custom_mappings_from_dict(groupings)
mm.create_custom_rels_from_list(class_relationships)

mod_a = model_applier.ModelApplier()
mod_a.define_refactor_indexes()
# IMPORTANT !!! in order to make it work make sure latest apoc is installed and procedures must be whitelisted:
# in neo4j.conf:
# dbms.security.procedures.unrestricted=jwt.security.*,apoc.*
# dbms.security.procedures.allowlist=apoc.*
# dbms.security.procedures.whitelist=apoc.*
mod_a.refactor_all()

print(f"--- {(time.time() - start_time):.3f}' seconds ---")