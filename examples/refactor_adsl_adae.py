from neointerface import NeoInterface
from model_appliers import model_applier
from model_managers import model_manager
import time

start_time = time.time()

neo = NeoInterface()


###############  START OF SPECIFICATIONS   ###############

# TODO: maybe move all specs to a JSON file to import

# Specifications to define nodes labeled `Class`and nodes labeled `Property`,
#   as well as "HAS_PROPERTY" relationships between `Class`and `Property` nodes,
#   plus "MAPS_TO_PROPERTY" relationships from `Source Data Column` to `Property` nodes.

# A "*" as a key indicates that "MAPS_TO_PROPERTY" will be created regardless of
#   the name of the `Source Data Table` node linked to `Source Data Column`

#   The sub-entries are also dictionaries: their keys are entities such as "Race",
#       and their values are list of high-level "properties", AS WELL AS data column names,
#       linked to that entity (such as ["RACE", "RACEN"])

groupings = {
    "ADSL":{
        "Study": ["STUDYID"],
        "Site": ["SITEID"],
        "Race": ["RACE", "RACEN"],
        "Treatment": ["TRT01A", "TRT01AN"],
        "Age": ["AGE"],
    },
    "*": {
        "Subject": ["USUBJID"],
        "Adverse Event": ["ASTDT", "ASTTM", "AETERM"],
        "MedDRA Preferred Term": ["AEDECOD"],
        "MedDRA Body System": ["AESOC"],
        "Adverse Event Outcome": ["AEOUT"]
    }
}


# TODO: discuss this possible alternate specifications format
#       Each dictionary key represents a `Class` node;
#       each dictionary value is a triplet:
#           1) name for the new `Property` node
#           2) name to match against in `Source Data Column` node (now we have freedom to make it different from 1)
#           3) optional `Source Data Table` name to restrict the above match
#       Note: Repeating the table names may seem unwieldy, but these specs will probably get created by a UI.
#             Triplets might get replaced by dictionaries {"property_name": ..., "data_column_name": ..., "data_table_name": ...}
alternate_definition_of_groupings_to_discuss = {
    "Study": [
        ("STUDYID", "STUDYID", "ADSL")
    ],
    "Race": [
        ("RACE", "RACE", "ADSL"),
        ("RACEN", "RACEN", "ADSL")
    ],
    "Subject": [
        ("USUBJID", "USUBJID", None)
    ]
}




# JULIAN'S NOTE: the nature of all the relationships in the pairs below
#   is always along the lines of "HAS" or related (such as "contains", "is described by", etc)
#   They later lead to the creation of Neo4j relationships with names such as
#   "HAS_SITE", "HAS_SUBJECT", "HAS_RACE", etc., where the suffix derives from the 2nd elements in the pairs
#   TODO: maybe generalize to allow for custom relationship names:
#         see https://teams.microsoft.com/l/message/19:4d7003551fab41a79861d4fbe206c3e0@thread.tacv2/1620719841160?tenantId=63982aff-fb6c-4c22-973b-70e4acfb63e6&groupId=15fb09c8-2bdc-4b7d-81fd-05bebfaf209d&parentMessageId=1620719841160&teamName=Clinical%20Graph-DB%20Dev%20Topics&channelName=Python%20Codebase&createdTime=1620719841160
class_relationships = [
    ["Study", "Site"],
    ["Study", "Subject"],
    ["Site", "Subject"],
    ["Subject", "Race"],
    ["Subject", "Treatment"],
    ["Subject", "Age"],
    ["Subject", "Adverse Event"],
    ["Subject", "MedDRA Preferred Term"],
    ["Subject", "MedDRA Body System"],
    ["Adverse Event", "Adverse Event Outcome"],
    ["Adverse Event", "MedDRA Preferred Term"],
    ["Adverse Event", "MedDRA Body System"],
    ["MedDRA Body System", "MedDRA Preferred Term"]
]

###############  END of specifications   ###############



# Only keep the nodes created during the load_file() operations
neo.clean_slate(keep_labels=["Source Data Row", "Source Data Table", "Source Data Folder", "Source Data Column"])
neo.drop_all_indexes()


##############################################################################################################
#
# (PART 1) Carry out preliminary database setup
# Goal: to create nodes labeled `Class`, interlinked with `CLASS_RELATES_TO` relationships,
#       as well as nodes labeled `Property`, reachable from `Class`nodes thru `HAS_PROPERTY` relationships,
#       plus "MAPS_TO_PROPERTY" relationships from `Source Data Column` to `Property` nodes
#
##############################################################################################################


model_mgr = model_manager.ModelManager()
model_mgr.create_custom_mappings_from_dict(groupings)           # Create the schema: a set of nodes labeled "Class",
                                                                #   nodes labeled "Property", and relationships named "HAS_PROPERTY"
model_mgr.create_custom_rels_from_list(class_relationships)     # Add "CLASS_RELATES_TO" relationships to "Class" nodes


##############################################################################################################
#
# (PART 2) Now do the main data transformation
#
##############################################################################################################

mod_a = model_applier.ModelApplier(debug=True)

mod_a.define_refactor_indexes()     # Create some indexes

"""
# ***IMPORTANT*** !!! in order to make it work make sure latest apoc is installed and procedures must be whitelisted:
# in neo4j.conf:
# dbms.security.procedures.unrestricted=jwt.security.*,apoc.*
# dbms.security.procedures.allowlist=apoc.*
# dbms.security.procedures.whitelist=apoc.*
"""

mod_a.refactor_all()
#mod_a.refactor_all(limit="")


print(f"--- {(time.time() - start_time):.3f}' seconds ---")