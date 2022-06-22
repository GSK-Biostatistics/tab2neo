import os
import pandas as pd
from model_appliers.model_applier import ModelApplier
from model_managers import ModelManager


class ExcelStandardLoader(ModelApplier):
    SHEET_TO_LABEL_MAPPING = {
        'Datasets': 'Dataset',
        'Variables': 'Variable',
        'Codelists': 'Term',
        'Comments': 'Comment',
        'Documents': 'Document',
        'Whereclauses': 'Where Clause'
    }

    def __init__(self, standards_folder: str = None, standards_file: str = None, terminology_file: str = None,
                 *args, **kwargs):
        super().__init__(rdf=True, *args, **kwargs)
        if not standards_folder:
            self.standards_folder = os.environ.get("STANDARDS_FOLDER")
        else:
            self.standards_folder = standards_folder
        if not standards_file:
            self.standards_file = os.environ.get("STANDARDS_FILE")
        else:
            self.standards_file = standards_file
        if not terminology_file:
            self.terminology_file = os.environ.get("TERMINOLOGY_FILE")
        else:
            self.terminology_file = terminology_file

    def load_standard(self, extract_terms: bool = True, extract_vld: bool = True):
        path_standards = os.path.join(self.standards_folder, self.standards_file)
        path_terminology = os.path.join(self.standards_folder, self.terminology_file)
        self.load_xlsx(path_standards)
        self.load_xlsx(path_terminology)
        self.link_mdr_data(extract_terms, extract_vld)
        # self.link_mdr_vldata() #as of 24.09.2021 the BiomedicalConcept nodes are not used for refactoring. Just created for completeness
        self.load_link_sdtm_ttl()
        self.propagate_relationships()

    def load_xlsx(self, path):
        dfs = pd.read_excel(path, engine="openpyxl", sheet_name=None)
        for sheet, df in dfs.items():
            label = self.SHEET_TO_LABEL_MAPPING.get(sheet)
            df = df.fillna('')
            if not label:
                label = sheet
            print(f"Loading {sheet} -> {label}")
            node_ids = self.load_df(df, label, merge=False)
            self.link_nodes_to_provenance(os.path.split(path)[-1] + ';' + 'sheet=' + sheet, node_ids)

    def link_nodes_to_provenance(self, provenance: str, node_ids: list):
        q = f"""
        MERGE (p:Provenance{{`{ModelManager.RDFSLABEL}`:$provenance}})
        WITH *
        MATCH (x)
        WHERE id(x) in $node_ids
        MERGE (x)-[:PROVENANCE]->(p)
        """
        params = {'provenance': provenance, 'node_ids': node_ids}
        self.query(q, params)

    def load_mdr_data(self, standards_file, terminology_file):
        path_standards = os.path.join(self.standards_folder, standards_file)
        path_terminology = os.path.join(self.standards_folder, terminology_file)
        self.load_xlsx(path_standards)
        self.load_xlsx(path_terminology)
        print("Standards and Terminology Loaded")

    def link_mdr_data(self, extract_terms: bool = True, extract_vld: bool = True):
        print("Running MDR Data Link")
        keep_labels = ["Standard", "Dataset", "Variable", "Valuelevel", "Where Clause", "Methods", "Comments",
                       "Documents", "VLdata", "Terminology", "Term"]
        self.clean_slate(keep_labels=keep_labels)

        # also deleting the relationships
        self.query("MATCH ()-[r]-() DELETE r")
        self.debug = True

        # (:Dataset)-[:SUPP_DATASET]->
        self.create_index("Dataset", "Dataset")
        q = """
        MATCH (d:Dataset), (suppd:Dataset)
        WHERE suppd.Dataset = 'SUPP' + d.Dataset
        MERGE (d)-[:SUPP_DATASET]->(suppd)
        """
        self.query(q)

        # (:Dataset)-[:FA_DATASET]->
        q = """
        MATCH (d:Dataset), (suppd:Dataset)
        WHERE suppd.Dataset = 'FA' + d.Dataset
        MERGE (d)-[:FA_DATASET]->(suppd)
        """
        self.query(q)

        # Class HAS_DATASET Datasets
        self.extract_entities(label="Dataset",
                              target_label="ObservationClass",
                              property_mapping=["Class"],
                              relationship="HAS_DATASET",
                              direction="<"
                              )

        self.extract_entities(label="Variable",
                              target_label="Dataset",
                              property_mapping=["Dataset"],
                              relationship="HAS_VARIABLE",
                              direction="<"
                              )

        # hardcode adding --SEQ into the 'Sort Order'
        self.query(q="""
                    MATCH (n:Dataset)-[:HAS_VARIABLE]->(v:Variable)
                    WHERE v.Variable ends with "SEQ"
                    WITH  n, v.Variable as seq
                    SET n.`Sort Order` = CASE WHEN seq in apoc.text.split(n["Sort Order"], ",") THEN n["Sort Order"] ELSE n["Sort Order"] + "," + seq END
                    RETURN n["Sort Order"]
                    """
                   )

        if extract_terms:
            q = """
            MATCH (t:Term)       
            SET t.`Codelist Code` = 
                CASE WHEN t.`NCI Codelist Code` IS NULL OR t.`NCI Codelist Code` = '' THEN 
                    t.GSK_Codelist_Code
                ELSE
                    t.`NCI Codelist Code`
                END
            SET t.`Term Code` = 
                CASE WHEN t.`NCI Term Code` IS NULL OR t.`NCI Term Code` = '' THEN 
                    t.GSK_Term_Code
                ELSE
                    t.`NCI Term Code`
                END
            """
            self.query(q)

            #this is to save the ID of Codelist - will remove to keep distinct Terms by Codelist Code and Term Code
            self.extract_entities(label="Term",
                                 target_label="Codelist ID",
                                 property_mapping=["ID"],
                                 relationship="HAS_CODELIST_ID",
                                 direction="<",
                                 mode='create'
                                 )

            #self.create_index("Codelist", "ID")
            self.extract_entities(label="Variable",
                                  cypher="MATCH (node:Variable) WHERE node.Codelist IS NOT NULL RETURN id(node)",
                                  target_label="Term",
                                  property_mapping={"Codelist": "ID"},
                                  relationship="HAS_CONTROLLED_TERM",
                                  direction=">"
                                  )

            # #merging duplicate Terms together
            q = """
            MATCH (t:Term)
            WITH t.`Codelist Code` as cl, t.`Term Code` as trm, collect(t) as coll
            CALL apoc.refactor.mergeNodes(coll) YIELD node
            REMOVE node.ID
            WITH node
            MATCH (node)<-[r:HAS_CONTROLLED_TERM]-(c)
            WITH c, node, collect(r) as coll
            WHERE size(coll)>1
            WITH coll[1..] as coll
            UNWIND coll as r
            DELETE r
            """
            self.query(q)

        if extract_vld:
            self.extract_entities(label="Valuelevel",
                                  target_label="Variable",
                                  property_mapping=["Dataset", "Variable"],
                                  relationship="HAS_VALUE_LEVEL_METADATA",
                                  direction="<"
                                  )

            self.extract_entities(label="Valuelevel",
                                  cypher="MATCH (node:Valuelevel) WHERE EXISTS(node.Codelist) RETURN id(node)",
                                  target_label="Term",
                                  property_mapping={"Codelist": "ID"},
                                  relationship="HAS_VL_TERM",
                                  direction=">"
                                  )

            # linking Variables directly to controlled terms if they are connected through value-level data.
            self.query("""
            MATCH (v:Variable)-[:HAS_VALUE_LEVEL_METADATA]->(vlddata:Valuelevel)-[:HAS_VL_TERM]->(t:Term)
            MERGE (v)-[:HAS_CONTROLLED_TERM]->(t)
            """)

            self.extract_entities(label="Valuelevel",
                                  target_label="Where Clause",
                                  property_mapping={"Where Clause": "ID"},
                                  relationship="HAS_WHERE_CLAUSE",
                                  direction=">"
                                  )

            self.extract_entities(label="Where Clause",
                                  target_label="Variable",
                                  property_mapping=["Dataset", "Variable"],
                                  relationship="ON_VARIABLE",
                                  direction=">"
                                  )

            # Linking Where Clause to Term
            q = """
            MATCH (wc:`Where Clause`)-[:ON_VARIABLE]->(v:Variable)-[:HAS_CONTROLLED_TERM]->(t:Term)
            WHERE t.Term = wc.Value
            MERGE (wc)-[:ON_VALUE]->(t)
            """
            self.query(q)


        # clean-up empty Terms
        q = """
            MATCH (cl:Term)
            WHERE toString(cl.ID) = 'NaN' or cl.`Codelist Code` is NULL or cl.`Term Code` is NULL 
            DETACH DELETE cl
            """
        self.query(q)

        # link related Terms sequentially according to their Order property
        q = """
        MATCH (v:Variable)-[:HAS_CONTROLLED_TERM]->(t:Term)
        WITH v,t ORDER BY v.label, t.Order, t.Term ASC
        WITH v, COLLECT(t) AS terms
        FOREACH (n IN RANGE(0, SIZE(terms)-2) |
            FOREACH (prev IN [terms[n]] |
                FOREACH (next IN [terms[n+1]] |
                    MERGE (prev)-[:NEXT]->(next))))
        """
        self.query(q)

        print("MDR Data Link Complete")

    def load_link_sdtm_ttl(self, local=True): #TODO: Change local=False
        self.rdf_config()
        if local:
            with open(os.path.join(os.environ.get('STANDARDS_FOLDER'), 'sdtm-1-3.ttl')) as f:
                rdf = f.read()
                print(
                    self.rdf_import_subgraph_inline(rdf, "Turtle")
                )
        else:
            print(
                self.rdf_import_fetch(
                    "https://raw.githubusercontent.com/phuse-org/rdf.cdisc.org/master/std/sdtm-1-3.ttl",
                    "Turtle"
                )
            )

        self.create_index(label="DataElement", key="dataElementName")

        # linking ObservationClass to VariableGrouping (only class specific)
        q = """
        MATCH (oc:ObservationClass), (vg:VariableGrouping)
        WHERE vg.contextLabel = 
            CASE oc.Class 
                WHEN "FINDINGS" THEN "Findings Observation Class Variables"
                WHEN "EVENTS" THEN "Event Observation Class Variables"
                WHEN "INTERVENTIONS" THEN "Interventions Observation Class Variables"
            END
        MERGE (oc)-[:CLASS_SPECIFIC_VARIABLE_GROUPING]->(vg)
        """
        self.query(q)

        # adding properties dataElementName
        q = """ 
        MATCH (v:Variable), (de:DataElement)
        WHERE v.Variable =~ apoc.text.replace(de.dataElementName, '-', '.')
           AND NOT (v.Variable STARTS WITH 'RF' and v.Dataset = 'DM')
        WITH DISTINCT v, de.dataElementName as dataElementName, de.dataElementLabel as dataElementLabel
        SET v.dataElementName = dataElementName, v.dataElementLabel = dataElementLabel 
        """
        self.query(q)

        # linking Variable to DataElements (on dataElementName there might be >1 DataElement per Variable - need to filter on VariableGrouping~ObservationClass)
        # (1) WHERE size(coll)=1
        q = """
        MATCH (v:Variable)<-[r:HAS_VARIABLE]-(ds:Dataset)    
        OPTIONAL MATCH (da:DataElement)
        WHERE da.dataElementName = v.dataElementName        
        WITH v, collect(da) as coll
        WHERE size(coll)=1
        UNWIND coll as da
        MERGE (v)-[:IS_DATA_ELEMENT]->(da)
        """
        self.query(q)

        # 2 else
        q = """
        MATCH (v:Variable)<-[r:HAS_VARIABLE]-(ds:Dataset)<-[:HAS_DATASET]-(oc:ObservationClass)
        , (da:DataElement)-[:context]->(vg:VariableGrouping)<-[:CLASS_SPECIFIC_VARIABLE_GROUPING]-(oc)        
        WHERE NOT EXISTS ((v)-[:IS_DATA_ELEMENT]->()) AND da.dataElementName = v.dataElementName        
        MERGE (v)-[:IS_DATA_ELEMENT]->(da)               
        """
        self.query(q)

        # saveing counts of data elements with the same name
        q = """
        MATCH (da:DataElement)
        OPTIONAL MATCH (da:DataElement)-[:context]->(vg:VariableGrouping)
        WITH *, 
        {
          `Interventions Observation Class Variables`: "Interventions",
          `General Observation Class Timing Variables`: "GO Timing",
          `Findings Observation Class Variables`: "Findings",
          `General Observation Class Identifier Variables`: "GO Identifier",
          `Event Observation Class Variables`: "Events",
          `Findings About Events or Interventions Variables`: "FA"
        } as vg_mapping 
        SET da.vg = vg.contextLabel                
        SET da.vg_short = vg_mapping[vg.contextLabel]
        WITH DISTINCT da.dataElementName as name, collect(da) as coll
        WITH *, size(coll) as sz
        UNWIND coll as da
        SET da.n_with_same_name = sz 
        """
        self.query(q)

        # saveing counts of variables with the same label
        q = """
        MATCH (v:Variable)
        WITH v.Label as lbl, collect(v) as coll
        WITH *, size(coll) as sz
        UNWIND coll as v
        SET v.n_with_same_label = sz
        """
        self.query(q)

        # saveing counts of variables with the same name
        q = """
        MATCH (v:Variable)
        WITH v.Variable as name, collect(v) as coll
        WITH *, size(coll) as sz
        UNWIND coll as v
        SET v.n_with_same_name = sz
        """
        self.query(q)

        # # Extracting Class-Property
        # self.extract_entities(label="Variable",
        #                              target_label="Property",
        #                              property_mapping={"dataElementLabel": "class", "dataElementName": "label"},
        #                              relationship="MAPS_TO",
        #                              direction=">"
        #                              )
        # self.extract_entities(label="Property",
        #                              target_label="Class",
        #                              property_mapping={"class": "label"},
        #                              relationship="HAS_PROPERTY",
        #                              direction="<"
        #                              )
        print("SDTM TTL Loaded and Linked")

    def link_mdr_vldata(self):

        self.delete_nodes_by_label(delete_labels=["BiomedicalConcept"])

        print("Adding for each vld_group a VLdata with WhereVar = 'DOMAIN' and Value = {CCR_Category}.Dataset")
        q = """
        MATCH (vl:VLdata)
        WITH DISTINCT vl.vld_group as vld_group, vl.CCR_Category as CCR_Category, vl.VLDsource as VLDsource
        MATCH (v:Variable{Variable:CCR_Category})
        MERGE (vl_new:VLdata{vld_group:vld_group, WhereVar:'DOMAIN', Value:v.Dataset})
        SET vl_new.VLDsource = VLDsource
        SET vl_new.CCR_Category = CCR_Category
        SET vl_new.CODELIST = 'DOMAIN_' + v.Dataset
        """
        self.query(q)

        print("Extracting BCs")
        self.create_index("VLdata", "vld_group")
        self.extract_entities(label="VLdata",
                              target_label="BiomedicalConcept",
                              property_mapping=["vld_group"],
                              relationship="HAS_VLDATA",
                              direction="<"
                              )

        print("Linking BCs to Variables and Terms")
        # query to get exact values if codelists to link Biomedical Concept (vld_group) to
        q = f"""
        call apoc.periodic.iterate(
            '
            MATCH (bc:BiomedicalConcept)-[:HAS_VLDATA]->(vl:VLdata)
            RETURN bc, vl
            '
        ,
            '
            WITH *, apoc.text.replace(vl.WhereVar, "^\s+", "") as WhereVar, apoc.text.split(vl.VLDsource, " ") as src
            WITH *, 
            CASE WHEN size(src)>1 or src[0] = "RELREC" or size(src[0])=2 THEN src[0]
              ELSE substring(src[0],1,2) END as domain    
            MATCH (v:Variable)<-[:HAS_VARIABLE]-(ds:Dataset)
            WHERE   (v.Variable = WhereVar or v.dataElementName = "--" + WhereVar) and 
                    //(ds.Dataset = vl.CCR_Category or exists ( (ds)-[:HAS_VARIABLE]->(:Variable{{Variable:vl.CCR_Category}}) ))
                    ds.Dataset = domain        
            OPTIONAL MATCH (cl:Codelists)<-[:HAS_TERM]-(v)
            WHERE cl.ID = vl.CODELIST and cl.Term = vl.Value
            MERGE (bc)-[r:LINKS_VARIABLE]->(v)
            MERGE (vl)-[:WHERE_VAR]->(v)
            WITH *
            //conditional query (do merge only when vl.VLM_TARGET = "True")
            FOREACH(_ IN CASE WHEN vl.VLM_TARGET = True THEN [1] ELSE [] END|
                SET r.VLM_TARGET = True
            )
            FOREACH(_ IN CASE WHEN vl.CCR_Category = v.Variable THEN [1] ELSE [] END|
                SET r.CCR_Category = True
            )   
            WITH *
            WHERE cl IS NOT NULL
            MERGE (bc)-[:LINKS_TERM]->(cl)
            '
        ,
            {{batchSize:300, parallel:false}}
        )
        YIELD total, batches, failedBatches
        RETURN total, batches, failedBatches                     
        """
        print(
            self.query(q)
        )

        print("VLdata Linked")
        # the above example does not capture value
        # CMTRT	CMTERMMEDARTTEAR	OCCUR	NY_NY		Y
        # since the codelist ID is actually just NY
        # and does not capture
        # CMTRT	CMTERMMEDARTTEAR	TRT	CMTRT_OC		ARTIFICIAL TEARS (PRESERVATIVE FREE)
        # as not codelist CMTRT_OC is attached to CMTRT

        # todo: for completeness link bc to all Classes directly (not via the variables) - then one could extract BC \
        # from classes using DataProvider

    def propagate_relationships(self, on_children=True, on_parents=True):  # not used kept for code referenc
        la = ('' if (on_children and on_parents) or not on_children else '<')
        ra = ('' if (on_children and on_parents) or not on_parents else '>')
        q = f"""
        //propagate_relationships_of_parents_on_children
        MATCH (c:Class)
        OPTIONAL MATCH path = (c){la}-[:SUBCLASS_OF*1..50]-{ra}(parent)<-[r1:FROM]-(r:Relationship)-[r2:TO]->(fromto)
        WITH c, collect(path) as coll
        OPTIONAL MATCH path = (c){la}-[:SUBCLASS_OF*1..50]-{ra}(parent)<-[r1:TO]-(r:Relationship)-[r2:FROM]->(fromto)
        WITH c, coll + collect(path) as coll
        UNWIND coll as path
        WITH 
            c, 
            nodes(path)[-1] as fromto, 
            nodes(path)[-2] as r,
            relationships(path)[-1] as r2, 
            relationships(path)[-2] as r1
        WITH *, apoc.text.join([k in [x in keys(r) WHERE x <> 'uri'] | '`' + k + '`: "' + r[k] + '"'], ", ") as r_params  
        WITH *, CASE WHEN r_params = '' THEN '' ELSE '{' + r_params + '}' END as r_params
        WITH *, 
         '
                WITH $c as c
                MATCH (fromto) WHERE id(fromto) = $id_fromto
                MERGE (c)<-[:`'+type(r1)+'`]-(:Relationship' + r_params +')-[:`'+type(r2)+'`]->(fromto) 
         ' as q 
        WITH c, fromto, q
        call apoc.do.when(
            NOT EXISTS ( (c)--(:Relationship)--(fromto) ), 
            q,
            '',    
            {{c:c, id_fromto:id(fromto)}}
        ) YIELD value
        RETURN value 
        """
        self.query(q)