MATCH (m:Method{id:$method_id, parent_id: $study_id})
OPTIONAL MATCH (m)-[:METHOD_ACTION]->(run_scr:Method)-[r2]->(ch:Changes) where (run_scr.type in ['run_script', 'call_api'])
OPTIONAL MATCH (m)-[:METHOD_ACTION]->(ass_cl:Method), (ass_cl_f:Class)<-[:CLASS]-(ass_cl)-[:ON]->(ass_cl_t:Class) where (ass_cl.type = 'assign_class')

WITH ch, apoc.coll.sort(apoc.coll.flatten(collect(distinct(ass_cl_f.short_label)))) as ASSIGN_CLASSES
WITH ch, ASSIGN_CLASSES, [x IN ch.cols_before WHERE not x contains '_id_']  as `CLASSES AFTER RUNSCRIPT`, [x IN ch.cols_after WHERE not x contains '_id_'] as `NEW RUNSCRIPT CLASSES`
WITH ch, ASSIGN_CLASSES, `CLASSES AFTER RUNSCRIPT`, [x IN `NEW RUNSCRIPT CLASSES` WHERE NOT x contains 'index'] as `NEW RUNSCRIPT CLASSES`
WITH ASSIGN_CLASSES, `CLASSES AFTER RUNSCRIPT`, [x IN `NEW RUNSCRIPT CLASSES` WHERE NOT x IN ch.cols_before] as `NEW RUNSCRIPT CLASSES`
RETURN ASSIGN_CLASSES, `CLASSES AFTER RUNSCRIPT`, `NEW RUNSCRIPT CLASSES`, apoc.coll.union(ASSIGN_CLASSES, `NEW RUNSCRIPT CLASSES`) as `PREDICTED CLASSES`