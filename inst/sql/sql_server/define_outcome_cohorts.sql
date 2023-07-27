--define set of concepts to be used as eligble concepts for the adverse reactions

IF OBJECT_ID('#concept_anc_group', 'U') IS NOT NULL
	drop table #concept_anc_group;

--create a temp table of concepts to aggregate to:
--create table #concept_anc_group  as
select ca1.ancestor_concept_id, ca1.descendant_concept_id
into #concept_anc_group
from @cdm_database_schema.concept_ancestor ca1
inner join
(
select c1.concept_id, c1.concept_name, c1.vocabulary_id, c1.domain_id
from @cdm_database_schema.concept c1
inner join @cdm_database_schema.concept_ancestor ca1
on ca1.ancestor_concept_id = 441840 /* clinical finding */
and c1.concept_id = ca1.descendant_concept_id
where c1.concept_name not like '%finding'
and c1.concept_name not like 'disorder of%'
and c1.concept_name not like 'finding of%'
and c1.concept_name not like 'finding related to%'
and c1.concept_name not like 'disease of%'
and c1.concept_name not like 'injury of%'
and c1.concept_name not like '%by site'
and c1.concept_name not like '%by body site'
and c1.concept_name not like '%by mechanism'
and c1.concept_name not like '%of body region'
and c1.concept_name not like '%of anatomical site'
and c1.concept_name not like '%of specific body structure%'
and c1.concept_name not in ('Disease','Clinical history and observation findings','General finding of soft tissue','Traumatic AND/OR non-traumatic injury','Drug-related disorder',
	'Traumatic injury', 'Mass of body structure','Soft tissue lesion','Neoplasm and/or hamartoma','Inflammatory disorder','Congenital disease','Inflammation of specific body systems','Disorder due to infection',
	'Musculoskeletal and connective tissue disorder','Inflammation of specific body organs','Complication','Finding by method','General finding of observation of patient',
	'O/E - specified examination findings','Skin or mucosa lesion','Skin lesion',	'Complication of procedure', 'Mass of trunk','Mass in head or neck', 'Mass of soft tissue','Bone injury','Head and neck injury',
	'Acute disease','Chronic disease', 'Lesion of skin and/or skin-associated mucous membrane')
and c1.domain_id = 'Condition'
) t1
on ca1.ancestor_concept_id = t1.concept_id
;