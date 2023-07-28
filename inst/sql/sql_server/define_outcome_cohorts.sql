drop table if exists @results_database_schema.IR_cohort_definition;
create table @results_database_schema.IR_cohort_definition
(
  how_often_id int IDENTITY(1,1),
	cohort_definition_id bigint,
	cohort_name varchar(500),
	concept_id bigint,
	cohort_type int,  --0: exposure, 1: outcome with 1st diagnosis; 2: outcome with 1st diagnosis + hospital
)
;




-- OUTCOMES
--define set of concepts to be used as eligble concepts
IF OBJECT_ID('#concept_anc_group', 'U') IS NOT NULL
	drop table #concept_anc_group;

--drop table #concept_anc_group;

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


--select count(*) from #concept_anc_group;


--outcome cohorts 1:  first diagnosis of any sort
IF OBJECT_ID('#outcome_cohort_1', 'U') IS NOT NULL
	drop table #outcome_cohort_1;


-- drop table #outcome_cohort_1;


--create table #outcome_cohort_1  as
select t1.person_id as subject_id, cast(t1.ancestor_concept_id as bigint)*100+1 as cohort_definition_id, t1.cohort_start_date, t1.cohort_start_date as cohort_end_date
into #outcome_cohort_1
from
(
select co1.person_id, ca1.ancestor_concept_id, min(co1.condition_start_date) as cohort_start_date
from @cdm_database_schema.condition_occurrence co1
inner join #concept_anc_group ca1
on co1.condition_concept_id = ca1.descendant_concept_id
group by co1.person_id, ca1.ancestor_concept_id
) t1
;


--outcome cohorts 2:  first diagnosis of a condition that is observed at hospital at some point
IF OBJECT_ID('#outcome_cohort_2', 'U') IS NOT NULL
	drop table #outcome_cohort_2;

-- drop table #outcome_cohort_2;
--create table #outcome_cohort_2  as
select t1.person_id as subject_id, cast(t1.ancestor_concept_id as bigint)*100+2 as cohort_definition_id, t1.cohort_start_date, t1.cohort_start_date as cohort_end_date
into #outcome_cohort_2
from
(
select co1.person_id, ca1.ancestor_concept_id, min(co1.condition_start_date) as cohort_start_date
from @cdm_database_schema.condition_occurrence co1
inner join #concept_anc_group ca1
on co1.condition_concept_id = ca1.descendant_concept_id
group by co1.person_id, ca1.ancestor_concept_id
) t1
inner join
(
select co1.person_id, ca1.ancestor_concept_id, min(vo1.visit_start_date) as cohort_start_date
from @cdm_database_schema.condition_occurrence co1
inner join @cdm_database_schema.visit_occurrence vo1
on co1.person_Id = vo1.person_id
and co1.visit_occurrence_id = vo1.visit_occurrence_id
and visit_concept_id = 9201
inner join #concept_anc_group ca1
on co1.condition_concept_id = ca1.descendant_concept_id
group by co1.person_id, ca1.ancestor_concept_id
) t2
on t1.person_id = t2.person_id
and t1.ancestor_concept_id = t2.ancestor_concept_id
;


--outcome cohorts:  combine both types together
IF OBJECT_ID('@results_database_schema.IR_outcome_cohort', 'U') IS NOT NULL
	drop table @results_database_schema.IR_outcome_cohort;

--create table #outcome_cohort  as
select t1.* into @results_database_schema.IR_outcome_cohort
from
(
select subject_id, cohort_definition_id, cohort_start_date, cohort_end_date
from #outcome_cohort_1

union

select subject_id, cohort_definition_id, cohort_start_date, cohort_end_date
from #outcome_cohort_2
) t1
;


insert into @results_database_schema.IR_cohort_definition (cohort_definition_id, cohort_name, concept_id, cohort_type)
select e1.cohort_definition_id, case when right(e1.cohort_definition_id, 1) = '1' then 'First diagnosis of: ' when right(e1.cohort_definition_id, 1) = '2' then 'First diagnosis and >=1 hospitalization with: ' else 'Other type of: ' end + c1.concept_name as cohort_name, c1.concept_id, right(e1.cohort_definition_id, 1) as cohort_type
from
(
select distinct cohort_definition_id
from @results_database_schema.IR_outcome_cohort
) e1
inner join @cdm_database_schema.concept c1
on left(e1.cohort_definition_id, len(e1.cohort_definition_id)-2) = c1.concept_id
;
