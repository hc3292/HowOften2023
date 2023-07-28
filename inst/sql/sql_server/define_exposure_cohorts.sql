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

--all exposure cohorts:  new users of drugs, newly diagnosed, 1yr washout
IF OBJECT_ID('@results_database_schema.IR_exposure_cohort', 'U') IS NOT NULL
	drop table @results_database_schema.IR_exposure_cohort;

--create table #exposure_cohort  as
select de1.person_id as subject_id, de1.cohort_definition_id, de1.cohort_start_date, de1.cohort_end_date, op1.observation_period_end_date
into @results_database_schema.IR_exposure_cohort
from
(select person_id, drug_concept_id as cohort_definition_id, drug_era_start_date as cohort_start_date, drug_era_end_date as cohort_end_date, row_number() over (partition by person_id, drug_concept_id order by drug_era_start_date asc) rn1
from @cdm_database_schema.drug_era
where drug_concept_id > 0
--and drug_concept_id in (select descendant_concept_id from concept_ancestor where ancestor_concept_id = 1308216)
) de1
inner join @cdm_database_schema.observation_period op1
on de1.person_id = op1.person_id
and de1.cohort_start_date >= dateadd(dd,365,op1.observation_period_start_date)
and de1.cohort_start_date <= op1.observation_period_end_date
and de1.rn1 = 1
;

insert into @results_database_schema.IR_cohort_definition (cohort_definition_id, cohort_name, concept_id, cohort_type)
select e1.cohort_definition_id, c1.concept_name as cohort_name, c1.concept_id, 0 as cohort_type
from
(
select distinct cohort_definition_id
from @results_database_schema.IR_exposure_cohort
) e1
inner join @cdm_database_schema.concept c1
on e1.cohort_definition_id = c1.concept_id
;
