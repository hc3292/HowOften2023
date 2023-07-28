library(devtools)
#install_github("ohdsi/SqlRender") # these should already be installed so no need
# install_github("ohdsi/DatabaseConnector")  # these should already be installed so no need

# this installs the package from github
devtools::install_github("hc3292/HowOften2023")

library("devtools")
library("SqlRender")
library("DatabaseConnector")
library("HowOften2023")
library("CohortGenerator")

connectionDetails <- createConnectionDetails(dbms= "sql server",
                                             server = "gem.dbmi.columbia.edu",
                                             user = "hc3292",
                                             password = keyring::key_get("sql server", "hc3292"),
                                             port="1433")

###################################################################################
###################################################################################
################## CREATE TABLE OF SQL FILE OF DRUG EXPOSURES #####################
###################################################################################
###################################################################################

rx_norms_list_sql = SqlRender::loadRenderTranslateSql(
  "define_exposure_cohorts.sql",
  "HowOften2023",
  dbms = "sql server",
  results_database_schema = "ohdsi_cumc_2022q4r1.results",
  cdm_database_schema = "ohdsi_cumc_2022q4r1.dbo"
)

DatabaseConnector::disconnect(con)
con <- DatabaseConnector::connect(connectionDetails)
# this creates the IR_exposure_cohort table
DatabaseConnector::executeSql(con,rx_norms_list_sql)

# returns the exposure cohort into a dataframe
exposure_cohort_sql = render("SELECT COHORT_DEFINITION_ID, COHORT_NAME FROM @a;", a = "ohdsi_cumc_2022q4r1.results.IR_cohort_definition")
exposure_cohort = querySql(
  connection = connect(connectionDetails),
  sql = exposure_cohort_sql,
  snakeCaseToCamelCase = FALSE
)

# clean up the table (underscores and colons will not work in the create sql function)
replace_non_alphanumeric_with_underscore <- function(column_data) {
  cleaned_column <- gsub("[^[:alnum:]]", "_", column_data)
  return(cleaned_column)
}
# Apply the function to the column with non-alphanumeric characters
exposure_cohort$COHORT_NAME <- replace_non_alphanumeric_with_underscore(exposure_cohort$COHORT_NAME)

# create the sql files for the drugs
result_vector <- mapply(create_exposure_sql, exposure_cohort$COHORT_DEFINITION_ID, exposure_cohort$COHORT_NAME)

# append the names of the sql files to the exposure_cohort table
exposure_cohort$fileName = paste0(replace_non_alphanumeric_with_underscore(exposure_cohort$COHORT_NAME), ".sql")
# generate a HowOftenID
exposure_cohort$cohortId = c(1:nrow(exposure_cohort))
# generate cohortName column
exposure_cohort$cohortName = exposure_cohort$COHORT_NAME

# save it as a file called targetRef.csv
write.csv(exposure_cohort, "~/HowOften2023/inst/settings/targetRef.csv", quote = F, row.names = F)

###################################################################################
###################################################################################
################## CREATE TABLE OF SQL FILE OF DRUG OUTCOMES  #####################
###################################################################################
###################################################################################

outcomes_list_sql = SqlRender::loadRenderTranslateSql(
  "define_outcome_cohorts.sql",
  "HowOften2023",
  dbms = "sql server",
  results_database_schema = "ohdsi_cumc_2022q4r1.results",
  cdm_database_schema = "ohdsi_cumc_2022q4r1.dbo"
)

DatabaseConnector::disconnect(con)
con <- DatabaseConnector::connect(connectionDetails)
# this creates the IR_exposure_cohort table
DatabaseConnector::executeSql(con,outcomes_list_sql)

# returns the exposure cohort into a dataframe
outcome_cohort_sql = render("SELECT CONCEPT_ID, COHORT_NAME, COHORT_TYPE FROM @a;", a = "ohdsi_cumc_2022q4r1.results.IR_cohort_definition")
outcome_cohort = querySql(
  connection = connect(connectionDetails),
  sql = outcome_cohort_sql,
  snakeCaseToCamelCase = FALSE
)

outcome_first_dx = outcome_cohort[which(outcome_cohort$COHORT_TYPE == 1), ]

# Apply the function to the column with non-alphanumeric characters
outcome_first_dx$COHORT_NAME <- replace_non_alphanumeric_with_underscore(outcome_first_dx$COHORT_NAME)

# create the sql files for the drugs
result_vector <- mapply(create_outcome_sql, outcome_first_dx$CONCEPT_ID, outcome_first_dx$COHORT_NAME)

# append the names of the sql files to the exposure_cohort table
outcome_first_dx$fileName = paste0(replace_non_alphanumeric_with_underscore(outcome_first_dx$COHORT_NAME), ".sql")
# generate a HowOftenID
outcome_first_dx$cohortId = c( (nrow(exposure_cohort) + 1): ((nrow(exposure_cohort) + nrow(outcome_first_dx))))
# generate cohortName column
outcome_first_dx$cohortName = outcome_first_dx$COHORT_NAME
outcome_first_dx$outcomeCohortDefinitionId = outcome_first_dx$cohortId
outcome_first_dx$cleanWindow = 0 #idk if we need this
outcome_first_dx$primaryTimeAtRiskStartOffset = 1
outcome_first_dx$primaryTimeAtRiskStartIndex = 0
outcome_first_dx$primaryTimeAtRiskEndOffset = 30
outcome_first_dx$primaryTimeAtRiskEndIndex = 0


# save it as a file called targetRef.csv
write.csv(outcome_first_dx, "~/HowOften2023/inst/settings/outcomeRef.csv", quote = F, row.names = F)



###################################################################################
###################################################################################
################## GENERATE TIME AT RISK CSV  #####################################
###################################################################################
###################################################################################

# set up time at risk
timeAtRisk = data.frame(rbind(c(1,0,0,30,0), c(2,0,0,365,0)))
colnames(timeAtRisk) = c("time_at_risk_id", "time_at_risk_start_offset",
                         "time_at_risk_start_index",
                         "time_at_risk_end_offset", "time_at_risk_end_index")

write.csv(timeAtRisk, "~/HowOften2023/inst/settings/timeAtRisk.csv", quote = F, row.names = F)


###################################################################################
###################################################################################
################## INSTANTIATE COHORTS  ###########################################
###################################################################################
###################################################################################

## create reference tables
# the table names start with HowOften_[etcetc]

cohortTablePrefix = "HowOften"
targetCohortTable = paste0(cohortTablePrefix, "_target")
targetRefTable = paste0(cohortTablePrefix, "_target_ref")
subgroupCohortTable = paste0(cohortTablePrefix, "_subgroup")
subgroupRefTable = paste0(cohortTablePrefix, "_subgroup_ref")
outcomeCohortTable = paste0(cohortTablePrefix, "_outcome")
outcomeRefTable = paste0(cohortTablePrefix, "_outcome_ref")
timeAtRiskTable = paste0(cohortTablePrefix, "_time_at_risk")
summaryTable = paste0(cohortTablePrefix, "_ir_summary")

createRefTablesSql <- SqlRender::loadRenderTranslateSql("CreateRefTables.sql",
                                                        packageName = "Covid19VaccineAesiIncidenceCharacterization",
                                                        dbms = "sql server",
                                                        tempEmulationSchema = NULL,
                                                        warnOnMissingParameters = TRUE,
                                                        cohort_database_schema = "ohdsi_cumc_2022q4r1.dbo",
                                                        summary_table = summaryTable,
                                                        target_ref_table = targetRefTable,
                                                        subgroup_ref_table = subgroupRefTable,
                                                        outcome_ref_table = outcomeRefTable,
                                                        time_at_risk_table = timeAtRiskTable
)
DatabaseConnector::executeSql(connection = connect(connectionDetails),
                              sql = createRefTablesSql,
                              progressBar = TRUE,
                              reportOverallTime = TRUE)


## instantiate cohorts

targetCohorts <- readCsv("inst/settings/targetRef.csv")
# subgroupCohorts <- readCsv("settings/subgroupRef.csv") # ignore for now
outcomeCohorts <- readCsv("inst/settings/outcomeRef.csv")
timeAtRisk <- readCsv("inst/settings/timeAtRisk.csv")

instantiatedTargetCohortIds <- c()

incremental = TRUE
incrementalFolder = "incrementalFolder"
cohortDatabaseSchema = "ohdsi_cumc_2022q4r1.dbo"
cdmDatabaseSchema = "ohdsi_cumc_2022q4r1.dbo"
tempEmulationSchema = "ohdsi_cumc_2022q4r1.dbo"
connection = connect(connectionDetails)


instantiatedTargetCohortIds <- instantiateCohortSet(connectionDetails = connectionDetails,
                                                    connection = connect(connectionDetails),
                                                    cdmDatabaseSchema = cdmDatabaseSchema,
                                                    tempEmulationSchema = tempEmulationSchema,
                                                    cohortDatabaseSchema = cohortDatabaseSchema,
                                                    cohortTable = targetCohortTable,
                                                    cohorts = targetCohorts,
                                                    cohortSqlFolder = "target",
                                                    createCohortTable = TRUE,
                                                    incremental = incremental,
                                                    incrementalFolder = incrementalFolder)
