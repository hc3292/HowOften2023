# this sets up the Sql files for the concept IDs we have to run

connectionDetails <- createConnectionDetails(dbms= "sql server",
                                             server = "gem.dbmi.columbia.edu",
                                             user = "hc3292",
                                             password = keyring::key_get("sql server", "hc3292"),
                                             port="1433")

# for now we will just do the 30 days
# this pulls the RxNorm IDs
rx_norms_list_sql = SqlRender::loadRenderTranslateSql(
  "define_exposure_cohorts.sql",
  getThisPackageName(),
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


# Apply the function to the column with non-alphanumeric characters
exposure_cohort$COHORT_NAME <- replace_non_alphanumeric_with_underscore(exposure_cohort$COHORT_NAME)

# create the sql files for the drugs
result_vector <- mapply(create_exposure_sql_30, exposure_cohort$COHORT_DEFINITION_ID, exposure_cohort$COHORT_NAME)

# append the names of the sql files to the exposure_cohort table
exposure_cohort$fileName = paste0(replace_non_alphanumeric_with_underscore(exposure_cohort$COHORT_NAME), ".sql")
# generate a HowOftenID
exposure_cohort$cohortId = c(1:nrow(exposure_cohort))
# generate cohortName column
exposure_cohort$cohortName = exposure_cohort$COHORT_NAME

# save it as a file called targetRef.csv
write.csv(exposure_cohort, "~/HowOften2023/inst/settings/targetRef_30days.csv", quote = F, row.names = F)


library("CohortGenerator")
###################################################################################
###################################################################################
################## TARGET COHORT  #################################################
###################################################################################
###################################################################################

# First construct a cohort definition set: an empty
# data frame with the cohorts to generate
targetsToCreate <- CohortGenerator::createEmptyCohortDefinitionSet()

# Fill the cohort set using  cohorts included in this
# package as an example
cohortSqlFiles = list.files(path = system.file("/sql/sql_server/target_30days", package = "HowOften2023"), full.names = TRUE)

for (i in 1:length(cohortSqlFiles)) {
  cohortSqlFileName <- cohortSqlFiles[i]
  cohortName <- tools::file_path_sans_ext(basename(cohortSqlFileName))
  cohortSql <- readChar(cohortSqlFileName, file.info(cohortSqlFileName)$size)
  targetsToCreate <- rbind(targetsToCreate, data.frame(cohortId = i,
                                                       cohortName = cohortName,
                                                       sql = cohortSql,
                                                       stringsAsFactors = FALSE))
}

# cohortsGenerated contains a list of the cohortIds
# successfully generated against the CDM
connectionDetails <- createConnectionDetails(dbms= "sql server",
                                             server = "gem.dbmi.columbia.edu",
                                             user = "hc3292",
                                             password = keyring::key_get("sql server", "hc3292"),
                                             port="1433")
connection = connect(connectionDetails)

# Create the cohort tables to hold the cohort generation results
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "HowOften_cohort_table")

cohortDatabaseSchema = "ohdsi_cumc_2022q4r1.results"
cdmDatabaseSchema = "ohdsi_cumc_2022q4r1.dbo"

# need to do this just once or else it wipes out all our tables lol
# CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
#                                     cohortDatabaseSchema = cohortDatabaseSchema,
#                                     cohortTableNames = cohortTableNames)

# Connecting using SQL Server driver
# Creating cohort tables
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_cohort_table
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_cohort_table_inclusion
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_cohort_table_inclusion_result
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_cohort_table_inclusion_stats
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_cohort_table_summary_stats
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_cohort_table_censor_stats
# Creating cohort tables took 1.15secs

# Generate the cohorts
cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                                       cohortTableNames = cohortTableNames,
                                                       cohortDefinitionSet = targetsToCreate,
                                                       incremental = TRUE,
                                                       incrementalFolder = "incrementalFolder")

# Get the cohort counts
cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,
                                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                                 cohortTable = cohortTableNames$cohortTable)
print(cohortCounts)

# % of cohorts that have cells > 5
nrow(cohortCounts[which(cohortCounts$cohortEntries>5), ]) / nrow(cohortCounts)


###################################################################################
###################################################################################
################## OUTCOME COHORT  ################################################
###################################################################################
###################################################################################

# First construct a cohort definition set: an empty
# data frame with the cohorts to generate
outcomesToCreate <- CohortGenerator::createEmptyCohortDefinitionSet()

# Fill the cohort set using  cohorts included in this
# package as an example
cohortSqlFiles = list.files(path = system.file("/sql/sql_server/outcome", package = "HowOften2023"), full.names = TRUE)

for (i in 1:length(cohortSqlFiles)) {
  cohortSqlFileName <- cohortSqlFiles[i]
  cohortName <- tools::file_path_sans_ext(basename(cohortSqlFileName))
  cohortSql <- readChar(cohortSqlFileName, file.info(cohortSqlFileName)$size)
  outcomesToCreate <- rbind(outcomesToCreate, data.frame(cohortId = i + nrow(targetsToCreate),
                                                       cohortName = cohortName,
                                                       sql = cohortSql,
                                                       stringsAsFactors = FALSE))
}

# cohortsGenerated contains a list of the cohortIds
# successfully generated against the CDM
connectionDetails <- createConnectionDetails(dbms= "sql server",
                                             server = "gem.dbmi.columbia.edu",
                                             user = "hc3292",
                                             password = keyring::key_get("sql server", "hc3292"),
                                             port="1433")
connection = connect(connectionDetails)

# Create the cohort tables to hold the cohort generation results
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "HowOften_outcome_table")

cohortDatabaseSchema = "ohdsi_cumc_2022q4r1.results"
cdmDatabaseSchema = "ohdsi_cumc_2022q4r1.dbo"
# need to do this just once or else it wipes out all our tables lol
# CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
#                                     cohortDatabaseSchema = cohortDatabaseSchema,
#                                     cohortTableNames = cohortTableNames)

# Connecting using SQL Server driver
# Creating cohort tables
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_outcome_table
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_outcome_table_inclusion
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_outcome_table_inclusion_result
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_outcome_table_inclusion_stats
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_outcome_table_summary_stats
# - Created table ohdsi_cumc_2022q4r1.results.HowOften_outcome_table_censor_stats
# Creating cohort tables took 0.83secs

# Generate the cohorts
cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                                       cohortTableNames = cohortTableNames,
                                                       cohortDefinitionSet = outcomesToCreate,
                                                       incremental = TRUE,
                                                       incrementalFolder = "incrementalFolder")

# Get the cohort counts
outcomeCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,
                                                  cohortDatabaseSchema = cohortDatabaseSchema,
                                                  cohortTable = cohortTableNames$cohortTable)
print(outcomeCounts)

# % of cohorts that have cells > 5
nrow(outcomeCounts[which(outcomeCounts$cohortEntries>5), ]) / nrow(outcomeCounts)
