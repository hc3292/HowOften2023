library("HowOften2023")
library("devtools")
library("SqlRender")
library("DatabaseConnector")
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
cohortSqlFiles = list.files(path = system.file("/sql/sql_server/target", package = "HowOften2023"), full.names = TRUE)

for (i in 1:length(cohortSqlFiles)) {
  cohortSqlFileName <- cohortSqlFiles[i]
  cohortName <- tools::file_path_sans_ext(basename(cohortSqlFileName))
  cohortSql <- readChar(cohortSqlFileName, file.info(cohortSqlFileName)$size)
  targetsToCreate <- rbind(targetsToCreate, data.frame(cohortId = i,
                                                       cohortName = cohortName,
                                                       sql = cohortSql,
                                                       stringsAsFactors = FALSE))
}

write.csv(targetsToCreate, "~/HowOften2023/inst/settings/targetsCreated.csv", quote = F, row.names = F)


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

# be careful with this! THIS ERASES OLD RESULTS
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
                                                       cohortDefinitionSet = targetsToCreate)

# Get the cohort counts
# cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,
#                                                 cohortDatabaseSchema = cohortDatabaseSchema,
#                                                 cohortTable = cohortTableNames$cohortTable)
#print(cohortCounts)

# % of cohorts that have cells > 5
#nrow(cohortCounts[which(cohortCounts$cohortEntries>5), ]) / nrow(cohortCounts)


# Optional: drop cohort statistics tables from the database
CohortGenerator::dropCohortStatsTables(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames
)


# for outcome generation see OutcomeGeneration.R
