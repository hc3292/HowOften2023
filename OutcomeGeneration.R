# this is a script to run for the outcome cohort generation

library("devtools")
library("SqlRender")
library("DatabaseConnector")
library("CohortGenerator")
connectionDetails <- createConnectionDetails(dbms= "sql server",
                                             server = "gem.dbmi.columbia.edu",
                                             user = "hc3292",
                                             password = keyring::key_get("sql server", "hc3292"),
                                             port="1433")

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

for (i in 1:1000) {
  cohortSqlFileName <- cohortSqlFiles[i]
  cohortName <- tools::file_path_sans_ext(basename(cohortSqlFileName))
  cohortSql <- readChar(cohortSqlFileName, file.info(cohortSqlFileName)$size)
  outcomesToCreate <- rbind(outcomesToCreate, data.frame(cohortId = i + 2674,
                                                         cohortName = cohortName,
                                                         sql = cohortSql,
                                                         stringsAsFactors = FALSE))
}

write.csv(targetsToCreate, "~/HowOften2023/inst/settings/outcomesCreated.csv", quote = F, row.names = F)


# cohortsGenerated contains a list of the cohortIds
# successfully generated against the CDM
connection = connect(connectionDetails)

# Create the cohort tables to hold the cohort generation results
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "HowOften_outcome_table")

cohortDatabaseSchema = "ohdsi_cumc_2022q4r1.results"
cdmDatabaseSchema = "ohdsi_cumc_2022q4r1.dbo"

# be careful with this! THIS ERASES OLD RESULTS
CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                    cohortDatabaseSchema = cohortDatabaseSchema,
                                    cohortTableNames = cohortTableNames)

# Generate the cohorts
cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                                       cohortTableNames = cohortTableNames,
                                                       cohortDefinitionSet = outcomesToCreate,
                                                       incremental = TRUE,
                                                       incrementalFolder = file.path("incrementalFolder", "OutcomeCohorts"))

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
