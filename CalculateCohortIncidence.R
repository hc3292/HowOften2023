## CALCULATE COHORT INCIDENCE

library("devtools")
library("SqlRender")
library("DatabaseConnector")
library("CohortGenerator")
library("CohortIncidence")

setwd("~/HowOften2023")

cohortDatabaseSchema = "ohdsi_cumc_2022q4r1.results"
cdmDatabaseSchema = "ohdsi_cumc_2022q4r1.dbo"
resultsDatabaseSchema = "ohdsi_cumc_2022q4r1.results"

connectionDetails <- createConnectionDetails(dbms= "sql server",
                                             server = "gem.dbmi.columbia.edu",
                                             user = "hc3292",
                                             password = keyring::key_get("sql server", "hc3292"),
                                             port="1433")

targetCohorts <- readCsv("inst/settings/targetsCreated.csv")
# subgroupCohorts <- readCsv("inst/settings/subgroupRef.csv") # ignore for now
outcomeCohorts <- readCsv("inst/settings/outcomesCreated.csv")
timeAtRisk <- readCsv("inst/settings/timeAtRisk.csv")

# for target cohort
cohortTable = "HowOften_cohort_table"

# for outcome cohort
outcomeTable = "HowOften_outcome_table"

# these IDs really only have what we want (i.e. the cohorts that are created)
targetIDs = targetCohorts$cohortId
outcomeIDs = outcomeCohorts$cohortId
tarIDs = c(1) # for now just 1 and 2; 1 = 30 days TAR and 2 = 60 days TAR


# target ref
target_ref_list = mapply(createCohortRef, id=targetCohorts$cohortId, name=targetCohorts$cohortName)

# outcome ref
outcome_ref_list = mapply(createOutcomeDef,
                         id=outcomeCohorts$cohortId,
                         name=outcomeCohorts$cohortName,
                         cohortId = outcomeCohorts$cohortId,
                         cleanWindow = 0)

# tar
tar_ref_list = list()
for (i in 1:length(tarIDs)){
  selectedrow = timeAtRisk[which(timeAtRisk$timeAtRiskId==tarIDs[i]),]
  t = CohortIncidence::createTimeAtRiskDef(id=selectedrow$timeAtRiskId,
                                           startWith= "start",
                                           startOffset = selectedrow$timeAtRiskStartOffset,
                                           endWith= "end",
                                           endOffset=selectedrow$timeAtRiskEndOffset)
  tar_ref_list = append(tar_ref_list, t)
}




###################################################################################
###################################################################################
################## Run cohort incidence ###########################################
###################################################################################
###################################################################################


# drop table if something is already there--BE CAREFUL
# drop_IR_table_sql = render("drop table @a;", a = "ohdsi_cumc_2022q4r1.dbo.incidence_summary")
# drop_IR_table = querySql(
#   connection = connect(connectionDetails),
#   sql = drop_IR_table_sql,
#   snakeCaseToCamelCase = FALSE
# )

# create incidence table--only need to run this once otherwise it bugs out!
ddl <- CohortIncidence::getResultsDdl()
ddl <- SqlRender::render(CohortIncidence::getResultsDdl(), schemaName = resultsDatabaseSchema)

buildOptions <- CohortIncidence::buildOptions(cohortTable = paste0(resultsDatabaseSchema, '.', cohortTable),
                                              outcomeCohortTable = paste0(resultsDatabaseSchema, '.', outcomeTable),
                                              sourceName = "strata_test",
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              resultsDatabaseSchema = resultsDatabaseSchema,
                                              vocabularySchema = cdmDatabaseSchema,
                                              useTempTables = F,
                                              refId = 1)


############################
#### run data 100 at a time
############################


# Takes in a subset of adverse effects (100 rows at a time for outcomeCohorts)
generateIRdesign <- function(targetCohorts_subset, outcomeCohorts_subset) {

  outcomeIDs = outcomeCohorts_subset
  targetIDs = targetCohorts_subset

  # define analysis
  analysis1 <- CohortIncidence::createIncidenceAnalysis(targets = targetIDs,
                                                        outcomes = outcomeIDs,
                                                        tars = tarIDs)

  # Create Design (note use of list() here):
  irDesign <- CohortIncidence::createIncidenceDesign(targetDefs = target_ref_list,
                                                     outcomeDefs = outcome_ref_list,
                                                     tars=tar_ref_list,
                                                     analysisList = list(analysis1),
                                                     strataSettings = CohortIncidence::createStrataSettings(byGender=T, byAge=T, ageBreaks = c(17,34,65)))

  # run result and insert into database
  analysisSql <- CohortIncidence::buildQuery(incidenceDesign =  as.character(irDesign$asJSON()),
                                             buildOptions = buildOptions)
  con <- DatabaseConnector::connect(connectionDetails)
  DatabaseConnector::executeSql(con, analysisSql)
}

# Assuming your big dataframe is named 'outcomeIDs'
# Determine the number of rows in the dataframe
num_outcomes <- length(outcomeIDs)

# Set the chunk size to 100
chunk_size <- 100

# Calculate the number of chunks needed
num_chunks <- ceiling(num_outcomes / chunk_size)
num_targets = 10 # length(targetIDs)

# Loop through the chunks and execute the function on each chunk
generated = data.frame()

for(j in 1:num_targets){
  for (i in 1:num_chunks) {
    # Calculate the starting and ending row index for the current chunk
    start <- (i - 1) * chunk_size + 1
    end <- min(i * chunk_size, num_outcomes)

    # Extract the current chunk
    current_outcome_chunk <- outcomeIDs[start:end]

    # Call the function to process the current chunk
    generateIRdesign(targetIDs[j], current_outcome_chunk)
    gen = cbind(rep(targetIDs[j], chunk_size), current_outcome_chunk)
    generated = rbind(generated, gen)
  }
}

# this saves all the IDs of the drugs and outcomes we've generated
write.csv(generated, "generatedCI.csv")

