library(devtools)
#install_github("ohdsi/SqlRender") # these should already be installed so no need
# install_github("ohdsi/DatabaseConnector")  # these should already be installed so no need

# this installs the package from github
devtools::install_github("hc3292/HowOften2023")

library("devtools")
library("SqlRender")
library("DatabaseConnector")
library("HowOften2023")

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

# save it as a file called targetRef.csv
