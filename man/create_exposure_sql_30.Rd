% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/create_exposure_30_days_sql.R
\name{create_exposure_sql_30}
\alias{create_exposure_sql_30}
\title{Create Sql Code for exposure variables
Event Persistence: 30 days from start}
\usage{
create_exposure_sql_30(
  concept_id,
  concept_name,
  filepath = "~/HowOften2023/inst/sql/sql_server/target_30days/"
)
}
\arguments{
\item{concept_id}{integer, RxNorm ID}

\item{concept_name}{string, name of the RxNormID}

\item{filepath}{string, filepath of where you want the sql files to be}
}
\value{
SQL file that can be run to generate cohorts
}
\description{
Create Sql Code for exposure variables
Event Persistence: 30 days from start
}
\examples{
create_exposure_sql(8941340, "testdrug")
}
