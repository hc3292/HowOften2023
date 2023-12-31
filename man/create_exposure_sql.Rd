% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/create_exposure_sql.R
\name{create_exposure_sql}
\alias{create_exposure_sql}
\title{Create Sql Code for exposure variables}
\usage{
create_exposure_sql(
  concept_id,
  concept_name,
  filepath = "~/HowOften2023/inst/sql/sql_server/target/"
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
}
\examples{
create_exposure_sql(8941340, "testdrug")
}
