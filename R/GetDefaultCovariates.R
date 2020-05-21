# Copyright 2019 Observational Health Data Sciences and Informatics
#
# This file is part of FeatureExtraction
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Get default covariate information from the database
#'
#' @description
#' Constructs a large default set of covariates for one or more cohorts using data in the CDM schema.
#' Includes covariates for all drugs, drug classes, condition, condition classes, procedures,
#' observations, etc.
#'
#' @param covariateSettings      Either an object of type \code{covariateSettings} as created using one
#'                               of the createCovariate functions, or a list of such objects.
#' @param targetDatabaseSchema   (Optional) The name of the database schema where the resulting covariates
#'                               should be stored.
#' @param targetCovariateTable  (Optional) The name of the table where the resulting covariates will
#'                               be stored. If not provided, results will be fetched to R. The table can be 
#'                               a permanent table in the \code{targetDatabaseSchema} or a temp table. If 
#'                               it is a temp table, do not specify \code{targetDatabaseSchema}.
#' @param targetCovariateRefTable (Optional) The name of the table where the covariate reference will be stored.
#' @param targetAnalysisRefTable (Optional) The name of the table where the analysis reference will be stored.
#'
#' @template GetCovarParams
#'
#' @export
getDbDefaultCovariateData <- function(connection,
                                      oracleTempSchema = NULL,
                                      cdmDatabaseSchema,
                                      cohortTable = "#cohort_person",
                                      cohortId = -1,
                                      cdmVersion = "5",
                                      rowIdField = "subject_id",
                                      covariateSettings,
                                      targetDatabaseSchema,
                                      targetCovariateTable,
                                      targetCovariateRefTable,
                                      targetAnalysisRefTable,
                                      aggregated = FALSE) {
  if (!is(covariateSettings, "covariateSettings")) {
    stop("Covariate settings object not of type covariateSettings")
  }
  if (cdmVersion == "4") {
    stop("Common Data Model version 4 is not supported")
  }
  if (!missing(targetCovariateTable) && !is.null(targetCovariateTable) && aggregated) {
    stop("Writing aggregated results to database is currently not supported")
  }
  
  settings <- .toJson(covariateSettings)
  rJava::J("org.ohdsi.featureExtraction.FeatureExtraction")$init(system.file("", package = "FeatureExtraction"))
  json <- rJava::J("org.ohdsi.featureExtraction.FeatureExtraction")$createSql(settings, aggregated, cohortTable, rowIdField, as.integer(cohortId), cdmDatabaseSchema)
  todo <- .fromJson(json)
  
  if (length(todo$tempTables) != 0) {
    writeLines("Sending temp tables to server")
  }
  
  writeLines("Constructing features on server")
  
  sql <- SqlRender::translate(sql = todo$sqlConstruction,
                              targetDialect = attr(connection, "dbms"),
                              oracleTempSchema = oracleTempSchema)
  profile <- (!is.null(getOption("dbProfile")) && getOption("dbProfile") == TRUE)
  print(sql)
  #DatabaseConnector::executeSql(connection, sql, profile = profile)
  
  if (missing(targetCovariateTable) || is.null(targetCovariateTable)) {
    writeLines("Fetching data from server")
    start <- Sys.time()
    # Binary or non-aggregated features
    if (!is.null(todo$sqlQueryFeatures)) {
      sql <- SqlRender::translate(sql = todo$sqlQueryFeatures,
                                  targetDialect = attr(connection, "dbms"),
                                  oracleTempSchema = oracleTempSchema)
      print(sql)
#      covariates <- DatabaseConnector::querySql.ffdf(connection, sql)
    }

    # Continuous aggregated features
    if (!is.null(todo$sqlQueryContinuousFeatures)) {
      sql <- SqlRender::translate(sql = todo$sqlQueryContinuousFeatures,
                                  targetDialect = attr(connection, "dbms"),
                                  oracleTempSchema = oracleTempSchema)
      print(sql)
    }
    
    # Covariate reference
    sql <- SqlRender::translate(sql = todo$sqlQueryFeatureRef,
                                targetDialect = attr(connection, "dbms"),
                                oracleTempSchema = oracleTempSchema)
    print(sql)   

    # Analysis reference
    sql <- SqlRender::translate(sql = todo$sqlQueryAnalysisRef,
                                targetDialect = attr(connection, "dbms"),
                                oracleTempSchema = oracleTempSchema)
    print(sql)

    delta <- Sys.time() - start
    writeLines(paste("Fetching data took", signif(delta, 3), attr(delta, "units")))
  }

  # Drop temp tables
  sql <- SqlRender::translate(sql = todo$sqlCleanup,
                              targetDialect = attr(connection, "dbms"),
                              oracleTempSchema = oracleTempSchema)
  print(sql)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
}
