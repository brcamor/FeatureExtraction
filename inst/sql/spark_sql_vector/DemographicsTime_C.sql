SELECT covariate_id,
{@sub_type == 'priorObservation'} ? {
	CAST('observation time (days) prior to index' AS VARCHAR(512)) AS covariate_name,
} 
{@sub_type == 'postObservation'} ? {
	CAST('observation time (days) after index' AS VARCHAR(512)) AS covariate_name,
} 
{@sub_type == 'inCohort'} ? {
	CAST('time (days) between cohort start and end' AS VARCHAR(512)) AS covariate_name,
}
	@analysis_id AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM @covariate_table
	) t1