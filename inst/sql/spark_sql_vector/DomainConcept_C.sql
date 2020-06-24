SELECT covariate_id,
{@temporal} ? {
	CAST(CONCAT('@domain_table: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END {@sub_type == 'inpatient'} ? {, ' (inpatient)'}) AS VARCHAR(512)) AS covariate_name,
} : {
{@start_day == 'anyTimePrior'} ? {
	CAST(CONCAT('@domain_table any time prior through @end_day days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END {@sub_type == 'inpatient'} ? {, ' (inpatient)'}) AS VARCHAR(512)) AS covariate_name,
} : {
	CAST(CONCAT('@domain_table during day @start_day through @end_day days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END {@sub_type == 'inpatient'} ? {, ' (inpatient)'}) AS VARCHAR(512)) AS covariate_name,
}
}
	@analysis_id AS analysis_id,
	CAST((covariate_id - @analysis_id) / 1000 AS INT) AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM @covariate_table
	) t1
LEFT JOIN concept
	ON concept_id = CAST((covariate_id - @analysis_id) / 1000 AS INT)
