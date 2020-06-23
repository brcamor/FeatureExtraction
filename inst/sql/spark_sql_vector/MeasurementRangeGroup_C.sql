SELECT covariate_id,
{@temporal} ? {
	CAST(CONCAT('measurement ', range_name, ': ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
} : {
{@start_day == 'anyTimePrior'} ? {
	CAST(CONCAT('measurement ', range_name, ' during any time prior through @end_day days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
} : {
	CAST(CONCAT('measurement ', range_name, ' during day @start_day through @end_day days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
}
}
	@analysis_id AS analysis_id,
	CAST(FLOOR(covariate_id / 10000.0) AS INT) AS concept_id
FROM (
	SELECT DISTINCT covariate_id,
	   CASE 
			WHEN FLOOR(covariate_id / 1000.0) - (FLOOR(covariate_id / 10000.0) * 10) = 1 THEN 'below normal range'
			WHEN FLOOR(covariate_id / 1000.0) - (FLOOR(covariate_id / 10000.0) * 10) = 2 THEN 'within normal range'
			WHEN FLOOR(covariate_id / 1000.0) - (FLOOR(covariate_id / 10000.0) * 10) = 3 THEN 'above normal range'
	  END AS range_name
	FROM @covariate_table
	) t1
LEFT JOIN concept
	ON concept_id = FLOOR(covariate_id / 10000.0)