SELECT covariate_id,
	CAST('Diabetes Comorbidity Severity Index (DCSI)' AS VARCHAR(512)) AS covariate_name,
	@analysis_id AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM @covariate_table
	) t1