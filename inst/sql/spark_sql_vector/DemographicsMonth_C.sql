SELECT covariate_id,
	CAST(CONCAT ('index month: ', CAST((covariate_id - @analysis_id) / 1000 AS INT)) AS VARCHAR(512)) AS covariate_name,
	@analysis_id AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM @covariate_table
	) t1