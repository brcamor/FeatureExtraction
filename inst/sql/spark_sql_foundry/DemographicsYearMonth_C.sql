SELECT covariate_id,
	CAST(CONCAT('index year and month: ', CAST((covariate_id - @analysis_id) / 1000 AS INT)) AS VARCHAR(512)) AS covariate_name,
	@analysis_id AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM `@output_path/@covariate_table`
	) t1