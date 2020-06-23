SELECT covariate_id,
	CAST(CONCAT('gender = ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
	@analysis_id AS analysis_id,
	 CAST((covariate_id - @analysis_id) / 1000 AS INT) AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM `@output_path/@covariate_table`
	) t1
LEFT JOIN `@vocab_path/concept`
	ON concept_id = CAST((covariate_id - @analysis_id) / 1000 AS INT)
