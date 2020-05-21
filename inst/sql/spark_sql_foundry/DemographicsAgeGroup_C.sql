-- Reference construction
SELECT covariate_id,
	CAST(CONCAT (
		'age group: ',
		RIGHT(CONCAT('00', CAST(5 * (covariate_id - @analysis_id) / 1000 AS INT)), 2),
		'-',
		RIGHT(CONCAT('00', CAST((5 * (covariate_id - @analysis_id) / 1000) + 4 AS INT)), 2)
		) AS VARCHAR(512)) AS covariate_name,
	@analysis_id AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM `@output_path/@covariate_table`
	) t1
