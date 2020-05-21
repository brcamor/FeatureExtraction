-- Reference construction
SELECT covariate_id,
{@temporal} ? {
{@sub_type == 'distinct'} ? {
	CAST('@domain_table distinct concept count' AS VARCHAR(512)) AS covariate_name,
} : { {@sub_type == 'stratified'} ? {
	CAST(CONCAT('@domain_table concept count: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
} : {
	CAST('@domain_table concept count' AS VARCHAR(512)) AS covariate_name,
}
}
} : {
{@sub_type == 'distinct'} ? {
	CAST('@domain_table distinct concept count during day @start_day through @end_day concept_count relative to index' AS VARCHAR(512)) AS covariate_name,
} : { {@sub_type == 'stratified'} ? {
	CAST(CONCAT('@domain_table concept count during day @start_day through @end_day concept_count relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
} : {
	CAST('@domain_table concept count during day @start_day through @end_day concept_count relative to index' AS VARCHAR(512)) AS covariate_name,
}
}
}
	@analysis_id AS analysis_id,
	0 AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM `@output_path/@covariate_table`
	) t1
{@sub_type == 'stratified'} ? {
LEFT JOIN `@vocab_path/concept`
	ON concept_id = CAST((covariate_id - @analysis_id) / 1000 AS INT)
}