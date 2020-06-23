-- Reference construction
SELECT temp.covariate_id,
{@temporal} ? {
	CAST(CASE WHEN unit_concept.concept_id IS NULL OR unit_concept.concept_id = 0 THEN
		CONCAT('measurement value: ', CASE WHEN measurement_concept.concept_name IS NULL THEN 'Unknown concept' ELSE measurement_concept.concept_name END, ' (Unknown unit)')
	ELSE 	
		CONCAT('measurement value: ',  CASE WHEN measurement_concept.concept_name IS NULL THEN 'Unknown concept' ELSE measurement_concept.concept_name END, ' (', unit_concept.concept_name, ')')
	END AS VARCHAR(512)) AS covariate_name,
} : {
{@start_day == 'anyTimePrior'} ? {
	CAST(CASE WHEN unit_concept.concept_id = 0 THEN
		CONCAT('measurement value during any time prior through @end_day days relative to index: ', measurement_concept.concept_name, ' (Unknown unit)')
	ELSE 	
		CONCAT('measurement value during any time prior through @end_day days relative to index: ', measurement_concept.concept_name, ' (', unit_concept.concept_name, ')')
	END AS VARCHAR(512)) AS covariate_name,

} : {
	CAST(CASE WHEN unit_concept.concept_id = 0 THEN
		CONCAT('measurement value during day @start_day through @end_day days relative to index: ', measurement_concept.concept_name, ' (Unknown unit)')
	ELSE 	
		CONCAT('measurement value during day @start_day through @end_day days relative to index: ', measurement_concept.concept_name, ' (', unit_concept.concept_name, ')')
	END AS VARCHAR(512)) AS covariate_name,
}
}
	@analysis_id AS analysis_id,
	covariate_ids.measurement_concept_id AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM `@output_path/@covariate_table`
	) temp
INNER JOIN (
	SELECT 
		DISTINCT measurement_concept_id,
		unit_concept_id,
		CAST((CAST(measurement_concept_id AS BIGINT) * 1000000) + ((unit_concept_id - (FLOOR(unit_concept_id / 1000) * 1000)) * 1000) + @analysis_id AS BIGINT) AS covariate_id
	FROM `@cdm_database_schema/measurement`
		WHERE value_as_number IS NOT NULL
		{@excluded_concept_table != ''} ? {		AND measurement_concept_id NOT IN (SELECT id FROM `@output_path/@excluded_concept_table`)}
		{@included_concept_table != ''} ? {		AND measurement_concept_id IN (SELECT id FROM `@output_path/@included_concept_table`)}
		{@included_cov_table != ''} ? {		AND CAST((CAST(measurement_concept_id AS BIGINT) * 1000000) + ((unit_concept_id - (FLOOR(unit_concept_id / 1000) * 1000)) * 1000) + @analysis_id AS BIGINT) IN (SELECT id FROM `@output_path/@included_cov_table`)}	
	) covariate_ids
	ON covariate_ids.covariate_id = temp.covariate_id
LEFT JOIN `@vocab_path/concept` measurement_concept
	ON covariate_ids.measurement_concept_id = measurement_concept.concept_id
LEFT JOIN `@vocab_path/concept` unit_concept
	ON covariate_ids.unit_concept_id = unit_concept.concept_id