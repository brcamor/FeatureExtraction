-- Feature construction
@covariate_table AS (
	SELECT 
		CAST(race_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
	{@temporal} ? {
		CAST(NULL AS INT) AS time_id,
	}		
	{@aggregated} ? {
		COUNT(*) AS sum_value
	} : {
		cohort.@row_id_field AS row_id,
		1 AS covariate_value 
	}
	FROM `@cohort_table` cohort
	INNER JOIN `@cdm_database_schema/person` person
		ON cohort.@row_id_field = person.person_id
	WHERE race_concept_id IN (
			SELECT concept_id
			FROM `@vocab_path/concept`
			WHERE LOWER(concept_class_id) = 'race'
			)
	{@excluded_concept_table != ''} ? {	AND race_concept_id NOT IN (SELECT id FROM @excluded_concept_table)}
	{@included_concept_table != ''} ? {	AND race_concept_id IN (SELECT id FROM @included_concept_table)}	
	{@included_cov_table != ''} ? {	AND CAST(race_concept_id AS BIGINT) * 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}	
	{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
	{@aggregated} ? {		
	GROUP BY race_concept_id
	}
)