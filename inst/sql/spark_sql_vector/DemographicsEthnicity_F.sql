-- Feature construction
@covariate_table AS (
	SELECT 
	CAST(ethnicity_concept_id  AS BIGINT) * 1000 + @analysis_id AS covariate_id,
{@temporal} ? {
    CAST(NULL AS INT) AS time_id,
}		
{@aggregated} ? {
	COUNT(*) AS sum_value
} : {
	cohort.@row_id_field AS row_id,
	1 AS covariate_value 
}
FROM global_temp.@cohort_table cohort
INNER JOIN global_temp.person person
	ON cohort.subject_id = person.person_id
WHERE ethnicity_concept_id  IN (
		SELECT concept_id
		FROM concept
		WHERE LOWER(concept_class_id) = 'ethnicity'
		)
{@excluded_concept_table != ''} ? {	AND ethnicity_concept_id  NOT IN (SELECT id FROM @excluded_concept_table)}
{@included_concept_table != ''} ? {	AND ethnicity_concept_id  IN (SELECT id FROM @included_concept_table)}	
{@included_cov_table != ''} ? {	AND CAST(ethnicity_concept_id  AS BIGINT) * 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}	
{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
{@aggregated} ? {		
GROUP BY ethnicity_concept_id 
}	
)