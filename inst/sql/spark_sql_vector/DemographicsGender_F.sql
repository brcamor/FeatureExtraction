-- Feature construction
@covariate_table AS (
SELECT 
	CAST(gender_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
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
WHERE gender_concept_id != 0
{@excluded_concept_table != ''} ? {	AND gender_concept_id NOT IN (SELECT id FROM global_temp.@excluded_concept_table)}
{@included_concept_table != ''} ? {	AND gender_concept_id IN (SELECT id FROM global_temp.@included_concept_table)}	
{@included_cov_table != ''} ? {	AND CAST(gender_concept_id AS BIGINT) * 1000 + @analysis_id IN (SELECT id FROM global_temp.@included_cov_table)}	
{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
{@aggregated} ? {		
GROUP BY gender_concept_id
}
)