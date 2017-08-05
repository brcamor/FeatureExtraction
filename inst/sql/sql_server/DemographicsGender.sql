-- Feature construction
SELECT 
	CAST(gender_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
{@temporal} ? {
    NULL AS time_id,
}		
{@aggregated} ? {
	COUNT(*) AS sum_value,
	CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM @cohort_table) THEN 1 ELSE 0 END AS min_value,
	1 AS max_value,
	COUNT(*) / (1.0 * (SELECT COUNT(*) FROM @cohort_table)) AS average_value,
	SQRT((COUNT(*) / (1.0 * (SELECT COUNT(*) FROM @cohort_table)))*(1 - (COUNT(*) / (1.0 * (SELECT COUNT(*) FROM @cohort_table))))/(1.0 * (SELECT COUNT(*) FROM @cohort_table)))  AS standard_deviation
} : {
	cohort.@row_id_field AS row_id,
	1 AS covariate_value 
}
INTO @covariate_table
FROM @cohort_table cohort
INNER JOIN @cdm_database_schema.person
	ON cohort.subject_id = person.person_id
WHERE gender_concept_id != 0
{@has_excluded_covariate_concept_ids} ? {	AND gender_concept_id NOT IN (SELECT concept_id FROM #excluded_cov)}
{@has_included_covariate_concept_ids} ? {	AND gender_concept_id IN (SELECT concept_id FROM #included_cov)}	
{@has_included_covariate_ids} ? {	AND CAST(gender_concept_id AS BIGINT) * 1000 + @analysis_id IN (SELECT covariate_id FROM #included_cov_by_id)}	
{@aggregated} ? {		
GROUP BY gender_concept_id
{@temporal} ? {
    ,time_id
}	
}
;

-- Reference construction
INSERT INTO #cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
SELECT covariate_id,
	CONCAT('Gender = ', concept_name) AS covariate_name,
	@analysis_id AS analysis_id,
	concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM @covariate_table
	) t1
INNER JOIN @cdm_database_schema.concept
	ON concept_id = CAST((covariate_id - @analysis_id) / 1000 AS INT);