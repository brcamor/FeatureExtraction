@covariate_table AS (
	SELECT CAST(MONTH(cohort_start_date) * 1000 + @analysis_id AS BIGINT) AS covariate_id,
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
{@included_cov_table != ''} ? {WHERE MONTH(cohort_start_date) * 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}	
{@cohort_definition_id != -1} ? {
	{@included_cov_table != ''} ? {		AND} :{WHERE} cohort.cohort_definition_id = @cohort_definition_id
}
{@aggregated} ? {		
GROUP BY MONTH(cohort_start_date)
}
)
