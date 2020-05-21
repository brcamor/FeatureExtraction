CREATE TABLE `@output_path/@covariate_table` AS (
	SELECT 
		CAST(@domain_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
	{@temporal} ? {
		time_id,
	}	
	{@aggregated} ? {
		COUNT(*) AS sum_value
	} : {
		row_id,
		1 AS covariate_value 
	}
	FROM (
		SELECT DISTINCT @domain_concept_id,
	{@temporal} ? {
			time_id,
	}	
	{@aggregated} ? {
			cohort.subject_id,
			cohort.cohort_start_date
	} : {
			cohort.@row_id_field AS row_id
	}
		FROM `@cohort_table` cohort
		INNER JOIN `@cdm_database_schema/@domain_table` @domain_table
			ON cohort.@row_id_field = @domain_table.person_id
	{@temporal} ? {
		INNER JOIN #time_period time_period
			ON @domain_start_date <= date_add(cohort.cohort_start_date, time_period.end_day)
			AND @domain_end_date >= date_add(cohort.cohort_start_date, time_period.start_day)
		WHERE @domain_concept_id != 0
	} : {
		WHERE @domain_start_date <= date_add( cohort.cohort_start_date, @end_day)
	{@start_day != 'anyTimePrior'} ? {		AND @domain_end_date >= date_add(cohort.cohort_start_date, @start_day)}
			AND @domain_concept_id != 0
	}
	{@sub_type == 'inpatient'} ? {	AND condition_type_concept_id IN (38000183, 38000184, 38000199, 38000200)}
	{@excluded_concept_table != ''} ? {		AND @domain_concept_id NOT IN (SELECT id FROM @excluded_concept_table)}
	{@included_concept_table != ''} ? {		AND @domain_concept_id IN (SELECT id FROM @included_concept_table)}
	{@included_cov_table != ''} ? {		AND CAST(@domain_concept_id AS BIGINT) * 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}
	{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
	) by_row_id
	{@aggregated} ? {		
	GROUP BY @domain_concept_id
	{@temporal} ? {
		,time_id
	} 
	} 
)


