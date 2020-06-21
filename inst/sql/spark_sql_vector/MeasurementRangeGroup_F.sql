WITH @covariate_table AS (
	SELECT 
		(CAST(measurement_concept_id AS BIGINT) * 10000) + (range_group * 1000) + @analysis_id AS covariate_id,
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
		SELECT measurement_concept_id,
			CASE 
				WHEN value_as_number < range_low THEN 1
				WHEN value_as_number > range_high THEN 3
				ELSE 2
			END AS range_group,		
	{@temporal} ? {
			time_id,
	}	
	{@aggregated} ? {
			cohort.@row_id_field,
			cohort.cohort_start_date
	} : {
			cohort.@row_id_field AS row_id
	}
		FROM `@cohort_table` cohort
		INNER JOIN `@cdm_database_schema/measurement` measurement
			ON cohort.@row_id_field = measurement.person_id
	{@temporal} ? {
		INNER JOIN #time_period time_period
			ON measurement_date <= date_add(cohort.cohort_start_date, time_period.end_day)
			AND measurement_date >= date_add(cohort.cohort_start_date, time_period.start_day)
		WHERE measurement_concept_id != 0
	} : {
		WHERE measurement_date <= date_add(cohort.cohort_start_date, @end_day)
	{@start_day != 'anyTimePrior'} ? {				AND measurement_date >= date_add(cohort.cohort_start_date, @start_day)}
			AND measurement_concept_id != 0
	}
			AND range_low IS NOT NULL
			AND range_high IS NOT NULL
	{@excluded_concept_table != ''} ? {		AND measurement_concept_id NOT IN (SELECT id FROM @excluded_concept_table)}
	{@included_concept_table != ''} ? {		AND measurement_concept_id IN (SELECT id FROM @included_concept_table)}
	{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
	) by_row_id
	{@included_cov_table != ''} ? {WHERE (CAST(measurement_concept_id AS BIGINT) * 10000) + (range_group * 1000) + @analysis_id IN (SELECT id FROM @included_cov_table)}
	GROUP BY measurement_concept_id,
		range_group
	{!@aggregated} ? {		
		,row_id
	} 
	{@temporal} ? {
		,time_id
	} 
) 
