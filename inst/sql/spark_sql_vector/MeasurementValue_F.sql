@covariate_table AS (
	WITH meas_cov AS (
		SELECT DISTINCT measurement_concept_id,
	unit_concept_id,
	CAST((CAST(measurement_concept_id AS BIGINT) * 1000000) + ((unit_concept_id - (FLOOR(unit_concept_id / 1000) * 1000)) * 1000) + @analysis_id AS BIGINT) AS covariate_id
	FROM `@cdm_database_schema/measurement`
	WHERE value_as_number IS NOT NULL
	{@excluded_concept_table != ''} ? {		AND measurement_concept_id NOT IN (SELECT id FROM @excluded_concept_table)}
	{@included_concept_table != ''} ? {		AND measurement_concept_id IN (SELECT id FROM @included_concept_table)}
	{@included_cov_table != ''} ? {		AND CAST((CAST(measurement_concept_id AS BIGINT) * 1000000) + ((unit_concept_id - (FLOOR(unit_concept_id / 1000) * 1000)) * 1000) + @analysis_id AS BIGINT) IN (SELECT id FROM @included_cov_table)}
	),

	meas_val_data AS (
		SELECT 
		{@aggregated} ? {
				subject_id,
				cohort_start_date,
		} : {
				row_id,
		}
		{@temporal} ? {
			time_id,
		}	
			covariate_id,
			value_as_number
		FROM (
			SELECT 
		{@aggregated} ? {
				subject_id,
				cohort_start_date,
		{@temporal} ? {
				time_id,
				ROW_NUMBER() OVER (PARTITION BY subject_id, cohort_start_date, measurement.measurement_concept_id, time_id ORDER BY measurement_date DESC) AS rn,
		} : {
				ROW_NUMBER() OVER (PARTITION BY subject_id, cohort_start_date, measurement.measurement_concept_id ORDER BY measurement_date DESC) AS rn,
		}
		} : {
				cohort.@row_id_field AS row_id,
		{@temporal} ? {
				time_id,
				ROW_NUMBER() OVER (PARTITION BY cohort.@row_id_field, measurement.measurement_concept_id, time_id ORDER BY measurement_date DESC) AS rn,
		} : {
				ROW_NUMBER() OVER (PARTITION BY cohort.@row_id_field, measurement.measurement_concept_id ORDER BY measurement_date DESC) AS rn,
		}
		}
				covariate_id,
				value_as_number
			FROM `@cohort_table` cohort
			INNER JOIN `@cdm_database_schema/measurement` measurement
				ON cohort.@row_id_field = measurement.person_id
			INNER JOIN meas_cov
				ON meas_cov.measurement_concept_id = measurement.measurement_concept_id 
					AND meas_cov.unit_concept_id = measurement.unit_concept_id 
		{@temporal} ? {
			INNER JOIN #time_period time_period
				ON measurement_date <= date_add(cohort.cohort_start_date, time_period.end_day)
				AND measurement_date >= date_add(cohort.cohort_start_date, time_period.start_day)
			WHERE measurement.measurement_concept_id != 0 
		} : {
			WHERE measurement_date <= date_add(cohort.cohort_start_date, @end_day)
		{@start_day != 'anyTimePrior'} ? {				AND measurement_date >= date_add(cohort.cohort_start_date, @start_day)}
				AND measurement.measurement_concept_id != 0
		}	
				AND value_as_number IS NOT NULL 			
		{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
		) temp
		WHERE rn = 1
	)
	-- Feature construction

	{@aggregated} ? {
	SELECT covariate_id,
	{@temporal} ? {
		time_id,
	}
		MIN(value_as_number) AS min_value,
		MAX(value_as_number) AS max_value,
		CAST(AVG(value_as_number) AS FLOAT) AS average_value,
		CAST(STDEV(value_as_number) AS FLOAT) AS standard_deviation,
		COUNT(*) AS count_value
	INTO #meas_val_stats
	FROM #meas_val_data
	GROUP BY covariate_id
	{@temporal} ? {
		,time_id
	}
	;

	SELECT covariate_id,
	{@temporal} ? {
		time_id,
	}	
		value_as_number,
		COUNT(*) AS total,
		ROW_NUMBER() OVER (PARTITION BY covariate_id ORDER BY value_as_number) AS rn
	INTO #meas_val_prep
	FROM #meas_val_data
	GROUP BY value_as_number,
	{@temporal} ? {
		time_id,
	}	
		covariate_id;
		
	SELECT s.covariate_id,
	{@temporal} ? {
		s.time_id,
	}	
		s.value_as_number,
		SUM(p.total) AS accumulated
	INTO #meas_val_prep2	
	FROM #meas_val_prep s
	INNER JOIN #meas_val_prep p
		ON p.rn <= s.rn
			AND p.covariate_id = s.covariate_id
	GROUP BY s.covariate_id,
	{@temporal} ? {
		s.time_id,
	}			
		s.value_as_number;
		
	SELECT o.covariate_id,
	{@temporal} ? {
		o.time_id,
	}
		o.count_value,
		o.min_value,
		o.max_value,
		CAST(o.average_value AS FLOAT) average_value,
		CAST(o.standard_deviation AS FLOAT) standard_deviation,
		MIN(CASE WHEN p.accumulated >= .50 * o.count_value THEN value_as_number END) AS median_value,
		MIN(CASE WHEN p.accumulated >= .10 * o.count_value THEN value_as_number END) AS p10_value,
		MIN(CASE WHEN p.accumulated >= .25 * o.count_value THEN value_as_number END) AS p25_value,
		MIN(CASE WHEN p.accumulated >= .75 * o.count_value THEN value_as_number END) AS p75_value,
		MIN(CASE WHEN p.accumulated >= .90 * o.count_value THEN value_as_number END) AS p90_value	
	INTO @covariate_table
	FROM #meas_val_prep2 p
	INNER JOIN #meas_val_stats o
		ON o.covariate_id = p.covariate_id
	{@temporal} ? {
			AND	o.time_id = p.time_id
	}		
	GROUP BY o.covariate_id,
	{@temporal} ? {
		o.time_id,
	}
		o.count_value,
		o.min_value,
		o.max_value,
		o.average_value,
		o.standard_deviation;
	} : {
	SELECT covariate_id,
	{@temporal} ? {
		time_id,
	}	
		row_id,
		value_as_number AS covariate_value 
	FROM meas_val_data
	}
)
