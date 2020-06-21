@covariate_table AS (
	{@aggregated} ? {
	SELECT subject_id,
		cohort_start_date,
	{@temporal} ? {
		time_id,
	}	
	{@sub_type == 'stratified'} ? {
		covariate_id,
	}
		concept_count
	INTO #concept_count_data
	} : {
	{@sub_type == 'stratified'} ? {
	SELECT covariate_id,
	} : {
	SELECT CAST(1000 + @analysis_id AS BIGINT) AS covariate_id,
	}
	{@temporal} ? {
		time_id,
	}	
		row_id,
		concept_count AS covariate_value
	}
	FROM (
		SELECT 
	{@temporal} ? {
			time_id,
	}	
	{@sub_type == 'stratified'} ? {
			CAST(@domain_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
	}
	{@aggregated} ? {
			@row_id_field,
			cohort_start_date,
	} : {
			cohort.@row_id_field AS row_id,
	}
	{@sub_type == 'distinct'} ? {
			COUNT(DISTINCT @domain_concept_id) AS concept_count
	} : {
			COUNT(*) AS concept_count
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
		WHERE @domain_start_date <= date_add(cohort.cohort_start_date, @end_day)
			AND @domain_end_date >= date_add(cohort.cohort_start_date, @start_day)
			AND @domain_concept_id != 0
	}
	{@excluded_concept_table != ''} ? {		AND @domain_concept_id NOT IN (SELECT id FROM @excluded_concept_table)}
	{@included_concept_table != ''} ? {		AND @domain_concept_id IN (SELECT id FROM @included_concept_table)}
	{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
		GROUP BY 
	{@temporal} ? {
			time_id,
	}	
	{@sub_type == 'stratified'} ? {
			@domain_concept_id,
	} 
	{@aggregated} ? {
			@row_id_field,
			cohort_start_date
	} : {

			cohort.@row_id_field		
	}	
		) raw_data

	{@aggregated} ? {
	WITH t1 AS (
		SELECT COUNT(*) AS cnt 
		FROM @cohort_table 
	{@cohort_definition_id != -1} ? {	WHERE cohort_definition_id = @cohort_definition_id}
		),
	t2 AS (
		SELECT COUNT(*) AS cnt, 
	{@sub_type == 'stratified'} ? {
			covariate_id,
	} 
			MIN(concept_count) AS min_concept_count, 
			MAX(concept_count) AS max_concept_count, 
			SUM(CAST(concept_count AS BIGINT)) AS sum_concept_count,
			SUM(CAST(concept_count AS BIGINT) * CAST(concept_count AS BIGINT)) AS squared_concept_count
		FROM #concept_count_data
	{@sub_type == 'stratified'} ? {
		GROUP BY covariate_id
	} 
		)
	SELECT CASE WHEN t2.cnt = t1.cnt THEN t2.min_concept_count ELSE 0 END AS min_value,
		t2.max_concept_count AS max_value,
	{@sub_type == 'stratified'} ? {
		covariate_id,
	} 
		CAST(t2.sum_concept_count / (1.0 * t1.cnt) AS FLOAT) AS average_value,
		CAST(CASE
			WHEN t2.cnt = 1 THEN 0 
			ELSE SQRT((1.0 * t2.cnt*t2.squared_concept_count - 1.0 * t2.sum_concept_count*t2.sum_concept_count) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) 
		END AS FLOAT) AS standard_deviation,
		t2.cnt AS count_value,
		t1.cnt - t2.cnt AS count_no_value,
		t1.cnt AS population_size
	INTO #concept_count_stats
	FROM t1, t2;

	SELECT concept_count,
		COUNT(*) AS total,
	{@sub_type == 'stratified'} ? {
		covariate_id,
		ROW_NUMBER() OVER (PARTITION BY covariate_id ORDER BY concept_count) AS rn
	} : {
		ROW_NUMBER() OVER (ORDER BY concept_count) AS rn
	}
	INTO #concept_count_prep
	FROM #concept_count_data
	GROUP BY concept_count
	{@sub_type == 'stratified'} ? {
		,covariate_id
	}
	;
		
	SELECT s.concept_count,
	{@sub_type == 'stratified'} ? {
		s.covariate_id,
	}
		SUM(p.total) AS accumulated
	INTO #concept_count_prep2	
	FROM #concept_count_prep s
	INNER JOIN #concept_count_prep p
		ON p.rn <= s.rn
	{@sub_type == 'stratified'} ? {
		AND p.covariate_id= s.covariate_id
	}
	GROUP BY s.concept_count
	{@sub_type == 'stratified'} ? {
		,s.covariate_id
	}
	;

	{@sub_type == 'stratified'} ? {
	SELECT o.covariate_id,
	} : {
	SELECT CAST(1000 + @analysis_id AS BIGINT) AS covariate_id,
	}
	{@temporal} ? {
		CAST(NULL AS INT) AS time_id,
	}
		o.count_value,
		o.min_value,
		o.max_value,
		CAST(o.average_value AS FLOAT) average_value,
		CAST(o.standard_deviation AS FLOAT) standard_deviation,
		CASE 
			WHEN .50 * o.population_size < count_no_value THEN 0
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .50 * o.population_size THEN concept_count	END) 
			END AS median_value,
		CASE 
			WHEN .10 * o.population_size < count_no_value THEN 0
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .10 * o.population_size THEN concept_count	END) 
			END AS p10_value,		
		CASE 
			WHEN .25 * o.population_size < count_no_value THEN 0
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .25 * o.population_size THEN concept_count	END) 
			END AS p25_value,	
		CASE 
			WHEN .75 * o.population_size < count_no_value THEN 0
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .75 * o.population_size THEN concept_count	END) 
			END AS p75_value,	
		CASE 
			WHEN .90 * o.population_size < count_no_value THEN 0
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .90 * o.population_size THEN concept_count	END) 
			END AS p90_value		
	INTO @covariate_table
	FROM #concept_count_prep2 p
	{@sub_type == 'stratified'} ? {
	INNER JOIN #concept_count_stats o
	ON p.covariate_id = o.covariate_id
	{@included_cov_table != ''} ? {WHERE covariate_id IN (SELECT id FROM @included_cov_table)}
	} : {
	CROSS JOIN #concept_count_stats o
	{@included_cov_table != ''} ? {WHERE 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}
	}
	GROUP BY o.count_value,
		o.count_no_value,
		o.min_value,
		o.max_value,
		o.average_value,
		o.standard_deviation,
	{@sub_type == 'stratified'} ? {
		o.covariate_id,
	}
		o.population_size
	}
)

