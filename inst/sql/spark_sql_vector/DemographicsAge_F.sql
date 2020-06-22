@covariate_table AS (
	{@aggregated} ? {
	WITH dem_age_data AS (
	SELECT subject_id,
		cohort_start_date,
		age

	} : {
	SELECT CAST(1000 + @analysis_id AS BIGINT) AS covariate_id,
	{@temporal} ? {
		CAST(NULL AS INT) AS time_id,
	}	
		row_id,
		age AS covariate_value
	}
	FROM (
		SELECT 
	{@aggregated} ? {
			@row_id_field,
			cohort_start_date,	
	} : {
			cohort.@row_id_field AS row_id,	
	}
			YEAR(cohort_start_date) - year_of_birth AS age
		FROM @cohort_table cohort
		INNER JOIN person person
			ON cohort.@row_id_field = person.person_id
	{@cohort_definition_id != -1} ? {	WHERE cohort.cohort_definition_id = @cohort_definition_id}
		) raw_data

	{@aggregated} ? {
	),

	t1 AS (
		SELECT COUNT(*) AS cnt 
		FROM @cohort_table
	{@cohort_definition_id != -1} ? {	WHERE cohort_definition_id = @cohort_definition_id}
	),
	t2 AS (
		SELECT COUNT(*) AS cnt, 
			MIN(age) AS min_age, 
			MAX(age) AS max_age, 
			SUM(CAST(age AS BIGINT)) AS sum_age, 
			SUM(CAST(age AS BIGINT) * CAST(age AS BIGINT)) AS squared_age 
		FROM dem_age_data
	),
	dem_age_stats AS (
		SELECT CASE WHEN t2.cnt = t1.cnt THEN t2.min_age ELSE 0 END AS min_value,
		t2.max_age AS max_value,
		CAST(t2.sum_age / (1.0 * t1.cnt) AS FLOAT) AS average_value,
		CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_age - 1.0 * t2.sum_age*t2.sum_age) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
		t2.cnt AS count_value,
		t1.cnt - t2.cnt AS count_no_value,
		t1.cnt AS population_size
		FROM t1, t2	
	),

	dem_age_prep AS (
	SELECT 
		age,
		COUNT(*) AS total,
		ROW_NUMBER() OVER (ORDER BY age) AS rn
	FROM dem_age_data
	GROUP BY age
	),

	dem_age_prep2 AS (
		SELECT s.age,
		SUM(p.total) AS accumulated
	FROM dem_age_prep s
	INNER JOIN dem_age_prep p
		ON p.rn <= s.rn
	GROUP BY s.age
	)

	SELECT CAST(1000 + @analysis_id AS BIGINT) AS covariate_id,
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
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .50 * o.population_size THEN age	END) 
			END AS median_value,
		CASE 
			WHEN .10 * o.population_size < count_no_value THEN 0
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .10 * o.population_size THEN age	END) 
			END AS p10_value,		
		CASE 
			WHEN .25 * o.population_size < count_no_value THEN 0
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .25 * o.population_size THEN age	END) 
			END AS p25_value,	
		CASE 
			WHEN .75 * o.population_size < count_no_value THEN 0
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .75 * o.population_size THEN age	END) 
			END AS p75_value,	
		CASE 
			WHEN .90 * o.population_size < count_no_value THEN 0
			ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .90 * o.population_size THEN age	END) 
			END AS p90_value		
	FROM dem_age_prep2 p
	CROSS JOIN dem_age_stats o
	{@included_cov_table != ''} ? {WHERE 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}
	GROUP BY o.count_value,
		o.count_no_value,
		o.min_value,
		o.max_value,
		o.average_value,
		o.standard_deviation,
		o.population_size

	} 
)