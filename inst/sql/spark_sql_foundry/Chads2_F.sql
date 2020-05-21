CREATE TABLE `@output_path/@covariate_table` AS 
-- SCORING
WITH chads2_scoring AS (
	--Congestive heart failure
	SELECT 
		1 as diag_category_id,
		'Congestive heart failure' as diag_category_name,
		1 as weight

	UNION ALL

	--Hypertension
	SELECT 
		2 as diag_category_id,
		'Hypertension' as diag_category_name,
		1 as weight

	UNION ALL

	--Diabetes
	SELECT 
		4 as diag_category_id,
		'Diabetes' as diag_category_name,
		1 as weight

	UNION ALL

	--Stroke
	SELECT
		5 as diag_category_id,
		'Stroke' as diag_category_name,
		2 as weight

),

-- CONCEPTS
chads2_concepts as (
	--Congestive heart failure
	SELECT 
		1 as diag_category_id,
		descendant_concept_id as concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (316139)

	UNION ALL

	--Hypertension
	SELECT 2 as diag_category_id,
		descendant_concept_id as concept_id
	FROM `@vocab_path/concept_ancestor`
	WHERE ancestor_concept_id IN (316866)

	UNION ALL

	--Diabetes
	SELECT 
		4 as diag_category_id,
		descendant_concept_id as concept_id
	FROM `@vocab_path/concept_ancestor`
	WHERE ancestor_concept_id IN (201820)

	UNION ALL 

	--Stroke
	SELECT 
		5 as diag_category_id,
		descendant_concept_id as concept_id
	FROM `@vocab_path/concept_ancestor`
	WHERE ancestor_concept_id IN (381591, 434056)
)

-- Feature construction
{@aggregated} ? {
SELECT subject_id,
	cohort_start_date,
	SUM(weight) AS score
INTO #chads2_data
} : {
SELECT CAST(1000 + @analysis_id AS BIGINT) AS covariate_id,
{@temporal} ? {
    CAST(NULL AS INT) AS time_id,
}	
	row_id,
	SUM(weight) AS covariate_value
}
FROM (
	SELECT DISTINCT chads2_scoring.diag_category_id,
		chads2_scoring.weight,
{@aggregated} ? {
		cohort.subject_id,
		cohort.cohort_start_date
} : {
		cohort.@row_id_field AS row_id
}			
	FROM `@cohort_table` cohort
	INNER JOIN `@cdm_database_schema/condition_era` condition_era
		ON cohort.@row_id_field = condition_era.person_id
	INNER JOIN chads2_concepts 
		ON condition_era.condition_concept_id = chads2_concepts.concept_id
	INNER JOIN chads2_scoring
		ON chads2_concepts.diag_category_id = chads2_scoring.diag_category_id
{@temporal} ? {		
	WHERE condition_era_start_date <= cohort.cohort_start_date
} : {
	WHERE condition_era_start_date <= date_add(cohort.cohort_start_date, @end_day)
}
{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}

	UNION
	
	SELECT 3 AS diag_category_id,
		CASE WHEN (YEAR(cohort_start_date) - year_of_birth) >= 75 THEN 1 ELSE 0 END AS weight,
{@aggregated} ? {
		cohort.subject_id,
		cohort.cohort_start_date
} : {
		cohort.@row_id_field AS row_id
}	  
	FROM `@cohort_table` cohort
	INNER JOIN `@cdm_database_schema/person` person
		ON cohort.@row_id_field = person.person_id
{@cohort_definition_id != -1} ? {	WHERE cohort.cohort_definition_id = @cohort_definition_id}		
	) temp
{@aggregated} ? {
GROUP BY subject_id,
			cohort_start_date
} : {
GROUP BY row_id
}

{@aggregated} ? {
WITH t1 AS (
	SELECT COUNT(*) AS cnt 
	FROM @cohort_table 
{@cohort_definition_id != -1} ? {	WHERE cohort_definition_id = @cohort_definition_id}
	),
t2 AS (
	SELECT COUNT(*) AS cnt, 
		MIN(score) AS min_score, 
		MAX(score) AS max_score, 
		SUM(score) AS sum_score, 
		SUM(score*score) AS squared_score 
	FROM #chads2_data
	)
SELECT CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
	t2.max_score AS max_value,
	CAST(t2.sum_score / (1.0 * t1.cnt) AS FLOAT) AS average_value,
	CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
	t2.cnt AS count_value,
	t1.cnt - t2.cnt AS count_no_value,
	t1.cnt AS population_size
INTO #chads2_stats
FROM t1, t2;

SELECT score,
	COUNT(*) AS total,
	ROW_NUMBER() OVER (ORDER BY score) AS rn
INTO #chads2_prep
FROM #chads2_data
GROUP BY score;
	
SELECT s.score,
	SUM(p.total) AS accumulated
INTO #chads2_prep2	
FROM #chads2_prep s
INNER JOIN #chads2_prep p
	ON p.rn <= s.rn
GROUP BY s.score;

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
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .50 * o.population_size THEN score	END) 
		END AS median_value,
	CASE 
		WHEN .10 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .10 * o.population_size THEN score	END) 
		END AS p10_value,		
	CASE 
		WHEN .25 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .25 * o.population_size THEN score	END) 
		END AS p25_value,	
	CASE 
		WHEN .75 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .75 * o.population_size THEN score	END) 
		END AS p75_value,	
	CASE 
		WHEN .90 * o.population_size < count_no_value THEN 0
		ELSE MIN(CASE WHEN p.accumulated + count_no_value >= .90 * o.population_size THEN score	END) 
		END AS p90_value		
INTO @covariate_table
FROM #chads2_prep2 p
CROSS JOIN #chads2_stats o
{@included_cov_table != ''} ? {WHERE 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}
GROUP BY o.count_value,
	o.count_no_value,
	o.min_value,
	o.max_value,
	o.average_value,
	o.standard_deviation,
	o.population_size;
		
} 