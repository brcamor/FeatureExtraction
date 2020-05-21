CREATE TABLE `@output_path/@covariate_table` AS

--Scoring
WITH chads2vasc_scoring as (

	-- C: Congestive heart failure
	SELECT 
		1 AS diag_category_id,
		'Congestive heart failure' AS diag_category_name,
		1 AS weight
	UNION ALL

-- H: Hypertension
	SELECT 
		2 AS diag_category_id,
		'Hypertension' AS diag_category_name,
		1 AS weight
	UNION ALL

-- D: Diabetes
	SELECT 
		4 AS diag_category_id,
		'Diabetes' AS diag_category_name,
		1 AS weight
	UNION ALL

-- S2: Stroke
	SELECT 
		5 AS diag_category_id,
		'Stroke' AS diag_category_name,
		2 AS weight
	UNION ALL

	SELECT 
		6 AS diag_category_id,
		'Vascular Disease' AS diag_category_name,
		1 AS weight
),

-- Concepts
 chads2vasc_concepts AS (
-- C: Congestive heart failure

	SELECT DISTINCT 
		1 AS diag_category_id ,
		c.concept_id as concept_id
	FROM (
		SELECT concept_id
		FROM `@vocab_path/concept`
		WHERE concept_id IN (316139, 314378, 318773, 321319)
			AND invalid_reason IS NULL
		
		UNION
		
		SELECT descendant_concept_id AS concept_id
		FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (316139, 314378)
	) c
	UNION ALL

-- H: Hypertension
	SELECT DISTINCT 
		2 AS diag_category_id ,
		i.descendant_concept_id as concept_id
	FROM (
		SELECT descendant_concept_id
		FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (320128, 442604, 201313)
		) i
	LEFT JOIN (
		SELECT descendant_concept_id
		FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (197930)
		) e
		ON i.descendant_concept_id = e.descendant_concept_id
	WHERE e.descendant_concept_id IS NULL
	UNION ALL

-- D: Diabetes
	SELECT DISTINCT 
		4 AS diag_category_id,
		i.descendant_concept_id as concept_id
	FROM (
		SELECT descendant_concept_id
		FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (201820, 442793)
		) i
	LEFT JOIN (
		SELECT descendant_concept_id
		FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (195771, 376112, 4174977, 4058243, 193323, 376979)
		) e
		ON i.descendant_concept_id = e.descendant_concept_id
	WHERE e.descendant_concept_id IS NULL
	UNION ALL

-- S2: Stroke
	SELECT DISTINCT 
		5 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
	WHERE ancestor_concept_id IN (4043731, 4110192, 375557, 4108356, 373503, 434656, 433505, 376714, 312337)
	UNION ALL

-- V: Vascular disease (e.g. peripheral artery disease, myocardial infarction, aortic plaque)
	SELECT DISTINCT 
		6 AS diag_category_id,
		c.concept_id AS concept_id
	FROM (
		SELECT concept_id
		FROM `@vocab_path/concept`
		WHERE concept_id IN (312327,43020432,314962,312939,315288,317309,134380,196438,200138,194393,319047,40486130,317003,4313767,321596,317305,321886,314659,321887,437312,134057)
			AND invalid_reason IS NULL
		
		UNION
		
		SELECT descendant_concept_id AS concept_id
		FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (312327,43020432,314962,312939,315288,317309,134380,196438,200138,194393,319047,40486130,317003,4313767,321596)
	) c

)
	

-- Feature construction
{@aggregated} ? {
SELECT subject_id,
	cohort_start_date,
	SUM(weight) AS score
INTO #chads2Vasc_data
} : {
SELECT CAST(1000 + @analysis_id AS BIGINT) AS covariate_id,
{@temporal} ? {
    CAST(NULL AS INT) AS time_id,
}	
	row_id,
	SUM(weight) AS covariate_value
}
FROM (
	SELECT DISTINCT chads2vasc_scoring.diag_category_id,
		chads2vasc_scoring.weight,
{@aggregated} ? {
		cohort.@row_id_field,
		cohort.cohort_start_date
} : {
		cohort.@row_id_field AS row_id
}			
	FROM `@cohort_table` cohort
	INNER JOIN `@cdm_database_schema/condition_era` condition_era
		ON cohort.@row_id_field = condition_era.person_id
	INNER JOIN chads2vasc_concepts
		ON condition_era.condition_concept_id = chads2vasc_concepts.concept_id
	INNER JOIN chads2vasc_scoring
		ON chads2vasc_concepts.diag_category_id = chads2vasc_scoring.diag_category_id
{@temporal} ? {		
	WHERE condition_era_start_date <= cohort.cohort_start_date
} : {
	WHERE condition_era_start_date <= date_add(cohort.cohort_start_date, @end_day)
}
{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}

	UNION
	
	SELECT 3 AS diag_category_id,
		CASE WHEN (YEAR(cohort_start_date) - year_of_birth) >= 75 THEN 2 
		     WHEN (YEAR(cohort_start_date) - year_of_birth) >= 65 THEN 1 
			 ELSE 0 END + CASE WHEN	gender_concept_id = 8532 THEN 1 ELSE 0 END AS weight,
{@aggregated} ? {
		cohort.@row_id_field,
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
	FROM #chads2Vasc_data
	)
SELECT CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
	t2.max_score AS max_value,
	CAST(t2.sum_score / (1.0 * t1.cnt) AS FLOAT) AS average_value,
	CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
	t2.cnt AS count_value,
	t1.cnt - t2.cnt AS count_no_value,
	t1.cnt AS population_size
INTO #chads2Vasc_stats
FROM t1, t2;

SELECT score,
	COUNT(*) AS total,
	ROW_NUMBER() OVER (ORDER BY score) AS rn
INTO #chads2Vasc_prep
FROM #chads2Vasc_data
GROUP BY score;
	
SELECT s.score,
	SUM(p.total) AS accumulated
INTO #chads2Vasc_prep2	
FROM #chads2Vasc_prep s
INNER JOIN #chads2Vasc_prep p
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
FROM #chads2Vasc_prep2 p
CROSS JOIN #chads2Vasc_stats o
{@included_cov_table != ''} ? {WHERE 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}
GROUP BY o.count_value,
	o.count_no_value,
	o.min_value,
	o.max_value,
	o.average_value,
	o.standard_deviation,
	o.population_size;
	
}