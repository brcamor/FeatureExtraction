CREATE TABLE `@output_path/@covariate_table` AS

WITH charlson_scoring AS (
--acute myocardial infarction
	SELECT 
		1 as diag_category_id,
		'Myocardial infarction' as diag_category_name,
		1 as weight
	
	UNION ALL

--Congestive heart failure
	SELECT 
		2 as diag_category_id,
		'Congestive heart failure' as diag_category_name,
		1 as weight
	
 	UNION ALL

--Peripheral vascular disease
	SELECT 
		3 as diag_category_id,
		'Peripheral vascular disease' as diag_category_name,
		1 as weight
	
	UNION ALL

--Cerebrovascular disease
	SELECT 
	4 as diag_category_id,
	'Cerebrovascular disease' as diag_category_name,
	1 as weight
	
	UNION ALL

--Dementia
	SELECT 
		5 AS diag_category_id,
		'Dementia' AS diag_category_name,
		1 AS weight
	
	UNION ALL

--Chronic pulmonary disease
	SELECT 
		6 AS diag_category_id,
		'Chronic pulmonary disease' AS diag_category_name,
		1 AS weight
	
	UNION ALL

--Rheumatologic disease
	SELECT
	7 AS diag_category_id,
	'Rheumatologic disease' AS diag_category_name,
	1 AS weight
	
	UNION ALL

--Peptic ulcer disease
	SELECT
		8 AS diag_category_id,
		'Peptic ulcer disease' AS diag_category_name,
		1 AS weight
	
	UNION ALL

--Mild liver disease
	SELECT
		9 AS diag_category_id,
		'Mild liver disease' AS diag_category_name,
		1 AS weight
	
	UNION ALL
--Diabetes (mild to moderate)
	SELECT
		10 AS diag_category_id,
		'Diabetes (mild to moderate)' AS diag_category_name,
		1 AS weight
	
	UNION ALL

--Diabetes with chronic complications
	SELECT
		11 AS diag_category_id,
		'Diabetes with chronic complications' AS diag_category_name,
		2 AS weight
	
	UNION ALL

--Hemoplegia or paralegia
	SELECT
		12 AS diag_category_id,
		'Hemoplegia or paralegia' AS diag_category_name,
		2 AS weight
	
	UNION ALL

--Renal disease
	SELECT
		13 AS diag_category_id,
		'Renal disease' AS diag_category_name,
		2 AS weight
	
	UNION ALL
--Any malignancy
	SELECT
		14 AS diag_category_id,
		'Any malignancy' AS diag_category_name,
		2 AS weight
	
	UNION ALL

--Moderate to severe liver disease
	SELECT
		15 AS diag_category_id,
		'Moderate to severe liver disease' AS diag_category_name,
		3 AS weight
	
	UNION ALL 

--Metastatic solid tumor
	SELECT
		16 AS diag_category_id,
		'Metastatic solid tumor' AS diag_category_name,
		6 AS weight
	
	UNION ALL

--AIDS
	SELECT
		17 AS diag_category_id,
		'AIDS' AS diag_category_name,
		6 AS weight
	
),


charlson_concepts AS (
--acute myocardial infarction
	SELECT 
		1 as diag_category_id
		, descendant_concept_id as concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (4329847)

	UNION ALL

--Congestive heart failure
	SELECT 
		2 as diag_category_id
		, descendant_concept_id as concept_id
	FROM  `@vocab_path/concept_ancestor`	
		WHERE ancestor_concept_id IN (316139)

	UNION ALL

--Peripheral vascular disease
	SELECT 
		3 as diag_category_id,
		descendant_concept_id as concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (321052)
	UNION ALL

--Cerebrovascular disease
	SELECT 
		4 as diag_category_id,
		descendant_concept_id as concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (381591, 434056)

	UNION ALL

--Dementia
	SELECT
		5 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (4182210)
	
	UNION ALL

--Chronic pulmonary disease
	SELECT  
		6 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (4063381)
	
	UNION ALL

--Rheumatologic disease
	SELECT 
		7 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (257628, 134442, 80800, 80809, 256197, 255348)
	UNION ALL

--Peptic ulcer disease
	SELECT 
		8 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (4247120)
	UNION ALL

--Mild liver disease
	SELECT 
		9 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (4064161, 4212540)
	UNION ALL

--Diabetes (mild to moderate)
	SELECT 
		10 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (201820)
	UNION ALL

--Diabetes with chronic complications
	SELECT 
		11 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (4192279, 443767, 442793)
	UNION ALL

--Hemoplegia or paralegia
	SELECT 
		12 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (192606, 374022)
	UNION ALL

--Renal disease
	SELECT 
		13 AS diag_category_id,
		descendant_concept_id as concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (4030518)
	UNION ALL

--Any malignancy
	SELECT 
		14 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (443392)
	UNION ALL

--Moderate to severe liver disease
	SELECT 
		15 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (4245975, 4029488, 192680, 24966)
	UNION ALL 

--Metastatic solid tumor
	SELECT 
		16 AS diag_category_id,
		descendant_concept_id as concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (432851)
	UNION ALL 

--AIDS
	SELECT 
		17 AS diag_category_id,
		descendant_concept_id AS concept_id
	FROM `@vocab_path/concept_ancestor`
		WHERE ancestor_concept_id IN (439727)
)


-- Feature construction
{@aggregated} ? {

WITH charlson_data AS (
	SELECT @row_id_field,
	cohort_start_date,
	SUM(weight) AS score
), 
} : {
SELECT CAST(1000 + @analysis_id AS BIGINT) AS covariate_id,
{@temporal} ? {
    CAST(NULL AS INT) AS time_id,
}	
	row_id,
	SUM(weight) AS covariate_value
}

FROM (
	SELECT DISTINCT charlson_scoring.diag_category_id,
		charlson_scoring.weight,
{@aggregated} ? {
		cohort.subject_id,
		cohort.cohort_start_date
} : {
		cohort.@row_id_field AS row_id
}			
	FROM `@cohort_table` cohort
	INNER JOIN `@cdm_database_schema/condition_era` condition_era
		ON cohort.@row_id_field = condition_era.person_id
	INNER JOIN charlson_concepts charlson_concepts
		ON condition_era.condition_concept_id = charlson_concepts.concept_id
	INNER JOIN charlson_scoring charlson_scoring
		ON charlson_concepts.diag_category_id = charlson_scoring.diag_category_id
{@temporal} ? {		
	WHERE condition_era_start_date <= cohort.cohort_start_date
} : {
	WHERE condition_era_start_date <= date_add(cohort.cohort_start_date, @end_day)
}
{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
	) temp
{@aggregated} ? {
GROUP BY @row_id_field,
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
		SUM(score * score) as squared_score
	FROM #charlson_data
	)
SELECT CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
	t2.max_score AS max_value,
	CAST(t2.sum_score / (1.0 * t1.cnt) AS FLOAT) AS average_value,
	CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
	t2.cnt AS count_value,
	t1.cnt - t2.cnt AS count_no_value,
	t1.cnt AS population_size
INTO #charlson_stats
FROM t1, t2;

SELECT score,
	COUNT(*) AS total,
	ROW_NUMBER() OVER (ORDER BY score) AS rn
INTO #charlson_prep
FROM #charlson_data
GROUP BY score;
	
SELECT s.score,
	SUM(p.total) AS accumulated
INTO #charlson_prep2	
FROM #charlson_prep s
INNER JOIN #charlson_prep p
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
FROM #charlson_prep2 p
CROSS JOIN #charlson_stats o
{@included_cov_table != ''} ? {WHERE 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}
GROUP BY o.count_value,
	o.count_no_value,
	o.min_value,
	o.max_value,
	o.average_value,
	o.standard_deviation,
	o.population_size;

} 

