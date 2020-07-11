@covariate_table AS (
	WITH dcsi_scoring AS (
		SELECT 
			CAST('Retinopathy' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)) AS dcsi_icd9_code,
			target_concept_id AS dcsi_concept_id,
			1 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE '250.5%'
			OR source_code IN ('362.01', '362.1', '362.83', '362.53', '362.81', '362.82')

		UNION ALL

		SELECT 
			CAST('Retinopathy' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			2 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE '361%'
			OR source_code LIKE '369%'
			OR source_code IN ('362.02', '379.23')

		UNION ALL

		SELECT 
			CAST('Nephropathy' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			1 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code IN ('250.4', '580', '581', '581.81', '582', '583')
			OR source_code LIKE '580%'
			OR source_code LIKE '581%'
			OR source_code LIKE '582%'
			OR source_code LIKE '583%'

		UNION ALL

		SELECT 
			CAST('Nephropathy' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			2 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code IN ('585', '586', '593.9')
			OR source_code LIKE '585%'
			OR source_code LIKE '586%'
			OR source_code LIKE '593.9%'

		UNION ALL

		SELECT 
			CAST('Neuropathy' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			1 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code IN ('356.9', '250.6', '358.1', '951.0', '951.1', '951.3', '713.5', '357.2', '596.54', '337.0', '337.1', '564.5', '536.3', '458.0')
			OR (
				source_code >= '354.0'
				AND source_code <= '355.99'
				)
			OR source_code LIKE '356.9%'
			OR source_code LIKE '250.6%'
			OR source_code LIKE '358.1%'
			OR source_code LIKE '951.0%'
			OR source_code LIKE '951.1%'
			OR source_code LIKE '951.3%'
			OR source_code LIKE '713.5%'
			OR source_code LIKE '357.2%'
			OR source_code LIKE '337.0%'
			OR source_code LIKE '337.1%'
			OR source_code LIKE '564.5%'
			OR source_code LIKE '536.3%'
			OR source_code LIKE '458.0%'

		UNION ALL

		SELECT 
			CAST('Cerebrovascular' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			2 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
		) source_to_concept_map
		WHERE source_code IN ('431', '433', '434', '436')
			OR source_code LIKE '431%'
			OR source_code LIKE '433%'
			OR source_code LIKE '434%'
			OR source_code LIKE '436%'

		UNION ALL

		SELECT 
			CAST('Cardiovascular' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			1 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
		) source_to_concept_map
		WHERE source_code LIKE '440%'
			OR source_code LIKE '411%'
			OR source_code LIKE '413%'
			OR source_code LIKE '414%'
			OR source_code LIKE '429.2%'
		
		UNION ALL

		SELECT CAST('Cardiovascular' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			2 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE '410%'
			OR source_code LIKE '427.1%'
			OR source_code LIKE '427.3%'
			OR source_code LIKE '427.4%'
			OR source_code LIKE '427.5%'
			OR source_code LIKE '412%'
			OR source_code LIKE '428%'
			OR source_code LIKE '441%'
			OR source_code IN ('440.23', '440.24')

		UNION ALL

		SELECT 
			CAST('Peripheral vascular disease' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			1 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE '250.7%'
			OR source_code LIKE '442.3%'
			OR source_code LIKE '892.1%'
			OR source_code LIKE '443.9%'
			OR source_code IN ('443.81')

		UNION ALL

		SELECT 
			CAST('Peripheral vascular disease' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			2 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE '785.4%'
			OR source_code LIKE '707.1%'
			OR source_code LIKE '040.0%'
			OR source_code IN ('444.22')

		UNION ALL

		SELECT 
			CAST('Metabolic' AS VARCHAR(255)) AS dcsi_category,
			CAST(source_code AS VARCHAR(255)),
			target_concept_id,
			2 AS dcsi_score
		FROM (
			SELECT 
				source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM global_temp.concept_relationship concept_relationship
			INNER JOIN global_temp.concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN global_temp.concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD9CM'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE '250.1%'
			OR source_code LIKE '250.2%'
			OR source_code LIKE '250.3%'
	)

	-- Feature construction
	{@aggregated} ? {
	, dcsi_data AS (
		SELECT 
			subject_id,
			cohort_start_date,
			SUM(max_score) AS score

	} : {
	SELECT CAST(1000 + @analysis_id AS BIGINT) AS covariate_id,
	{@temporal} ? {
		CAST(NULL AS INT) AS time_id,
	}	
		row_id,
		SUM(max_score) AS covariate_value
	}
	FROM (
		SELECT dcsi_category,
			MAX(dcsi_score) AS max_score,
	{@aggregated} ? {
			cohort.subject_id,
			cohort.cohort_start_date
	} : {
			cohort.@row_id_field AS row_id
	}			
		FROM global_temp.@cohort_table cohort
		INNER JOIN global_temp.condition_era condition_era
			ON cohort.subject_id = condition_era.person_id
		INNER JOIN dcsi_scoring
			ON condition_concept_id = dcsi_scoring.dcsi_concept_id
	{@temporal} ? {		
		WHERE condition_era_start_date <= cohort.cohort_start_date
	} : {
		WHERE condition_era_start_date <= date_add(cohort.cohort_start_date, @end_day)
	}
	{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
	{@aggregated} ? {
		GROUP BY subject_id,
			cohort_start_date,
			dcsi_category
	} : {
		GROUP BY cohort.@row_id_field,
			dcsi_category
	}
		) temp
	{@aggregated} ? {
	GROUP BY cohort.subject_id,
				cohort_start_date
	} : {
	GROUP BY row_id
	}

	{@aggregated} ? {
	), 
	t1 AS (
		SELECT COUNT(*) AS cnt 
		FROM global_temp.@cohort_table
	{@cohort_definition_id != -1} ? {	WHERE cohort_definition_id = @cohort_definition_id}
	),
	t2 AS (
		SELECT COUNT(*) AS cnt, 
			MIN(score) AS min_score, 
			MAX(score) AS max_score, 
			SUM(score) AS sum_score, 
			SUM(score*score) AS squared_score 
		FROM dcsi_data
	),
	dcsi_stats AS (
		SELECT CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
		t2.max_score AS max_value,
		CAST(t2.sum_score / (1.0 * t1.cnt) AS FLOAT) AS average_value,
		CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
		t2.cnt AS count_value,
		t1.cnt - t2.cnt AS count_no_value,
		t1.cnt AS population_size
		FROM t1 CROSS JOIN t2
	),
	dcsi_prep AS (
		SELECT score,
		COUNT(*) AS total,
		ROW_NUMBER() OVER (ORDER BY score) AS rn
		FROM dcsi_data
		GROUP BY score
	),
	dcsi_prep2 AS (
		SELECT 
			s.score,
			SUM(p.total) AS accumulated
		FROM dcsi_prep s
		INNER JOIN dcsi_prep p
			ON p.rn <= s.rn
		GROUP BY s.score
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
	FROM dcsi_prep2 p
	CROSS JOIN dcsi_stats o
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

