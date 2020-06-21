@covariate_table AS (

	WITH groups AS (
		{@domain_table == 'drug_exposure' | @domain_table == 'drug_era'} ? {
			SELECT DISTINCT 
				descendant_concept_id,
				ancestor_concept_id
			FROM `@vocab_path/concept_ancestor`
			INNER JOIN `@vocab_path/concept`
				ON ancestor_concept_id = concept_id
			WHERE ((vocabulary_id = 'ATC'
					AND LEN(concept_code) IN (1, 3, 4, 5))
				OR (standard_concept = 'S' 
			{@domain_table == 'drug_era'} ? {		AND concept_class_id = 'Ingredient'}
					AND domain_id = 'Drug'))
				AND concept_id != 0
			{@excluded_concept_table != ''} ? {	AND descendant_concept_id NOT IN (SELECT id FROM @excluded_concept_table)}
			{@included_concept_table != ''} ? {	AND descendant_concept_id IN (SELECT id FROM @included_concept_table)}
			{@excluded_concept_table != ''} ? {	AND ancestor_concept_id NOT IN (SELECT id FROM @excluded_concept_table)}
			{@included_concept_table != ''} ? {	AND ancestor_concept_id IN (SELECT id FROM @included_concept_table)}
		}

		{@domain_table == 'condition_occurrence' | @domain_table == 'condition_era'} ? {
			SELECT DISTINCT 
				descendant_concept_id,
				ancestor_concept_id
			FROM `@vocab_path/concept_ancestor`
			INNER JOIN (
				SELECT concept_id
				FROM `@vocab_path/concept`
				INNER JOIN (
				SELECT *
				FROM `@vocab_path/concept_ancestor`
				WHERE ancestor_concept_id = 441840 /* SNOMED clinical finding */
				AND (min_levels_of_separation > 2
					OR descendant_concept_id IN (433736, 433595, 441408, 72404, 192671, 137977, 434621, 437312, 439847, 4171917, 438555, 4299449, 375258, 76784, 40483532, 4145627, 434157, 433778, 258449, 313878)
					) 
				) temp
				ON concept_id = descendant_concept_id
				WHERE concept_name NOT LIKE '%finding'
					AND concept_name NOT LIKE 'Disorder of%'
					AND concept_name NOT LIKE 'Finding of%'
					AND concept_name NOT LIKE 'Disease of%'
					AND concept_name NOT LIKE 'Injury of%'
					AND concept_name NOT LIKE '%by site'
					AND concept_name NOT LIKE '%by body site'
					AND concept_name NOT LIKE '%by mechanism'
					AND concept_name NOT LIKE '%of body region'
					AND concept_name NOT LIKE '%of anatomical site'
					AND concept_name NOT LIKE '%of specific body structure%'
					AND domain_id = 'Condition'
			{@excluded_concept_table != ''} ? {		AND concept_id NOT IN (SELECT id FROM @excluded_concept_table)}
			{@included_concept_table != ''} ? {		AND concept_id IN (SELECT id FROM @included_concept_table)}
			) valid_groups
				ON ancestor_concept_id = valid_groups.concept_id
			{@excluded_concept_table != '' | @included_concept_table != ''} ? {
			WHERE 
			{@excluded_concept_table != ''} ? {	
				ancestor_concept_id NOT IN (SELECT id FROM @excluded_concept_table)
				AND descendant_concept_id NOT IN (SELECT id FROM @excluded_concept_table)
			}
			{@included_concept_table != ''} ? {
			{@excluded_concept_table != ''} ? {	AND } : {	}ancestor_concept_id IN (SELECT id FROM @included_concept_table)
				AND descendant_concept_id IN (SELECT id FROM @included_concept_table)
			}
			}
		}
	)

	-- Feature construction
	SELECT 
		CAST(ancestor_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
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
		SELECT DISTINCT ancestor_concept_id,
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
		INNER JOIN groups
			ON @domain_concept_id = descendant_concept_id
	{@temporal} ? {
		INNER JOIN #time_period time_period
			ON @domain_start_date <= date_add(cohort.cohort_start_date, time_period.end_day)
			AND @domain_end_date >= date_add(cohort.cohort_start_date, time_period.start_day)
		WHERE @domain_concept_id != 0
	} : {
		WHERE @domain_start_date <= date_add(cohort.cohort_start_date, @end_day)
	{@start_day != 'anyTimePrior'} ? {				AND @domain_end_date >= date_add(cohort.cohort_start_date, @start_day)}
			AND @domain_concept_id != 0
	}
	{@sub_type == 'inpatient'} ? {	AND condition_type_concept_id IN (38000183, 38000184, 38000199, 38000200)}
	{@included_cov_table != ''} ? {		AND CAST(ancestor_concept_id AS BIGINT) * 1000 + @analysis_id IN (SELECT id FROM @included_cov_table)}
	{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
	) temp
	{@aggregated} ? {		
	GROUP BY ancestor_concept_id
	{@temporal} ? {
		,time_id
	}	
	}
)