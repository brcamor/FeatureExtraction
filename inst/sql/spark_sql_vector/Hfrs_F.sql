--Hospital Frailty Risk Score (HFRS)
--Reference: Gilbert et al. “Development and Validation of a Hospital Frailty Risk Score Focusing on Older People in Acute Care Settings Using Electronic Hospital Records: An Observational Study.” The Lancet 391, no. 10132 (May 5, 2018): 1775–82. https://doi.org/10.1016/S0140-6736(18)30668-8.
@covariate_table AS (
	WITH hfrs_scoring AS (
		SELECT CAST('Dementia in Alzheimers disease' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			7.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'F00%'

		UNION ALL
		SELECT CAST('Hemiplegia' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			4.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'G81%'


		UNION ALL
		SELECT CAST('Alzheimers disease' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			4.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'G30%'


		UNION ALL
		SELECT CAST('Sequelae of cerebrovascular disease' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			3.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'I69%'

		UNION ALL
		SELECT CAST('Other nervous and musculoskeletal systems' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			3.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R29%'

		UNION ALL
		SELECT CAST('Other disorders of urinary system' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			3.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'N39%'

		UNION ALL
		SELECT CAST('Delirium' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			3.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'F05%'

		UNION ALL
		SELECT CAST('Unspecified fall' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			3.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'W19%'

		UNION ALL
		SELECT CAST('Superficial injury of head' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			3.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S00%'


		UNION ALL
		SELECT CAST('Unspecified haematuria' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			3.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R31%'


		UNION ALL
		SELECT CAST('Other bacterial agents' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.9 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'B96%'


		UNION ALL
		SELECT CAST('Other cognitive functions and awareness' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R41%'

		UNION ALL
		SELECT CAST('Abnormalities of gait and mobility' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R26%'


		UNION ALL
		SELECT CAST('Other cerebrovascular diseases' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'I67%'


		UNION ALL
		SELECT CAST('Convulsions not elsewhere classified' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R56%'


		UNION ALL
		SELECT CAST('Somnolence stupor and coma' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R40%'


		UNION ALL
		SELECT CAST('Complications of genitourinary prosthesis' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'T83%'


		UNION ALL
		SELECT CAST('Intracranial injury' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S06%'

		UNION ALL
		SELECT CAST('Fracture of shoulder and upper arm' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.3 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S42%'


		UNION ALL
		SELECT CAST('fluid electrolyte and acid base balance' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.3 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'E87%'


		UNION ALL
		SELECT CAST('Other joint disorders' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.3 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'M25%'


		UNION ALL
		SELECT CAST('Volume depletion' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.3 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'E86%'


		UNION ALL
		SELECT CAST('Senility' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R54%'


		UNION ALL
		SELECT CAST('rehabilitation procedures' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z50%'


		UNION ALL
		SELECT CAST('Unspecified dementia' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'F03%'


		UNION ALL
		SELECT CAST('Other fall on same level' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'W18%'


		UNION ALL
		SELECT CAST('Problems related to medical facilities' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z75%'

		UNION ALL
		SELECT CAST('Vascular dementia' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'F01%'

		UNION ALL
		SELECT CAST('Superficial injury of lower leg' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S80%'
		--completed until here

		UNION ALL
		SELECT CAST('Cellulitis' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			2.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'L03%'

		UNION ALL
		SELECT CAST('Blindness and low vision' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.9 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'H54%'

		UNION ALL
		SELECT CAST('Deficiency of other B group vitamins' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.9 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'E53%'

		UNION ALL
		SELECT CAST('Problems related to social environment' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z60%'

		UNION ALL
		SELECT CAST('Parkinsons disease' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'G20%'

		UNION ALL
		SELECT CAST('Syncope and collapse' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R55%'

		UNION ALL
		SELECT CAST('Fracture of rib sternum and thoracic spine' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S22%'

		UNION ALL
		SELECT CAST('Other functional intestinal disorders' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'K59%'

		UNION ALL
		SELECT CAST('Acute renal failure' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'N17%'

		UNION ALL
		SELECT CAST('Decubitus ulcer' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'L89%'

		UNION ALL
		SELECT CAST('Carrier of infectious disease' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z22%'

		UNION ALL
		SELECT CAST('Streptococcus and staphylococcus' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'B95%'

		UNION ALL
		SELECT CAST('Ulcer of lower limb' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'L97%'

		UNION ALL
		SELECT CAST('Other symptoms involving general sensations and perceptions' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R44%'

		UNION ALL
		SELECT CAST('Duodenal ulcer' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'K26%'

		UNION ALL
		SELECT CAST('Hypotension' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'I95%'

		UNION ALL
		SELECT CAST('Unspecified renal failure' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'N19%'

		UNION ALL
		SELECT CAST('Other septicaemia' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'A41%'

		UNION ALL
		SELECT CAST('Personal history of other diseases and conditions' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z87%'

		UNION ALL
		SELECT CAST('Respiratory failure' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'J96%'

		UNION ALL
		SELECT CAST('Exposure to unspecified factor' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'X59%'

		UNION ALL
		SELECT CAST('Other arthrosis' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'M19%'

		UNION ALL
		SELECT CAST('Epilepsy' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'G40%'

		UNION ALL
		SELECT CAST('Osteoporosis without pathological fracture' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'M81%'

		UNION ALL
		SELECT CAST('Fracture of femur' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S72%'

		UNION ALL
		SELECT CAST('Fracture of lumbar spine and pelvis' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S32%'

		UNION ALL
		SELECT CAST('Other disorders of pancreatic internal secretion' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'E16%'

		UNION ALL
		SELECT CAST('Abnormal results of function studies' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R94%'

		UNION ALL
		SELECT CAST('Chronic renal failure' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'N18%'


		UNION ALL
		SELECT CAST('Retention of urine' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.3 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R33%'

		UNION ALL
		SELECT CAST('Unknown and unspecified causes of morbidity' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.3 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R69%'

		UNION ALL
		SELECT CAST('Other disorders of kidney and ureter' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.3 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'N28%'


		UNION ALL
		SELECT CAST('Unspecified urinary incontinence' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R32%'

		UNION ALL
		SELECT CAST('Other degenerative diseases of nervous system' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'G31%'

		UNION ALL
		SELECT CAST('Nosocomial condition' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Y95%'

		--completed from here
		UNION ALL
		SELECT CAST('Other and unspecified injuries of head' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S09%'

		UNION ALL
		SELECT CAST('Symptoms and signs involving emotional state' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R45%'

		UNION ALL
		SELECT CAST('Transient cerebral ischaemic attacks' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.2 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'G45%'

		UNION ALL
		SELECT CAST('Problems related to careprovider dependency' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z74%'

		UNION ALL
		SELECT CAST('Other soft tissue disorders not elsewhere classified' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'M79%'

		UNION ALL
		SELECT CAST('Fall involving bed' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'W06%'

		UNION ALL
		SELECT CAST('Open wound of head' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S01%'

		UNION ALL
		SELECT CAST('Other bacterial intestinal infections' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'A04%'

		UNION ALL
		SELECT CAST('Infectious Diarrhoea and gastroenteritis' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'A09%'

		UNION ALL
		SELECT CAST('Pneumonia organism unspecified' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'J18%'

		UNION ALL
		SELECT CAST('Pneumonitis due to solids and liquids' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'J69%'

		UNION ALL
		SELECT CAST('Speech disturbances not elsewhere classified' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R47%'

		UNION ALL
		SELECT CAST('Vitamin D deficiency' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'E55%'

		UNION ALL
		SELECT CAST('Artificial opening status' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z93%'

		UNION ALL
		SELECT CAST('Gangrene not elsewhere classified' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			1.0 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R02%'

		UNION ALL
		SELECT CAST('Symptoms and signs concerning food and fluid intake' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.9 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R63%'

		UNION ALL
		SELECT CAST('Other hearing loss' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.9 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'H91%'

		UNION ALL
		SELECT CAST('Fall on and from stairs and steps' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.9 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'W10%'

		UNION ALL
		SELECT CAST('Fall on same level from slipping tripping and stumbling' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.9 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'W01%'

		UNION ALL
		SELECT CAST('Thyrotoxicosis' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.9 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'E05%'

		UNION ALL
		SELECT CAST('Scoliosis' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.9 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'M41%'

		UNION ALL
		SELECT CAST('Dysphagia' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R13%'

		UNION ALL
		SELECT CAST('Dependence on enabling machines and devices' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z99%'

		UNION ALL
		SELECT CAST('Agent resistant to penicillin and related antibiotics' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'U80%'

		UNION ALL
		SELECT CAST('Osteoporosis with pathological fracture' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'M80%'

		UNION ALL
		SELECT CAST('Other diseases of digestive system' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'K92%'

		UNION ALL
		SELECT CAST('Cerebral Infarction' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.8 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'I63%'

		UNION ALL
		SELECT CAST('Calculus of kidney and ureter' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'N20%'

		UNION ALL
		SELECT CAST('Mental and behavioural disorders due to use of alcohol' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'F10%'

		UNION ALL
		SELECT CAST('Other medical procedures causing abnormal reaction' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Y84%'

		UNION ALL
		SELECT CAST('Abnormalities of heart beat' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R00%'

		UNION ALL
		SELECT CAST('Unspecified acute lower respiratory infection' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.7 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'J22%'

		UNION ALL
		SELECT CAST('Problems related to lifemanagement difficulty' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z73%'

		UNION ALL
		SELECT CAST('Other abnormal findings of blood chemistry' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.6 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R79%'

		UNION ALL
		SELECT CAST('Personal history of riskfactors' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'Z91%'

		UNION ALL
		SELECT CAST('Open wound of forearm' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'S51%'

		UNION ALL
		SELECT CAST('Depressive episode' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'F32%'

		UNION ALL
		SELECT CAST('Spinal stenosis secondary code only' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.5 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'M48%'

		UNION ALL
		SELECT CAST('Disorders of mineral metabolism' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'E83%'

		UNION ALL
		SELECT CAST('Polyarthrosis' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'M15%'

		UNION ALL
		SELECT CAST('Other anaemias' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'D64%'

		UNION ALL
		SELECT CAST('Other local infections of skin and subcutaneous tissue' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.4 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'L08%'

		UNION ALL
		SELECT CAST('Nausea and vomiting' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.3 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R11%'

		UNION ALL
		SELECT CAST('Other noninfective gastroenteritis and colitis' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.3 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'K52%'

		UNION ALL
		SELECT CAST('Fever of unknown origin' AS VARCHAR(255)) AS hfrs_category,
			CAST(source_code AS VARCHAR(255)) AS hfrs_icd10_code,
			target_concept_id AS hfrs_concept_id,
			0.1 AS hfrs_score
		FROM (
			SELECT source.concept_code AS source_code,
				target.concept_id AS target_concept_id
			FROM concept_relationship concept_relationship
			INNER JOIN concept source
				ON source.concept_id = concept_relationship.concept_id_1
			INNER JOIN concept target
				ON target.concept_id = concept_relationship.concept_id_2
			WHERE source.vocabulary_id = 'ICD10'
				AND target.vocabulary_id = 'SNOMED'
				AND relationship_id = 'Maps to'
			) source_to_concept_map
		WHERE source_code LIKE 'R50%'
	)
	--completed until here
	-- Feature construction
	{@aggregated} ? {
	, hfrs_data AS (
	SELECT subject_id,
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
		SELECT hfrs_category,
			MAX(hfrs_score) AS max_score,
	{@aggregated} ? {
			cohort.subject_id,
			cohort.cohort_start_date
	} : {
			cohort.@row_id_field AS row_id
	}			
		FROM @cohort_table cohort
		INNER JOIN condition_era condition_era
			ON cohort.subject_id = condition_era.person_id
		INNER JOIN hfrs_scoring
			ON condition_concept_id = hfrs_scoring.hfrs_concept_id
	{@temporal} ? {		
		WHERE condition_era_start_date <= cohort.cohort_start_date
	} : {
		WHERE condition_era_start_date <= date_add(cohort.cohort_start_date, @end_day)
	}
	{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id = @cohort_definition_id}
	{@aggregated} ? {
		GROUP BY subject_id,
			cohort_start_date,
			hfrs_category
	} : {
		GROUP BY cohort.@row_id_field,
			hfrs_category
	}
		) temp
	{@aggregated} ? {
	GROUP BY subject_id,
				cohort_start_date
	} : {
	GROUP BY row_id
	}	


	{@aggregated} ? {
	), 
	t1 AS (
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
		FROM hfrs_data
	),
	hfrs_stats AS (
		SELECT CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
			t2.max_score AS max_value,
			CAST(t2.sum_score / (1.0 * t1.cnt) AS FLOAT) AS average_value,
			CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
			t2.cnt AS count_value,
			t1.cnt - t2.cnt AS count_no_value,
			t1.cnt AS population_size
		FROM t1 CROSS JOIN t2
	),
	hfrs_prep AS (
		SELECT score,
			COUNT(*) AS total,
			ROW_NUMBER() OVER (ORDER BY score) AS rn
		FROM hfrs_data
		GROUP BY score
	),
	hfrs_prep2 AS (
		SELECT s.score,
			SUM(p.total) AS accumulated
		FROM hfrs_prep s
		INNER JOIN hfrs_prep p
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
	FROM hfrs_prep2 p
	CROSS JOIN hfrs_stats o
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

