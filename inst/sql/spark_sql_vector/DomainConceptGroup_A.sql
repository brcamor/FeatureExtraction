SELECT @analysis_id AS analysis_id,
	CAST('@analysis_name' AS VARCHAR(512)) AS analysis_name,
	CAST('@domain_id' AS VARCHAR(20)) AS domain_id,
{!@temporal} ? {
{@start_day == 'anyTimePrior'} ? {
	CAST(NULL AS INT) AS start_day,
} : {
	@start_day AS start_day,
}
	@end_day AS end_day,
}
	CAST('Y' AS VARCHAR(1)) AS is_binary,
	CAST(NULL AS VARCHAR(1)) AS missing_means_zero
