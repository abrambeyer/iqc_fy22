import helpers

#choose domain rank type.  Choose either 'Domain Rank Result' or 'Target Domain Ranking'
domain_rank_type = 'Domain Rank Result'

time_period_type = 'NM_FSCL_YTD'

time_period_end_datetime = '2021-04-30 23:59:59.0000000'


domain_result_df = helpers.parse_calculator_domain_ranks(domain_rank_type,time_period_type,time_period_end_datetime)


print(domain_result_df)
#helpers.insert_domain_ranks(domain_result_df)

