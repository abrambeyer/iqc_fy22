import helpers

time_period_type = 'NM_FSCL_YTD'

time_period_end_datetime = '2021-04-30 23:59:59.0000000'

what_if_result_df = helpers.parse_calculator_what_if_section(time_period_type,time_period_end_datetime)


print(what_if_result_df)
#what_if_result_df.to_csv('test_wi.csv')
#helpers.insert_what_if_section(what_if_result_df)



