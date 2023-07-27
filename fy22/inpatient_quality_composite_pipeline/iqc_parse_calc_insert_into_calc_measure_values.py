import helpers

time_period_type = 'VIZIENT_CALC_PLACEHOLDER'

time_period_end_datetime = '1900-01-01 00:00:00.0000000'

measures_and_links_df = helpers.parse_calculator_measures_and_links(time_period_type,time_period_end_datetime)


print(measures_and_links_df)


helpers.insert_into_calc_measure_values_tb(measures_and_links_df)