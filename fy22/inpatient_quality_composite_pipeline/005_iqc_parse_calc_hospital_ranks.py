import helpers


#choose hospital rank type.  Choose either 'Possible Rank' or 'Target Ranking: '
hospital_rank_type = 'Possible Rank'

time_period_type = 'NM_FSCL_YTD'

time_period_end_datetime = '2021-04-30 23:59:59.0000000'

result_df = helpers.parse_calculator_hosp_ranks(hospital_rank_type,time_period_type,time_period_end_datetime)

print(result_df)


#result_df.to_csv(r'S:\Datastore02\Analytics\200 NM Performance\Analytics Requests\iqc_dec_mortality_without_covid_pats\dec_orig_ranks_no_covid_pats.csv')


#helpers.insert_hospital_ranks(result_df)
