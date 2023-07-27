import helpers



measure_weight_df = helpers.parse_calculator_measure_weights()

print(measure_weight_df)

helpers.insert_measure_weights(measure_weight_df)
