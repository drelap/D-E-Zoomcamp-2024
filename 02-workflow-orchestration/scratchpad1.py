from mage_ai.data_preparation.variable_manager import get_variable

df = get_variable('homework2_pipeline', 'load_ny_green_taxi', 'output_0')
print("Columns before transform : ")
print(df.columns)