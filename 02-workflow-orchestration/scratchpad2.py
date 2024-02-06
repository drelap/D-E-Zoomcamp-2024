from mage_ai.data_preparation.variable_manager import get_variable

df = get_variable('homework2_pipeline', 'transform_ny_green_taxi', 'output_0')
tmpset = set(df['vendor_id'])
print("Values of vendor_id : ")
print(tmpset)