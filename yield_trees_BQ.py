import pandas as pd
import os
import constants as cnst
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

def connectBQ():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            cnst.GCP_CREDENTIALS_FILE, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials, project=cnst.PROJECT_ID)
    except:
        client = bigquery.Client(project=cnst.PROJECT_ID)
    return client

def clean_primal_code(primal_code):

    original_code = primal_code
    # Remove any added '-something' at the end of the code
    if '-' in primal_code:
        primal_code = primal_code.split('-')[0]

    # Sometimes numbers are imported as float format, remove the .0
    if '.' in primal_code:
        primal_code = primal_code.split('.')[0]

    # Remove any non-numbers
    numbers = ''
    for char in primal_code:
        if char.isdigit():
            numbers = numbers + char
    if primal_code != numbers:
    	primal_code = numbers

    # If we ended up with no numbers treat the code as nan
    if primal_code == '':
        primal_code = 'nan'

    return primal_code


d = {
	'694501_-_Lamb_Carcase_-_Frews_-_Corrected_Rack_Yields_-_Version_4.7.xlsx' : 'Frews',
	'694501_-_Lamb_Carcase_-_Junee_-_Corrected_Rack_Yields_-_Version_5.0.xlsx' : 'Junee',
	'694501_-_Lamb_Carcase_-_V_V_Walsh_-_Corrected_Rack_Yields_-_Version_2.2 (1).xlsx' : 'Walsh',
	}

forDF = []

xlFiles = [f for f in os.listdir(os.getcwd()) if "xlsx" in f]
cwd = os.getcwd()
for xl in xlFiles:
	print(xl)
	yield_tree_raw_data = pd.read_excel(xl)

	species = 'lamb'
	abattoir_name = d[xl]
	columns = ['species','abattoir','level','parent_product_code','product_name','product_code','meat','primary_drop', 'yield', 'rank_a', 'rank_b', 'rank_c']
	rank_1 = 0
	rank_2 = 1
	rank_3 = 1
	for i, row in yield_tree_raw_data.iterrows():
		code = str(row['Product Code'])
		name = str(row['Product Name'])
		yield_pct = row['Yield']
		primary = row['Primary drop']
		meat = row['Meat / Non-Meat']

		if code != 'nan':
			code = clean_primal_code(code)
		# level 1 data
		if(primary > 0):
			rank_1 = rank_1 + 1
			rank_2 = 1
			rank_3 = 1
			level = 1
			parent_code = None
			r = [species, abattoir_name, level, parent_code, name, code, meat, primary, yield_pct, rank_1, rank_2, rank_3]
			o_parent = code
			forDF.append(r)
		# level 2 data
		elif(np.isnan(primary) and code != 'nan'):
			rank_2 = rank_2 + 1
			rank_3 = 1
			level = 2
			parent_code = o_parent
			r = [species, abattoir_name, level, parent_code, name, code, meat, primary, yield_pct, rank_1, rank_2, rank_3]
			t_parent = code
			forDF.append(r)
		# level 3 data
		elif(code == 'nan' and name != 'nan'):
			rank_3 = rank_3 + 1
			level = 3
			parent_code = t_parent
			code = name[0:6]
			code = code.strip()
			name = name.replace(code + " - ", "").strip()
			r = [species, abattoir_name, level, parent_code, name, code, meat, primary, yield_pct, rank_1, rank_2, rank_3]
			forDF.append(r)

out_df = pd.DataFrame(forDF, columns = columns)

print(f"saving to {cnst.DS_TABLE_T_SAVE_TO}")
job_config = bigquery.LoadJobConfig(
	write_disposition="WRITE_TRUNCATE",
)
client = connectBQ()
job = client.load_table_from_dataframe(
	out_df, cnst.DS_TABLE_T_SAVE_TO, job_config=job_config
)
print(f"saved to {cnst.DS_TABLE_T_SAVE_TO}")

