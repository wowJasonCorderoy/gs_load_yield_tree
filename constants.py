PROJECT_ID = 'gcp-wow-pvc-grnstck-dev'
DATASET_NAME = 'optimiser' # bq dataset (unappended) to save to
SPECIES = 'lamb'

GCP_CREDENTIALS_FILE = {
    'gcp-wow-pvc-grnstck-prod' : r"C:\dev\greenstock\optimiser_files\key_prod.json",
    'gcp-wow-pvc-grnstck-dev' : r"C:\dev\greenstock\optimiser_files\key_dev.json",
}[PROJECT_ID] # if file extension exists then local run else will just run in cloud.

PREPEND_DATASET_NAME = { # append value (if any) to dataset name based on project
    'gcp-wow-pvc-grnstck-prod':'',
    'gcp-wow-pvc-grnstck-dev':'dev_',
    'gcp-wow-pvc-grnstck-uat':'uat_',
    'gcp-wow-pvc-grnstck-test':'test_',
}[PROJECT_ID]

DS_TABLE_T_SAVE_TO = f"{PREPEND_DATASET_NAME}{DATASET_NAME}.{SPECIES}_yield_tree_specs"
