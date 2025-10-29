import pathlib
import sys
import yaml

import numpy as np
import pandas as pd


BATCH_NAME = "SN0313537"

QC_DIR = pathlib.Path("./whole_img_qc_output")
QC_DIR.resolve(strict=True)
if not QC_DIR.exists() and not QC_DIR.is_dir():
    print(f"QC directory {QC_DIR} does not exist.")
    sys.exit(1)

PLATEMAP_CSV_DIR = pathlib.Path("./metadata/platemaps")
PLATEMAP_CSV_DIR.resolve(strict=True)
if not PLATEMAP_CSV_DIR.exists() and not PLATEMAP_CSV_DIR.is_dir():
    print(f"Platemap CSV directory {PLATEMAP_CSV_DIR} does not exist.")
    sys.exit(1)

QC_FILE = pathlib.Path("./whole_img_qc_output/qc_exclusion.csv")
QC_FILE.resolve(strict=True)
if not QC_FILE.exists() and not QC_FILE.is_file():
    print(f"QC file {QC_FILE} does not exist.")
    sys.exit(1)

LOADDATA_CSV_DIR = pathlib.Path(f"../alsf_preprocess/{BATCH_NAME}/pooled_loaddata_csvs")
LOADDATA_CSV_DIR.resolve(strict=True)
if not LOADDATA_CSV_DIR.exists() and not LOADDATA_CSV_DIR.is_dir():
    print(f"LoadData CSV directory {LOADDATA_CSV_DIR} does not exist.")
    sys.exit(1)
if not list(LOADDATA_CSV_DIR.glob("*.csv")):
    print(f"No CSV files found in LoadData CSV directory {LOADDATA_CSV_DIR}.")
    sys.exit(1)


DATASPLIT_OUTPUT_DIR = pathlib.Path(f"../alsf_preprocess/{BATCH_NAME}/data_split_loaddata")
DATASPLIT_OUTPUT_DIR.mkdir(exist_ok=True)


# Whether to remove sites with low QC score
QC = True

# Define columns in loaddata
SITE_COLUMN = 'Metadata_Site'
WELL_COLUMN = 'Metadata_Well'
PLATE_COLUMN = 'Metadata_Plate'

# Wells are uniquely identified by the combination of these columns
UNIQUE_IDENTIFIERS = [SITE_COLUMN, WELL_COLUMN, PLATE_COLUMN]

# Condition for train data (every other condition will be saved for evaluation)
TRAIN_CONDITION_KWARGS = {
    'cell_line': 'U2-OS',
    'platemap_file': 'Assay_Plate1_platemap',
    'seeding_density': [1_000, 2_000, 4_000, 8_000, 12_000]
}
# Conditions are uniquely identified by the combination of keys from TRAIN_CONDITION_KWARGS
CONDITIONS = list(TRAIN_CONDITION_KWARGS.keys())


## Load platemap and well cell line metadata
barcode_df = pd.concat([pd.read_csv(f) for f in LOADDATA_CSV_DIR.glob('Barcode_*.csv')])

platemap_df = pd.DataFrame()
for platemap in barcode_df['platemap_file'].unique():
    df = pd.read_csv(LOADDATA_CSV_DIR / f'{platemap}.csv')
    df['platemap_file'] = platemap
    platemap_df = pd.concat([platemap_df, df])    
barcode_platemap_df = pd.merge(barcode_df, platemap_df, on='platemap_file', how='inner')

## QC removal
remove_sites = pd.read_csv(QC_FILE)

## Load data csvs
loaddata_df = pd.concat(
    [pd.read_csv(f) for f in LOADDATA_CSV_DIR.glob('*.csv')], 
    ignore_index=True)

## Merge loaddata with barcode/platemap metadata to map condition to well
loaddata_barcode_platemap_df = pd.merge(
    barcode_platemap_df.rename(columns={'barcode': PLATE_COLUMN, 'well': WELL_COLUMN}),
    loaddata_df,
    on=[PLATE_COLUMN, WELL_COLUMN], 
    how='left')

## Perform QC removal
if QC:
    print(f"{loaddata_barcode_platemap_df.shape[0]} sites prior to QC")
    # Merge to correctly identify rows to be removed
    qc_merge_df = loaddata_barcode_platemap_df.merge(
        remove_sites, 
        on=UNIQUE_IDENTIFIERS, 
        how='left', 
        indicator=True
        )

    # Keep only rows that were NOT found in remove_sites
    loaddata_barcode_platemap_df = qc_merge_df[qc_merge_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    print(f"{loaddata_barcode_platemap_df.shape[0]} sites after QC")


loaddata_barcode_platemap_train_df = loaddata_barcode_platemap_df.copy()
## Filter load data csvs dynamically with CONDITION_KWARGS
for k, v in TRAIN_CONDITION_KWARGS.items():
    if isinstance(v, list):
        loaddata_barcode_platemap_train_df = loaddata_barcode_platemap_train_df[loaddata_barcode_platemap_train_df[k].isin(v)]
    else:
        loaddata_barcode_platemap_train_df = loaddata_barcode_platemap_train_df[loaddata_barcode_platemap_train_df[k] == v]
    if len(loaddata_barcode_platemap_train_df) == 0:
        raise ValueError(f'No data found for {k}={v}')
print(f"{loaddata_barcode_platemap_train_df.shape[0]} sites for train and heldout")

loaddata_barcode_platemap_eval_df = loaddata_barcode_platemap_df.loc[
    ~loaddata_barcode_platemap_df.index.isin(loaddata_barcode_platemap_train_df.index)
]
print(f"{loaddata_barcode_platemap_eval_df.shape[0]} sites for evaluation")

seed = 42
np.random.seed(seed)

# Group by seeding density and cell line
grouped = loaddata_barcode_platemap_train_df.groupby(CONDITIONS)

# Initialize lists to store holdout and train data
heldout_list = []
train_list = []

# Iterate over each group
for _, group in grouped:

    held_out_well = [np.random.choice(group[WELL_COLUMN].unique())]
    train_wells = group[~group[WELL_COLUMN].isin(held_out_well)][WELL_COLUMN].unique()

    loaddata_held_out_df = group[group[WELL_COLUMN].isin(held_out_well)].copy()
    loaddata_train_df = group[group[WELL_COLUMN].isin(train_wells)].copy()

    condition = group[CONDITIONS].iloc[0].to_dict()
    print(f"For Condition: {condition} Heldout well: {held_out_well} Train wells: {train_wells}")

    heldout_list.append(loaddata_held_out_df)
    train_list.append(loaddata_train_df)

# Concatenate the lists into dataframes
loaddata_heldout_df = pd.concat(heldout_list).reset_index(drop=True)
print(f"{loaddata_heldout_df.shape[0]} sites Heldout")
loaddata_train_df = pd.concat(train_list).reset_index(drop=True)
print(f"{loaddata_train_df.shape[0]} sites for Training")

loaddata_heldout_df.to_csv(DATASPLIT_OUTPUT_DIR / 'loaddata_heldout.csv')
loaddata_train_df.to_csv(DATASPLIT_OUTPUT_DIR / 'loaddata_train.csv')
loaddata_barcode_platemap_eval_df.to_csv(DATASPLIT_OUTPUT_DIR / 'loaddata_eval.csv')
