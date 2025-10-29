import pathlib
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from virtual_stain_flow.transforms.normalizations import MaxScaleNormalize
from virtual_stain_flow.datasets.cp_loaddata_dataset import CPLoadDataImageDataset
from virtual_stain_flow.datasets.crop_cell_dataset import CropCellImageDataset


BATCH_NAME = "SN0313537"

DATASPLIT_OUTPUT_DIR = pathlib.Path(f"../alsf_preprocess/{BATCH_NAME}/data_split_loaddata")
DATASPLIT_OUTPUT_DIR.resolve(strict=True)

if not DATASPLIT_OUTPUT_DIR.exists() and not DATASPLIT_OUTPUT_DIR.is_dir():
    print(f"Data split output directory {DATASPLIT_OUTPUT_DIR} does not exist.")
    sys.exit(1)

LOADDATA_FILE_PATH = DATASPLIT_OUTPUT_DIR / "loaddata_train.csv"
if not LOADDATA_FILE_PATH.exists() and not LOADDATA_FILE_PATH.is_file():
    print(f"LoadData file {LOADDATA_FILE_PATH} does not exist.")
    sys.exit(1)    

SC_FEATURES_DIR = pathlib.Path(
    f"/pl/active/koala/ALSF_pilot_data/PREPROCESSED_PROFILES_{BATCH_NAME}/single_cell_profiles"
)
SC_FEATURES_DIR.resolve(strict=True)
if not SC_FEATURES_DIR.exists() and not SC_FEATURES_DIR.is_dir():
    print(f"Single-cell features directory {SC_FEATURES_DIR} does not exist.")
    sys.exit(1)
if not list(SC_FEATURES_DIR.glob("*.parquet")):
    print(f"No parquet files found in {SC_FEATURES_DIR}.")
    sys.exit(1)

INPUT_CHANNEL_NAMES = ["OrigBrightfield"]
TARGET_CHANNEL_NAMES = ["OrigDNA", "OrigER", "OrigAGP", "OrigMito", "OrigRNA"]

CONFLUENCE = 1000

CONFLUENCE = 1000

loaddata_df = pd.read_csv(LOADDATA_FILE_PATH)
print(f"Initial loaddata_df shape: {loaddata_df.shape}")
loaddata_df = loaddata_df.loc[loaddata_df['seeding_density'] == CONFLUENCE]
print(f"Filtered loaddata_df shape: {loaddata_df.shape}")

sc_feature_files = list(
        SC_FEATURES_DIR.glob('*_sc_normalized.parquet')
    )

sc_features = pd.DataFrame()
for plate in loaddata_df['Metadata_Plate'].unique():
    sc_features_parquet = SC_FEATURES_DIR / f'{plate}_sc_normalized.parquet'
    if not sc_features_parquet.exists():
        print(f'{sc_features_parquet} does not exist, skipping...')
        continue 
    else:
        sc_features = pd.concat([
            sc_features, 
            pd.read_parquet(
                sc_features_parquet,
                columns=['Metadata_Plate', 'Metadata_Well', 'Metadata_Site', 'Metadata_Cells_Location_Center_X', 'Metadata_Cells_Location_Center_Y']
            )
        ])
print(f"Single-cell features shape: {sc_features.shape}")


def plot_dataset(ids, i, save_path):
    i, t = ids[i]
    fig, axes = plt.subplots(1, 2, figsize=(10, 5))
    axes[0].imshow(i[0], cmap='gray')
    axes[0].set_title(f'Input Image: {i.shape}')
    axes[0].set_xticks([])
    axes[0].set_yticks([])
    axes[1].imshow(t[0], cmap='gray')
    axes[1].set_title(f'Target Image: {t.shape}')
    axes[1].set_xticks([])
    axes[1].set_yticks([])
    plt.suptitle('Example Input and Target Images')
    plt.tight_layout()
    plt.savefig(save_path)


cp_ids = CPLoadDataImageDataset(
    loaddata=loaddata_df,
    sc_feature=sc_feature_files,
    pil_image_mode='I;16',
)
cp_ids.input_channel_keys = ['OrigBrightfield']
cp_ids.target_channel_keys = ['OrigDNA']
cp_ids.transform = MaxScaleNormalize(
    p=1, 
    normalization_factor=2**16-1
)
print(f"Number of images in dataset: {len(cp_ids)}")
plot_dataset(cp_ids, 0, f'demo_whole_{CONFLUENCE}.png')

crop_ds = CropCellImageDataset.from_dataset(
    cp_ids,
    patch_size=256,
    object_coord_x_field='Metadata_Cells_Location_Center_X',
    object_coord_y_field='Metadata_Cells_Location_Center_Y',
    fov=(1080, 1080)
)
crop_ds.input_channel_keys = INPUT_CHANNEL_NAMES
crop_ds.target_channel_keys = TARGET_CHANNEL_NAMES
crop_ds.transform = MaxScaleNormalize(
    p=1, 
    normalization_factor=2**16 - 1,
)
print(f"Number of cropped cells in dataset: {len(crop_ds)}")
plot_dataset(crop_ds, 0, f'demo_cropped_{CONFLUENCE}.png')
