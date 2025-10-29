import pathlib
import sys

import pandas as pd
from scipy.stats import zscore


QC_DIR = pathlib.Path("./whole_img_qc_output")
QC_DIR.resolve(strict=True)
if not QC_DIR.exists() and not QC_DIR.is_dir():
    print(f"QC directory {QC_DIR} does not exist.")
    sys.exit(1)

QC_OUTPUT_DIR = pathlib.Path("./qc_output")
QC_OUTPUT_DIR.resolve(strict=True)
QC_OUTPUT_DIR.mkdir(exist_ok=True)

target_channel_keys = ["OrigDNA", "OrigER", "OrigAGP", "OrigMito", "OrigRNA"]

# Create an empty dictionary to store data frames for each plate
all_qc_data_frames = {}

# List all plate directories
plates = [plate.name for plate in QC_DIR.iterdir() if plate.is_dir()]

# Loop through each plate
for plate in plates:
    # Read in CSV with all image quality metrics per image for the current plate
    qc_df = pd.read_csv(QC_DIR / plate / "Image.csv")

    # Store the data frame for the current plate in the dictionary
    all_qc_data_frames[plate] = qc_df

# Print the plate names to ensure they were loaded correctly
print(all_qc_data_frames.keys())

# Select the first plate in the list
first_plate = plates[0]
print(f"Showing example for the first plate: {first_plate}")

# Access the dataframe for the first plate
example_df = all_qc_data_frames[first_plate]

# Show the shape and the first few rows of the dataframe for the first plate
print(example_df.shape)

# Create an empty dictionary to store data frames for each channel
all_combined_dfs = {}

# Iterate through each channel
for channel in target_channel_keys: # excluding input Brightfield since the metrics are not robust to this type of channel
    # Create an empty list to store data frames for each plate
    plate_dfs = []

    # Iterate through each plate and create the specified data frame for the channel
    for plate, qc_df in all_qc_data_frames.items():
        plate_df = qc_df.filter(like="Metadata_").copy()

        # Add PowerLogLogSlope column (blur metric)
        plate_df["ImageQuality_PowerLogLogSlope"] = qc_df[
            f"ImageQuality_PowerLogLogSlope_{channel}"
        ]

        # Add PercentMaximal column (saturation metric)
        plate_df["ImageQuality_PercentMaximal"] = qc_df[
            f"ImageQuality_PercentMaximal_{channel}"
        ]

        # Add "Channel" column
        plate_df["Channel"] = channel

        # Add "Metadata_Plate" column
        plate_df["Metadata_Plate"] = plate

        # Append the data frame to the list
        plate_dfs.append(plate_df)

    # Concatenate data frames for each plate for the current channel
    all_combined_dfs[channel] = pd.concat(
        plate_dfs, keys=list(all_qc_data_frames.keys()), names=["Metadata_Plate", None]
    )

# Concatenate the channel data frames together for plotting
df = pd.concat(list(all_combined_dfs.values()), ignore_index=True)

print(df.shape)
print(df.head())

# Calculate Z-scores for the column with all plates
metric_z_thresh_dict = {
    "ImageQuality_PowerLogLogSlope": 2.5,
    "ImageQuality_PercentMaximal": 2,
}

total_plate_well_site = df[["Metadata_Plate", "Metadata_Well", "Metadata_Site"]].drop_duplicates()
removed_plate_well_site = pd.DataFrame()

for metric, z_thresh in metric_z_thresh_dict.items():
    z_scores = zscore(df[metric])
    outliers = df[abs(z_scores) > z_thresh]
    removed_plate_well_site = pd.concat(
        [removed_plate_well_site, outliers[["Metadata_Plate", "Metadata_Well", "Metadata_Site"]].drop_duplicates()]
    )

print(f"Out of a total of {total_plate_well_site.shape[0]} plate, well and site combos, {removed_plate_well_site.shape[0]} ({removed_plate_well_site.shape[0] * 100 / total_plate_well_site.shape[0]:.2f}%) removed due to low quality.")

removed_plate_well_site.to_csv(QC_OUTPUT_DIR / 'qc_exclusion.csv', index=False)
print("QC exclusion CSV saved.")
