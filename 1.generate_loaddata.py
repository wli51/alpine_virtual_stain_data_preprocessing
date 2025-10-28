import pathlib
import pandas as pd
import re
import sys

import utils.loaddata_utils as ld_utils

batch_name = "SN0313537"
index_directory = pathlib.Path(f"/pl/active/koala/ALSF_pilot_data/{batch_name}/")
index_directory.resolve(strict=True)
if not index_directory.exists() and not index_directory.is_dir():
    print(f"Index directory {index_directory} does not exist or is not a directory")
    sys.exit(1)
print(f"Index directory: {index_directory}")

config_dir_path = pathlib.Path("./config_files").absolute()
config_dir_path.resolve(strict=True)
if not config_dir_path.exists() or not config_dir_path.is_dir():
    print(f"Config directory {config_dir_path} does not exist or is not a directory")
    sys.exit(1)

output_csv_dir = pathlib.Path(f"/projects/wli19@xsede.org/alsf_preprocess/{batch_name}/loaddata_csvs")
output_csv_dir.mkdir(parents=True, exist_ok=True)
print(f"Output CSV directory: {output_csv_dir}")

images_folders = list(index_directory.rglob('Images'))

# Loop through each folder and create a LoadData CSV
for folder in images_folders:
    # Get the first folder directly under the index_directory
    relative_path = folder.relative_to(index_directory)
    first_folder = relative_path.parts[0]  # First-level folder
    
    # Generate the plate name and find matching config file based on folder structure
    if first_folder.startswith('BR00'):
        plate_name = first_folder.split('_')[0]  # Take the first part
        config_path = config_dir_path / "config.yml"  # Use default config for BR00

    elif first_folder.startswith('2024'):
        second_folder = relative_path.parts[1]  # Second-level folder
        part1 = '_'.join(first_folder.split('_')[-2:])  # Last two parts of first folder ("CellLine_Re-imaged")
        part2 = second_folder.split('_')[0]  # First part of second folder (BR00 ID)

        # Combine to make plate name for saving CSV
        plate_name = f"{part1}_{part2}" 

        # Find the matching config file by matching part1 with the config file's prefix
        matching_configs = list(config_dir_path.glob(f"{part1.split('_')[0]}_*.yml"))
        
        # Debugging print to check if matching config files are found
        print(f"Matching configs found: {matching_configs}")

        if matching_configs:
            config_path = matching_configs[0]  # Take the first match
        else:
            print(f"No matching config file for: {part1}")
            continue  # Skip if no matching config file

    else:
        print(f"Unexpected folder pattern: {folder}")
        continue  # Skip if not matching patterns

    # Create LoadData output path per plate
    path_to_output_csv = (output_csv_dir / f"{plate_name}_loaddata_original.csv").absolute()

    # Call the function to create the LoadData CSV
    ld_utils.create_loaddata_csv(
        index_directory=folder,
        config_path=config_path,  # Use the matched config file
        path_to_output=path_to_output_csv,
    )
