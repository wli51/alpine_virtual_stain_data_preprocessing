import pathlib
import pandas as pd
import re
import sys

import utils.loaddata_utils as ld_utils

batch_name = "SN0313537"
index_directory = pathlib.Path(f"/pl/active/koala/ALSF_pilot_data/{batch_name}")
index_directory.resolve(strict=True)
if not index_directory.exists() and not index_directory.is_dir():
    print(f"Index directory {index_directory} does not exist or is not a directory")
    sys.exit(1)


config_path = pathlib.Path("./config.yml")
config_path.resolve(strict=True)
if not config_path.exists() or not config_path.is_file():
    print(f"Config file {config_path} does not exist or is not a file")
    sys.exit(1)

output_csv_dir = pathlib.Path(f"/projects/wli19@xsede.org/alsf_preprocess/{batch_name}/loaddata_csvs")
output_csv_dir.mkdir(parents=True, exist_ok=True)

images_folders = list(index_directory.rglob("Images"))
plate_folders = list(index_directory.glob("Plate *"))

# Build mapping of BR00 IDs to plate number
br00_to_plate = {}

# First parse "All Cell Lines" folders to get BR00 IDs
for plate_folder in plate_folders:
    if "All Cell Lines" in plate_folder.name:
        for subfolder in plate_folder.iterdir():
            if subfolder.is_dir():
                br00_id = subfolder.name.split("__")[0]
                plate_num = plate_folder.name.split()[1]
                br00_to_plate[br00_id] = plate_num

# Now process everything
for plate_folder in plate_folders:
    for subfolder in plate_folder.iterdir():
        if not subfolder.is_dir():
            continue

        br00_id = subfolder.name.split("__")[0]

        if "All Cell Lines" in plate_folder.name:
            # Original run
            plate_name = f"{br00_id}"
        else:
            # Reimaged
            parts = plate_folder.name.split()
            plate_num = parts[1]
            cell_line = (
                " ".join(parts[2:]).replace("Reimage", "").strip().replace(" ", "_")
            )
            plate_name = f"{br00_id}_{cell_line}_Reimage"

        path_to_output_csv = (
            output_csv_dir / f"{plate_name}_loaddata_original.csv"
        ).absolute()

        ld_utils.create_loaddata_csv(
            index_directory=subfolder / "Images",
            config_path=config_path,
            path_to_output=path_to_output_csv,
        )
        print(f"Created LoadData CSV for {plate_name} at {path_to_output_csv}")
