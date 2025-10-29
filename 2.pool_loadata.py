import pathlib
import pandas as pd
import re
import sys


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
if not output_csv_dir.exists():
    print(f"Output CSV directory {output_csv_dir} does not exist.")
    sys.exit(1)
output_csv_files = list(output_csv_dir.glob("*.csv"))
if not output_csv_files:
    print(f"No CSV files found in output directory {output_csv_dir}.")
    sys.exit(1)
else:
    print(f"Found {len(output_csv_files)} CSV files in output directory {output_csv_dir}.")

pooled_csv_dir = pathlib.Path(f"/projects/wli19@xsede.org/alsf_preprocess/{batch_name}/pooled_loaddata_csvs")
pooled_csv_dir.mkdir(parents=True, exist_ok=True)

# Identify unique BR00 IDs from the filenames
br00_pattern = re.compile(r"(BR00\d+)")
br00_ids = {br00_pattern.search(csv_file.stem).group(1) 
            for csv_file in output_csv_files 
            if br00_pattern.search(csv_file.stem)}
br00_ids = sorted(br00_ids, key=lambda x: int(x[4:]))
print(f"Found {len(br00_ids)} BR00 IDs: {br00_ids}")

br00_dataframes = {br_id: [] for br_id in br00_ids}
used_files = set()  # Store filenames used in the process
concat_files = []  # Track new concatenated CSV files

# Load one BR00 starting CSV that will have the correct column order
column_order = pd.read_csv(pathlib.Path(f"{output_csv_dir}/{list(br00_ids)[0]}_loaddata_original.csv"), nrows=0).columns.tolist()

# Step 4: Add 'Metadata_Reimaged' column and group by BR00 ID
for csv_file in output_csv_files:
    filename = csv_file.stem
    match = br00_pattern.search(filename)

    if match:
        br_id = match.group(1)

        # Read the CSV file into a DataFrame
        loaddata_df = pd.read_csv(csv_file)

        # Reorder DataFrame columns to match the correct column order
        loaddata_df = loaddata_df[column_order]  # Ensure the columns are in the correct order

        # Add 'Metadata_Reimaged' column based on filename
        loaddata_df['Metadata_Reimaged'] = 'Re-imaged' in filename

        # Append the DataFrame to the corresponding BR00 group
        br00_dataframes[br_id].append(loaddata_df)

        # Track this file as used
        used_files.add(csv_file.name)

example_id = next(iter(br00_dataframes))  # Get the first BR00 ID
example_df = pd.concat(br00_dataframes[example_id], ignore_index=True)
print(f"\nExample DataFrame for BR00 ID: {example_id}")
example_df.iloc[:, [0, 1, -1]] # Display only the first two and last column

# Concatenate DataFrames, drop duplicates, and save per BR00 ID
for br_id, dfs in br00_dataframes.items():
    if dfs:  # Only process if there are matching files
        concatenated_df = pd.concat(dfs, ignore_index=True)

        # Drop duplicates, prioritizing rows with 'Metadata_Reimaged' == True
        deduplicated_df = concatenated_df.sort_values(
            'Metadata_Reimaged', ascending=False
        ).drop_duplicates(subset=['Metadata_Well', 'Metadata_Site'], keep='first')

        # Sort by 'Metadata_Col', 'Metadata_Row', and 'Metadata_Site
        sorted_df = deduplicated_df.sort_values(
            ['Metadata_Col', 'Metadata_Row', "Metadata_Site"], ascending=True
        )

        # Save the cleaned, concatenated, and sorted DataFrame to a new CSV file
        output_path = pooled_csv_dir / f"{br_id}_concatenated.csv"
        sorted_df.to_csv(output_path, index=False)

        print(f"Saved: {output_path}")
        concat_files.append(output_path)  # Track new concatenated files
    else:
        print(f"No files found for {br_id}")

unused_files = set(csv_file.name for csv_file in output_csv_files) - used_files

if unused_files:
    print("Warning: Some files were not used in the concatenation!")
    for file in unused_files:
        print(f"Unused: {file}")
else:
    print("All files were successfully used.")
