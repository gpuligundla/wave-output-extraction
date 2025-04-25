import pandas as pd
import os
from pathlib import Path
import logging
import sys
from datetime import datetime
import warnings
import traceback

warnings.simplefilter("error", category=RuntimeWarning)

logger = logging.getLogger(__name__)


def setup_logger():
    """Setup logger for the extraction"""

    if not os.path.exists("logs"):
        os.makedirs("logs")
        print("Created logs directory: logs")

    log_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_extraction.log"
    logging.basicConfig(
        filename=os.path.join("logs", log_filename),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    # Add console handler to show logs in the terminal as well
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)


def convert_xls_to_csv(xls_file):
    """Convert XLS file to CSV with UTF-8 encoding"""

    csv_output = os.path.splitext(xls_file)[0] + "_output.csv"
    logger.info(f"Converting {xls_file} to CSV")
    try:
        # Read the Excel file
        df = pd.read_excel(xls_file, sheet_name="Sheet2")

        # Save as CSV with UTF-8 encoding
        df.to_csv(csv_output, index=False, encoding="utf-8")
        logger.info(f"Successfully converted to {csv_output}")
        return csv_output
    except Exception as e:
        stack_trace = traceback.format_exc()
        logger.error(f"Error converting {xls_file} to CSV: {str(e)}\n{stack_trace}")
        raise


def find_table_start(df, marker, column_idx=3, exact_match=True):
    """Find the starting row index of a table based on a marker"""
    if exact_match:
        indices = df.index[df.iloc[:, column_idx] == marker].tolist()
    else:
        indices = df.index[
            df.iloc[:, column_idx].astype(str).str.contains(marker, na=False)
        ].tolist()

    if indices:
        return indices[0]
    return None


def find_next_empty_row(df, start_row):
    """Find the next empty row after start_row"""
    for i in range(start_row + 1, len(df)):
        if df.iloc[i].isna().all() or (df.iloc[i] == "").all():
            return i
    return len(df)


def clean_table(table):
    """Drop the empty or NaN columns in the table with reset index"""
    return table.dropna(axis=1, how="all").reset_index(drop=True)


def extract_summary_table(df):
    """Extract the Description details section"""
    start_row = find_table_start(df, "#")
    if start_row is None:
        return pd.DataFrame(), "Description table not found"

    # there is a empty line
    start_row += 2
    end_row = find_next_empty_row(df, start_row)

    # Extract and clean the table
    table = df.iloc[start_row:end_row].copy()
    table_cleaned = clean_table(table)
    data = {}
    row_keys = ["Raw_Feed", "Net_Feed", "Total_Conc", "Net_Product"]
    col_keys = ["#number", "Description", "Flow", "TDS", "Pressure"]

    for r_key, row in zip(row_keys, table_cleaned.itertuples(index=False)):
        for idx, c_key in enumerate(col_keys):
            data[f"{r_key}_{c_key}"] = row[idx]

    result_df = pd.DataFrame([data])
    return result_df, None


def extract_system_overview(df):
    """Extract the RO System Overview section"""
    start_row = find_table_start(df, "Total # of Trains")
    if start_row is None:
        return pd.DataFrame(), "System Overview table not found"

    end_row = find_next_empty_row(df, start_row)

    # Extract and clean the table
    table = df.iloc[start_row:end_row].copy()
    # remove the empty coloumns and reset the indexes
    table_cleaned = clean_table(table)
    # Process to create a more structured table
    data = {}
    # First row
    data["Trains"] = table_cleaned.iloc[0, 1]
    data["Online"] = table_cleaned.iloc[0, 3]
    data["Standby"] = table_cleaned.iloc[0, 5]
    data["RORecovery"] = table_cleaned.iloc[0, 7]

    # second row
    data["NetFeed"] = table_cleaned.iloc[1, 3]
    data["NetProduct"] = table_cleaned.iloc[1, 5]

    result_df = pd.DataFrame([data])
    return result_df, None


def extract_pass_details(df):
    """Extract the Pass details section"""
    start_row = find_table_start(df, "Pass")
    if start_row is None:
        return pd.DataFrame(), "Pass details table not found"

    end_row = find_next_empty_row(df, start_row)

    # Extract the table with all columns to capture values
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)

    # dropping the units col
    cleaned_table = cleaned_table.drop(cleaned_table.columns[1], axis=1)

    transpose_table = cleaned_table.T

    transpose_table.columns = transpose_table.iloc[0]
    transpose_table = transpose_table.iloc[1:]

    return transpose_table, None


def extract_stage_level_flow_table(df):
    """Extract the RO Flow Table (Stage Level)"""
    start_row = find_table_start(df, "Stage")
    if start_row is None:
        return pd.DataFrame(), "Stage Level Flow table not found"

    if start_row is None:
        return pd.DataFrame(), "Stage Level Flow table headers not found"

    # skipping row(moving cursor point to first row)
    start_row += 2

    end_row = find_next_empty_row(df, start_row)

    # Extract the table with all columns to capture values
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)

    data = {}
    col_keys = ["ElementType", "PV", "ElsPer", "Feed_Flow", "Feed_Recirc", "Feed_Press", "Feed_Boost_Press", "Conc_Flow", "Conc_Press", "Conc_Press_Drop", "Perm_Flow",
                "Perm_Avg_Flux", "Perm_Press", "Perm_TDS"]
    for idx, row in enumerate(cleaned_table.itertuples(index=False), start=1):
        for i, col in enumerate(col_keys, start=1):
            data[f'Stage_{idx}_{col}'] = row[i]

    dataframe = pd.DataFrame([data])
    return dataframe, None


def extract_solute_concentrations(df):
    """Extract the RO Solute Concentrations table"""
    start_row = find_table_start(df, "NH₄⁺")
    if start_row is None:
        return pd.DataFrame(), "Solute Concentrations table not found"

    # Find the end row
    end_row = find_next_empty_row(df, start_row)

    # Extract the table with all columns to capture values
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)

    # find no of stages
    # stage 1 -> 5, stage 2 -> 7, stage 3 -> 9
    col_len = cleaned_table.shape[1]
    stages = (col_len - 5) // 2 + 1

    data = {}
    keys = [
        "NH₄⁺",
        "K⁺",
        "Na⁺",
        "Mg⁺²",
        "Ca⁺²",
        "Sr⁺²",
        "Ba⁺²",
        "CO₃⁻²",
        "HCO₃⁻",
        "NO₃⁻",
        "F⁻",
        "Cl⁻",
        "Br⁻¹",
        "SO₄⁻²",
        "PO₄⁻³",
        "SiO₂",
        "Boron",
        "CO₂",
        "TDS",
        "Cond.",
        "pH",
    ]

    for key, row in zip(keys, cleaned_table.itertuples(index=False)):
        if not row[0].startswith(key):
            raise Exception(f"item {key} is not found.")

        data[f"{key}_Feed"] = row[1]
        col_pos = 1

        # concentrate
        for i in range(1, stages + 1):
            col_pos += 1
            data[f"{key}_Stage{i}_Conc"] = row[col_pos]

        # Premeate
        for i in range(1, stages + 1):
            col_pos += 1
            data[f"{key}_Stage{i}_Prem"] = row[col_pos]

        data[f"{key}_Total"] = row[col_pos + 1]

    result_df = pd.DataFrame([data])
    return result_df, None


def extract_design_warnings(df):
    """Extract the RO Design Warnings section if present"""
    start_row = find_table_start(df, "Design Warning")
    if start_row is None:
        return "No design warnings found", None

    # skipping the row, which are column names
    start_row += 1

    end_row = find_next_empty_row(df, start_row)

    # Extract the table with all columns to capture values
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)
    cleaned_table = cleaned_table.drop(cleaned_table.columns[1], axis=1)

    warnings = []
    columns = [
        "Design Warning",
        "Limit",
        "Value",
        "Pass",
        "Stage",
        "Element",
        "Product",
    ]

    for _, row in cleaned_table.iterrows():
        warnings.append(
            " ".join([f"{col}: {row.iloc[idx]}" for idx, col in enumerate(columns)])
        )

    # merging all warnings into 1 string
    warnings = "\n".join(warnings)
    result_df = pd.DataFrame([{"RO Design Warnings": warnings}])
    return result_df, None


def extract_element_level_flow_table(df):
    """Extract the last row of the RO Flow Table (Element Level)"""
    start_row = find_table_start(
        df, r"RO Flow Table \(Element Level\)", exact_match=False
    )
    if start_row is None:
        return pd.DataFrame(), "Element Level Flow table not found"

    # skipping the table name row and get actual data
    start_row += 1
    end_row = find_next_empty_row(df, start_row)

    # Extract the table with all columns to capture values
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)

    # get keys from column names
    keys = [item.strip().replace(" ", "_") for item in cleaned_table.iloc[0]]

    data = {}
    # skip the header row and process the rows
    for index, row in cleaned_table.iloc[2:].iterrows():
        for idx, key in enumerate(keys):
            data[f"row_{index-1}_{key}"] = row.iloc[idx]

    result_df = pd.DataFrame([data])
    return result_df, None


def extract_solubility_warnings(df):
    """Extract the RO Solubility Warnings section if present"""
    start_row = find_table_start(df, "Warning")
    if start_row is None:
        return "No solubility warnings found", None

    end_row = find_next_empty_row(df, start_row)

    # skipping the row, which are column names
    start_row += 1

    end_row = find_next_empty_row(df, start_row)

    # Extract the table with all columns to capture values
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)
    warnings = []
    columns = ["Warnings", "PassNo"]

    for _, row in cleaned_table.iterrows():
        warnings.append(
            " ".join([f"{col}: {row.iloc[idx]}" for idx, col in enumerate(columns)])
        )

     # merging all warnings into 1 string
    warnings = "\n".join(warnings)
    result_df = pd.DataFrame([{"RO Solubility Warnings": warnings}])
    return result_df, None


def extract_chemical_adjustments(df):
    """Extract the RO Chemical Adjustments table"""
    start_row = find_table_start(df, "RO Chemical Adjustments", exact_match=False)
    if start_row is None:
        return pd.DataFrame(), "Chemical Adjustments table not found"

    # Find the header row (the row after RO Chemical Adjustments)
    header_row = start_row + 3

    # Ensure header row exists
    if header_row >= len(df):
        return pd.DataFrame(), "Chemical Adjustments headers not found"

    end_row = find_next_empty_row(df, header_row)

    # Extract the table
    table = df.iloc[header_row:end_row].copy()
    cleaned_table = clean_table(table)

    keys = [
        "pH",
        "Langelier Saturation Index",
        "Stiff & Davis Stability Index",
        "TDSᵃ_adjustment",
        "Ionic Strength",
        "HCO₃⁻",
        "CO₂",
        "CO₃⁻²",
        "CaSO₄_saturation",
        "BaSO₄_saturation",
        "SrSO₄_saturation",
        "CaF₂_saturation",
        "SiO₂_saturation",
        "Mg(OH)₂_saturation",
    ]

    columns = ["Pass_1_Feed", "RO_Pass_1_Conc"]

    data = {}
    for key, row in zip(keys, cleaned_table.itertuples(index=False)):
        if not key.startswith(row[0].split(" ")[0]):
            raise Exception(f"Key {key} not found")
        key = key.strip().replace(" ", "_")
        for index, col in enumerate(columns, start=1):
            data[f"{key}_{col}"] = row[index]

    result_df = pd.DataFrame([data])

    return result_df, None


def extract_utility_costs(df):
    """Extract the RO Utility and Chemical Costs section"""
    start_row = find_table_start(df, "Non-Product Feed Water")
    if start_row is None:
        return pd.DataFrame(), "Utility Costs table not found"

    end_row = find_next_empty_row(df, start_row)

    # Extract the table
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)

    data = {}
    # assuming Non product feed water is in row 1 in the cleaned table
    data["Non_prod_feed_water_pass1_Flow"] = cleaned_table.iloc[1, 1]
    data["Non_prod_unit_cost"] = cleaned_table.iloc[1, 2]
    data["Non_prod_hourly_cost"] = cleaned_table.iloc[1, 3]
    data["Non_prod_daily_cost"] = cleaned_table.iloc[1, 4]

    # assuming waste water disposal is in 4th row
    data["Waste_water_feed_water_pass1_Flow"] = cleaned_table.iloc[4, 1]
    data["Waste_water_unit_cost"] = cleaned_table.iloc[4, 2]
    data["Waste_water_hourly_cost"] = cleaned_table.iloc[4, 3]
    data["Waste_water_daily_cost"] = cleaned_table.iloc[4, 4]

    data["Total_service_water_cost"] = cleaned_table.iloc[6, 4]

    result_df = pd.DataFrame([data])

    return result_df, None


def extract_electricity_details(df):
    """Extract the Electricity Details section"""
    start_row = find_table_start(df, "Peak Power")
    if start_row is None:
        return pd.DataFrame(), "Electricity Details table not found"

    end_row = find_next_empty_row(df, start_row)

    # Extract the table
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)

    # Process into structured data
    data = {}

    data["Peak_Power"] = cleaned_table.iloc[0, 2]
    data["Energy"] = cleaned_table.iloc[1, 2]
    data["Electricity_unit_cost"] = cleaned_table.iloc[2, 2]
    data["Electricity_cost"] = cleaned_table.iloc[3, 2]
    data["Specific_Energy"] = cleaned_table.iloc[4, 2]

    result_df = pd.DataFrame([data])

    return result_df, None


def extract_pump_details(df):
    """Extract the Pump Details section"""
    start_row = find_table_start(df, "Pump")
    if start_row is None:
        return pd.DataFrame(), "Pump Details table not found"

    # skipping the headers and assuming actual details start from here
    start_row += 3

    end_row = find_next_empty_row(df, start_row)

    # Extract the table with all columns to capture values
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)

    data = {}
    keys = [item.strip().replace(" ", "_") for item in cleaned_table.iloc[:, 0]]

    for key, row in zip(keys, cleaned_table.itertuples(index=False)):
        if not key.startswith(row[0].strip().split(" ")[0]):
            raise Exception(f"item {key} is not found.")

        for i, param in enumerate(["Flow_rate", "Power", "Energy", "Cost"], start=1):
            data[f"{key}_{param}"] = row[i]

    result_df = pd.DataFrame([data])
    return result_df, None


def extract_chemical_details(df):
    """Extract the Chemical Details section"""
    start_row = find_table_start(df, "Chemical")
    if start_row is None:
        return pd.DataFrame(), "Chemical Details table not found"

    end_row = find_next_empty_row(df, start_row)

    # Extract the table
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)

    data = {}

    data["Total_chemical_cost_per_unit"] = cleaned_table.iloc[3, 1]
    data["Total_chemical_cost_per_Dose"] = cleaned_table.iloc[3, 2]
    data["Total_chemical_cost_per_volume"] = cleaned_table.iloc[3, 3]
    data["Total_chemical_cost_per_total_cost"] = cleaned_table.iloc[3, 4]

    # Simple conversion to DataFrame
    result_df = pd.DataFrame([data])

    return result_df, None


def extract_final_costs(df):
    """Extract the final tables Utility and Chemical Cost and Specific Water Cost"""
    start_row = find_table_start(df, "Utility and Chemical Cost")
    if start_row is None:
        return pd.DataFrame(), "Utility and Chemical Cost table not found"

    end_row = find_next_empty_row(df, start_row)

    # Extract the table
    table = df.iloc[start_row:end_row].copy()
    cleaned_table = clean_table(table)

    data = {}

    data["Utility_Chemical_Cost"] = cleaned_table.iloc[0, 2]
    data["Specific_Water_Cost"] = cleaned_table.iloc[1, 2]

    # Simple conversion to DataFrame
    result_df = pd.DataFrame([data])

    return result_df, None


def process_file(filepath):
    """Process a single file and extract all tables"""
    logger.info(f"Processing file: {filepath}")

    try:
        # Check if it's an Excel file and convert to CSV
        if filepath.endswith(".xls"):
            csv_filepath = convert_xls_to_csv(filepath)

            # Read the CSV file
            df = pd.read_csv(csv_filepath, header=None)

            # Dictionary to store all tables and any extraction errors
            tables = {}
            errors = {}

            # List of extraction functions with their names for better logging
            extraction_functions = [
                ("summary", extract_summary_table),
                ("system_overview", extract_system_overview),
                ("pass_details", extract_pass_details),
                ("stage_flow", extract_stage_level_flow_table),
                ("solute_concentrations", extract_solute_concentrations),
                ("design_warnings", extract_design_warnings),
                ("element_flow", extract_element_level_flow_table),
                ("solubility_warnings", extract_solubility_warnings),
                ("chemical_adjustments", extract_chemical_adjustments),
                ("utility_costs", extract_utility_costs),
                ("electricity_details", extract_electricity_details),
                ("pump_details", extract_pump_details),
                ("chemical_details", extract_chemical_details),
                ("final_costs", extract_final_costs),
            ]

            # Extract all tables with better error handling
            logger.info(f"Extracting tables from {filepath}")
            for table_name, extract_function in extraction_functions:
                try:
                    logger.info(f"Extracting {table_name} from {filepath}")
                    tables[table_name], errors[table_name] = extract_function(df)
                    if errors[table_name]:
                        logger.warning(f"Error in {table_name} extraction: {errors[table_name]}")
                except Exception as e:
                    stack_trace = traceback.format_exc()
                    error_msg = f"Failed to extract {table_name}: {str(e)}"
                    logger.error(f"{error_msg}\n{stack_trace}")
                    tables[table_name] = pd.DataFrame()
                    errors[table_name] = error_msg

            # Record errors
            error_msgs = [f"{k}: {v}" for k, v in errors.items() if v is not None]
            if error_msgs:
                logger.warning(
                    f"Extraction errors in {filepath}: {', '.join(error_msgs)}"
                )
                tables["extraction_errors"] = pd.DataFrame({"Error": error_msgs})

            # Clean up the temporary CSV file
            try:
                os.remove(csv_filepath)
                logger.info(f"Removed temporary CSV file: {csv_filepath}")
            except Exception as e:
                stack_trace = traceback.format_exc()
                logger.warning(
                    f"Could not remove temporary CSV file {csv_filepath}: {str(e)}\n{stack_trace}"
                )

            return tables
        else:
            logger.warning(f"Skipping non-Excel file: {filepath}")
            return None
    except Exception as e:
        stack_trace = traceback.format_exc()
        logger.error(f"Error processing {filepath}: {str(e)}\n{stack_trace}")
        return None


def process_directory(directory_path, output_file=None):
    """Process all Excel files in a directory and create output file"""
    logger.info(f"Processing directory: {directory_path}")

    if output_file is None:
        output_file = os.path.join(directory_path, "WAVE_RO_Extraction.xlsx")

    # Find only XLS files
    try:
        all_files = list(Path(directory_path).glob("*.xls"))
    except Exception as e:
        stack_trace = traceback.format_exc()
        logger.error(f"Error finding XLS files in {directory_path}: {str(e)}\n{stack_trace}")
        print(f"Error finding XLS files: {str(e)}")
        return

    if not all_files:
        logger.warning(f"No Excel files found in {directory_path}")
        print(f"No Excel files found in {directory_path}")
        return

    logger.info(f"Found {len(all_files)} Excel files to process")

    # Process each file
    all_tables = {}
    file_count = 0
    failed_files = []

    try:
        with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
            for file_path in all_files:
                logger.info(
                    f"Processing file {file_count+1}/{len(all_files)}: {file_path.name}"
                )
                print(f"Processing {file_path.name}...")

                try:
                    tables = process_file(str(file_path))

                    if tables is None:
                        logger.warning(
                            f"Skipping {file_path.name} due to processing errors"
                        )
                        failed_files.append(file_path.name)
                        continue

                    # For the first file, initialize all_tables
                    if file_count == 0:
                        for key in tables:
                            if isinstance(tables[key], pd.DataFrame):
                                df = tables[key].copy()
                                # Add SourceFile as first column
                                df.insert(0, "SourceFile", file_path.name)
                                all_tables[key] = df
                            else:
                                all_tables[key] = pd.DataFrame(
                                    {
                                        "SourceFile": [file_path.name],
                                        "Content": [tables[key]],
                                    }
                                )
                    else:
                        # Append data from this file to all_tables
                        for key in tables:
                            if key in all_tables:
                                if (
                                    isinstance(tables[key], pd.DataFrame)
                                    and not tables[key].empty
                                ):
                                    df_to_append = tables[key].copy()
                                    # Add SourceFile as first column
                                    df_to_append.insert(0, "SourceFile", file_path.name)
                                    all_tables[key] = pd.concat(
                                        [all_tables[key], df_to_append], ignore_index=True
                                    )
                                elif isinstance(tables[key], str):
                                    new_row = pd.DataFrame(
                                        {
                                            "SourceFile": [file_path.name],
                                            "Content": [tables[key]],
                                        }
                                    )
                                    all_tables[key] = pd.concat(
                                        [all_tables[key], new_row], ignore_index=True
                                    )
                    
                    file_count += 1
                except Exception as e:
                    stack_trace = traceback.format_exc()
                    logger.error(f"Error processing file {file_path.name}: {str(e)}\n{stack_trace}")
                    failed_files.append(file_path.name)
                    continue

            # Add a summary sheet with processing statistics
            summary_data = {
                "Statistic": [
                    "Total Files Processed", 
                    "Successfully Processed", 
                    "Failed Files", 
                    "Failure Rate (%)",
                    "Failed Files List"
                ],
                "Value": [
                    len(all_files),
                    file_count,
                    len(failed_files),
                    round(len(failed_files) / len(all_files) * 100, 2) if all_files else 0,
                    ", ".join(failed_files) if failed_files else "None"
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="Processing_Summary", index=False)
                
            # Write all tables to separate sheets with formatting
            logger.info(
                f"Writing {len(all_tables)} tables to output file: {output_file}"
            )
            for key, table in all_tables.items():
                if not table.empty:
                    sheet_name = key[:31]  # Excel sheet names limited to 31 chars
                    table.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"Wrote table '{key}' to sheet '{sheet_name}'")

                    # Get the xlsxwriter workbook and worksheet objects
                    workbook = writer.book
                    worksheet = writer.sheets[sheet_name]

                    # Add a cell format with text wrapping
                    wrap_format = workbook.add_format({"text_wrap": True})

                    # Apply the format to all columns
                    for col_num, column in enumerate(table.columns):
                        # Set the column width based on content
                        max_len = max(
                            table[column].astype(str).apply(len).max(), len(str(column))
                        )
                        # Limiting the column width to be between 10 and 50
                        col_width = min(max(max_len + 2, 10), 50)
                        worksheet.set_column(col_num, col_num, col_width, wrap_format)

        logger.info(
            f"Successfully processed {file_count} files, {len(failed_files)} failures. Output saved to {output_file}"
        )
        print(f"Processed {file_count} files, {len(failed_files)} failures. Output saved to {output_file}")
        if failed_files:
            print(f"Failed files: {', '.join(failed_files)}")
    except Exception as e:
        stack_trace = traceback.format_exc()
        logger.error(f"Error during directory processing: {str(e)}\n{stack_trace}")
        print(f"Error during processing: {str(e)}")


if __name__ == "__main__":
    setup_logger()
    logger.info("Script started")

    if len(sys.argv) > 1:
        directory_path = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        logger.info(
            f"Command line arguments: directory={directory_path}, output_file={output_file}"
        )
        process_directory(directory_path, output_file)
    else:
        logger.warning("No command line arguments provided")
        print(
            "Usage: python extract.py <directory_path> [output_file_path(default: WAVE_RO_Extraction.xlsx)]"
        )

    logger.info("Script completed")
