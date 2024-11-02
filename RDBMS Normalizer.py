import pandas as pd
import re
import csv
from itertools import combinations

# Function to clean curly braces and extra spaces from strings
def clean_data(value):
    if isinstance(value, str):
        return value.replace('{', '').replace('}', '').strip()
    return value

# Function to parse functional dependencies correctly into (lhs, rhs) pairs
def parse_functional_dependencies(fd_input):
    fds = []
    for fd in fd_input.split(';'):
        if '->' in fd:
            lhs, rhs = fd.split('->')
            lhs = [x.strip() for x in lhs.split(',')]
            rhs = [x.strip() for x in rhs.split(',')]
            fds.append((lhs, rhs))  # Ensure each fd is a tuple of (lhs list, rhs list)
    return fds

# Identify partial dependencies
def find_partial_dependencies(fds, primary_key_columns):
    return [(lhs, rhs) for lhs, rhs in fds if set(lhs).issubset(primary_key_columns) and len(lhs) < len(primary_key_columns)]

# Identify transitive dependencies
def find_transitive_dependencies(fds, primary_key_columns):
    return [(lhs, rhs) for lhs, rhs in fds if not set(lhs).issubset(primary_key_columns) and not set(lhs).intersection(primary_key_columns)]

# Function to parse multi-valued dependencies
def parse_multivalued_dependencies(mvd_input):
    mvds = []
    for mvd in mvd_input.split(';'):
        if '->>' in mvd:
            lhs, rhs = mvd.split('->>')
            lhs = [x.strip() for x in lhs.split(',')]
            rhs = [x.strip() for x in rhs.split(',')]
            mvds.append((lhs, rhs))
    return mvds


# Generate SQL query for table creation
def generate_sql_query(table_name, columns, primary_key_columns=None):
    sql_query = f"CREATE TABLE {table_name} (\n"
    for col in columns:
        sql_query += f"    {col} VARCHAR(255),\n"
    if primary_key_columns:
        pk_str = ", ".join(primary_key_columns)
        sql_query += f"    PRIMARY KEY ({pk_str}),\n"
    return sql_query.rstrip(',\n') + "\n);"



# Function to check if a column contains multi-valued attributes (not in 1NF)
def check_1NF(df):
    for col in df.columns:
        for value in df[col]:
            if isinstance(value, str) and ',' in value:
                return False  # Not in 1NF
    return True  # In 1NF


# Function to decompose multi-valued attributes into separate tables with atomic values
def decompose_multivalued_attributes(df, fds):
    decomposed_tables = {}
    
    # Helper function to find minimal determinant for each multi-valued attribute
    def get_primary_key_for_attribute(attr):
        for lhs, rhs in fds:
            if attr in rhs:
                return lhs  # Return the LHS (determinant) as primary key for the table containing attr
        return []  # If no dependency found, return empty list (shouldn't happen with valid FDs)

    for col in df.columns:
        # Clean each value in the column
        df[col] = df[col].apply(clean_data)

        # Check if the column has multi-valued attributes
        if df[col].apply(lambda x: isinstance(x, str) and ',' in x).any():
            # Determine the minimal determinant for this multi-valued attribute
            primary_key_columns = get_primary_key_for_attribute(col)

            if not primary_key_columns:
                raise ValueError(f"No determinant found for attribute {col}. Please check FDs.")

            # Create a separate table for the multi-valued attribute
            mv_table = df[primary_key_columns + [col]].copy()
            mv_table[col] = mv_table[col].apply(lambda x: x.split(',') if isinstance(x, str) else x)
            mv_table = mv_table.explode(col).dropna().drop_duplicates().reset_index(drop=True)
            
            # Generate table name and add it to the dictionary of decomposed tables
            table_name = f"{col}_relation"
            decomposed_tables[table_name] = mv_table
            print(f"Generated decomposed 1NF table: {table_name}")
    
    return decomposed_tables

# Function to check and convert a DataFrame to 1NF, returning a decomposed main table and additional decomposed tables
def check_and_convert_to_1nf(df, primary_key_columns, fds):
    # Step 1: Check if data is already in 1NF
    if check_1NF(df):
        print("Data is already in 1NF.")
        return df, {}, False  # No conversion needed

    # Step 2: If not in 1NF, decompose multi-valued attributes into separate tables
    print("Converting to 1NF by decomposing multi-valued attributes.")
    decomposed_tables = decompose_multivalued_attributes(df, fds)
    
    # The main table should exclude any columns moved to decomposed tables
    main_table_columns = [col for col in df.columns if col not in decomposed_tables.keys()]
    df_1nf = df[main_table_columns].drop_duplicates().reset_index(drop=True)
    
    return df_1nf, decomposed_tables, True  # Conversion applied

# Example generate_sql_query function that accepts primary key columns as input
def generate_sql_query(table_name, columns, primary_key_columns=None):
    sql_query = f"CREATE TABLE {table_name} (\n"
    for col in columns:
        sql_query += f"    {col} VARCHAR(255),\n"
    if primary_key_columns:
        pk_str = ", ".join(primary_key_columns)
        sql_query += f"    PRIMARY KEY ({pk_str}),\n"
    return sql_query.rstrip(',\n') + "\n);"


# Adjusted function for 2NF conversion
def convert_to_2NF(df, primary_key, fds, output_csv):
    primary_key_columns = [col.strip() for col in primary_key.split(',')]
    partial_dependencies = find_partial_dependencies(fds, primary_key_columns)
    tables_in_2NF = []
    main_table_generated = False  # Flag to track if the main table is generated

    # Step 1: Create tables for each partial dependency
    unique_names = set()  # Track unique table names
    for lhs, rhs in partial_dependencies:
        table_name = '_'.join(lhs) + "_relation"
        if table_name in unique_names:
            table_name += f"_{len(unique_names)}"  # Make table name unique if needed
        unique_names.add(table_name)
        
        new_table = df[lhs + rhs].drop_duplicates()
        tables_in_2NF.append((table_name, new_table, lhs))
        print(f"Generated SQL for 2NF Partial Dependency Table: {table_name}")
        print(generate_sql_query(table_name, new_table.columns, lhs))

    # Step 2: Define columns for the main 2NF table
    moved_columns = {col for _, table, _ in tables_in_2NF for col in table.columns}
    remaining_columns = [col for col in df.columns if col in primary_key_columns or col not in moved_columns]
    
    if remaining_columns and not main_table_generated:
        main_table_name = f"{'_'.join(primary_key_columns)}_main_relation"
        df_2NF = df[remaining_columns].drop_duplicates()
        tables_in_2NF.append((main_table_name, df_2NF, primary_key_columns))
        
        # Generate SQL for main table once
        print(f"Generated SQL for Main 2NF Table: {main_table_name}")
        print(generate_sql_query(main_table_name, df_2NF.columns, primary_key_columns))
        main_table_generated = True

    # Write 2NF tables to the output CSV
    with open(output_csv, 'w') as f:
        f.write("=== 2NF Normalized Tables ===\n\n")
        for name, table, pk in tables_in_2NF:
            f.write(f"=== {name} ===\n")
            table.to_csv(f, index=False)
            f.write("\n\n")

    return tables_in_2NF

def convert_to_3NF(df, primary_key, fds, output_csv, tables_in_2NF):
    primary_key_columns = [col.strip() for col in primary_key.split(',')]
    transitive_dependencies = find_transitive_dependencies(fds, primary_key_columns)
    tables_in_3NF = []

    # Convert tables_in_2NF values to the expected format
    formatted_2nf_tables = []
    for table_name, (table, pk_cols) in tables_in_2NF.items():
        formatted_2nf_tables.append((table_name, table, pk_cols))

    # Reevaluate each 2NF table for transitive dependencies
    unique_names = set()  # Track unique table names
    for table_name, table, pk_columns in formatted_2nf_tables:
        moved_columns = set()

        # Identify transitive dependencies specific to this table
        for lhs, rhs in transitive_dependencies:
            if set(lhs).issubset(table.columns) and any(col in table.columns for col in rhs):
                transitive_table_name = '_'.join(lhs) + "_relation"
                if transitive_table_name in unique_names:
                    transitive_table_name += f"_{len(unique_names)}"  # Ensure unique table name
                unique_names.add(transitive_table_name)
                
                transitive_table = table[lhs + rhs].drop_duplicates()
                tables_in_3NF.append((transitive_table_name, transitive_table, lhs))
                print(f"3NF Table: {transitive_table_name} with columns {lhs + rhs}")
                
                # Mark these columns as moved to avoid duplicating them in the final relation
                moved_columns.update(rhs)
        
        remaining_columns = [col for col in table.columns if col not in moved_columns]
        final_relation_table = table[remaining_columns].drop_duplicates()
        
        if remaining_columns:
            tables_in_3NF.append((table_name, final_relation_table, pk_columns))

    # Write 3NF tables to the output CSV
    with open(output_csv, 'w') as f:
        f.write("=== 3NF Normalized Tables ===\n\n")
        for name, table, pk in tables_in_3NF:
            f.write(f"=== {name} ===\n")
            table.to_csv(f, index=False)
            f.write("\n\n")
            print(generate_sql_query(name, table.columns, pk))

    return tables_in_3NF

# Function to check if a table is in BCNF
def is_in_bcnf(table, fds, pk_columns):
    for lhs, rhs in fds:
        # If LHS is not a superkey, the table violates BCNF
        lhs_is_superkey = set(pk_columns).issubset(lhs) or set(lhs) == set(pk_columns)
        if not lhs_is_superkey:
            return False
    return True

# Function to convert to BCNF
def check_and_convert_to_bcnf(tables_in_3NF, fds, output_sql_file="bcnf_queries.sql"):
    tables_in_bcnf = []
    processed_dependencies = set()

    with open(output_sql_file, 'w') as sql_file:
        for table_name, table, pk_columns in tables_in_3NF:
            relevant_fds = [(lhs, rhs) for lhs, rhs in fds if set(lhs).issubset(table.columns) and set(rhs).issubset(table.columns)]
            if is_in_bcnf(table, relevant_fds, pk_columns):
                tables_in_bcnf.append((table_name, table, pk_columns))
                print(f"Table {table_name} is already in BCNF.")
                sql_query = generate_sql_query(table_name, table.columns, pk_columns)
                sql_file.write(sql_query + "\n\n")
            else:
                for lhs, rhs in relevant_fds:
                    dependency = (tuple(lhs), tuple(rhs))
                    if dependency in processed_dependencies:
                        continue
                    if set(lhs).issubset(table.columns) and set(rhs).issubset(table.columns):
                        processed_dependencies.add(dependency)
                        bcnf_table_name = '_'.join(lhs) + "_BCNF_relation"
                        decomposed_table = table[lhs + rhs].drop_duplicates()
                        tables_in_bcnf.append((bcnf_table_name, decomposed_table, lhs))
                        sql_query = generate_sql_query(bcnf_table_name, decomposed_table.columns, lhs)
                        sql_file.write(sql_query + "\n\n")
                all_moved_columns = {col for lhs, rhs in relevant_fds for col in rhs}
                remaining_columns = [col for col in table.columns if col not in all_moved_columns]
                if remaining_columns:
                    final_relation_name = '_'.join(pk_columns) + "_final_relation"
                    final_relation = table[remaining_columns].drop_duplicates()
                    tables_in_bcnf.append((final_relation_name, final_relation, pk_columns))
                    sql_query = generate_sql_query(final_relation_name, final_relation.columns, pk_columns)
                    sql_file.write(sql_query + "\n\n")
    return tables_in_bcnf

# Function to validate MVDs against actual data instances
def validate_mvd(df, lhs, rhs):
    # Group by LHS attributes and check if RHS attributes are independent
    grouped = df.groupby(lhs)
    for _, group in grouped:
        rhs_values = group[rhs].drop_duplicates()
        if len(rhs_values) > 1:
            return False  # MVD is invalid if there are multiple RHS values for a single LHS group
    return True

# Function to check if a table is in 4NF
def is_in_4nf(table, mvds, pk_columns):
    for lhs, rhs in mvds:
        # Validate MVD (A ->> B) to confirm it holds in the data
        if not set(lhs).issuperset(pk_columns) and validate_mvd(table, lhs, rhs):
            return False  # Not in 4NF if MVD holds without lhs being a superkey
    return True

# Function to decompose to 4NF based on validated MVDs
def decompose_to_4nf(tables_in_bcnf, mvds, output_sql_file="4nf_queries.sql"):
    tables_in_4nf = []
    processed_dependencies = set()

    with open(output_sql_file, 'w') as sql_file:
        for table_name, table, pk_columns in tables_in_bcnf:
            relevant_mvds = [(lhs, rhs) for lhs, rhs in mvds if set(lhs).issubset(table.columns) and set(rhs).issubset(table.columns)]
            if is_in_4nf(table, relevant_mvds, pk_columns):
                tables_in_4nf.append((table_name, table, pk_columns))
                print(f"Table {table_name} is already in 4NF.")
                sql_query = generate_sql_query(table_name, table.columns, pk_columns)
                print(sql_query)
                sql_file.write(sql_query + "\n\n")
            else:
                print(f"Table {table_name} is not in 4NF. Decomposing...")
                for lhs, rhs in relevant_mvds:
                    dependency = (tuple(lhs), tuple(rhs))
                    if dependency in processed_dependencies:
                        continue
                    
                    if validate_mvd(table, lhs, rhs):
                        processed_dependencies.add(dependency)
                        new_table_name = '_'.join(lhs) + "_4NF_relation"
                        decomposed_table = table[lhs + rhs].drop_duplicates()
                        tables_in_4nf.append((new_table_name, decomposed_table, lhs))
                        
                        sql_query = generate_sql_query(new_table_name, decomposed_table.columns, lhs)
                        print(sql_query)
                        sql_file.write(sql_query + "\n\n")
                
                # Handle remaining columns not moved
                all_moved_columns = {col for lhs, rhs in relevant_mvds for col in rhs}
                remaining_columns = [col for col in table.columns if col not in all_moved_columns]
                if remaining_columns:
                    final_relation_name = '_'.join(pk_columns) + "_final_relation"
                    final_relation = table[remaining_columns].drop_duplicates()
                    tables_in_4nf.append((final_relation_name, final_relation, pk_columns))
                    
                    sql_query = generate_sql_query(final_relation_name, final_relation.columns, pk_columns)
                    print(sql_query)
                    sql_file.write(sql_query + "\n\n")

    return tables_in_4nf

# Function to check if two projections (subsets of columns) can be joined without redundancy
def can_reconstruct(original_df, projection_dfs):
    # Perform sequential joins on all projections
    rejoined_df = projection_dfs[0]
    for proj_df in projection_dfs[1:]:
        # Find common columns for the join
        common_columns = list(set(rejoined_df.columns) & set(proj_df.columns))
        
        # Only join if there are common columns
        if common_columns:
            rejoined_df = pd.merge(rejoined_df, proj_df, on=common_columns, how="inner")
        else:
            # If no common columns, skip join as it is not a valid join for reconstruction
            #print("No common columns to join on for projections, skipping join.")
            return False
    
    # Check if rejoined DataFrame matches the original DataFrame
    return rejoined_df.equals(original_df)


# DP-based function to find join dependencies and check for 5NF
def find_join_dependencies_and_check_5nf(df):
    # Dictionary to store results of join dependency tests on subsets
    dp_results = {}
    inferred_jds = []
    attributes = df.columns.tolist()
    
    # Get all subsets of attributes in increasing size
    for subset_size in range(2, len(attributes)):
        for subset in combinations(attributes, subset_size):
            subset_key = tuple(sorted(subset))
            # Split the attributes into this subset and its complement
            remaining_attributes = list(set(attributes) - set(subset))
            
            # Check if the subset and its complement share common columns
            if set(subset).intersection(remaining_attributes):
                projection_dfs = [df[list(projection)].drop_duplicates() for projection in [subset, remaining_attributes]]
                
                # Store the result of the join test for this subset in the DP table
                dp_results[subset_key] = can_reconstruct(df, projection_dfs)

                # If join dependency holds, record it as a potential JD
                if dp_results[subset_key]:
                    inferred_jds.append((list(subset), remaining_attributes))
                    #print(f"Inferred JD: {subset} -> {remaining_attributes}")
            else:
                #print(f"Skipping subset {subset} and complement {remaining_attributes} due to no common columns.")
                print()
    
    # Check if any join dependency introduces redundancy
    is_5nf = all(dp_results[key] for key in dp_results if dp_results[key])
    
    return is_5nf, inferred_jds


# Function to decompose table into 5NF projections if it is not in 5NF
def decompose_to_5nf(df, inferred_jds, original_pk):
    decomposed_tables = []
    # Create a projection for each inferred JD
    for left, right in inferred_jds:
        projection_df = df[left + right].drop_duplicates()
        # Determine primary key for the decomposed projection
        pk_for_projection = left if set(left).issuperset(set(original_pk)) else original_pk
        decomposed_tables.append((left + right, projection_df, pk_for_projection))
        print(f"Decomposed projection for JD {left} -> {right}:\n{projection_df}\n")
    
    return decomposed_tables

# Function to generate SQL query for 5NF tables with specific primary key
def generate_sql_query_5nf(table_name, columns, pk_columns):
    sql_query = f"CREATE TABLE {table_name} (\n"
    for col in columns:
        sql_query += f"    {col} VARCHAR(255),\n"
    sql_query += f"    PRIMARY KEY ({', '.join(pk_columns)})\n"
    sql_query += ");"
    return sql_query

# Modified function to check and convert to 5NF with SQL generation and primary key handling
def check_and_convert_to_5nf(df, primary_key):
    # Step 1: Find JDs and check if table is in 5NF
    is_5nf, inferred_jds = find_join_dependencies_and_check_5nf(df)
    
    sql_queries = []  # To store generated SQL queries
    decomposed_tables = []  # To store tables in 5NF as (name_suffix, DataFrame)

    # Step 2: If not in 5NF, decompose into projections based on inferred JDs
    if is_5nf:
        print("The table is already in 5NF.")
        table_name = "Original_Table"
        sql_query = generate_sql_query_5nf(table_name, df.columns.tolist(), primary_key)
        sql_queries.append((table_name, sql_query))
        decomposed_tables.append((table_name, df))  # Append as (name_suffix, DataFrame)
    else:
        print("The table is not in 5NF. Decomposing into 5NF projections...")
        decomposed_tables_raw = decompose_to_5nf(df, inferred_jds, primary_key)
        
        for columns, result_table, pk_columns in decomposed_tables_raw:
            table_name = f"{'_'.join(columns)}_5NF_relation"
            sql_query = generate_sql_query_5nf(table_name, columns, pk_columns)
            sql_queries.append((table_name, sql_query))
            decomposed_tables.append((table_name, result_table))  # Store with name and DataFrame
            print(f"Generated 5NF Table: {table_name}")
            print(sql_query)
        
    return decomposed_tables, sql_queries

def normalize_csv(input_csv, output_2NF, output_3NF, output_bcnf, output_4nf, output_5nf, primary_key, target_nf):
    try:
        df = pd.read_csv(input_csv, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(input_csv, encoding='ISO-8859-1')
    fd_input = input("Enter the functional dependencies (e.g., 'A -> B; B -> C'): ")
    fds = parse_functional_dependencies(fd_input)

    mvd_input = input("Enter the multi-valued dependencies (use 'A ->> B' format, separated by semicolons): ")
    mvds = parse_multivalued_dependencies(mvd_input)

    
    # Strip any extra whitespace from column names to ensure consistency
    df.columns = df.columns.str.strip()
    primary_key_columns = [col.strip() for col in primary_key.split(",")]
    
    # Step 1: Decompose to 1NF
    if target_nf >= 1:
        df_1nf, decomposed_tables_1nf, converted_to_1nf = check_and_convert_to_1nf(df, primary_key_columns, fds)
        
        # Store 1NF decomposed tables in a dictionary with a consistent structure
        all_tables = {"BaseTable_1NF": (df_1nf, primary_key_columns)}
        for table_name, decomposed_table in decomposed_tables_1nf.items():
            all_tables[table_name] = (decomposed_table, primary_key_columns)

        # Print SQL for base and decomposed tables
        if converted_to_1nf:
            print("Converted data to 1NF.")
            print(generate_sql_query("BaseTable_1NF", df_1nf.columns.tolist(), primary_key_columns))
            for table_name, decomposed_table in decomposed_tables_1nf.items():
                print(generate_sql_query(table_name, decomposed_table.columns.tolist(), primary_key_columns))
        elif target_nf == 1:
            print("Data is already in 1NF.")

    # Step 2: Convert each table to 2NF and beyond
    tables_2nf_dict = {}
    for table_name, (table_df, pk) in all_tables.items():
        relevant_fds = [fd for fd in fds if set(fd[0]).issubset(table_df.columns) and set(fd[1]).issubset(table_df.columns)]
        
        # Convert each table to 2NF
        if target_nf >= 2:
            print(f"\nConverting {table_name} to 2NF...")
            tables_in_2NF_result = convert_to_2NF(table_df, ",".join(pk), relevant_fds, output_2NF)
            for name, tbl, pk_columns in tables_in_2NF_result:
                tables_2nf_dict[name] = (tbl, pk_columns)
        
        # Convert each table to 3NF
        if target_nf >= 3:
            print(f"\nConverting {table_name} to 3NF...")
            tables_in_3NF = convert_to_3NF(table_df, ",".join(pk), relevant_fds, output_3NF, tables_2nf_dict)
        
        if target_nf >= 3.5:
            print(f"\nConverting {table_name} to BCNF...")
            tables_in_bcnf = check_and_convert_to_bcnf(tables_in_3NF, relevant_fds, output_bcnf)

        if target_nf >= 4:
            
            print("\nConverting to 4NF...")
            tables_in_4nf = decompose_to_4nf(tables_in_bcnf, mvds, output_4nf)
        
        if target_nf >= 5:
            print("\nConverting to 5NF...")
            tables_in_5nf = []
            for table_name, table, pk_columns in tables_in_4nf:
                result_tables, sql_queries = check_and_convert_to_5nf(table, pk_columns)
                tables_in_5nf.extend(result_tables)

                # Write 5NF tables to the output file and display SQL
                with open(output_5nf, 'w') as f:
                    f.write("=== 5NF Normalized Tables ===\n\n")
                    for name_suffix, result_table in result_tables:
                        new_table_name = f"{table_name}_{name_suffix}"
                        f.write(f"=== {new_table_name} ===\n")
                        result_table.to_csv(f, index=False)
                        f.write("\n\n")
                        print(f"5NF Table: {new_table_name}")

                    for table_name, query in sql_queries:
                        print(f"\nGenerated SQL for {table_name}:\n{query}\n\n")
                        f.write(f"-- SQL for {table_name} --\n{query}\n\n")

    print(f"Normalization process up to {target_nf}NF completed.")

# Example usage
if __name__ == "__main__":
    input_csv = input("Enter the path to the input CSV file: ")
    output_2NF = "converted2NF.csv"
    output_3NF = "converted3NF.csv"
    output_bcnf = "convertedBCNF.csv"
    output_4nf = "converted4NF.csv"
    output_5nf = "converted5NF.csv"
    primary_key = input("Enter the primary key column names separated by commas: ")
    target_nf = float(input("Enter the target normal form (1, 2, 3, 3.5, 4, or 5): "))
    
    normalize_csv(input_csv, output_2NF, output_3NF, output_bcnf, output_4nf, output_5nf, primary_key,target_nf)
