# RDBMS_Normalization

CS5300: DATABASE SYSTEMS
PROGRAMMING PROJECT:
RDMS NORMALIZER



Objective:
To develop a program that takes a database (relations) and functional dependencies as input, normalizes the relations based on the provided functional dependencies, produces SQL queries to generate the normalized database tables, and optionally determines the highest normal form of the input table.

Group Members:
Supraja Yadav Rajaboina - 12619504
Pujitha Ganamukula - 12608977


Input – 
Database Table (.csv) file path.
Functional Dependencies
Multi Valued Dependencies
Primary Key
Choice of the highest normal form to reach (1: 1NF, 2: 2NF, 3: 3NF, 3.5: BCNF, 4: 4NF, 5: 5NF).
Output – 
If the input table is not in 2NF then the table after converting to 1NF would be written to the Console and to converted2NF.csv file and similar.
convertedxNF.csv file which stores the tables generated after being normalized to the xth Normal Form
SQL Queries after normalization are generated and output to the Console.
Requirements –
Database Dataset Table must be stored in a .csv File.

The entire project can be divided into 3 stages:
Stage 1: Taking inputs from the user.
Stage 2: Converting the table to the target Normal Form as per user request.
Stage 3: Writing the output to convertedxNF.csv file and generating SQL Queries to the console.


Stage 1: Taking Inputs
The code initially asks the user for inputs.
Prompting for the CSV File Path:
We prompt the user to provide the path to the CSV file that contains the relational data.
 This file will be read into a DataFrame, forming the base table we aim to normalize.
Loaded as a DataFrame (df) using pd.read_csv()
Prompting for the Primary Key: 
The user provides the primary key columns, separated by commas. 
These are crucial for defining how data is uniquely identified in the table.
This is given in a format separated by commas (M, N, etc)
The primary key columns are stored as a list of strings (primary_key_columns), which is obtained by splitting the primary_key input on commas and removing extra whitespace.
Target Normal Form:
The target normalization form specifies the desired level of normalization (e.g., 1NF, 2NF, 3NF, etc.). 
This guides the normalization process, allowing us to stop once the data reaches the specified form.
It is stored as an integer in the target_nf variable.
Functional Dependencies (FDs):
Functional dependencies represent the relationships between columns. 
The user inputs FDs in the format "A -> B; C,D -> E", where "A -> B" signifies that B is functionally dependent on A, meaning A determines B. 
The FDs are collected as a string from the user
It is then parsed by parse_functional_dependencies, which splits each dependency by -> to create a list of tuples, where each tuple is a dependency pair (LHS and RHS).
Then, each dependency is stored as a tuple of lists in fds
Multi-Valued Dependencies (MVDs):
If the target NF is 4 or higher, we prompt for MVDs. 
An MVD, denoted as A ->> B, indicates that B is multi-valued with respect to A. 
This is particularly relevant for 4NF and 5NF. 
We parse MVDs similarly to FDs, storing them in a structured list.
Similar to FDs, MVDs are collected as a string and parsed by parse_multivalued_dependencies
Each MVD is stored as a tuple of lists in mvd

Stage 2: Converting the Table to the Target Normal Form
When the user specifies the target NF as an integer (1, 2, 3, 3.5, 4, or 5), this value becomes a key control parameter that directs the normalize_csv function. 
This input specifies how far the program should go in the normalization process, from ensuring basic atomicity (1NF) to eliminating join dependencies (5NF). 
Each normalization level (NF) builds upon the previous one, meaning a higher NF automatically requires compliance with all lower NFs.
The conditional branching is processed as follows:
1: 1NF
2: 2NF
3: 3NF
3.5: BCNF
4: 4NF
5: 5NF
The target NF input is received and stored as an integer in target_nf, which is then passed to the normalize_csv function. 
Within normalize_csv, this input drives the conditional execution of specific normalization functions, effectively controlling the flow of the entire process.
target_nf = int(input("Enter the target normal form (1, 2, 3, 3.5, 4, or 5): "))
normalize_csv(input_csv, output_2NF, output_3NF, output_bcnf, output_4nf, output_5nf, primary_key, target_nf)
The normalize_csv function contains conditional checks (if target_nf >= x) for each NF level. These checks ensure that only the necessary steps are executed up to the specified target NF.

1NF:
Ensure atomicity by making each cell contain only single values, removing any multi-valued attributes.
if target_nf >= 1:
    df_1nf, decomposed_tables_1nf, converted_to_1nf = check_and_convert_to_1nf(df, primary_key_columns, fds)
This conditional statement executes if target_nf >= 1
check_1NF: Checks if each cell contains a single, atomic value.
decompose_multivalued_attributes: If multi-valued attributes are found, they are decomposed into separate tables.
check_and_convert_to_1nf: Wraps the above functions, returning the main table in 1NF and any additional tables with decomposed attributes.
2NF: 
Eliminate partial dependencies, where non-key columns depend only on part of a composite primary key.
if target_nf >= 2:
    tables_in_2NF_result = convert_to_2NF(table_df, ",".join(pk), relevant_fds, output_2NF)
This conditional statement executes if target_nf >= 2
find_partial_dependencies: Identifies FDs where non-key attributes are only partially dependent on the primary key.
convert_to_2NF: Uses identified partial dependencies to create new tables, ensuring non-key attributes fully depend on the primary key.
3NF:
Eliminate transitive dependencies, where non-key attributes depend on other non-key attributes
if target_nf >= 3:
   tables_in_3NF = convert_to_3NF(table_df, ",".join(pk), relevant_fds, output_3NF, tables_2nf_dict)
This conditional statement executes if target_nf >= 3
find_transitive_dependencies: Identifies FDs where non-key attributes determine other non-key attributes, indicating transitive dependencies.
convert_to_3NF: Uses identified transitive dependencies to create new tables, ensuring all attributes directly depend only on the primary key.
BCNF:
Ensure that every determinant in an FD is a superkey
if target_nf >= 3.5:
    tables_in_bcnf = check_and_convert_to_bcnf(tables_in_3NF, relevant_fds, output_bcnf)
This conditional statement executes if target_nf >= 3.5
is_in_bcnf: Verifies if each determinant (left-hand side of FD) is a superkey.
check_and_convert_to_bcnf: Uses non-superkey determinants to decompose tables, ensuring BCNF compliance.
4NF:
Eliminate multi-valued dependencies (MVDs) unless the left-hand side is a superkey.
if target_nf >= 4:
    mvd_input = input("Enter the multi-valued dependencies (use 'A ->> B' format, separated by semicolons): ")
    mvds = parse_multivalued_dependencies(mvd_input)
  tables_in_4nf = decompose_to_4nf(tables_in_bcnf, mvds, output_4nf)
This conditional statement executes if target_nf >= 4
is_in_4nf: Checks if MVDs are correctly structured so that each LHS is a superkey.
decompose_to_4nf: Uses MVDs to decompose tables, removing independent multi-valued attributes that create redundancy.
5NF:
Eliminate join dependencies, ensuring no redundancy in projections
if target_nf == 5:
   tables_in_5nf = []
    for table_name, table, pk_columns in tables_in_4nf:
        result_tables, sql_queries = check_and_convert_to_5nf(table, pk_columns)
        tables_in_5nf.extend(result_tables)
This conditional statement executes if target_nf == 5
find_join_dependencies_and_check_5nf: Identifies join dependencies and checks if further decomposition is required to eliminate redundancy.
check_and_convert_to_5nf: Decomposes tables based on join dependencies, creating projections as separate tables.


The normalize_csv function controls the conditional execution of each normalization function based on the target_nf input, with each step addressing a specific type of dependency violation.

check_1NF:
def check_1NF(df):
    for col in df.columns:
       for value in df[col]:
            if isinstance(value, str) and ',' in value:
                return False  # Not in 1NF
   return True  # In 1NF
Determines if a table is in 1NF by checking if any column contains multiple values within a single cell.
Column Scan: Iterates over each column, checking if values contain commas (indicating multiple values).
Returns Boolean: Returns False if a multi-valued attribute is detected; otherwise, it returns True.

check_and_convert_to_1nf:
def check_and_convert_to_1nf(df, primary_key_columns, fds):
    if check_1NF(df):
        print("Data is already in 1NF.")
        return df, {}, False

    print("Converting to 1NF by decomposing multi-valued attributes.")
    decomposed_tables = decompose_multivalued_attributes(df, fds)
    main_table_columns = [col for col in df.columns if col not in decomposed_tables.keys()]
    df_1nf = df[main_table_columns].drop_duplicates().reset_index(drop=True)

    return df_1nf, decomposed_tables, True


Checks if a DataFrame is in 1NF and decomposes it if necessary.
Checks 1NF Compliance: Calls check_1NF to verify 1NF compliance.
Decomposes Multi-Valued Attributes: If not in 1NF, it decomposes the DataFrame using decompose_multivalued_attributes.
Returns Updated Data: Returns the main table in 1NF, any decomposed tables, and a boolean indicating if conversion was applied.

convert_to_2NF:
def convert_to_2NF(df, primary_key, fds, output_csv):
    primary_key_columns = [col.strip() for col in primary_key.split(',')]
    partial_dependencies = find_partial_dependencies(fds, primary_key_columns)

    tables_in_2NF = []
    for lhs, rhs in partial_dependencies:
        table_name = '_'.join(lhs) + "_relation"
        new_table = df[lhs + rhs].drop_duplicates()
        tables_in_2NF.append((table_name, new_table, lhs))
        print(f"Generated SQL for 2NF Partial Dependency Table: {table_name}")
        print(generate_sql_query(table_name, new_table.columns, lhs))

    moved_columns = {col for _, table, _ in tables_in_2NF for col in table.columns}
    remaining_columns = [col for col in df.columns if col in primary_key_columns or col not in moved_columns]

    if remaining_columns:
        main_table_name = f"{'_'.join(primary_key_columns)}_main_relation"
        df_2NF = df[remaining_columns].drop_duplicates()
        tables_in_2NF.append((main_table_name, df_2NF, primary_key_columns))
        print(generate_sql_query(main_table_name, df_2NF.columns, primary_key_columns))

    return tables_in_2NF


Converts a DataFrame to 2NF by removing partial dependencies.
Identifies Partial Dependencies: Calls find_partial_dependencies to identify partial dependencies.
Decomposes Table: Creates separate tables for each partial dependency.
Returns 2NF Tables: Returns a list of tables in 2NF, including the main table and tables for each partial dependency.

convert_to_3NF:
def convert_to_3NF(df, primary_key, fds, output_csv, tables_in_2NF):
    primary_key_columns = [col.strip() for col in primary_key.split(',')]
    transitive_dependencies = find_transitive_dependencies(fds, primary_key_columns)

    tables_in_3NF = []
    for table_name, table, pk_columns in tables_in_2NF:
        moved_columns = set()
        for lhs, rhs in transitive_dependencies:
            if set(lhs).issubset(table.columns) and any(col in table.columns for col in rhs):
                transitive_table_name = '_'.join(lhs) + "_relation"
                transitive_table = table[lhs + rhs].drop_duplicates()
                tables_in_3NF.append((transitive_table_name, transitive_table, lhs))
                moved_columns.update(rhs)

        remaining_columns = [col for col in table.columns if col not in moved_columns]
        if remaining_columns:
            final_relation_table = table[remaining_columns].drop_duplicates()
            tables_in_3NF.append((table_name, final_relation_table, pk_columns))

    return tables_in_3NF


Converts a DataFrame to 3NF by removing transitive dependencies. Transitive dependencies exist when a non-key attribute depends on another non-key attribute rather than directly on the primary key.
Identifies Transitive Dependencies: Uses find_transitive_dependencies to find dependencies where a non-key attribute depends on another non-key attribute.
Decomposes Each Dependency: For each transitive dependency, it creates a new table with the left-hand side (LHS) and right-hand side (RHS) attributes of the dependency.
Updates Main Table: After extracting transitive dependencies, the original table is updated to remove these dependencies, ensuring that all remaining columns depend directly on the primary key.
Returns 3NF Tables: It returns a list of tables in 3NF, including both the main table and any tables created for transitive dependencies.

is_in_bcnf:
def is_in_bcnf(table, fds, pk_columns):
    for lhs, rhs in fds:
        lhs_is_superkey = set(pk_columns).issubset(lhs) or set(lhs) == set(pk_columns)
        if not lhs_is_superkey:
            return False
    return True
Checks if a table is in BCNF (Boyce-Codd Normal Form). 
In BCNF, every determinant (LHS of an FD) must be a superkey, meaning that it uniquely identifies each row.
Superkey Check: For each FD, it checks if the LHS is a superkey by comparing it to the primary key columns.
Returns Boolean: If any FD has a determinant that is not a superkey, the function returns False, indicating that the table violates BCNF. If all FDs are superkey-determined, it returns True.

check_and_convert_to_bcnf:
def check_and_convert_to_bcnf(tables_in_3NF, fds, output_sql_file="bcnf_queries.sql"):
    tables_in_bcnf = []
    with open(output_sql_file, 'w') as sql_file:
        for table_name, table, pk_columns in tables_in_3NF:
            relevant_fds = [(lhs, rhs) for lhs, rhs in fds if set(lhs).issubset(table.columns)]
            if is_in_bcnf(table, relevant_fds, pk_columns):
                tables_in_bcnf.append((table_name, table, pk_columns))
                sql_query = generate_sql_query(table_name, table.columns, pk_columns)
                sql_file.write(sql_query + "\n\n")
            else:
                for lhs, rhs in relevant_fds:
                    bcnf_table_name = '_'.join(lhs) + "_BCNF_relation"
                    decomposed_table = table[lhs + rhs].drop_duplicates()
                    tables_in_bcnf.append((bcnf_table_name, decomposed_table, lhs))
                    sql_query = generate_sql_query(bcnf_table_name, decomposed_table.columns, lhs)
                    sql_file.write(sql_query + "\n\n")
 
    return tables_in_bcnf
Converts a table to BCNF by decomposing any FDs where the LHS is not a superkey. This ensures that every determinant is a superkey.
Identifies BCNF Violations: Uses is_in_bcnf to check if each table is in BCNF.
Decomposes Tables: For any table with FDs that violate BCNF, it creates a new table containing the LHS and RHS of the violating FD.
Generates SQL: Writes CREATE TABLE statements for each decomposed table into a .sql file.
Returns BCNF Tables: It returns a list of tables in BCNF, including both compliant tables and any newly created tables to resolve BCNF violations.

is_in_4nf:
def is_in_4nf(table, mvds, pk_columns):
    for lhs, rhs in mvds:
        if not set(lhs).issuperset(pk_columns) and validate_mvd(table, lhs, rhs):
            return False
    return True
Checks if a table is in 4NF (Fourth Normal Form). In 4NF, there should be no multi-valued dependencies unless the LHS is a superkey.
Validates MVDs: For each MVD, it uses validate_mvd to check if the MVD holds.
Superkey Check: If the LHS of the MVD is not a superkey and the MVD holds, this violates 4NF.
Returns Boolean: Returns True if the table is in 4NF; otherwise, it returns False.

decompose_to_4nf:
def decompose_to_4nf(tables_in_bcnf, mvds, output_sql_file="4nf_queries.sql"):
    tables_in_4nf = []
    with open(output_sql_file, 'w') as sql_file:
        for table_name, table, pk_columns in tables_in_bcnf:
            relevant_mvds = [(lhs, rhs) for lhs, rhs in mvds if set(lhs).issubset(table.columns)]
            if is_in_4nf(table, relevant_mvds, pk_columns):
                tables_in_4nf.append((table_name, table, pk_columns))
                sql_query = generate_sql_query(table_name, table.columns, pk_columns)
                sql_file.write(sql_query + "\n\n")
            else:
                for lhs, rhs in relevant_mvds:
                    new_table_name = '_'.join(lhs) + "_4NF_relation"
                    decomposed_table = table[lhs + rhs].drop_duplicates()
                    tables_in_4nf.append((new_table_name, decomposed_table, lhs))
                    sql_query = generate_sql_query(new_table_name, decomposed_table.columns, lhs)
                    sql_file.write(sql_query + "\n\n")

    return tables_in_4nf

Decomposes a table to 4NF by separating any MVDs where the LHS is not a superkey.
Identifies 4NF Violations: Uses is_in_4nf to check if the table violates 4NF.
Decomposes MVDs: For each MVD violation, it creates a new table containing the LHS and RHS of the MVD.
Generates SQL: Writes CREATE TABLE statements for each decomposed table into a .sql file.
Returns 4NF Tables: Returns a list of tables in 4NF, including both compliant tables and any newly created tables for MVD violations.

find_join_dependencies_and_check_5nf:
def find_join_dependencies_and_check_5nf(df):
    dp_results = {}
    inferred_jds = []
    attributes = df.columns.tolist()

    for subset_size in range(2, len(attributes)):
        for subset in combinations(attributes, subset_size):
            subset_key = tuple(sorted(subset))
            remaining_attributes = list(set(attributes) - set(subset))
            
            if set(subset).intersection(remaining_attributes):
                projection_dfs = [df[list(projection)].drop_duplicates() for projection in [subset, remaining_attributes]]
                dp_results[subset_key] = can_reconstruct(df, projection_dfs)

                if dp_results[subset_key]:
                    inferred_jds.append((list(subset), remaining_attributes))
    is_5nf = all(dp_results[key] for key in dp_results if dp_results[key])
    
    return is_5nf, inferred_jds
Identifies join dependencies (JDs) in a table, which cause redundancy when joining projections, and checks if the table is in 5NF (Fifth Normal Form)
Subsets of Attributes: Examines all attribute subsets to identify potential JDs.
Checks for Redundancy: For each JD, it projects subsets of attributes and tests if joining them recreates the original table without redundancy.
Returns JD Information: If the table is not in 5NF, it returns the inferred JDs for further decomposition.
decompose_to_5nf:
def decompose_to_5nf(df, inferred_jds, original_pk):
    decomposed_tables = []
    for left, right in inferred_jds:
        projection_df = df[left + right].drop_duplicates()
        pk_for_projection = left if set(left).issuperset(set(original_pk)) else original_pk
        decomposed_tables.append((left + right, projection_df, pk_for_projection))
        print(f"Decomposed projection for JD {left} -> {right}:\n{projection_df}\n")
    
    return decomposed_tables
Decomposes tables to 5NF by creating separate projections based on identified JDs, ensuring that no redundancy exists from joining subsets.
Creates Projections: For each JD, it creates a new table containing the projection of attributes based on the JD.
Adds Primary Key: Determines an appropriate primary key for each projection.
Returns 5NF Tables: Returns a list of decomposed tables that are in 5NF.


Stage 3: Writing Output and Generating SQL Queries
After the normalization process completes up to the specified target NF, the program will:
Save each decomposed table to CSV files, clearly indicating which normal form each table meets.
Generate SQL CREATE TABLE statements for each table, specifying the structure (columns and primary keys) of each normalized table.
These outputs provide the user with:
Data Storage: CSV files that contain each normalized table.
Database Creation: SQL queries that can be used to recreate the schema in a relational database.
Saving Decomposed Tables to CSV Files:
Each normalization function in Stage 2 saves its decomposed tables to a CSV file labeled with the target normal form. For example:
output_2NF: Stores the tables decomposed into 2NF.
output_3NF: Stores the tables decomposed into 3NF.
output_bcnf: Stores tables decomposed to BCNF.
output_4nf: Stores tables decomposed to 4NF.
output_5nf: Stores tables decomposed to 5NF.
What’s Stored in Each output File?
Each output file for a given NF contains:
Table Names: A header with the name of each decomposed table.
Table Data: The columns and rows of each table, saved using table.to_csv.
Primary Key Columns: These columns are preserved for each table, ensuring that each decomposed table retains a primary key.
This structure makes it easy to visually inspect the decomposed tables and provides ready-to-use data files for importing into a relational database or further analysis.
Example: convert_to_2nf
def convert_to_2NF(df, primary_key, fds, output_csv):
    primary_key_columns = [col.strip() for col in primary_key.split(',')]
    partial_dependencies = find_partial_dependencies(fds, primary_key_columns)

    tables_in_2NF = []
    for lhs, rhs in partial_dependencies:
        table_name = '_'.join(lhs) + "_relation"
        new_table = df[lhs + rhs].drop_duplicates()
        tables_in_2NF.append((table_name, new_table, lhs))
        print(f"Generated SQL for 2NF Partial Dependency Table: {table_name}")
        print(generate_sql_query(table_name, new_table.columns, lhs))

    moved_columns = {col for _, table, _ in tables_in_2NF for col in table.columns}
    remaining_columns = [col for col in df.columns if col in primary_key_columns or col not in moved_columns]

    if remaining_columns:
        main_table_name = f"{'_'.join(primary_key_columns)}_main_relation"
        df_2NF = df[remaining_columns].drop_duplicates()
        tables_in_2NF.append((main_table_name, df_2NF, primary_key_columns))
        print(generate_sql_query(main_table_name, df_2NF.columns, primary_key_columns))

    return tables_in_2NF

# Write 2NF tables to the output CSV 
with open(output_csv, 'w') as f: 
f.write("=== 2NF Normalized Tables ===\n\n") 
for name, table, pk in tables_in_2NF: 
f.write(f"=== {name} ===\n") table.to_csv(f, index=False) 
f.write("\n\n")

Generating SQL Queries for Table Creation
SQL CREATE TABLE statements are generated for each decomposed table using the generate_sql_query function. Each SQL statement specifies:
Table Name: The name of the table, which often reflects the columns that were decomposed.
Column Definitions: Each column in the table, typically set to VARCHAR(255) for simplicity.
Primary Key Constraints: A primary key constraint that enforces the unique identifier(s) for each table.
This function constructs SQL statements by iterating over the columns and adding a primary key constraint at the end:
def generate_sql_query(table_name, columns, primary_key_columns=None):
    sql_query = f"CREATE TABLE {table_name} (\n"
    for col in columns:
        sql_query += f"    {col} VARCHAR(255),\n"
    if primary_key_columns:
        pk_str = ", ".join(primary_key_columns)
        sql_query += f"    PRIMARY KEY ({pk_str}),\n"
    return sql_query.rstrip(',\n') + "\n);"
Where SQL Statements Are Generated
For each normalization function, SQL statements are printed to the console and, in certain cases, saved to an SQL file.
After decomposing a table to 2NF, generate_sql_query is called for each table to print the SQL statement:
for name, table, pk in tables_in_2NF:
    print(f"Generated SQL for 2NF Partial Dependency Table: {name}")
    print(generate_sql_query(name, table.columns, pk))
The SQL output serves as a blueprint for reconstructing the normalized tables in a relational database. Each table’s SQL query includes the CREATE TABLE command, column definitions, and the primary key constraint, ensuring that:
Column Structure: The table’s columns and data types are defined.
Primary Keys: Each table enforces primary keys, essential for maintaining referential integrity.
Conditional Execution Based on Target NF
Each normalization level generates its own SQL queries and CSV files, and this process is controlled by the target_nf input. The program conditionally saves each normalized table only up to the specified target NF. This prevents unnecessary decompositions or output generation for higher NFs than needed.
In normalize_csv, tables are saved only up to the target NF, using conditional statements to control the flow.
If target_nf = 2:
Only 1NF and 2NF tables are saved to CSV and SQL.
Higher NFs (3NF, BCNF, etc.) are not processed, as they exceed the target NF.
If target_nf = 4:
The program saves tables normalized up to 4NF, including 1NF, 2NF, 3NF, and BCNF tables.
The program skips 5NF processing since it exceeds the specified target.
Example:
if target_nf >= 1:
 # Decompose to 1NF and save output
if target_nf >= 2:
# Decompose to 2NF and save output
if target_nf >= 3:
# Decompose to 3NF and save output
if target_nf >= 3.5:
# Decompose to BCNF and save output
if target_nf >= 4:
 # Decompose to 4NF and save output
if target_nf == 5:
# Decompose to 5NF and save output
Each normalization step builds upon the previous one, ensuring that data is only normalized and saved up to the specified target NF, avoiding extra processing.
In conclusion,
After Stage 3 completes, the user has:
CSV Files: Each CSV file (converted2NF.csv, converted3NF.csv, etc.) contains normalized tables up to the specified NF.
SQL Queries: SQL CREATE TABLE statements for each table are printed to the console and/or saved to SQL files (output_2NF.sql, output_3NF.sql, etc.).
Logical and Physical Data Structure:
The normalized data is organized into tables, each enforcing referential integrity through primary keys.
SQL queries provide a structured schema for database creation.

