import pandas as pd

# Read input data
input_1 = Alteryx.read("#1")
input_2 = Alteryx.read("#2")
input_3 = Alteryx.read("#3")
input_4 = Alteryx.read("#4")

# Function to combine columns based on a keyword
def combine_columns(df, keyword):
    combined = df.filter(regex=keyword).apply(lambda row: ','.join(row.dropna()), axis=1)
    return combined

# Combine Impacted Project(s) and Impacted Items columns
input_1['Impacted Projects_Programs Combined'] = combine_columns(input_1, 'Impacted Project')
input_1['Impacted Items Combined'] = combine_columns(input_1, 'Impacted Items')

# Read the keys from input streams
program_keys = set(input_2['Program JIRA Key'].tolist())
project_keys = set(input_3['Project JIRA Key'].tolist())
business_outcome_keys = set(input_4['Business Outcome JIRA Key'].tolist())

# Initialize list to store all rows
all_rows = []

# Initialize columns for failed linkages
input_1['Failed Program Linkages'] = ''
input_1['Failed Project Linkages'] = ''
input_1['Failed Business Outcome Linkages'] = []

# Process each RAID record
for index, row in input_1.iterrows():
    combined_program_project_keys = [key.strip() for key in row['Impacted Projects_Programs Combined'].split(',')]
    combined_items_keys = [key.strip() for key in row['Impacted Items Combined'].split(',')]
    
    # Track if the row has been linked
    linked = False

    # Track failed linkages
    failed_programs = []
    failed_projects = []
    failed_business_outcomes = []

    # Process Program and Project keys
    for key in combined_program_project_keys:
        if key in program_keys:
            new_row = row.copy()
            new_row['Linked Program Key'] = key
            new_row['Linked Project Key'] = ''
            new_row['Linked Business Outcome Key'] = ''
            all_rows.append(new_row)
            linked = True
        elif key in project_keys:
            new_row = row.copy()
            new_row['Linked Program Key'] = ''
            new_row['Linked Project Key'] = key
            new_row['Linked Business Outcome Key'] = ''
            all_rows.append(new_row)
            linked = True
        else:
            failed_programs.append(key) if key not in program_keys else failed_projects.append(key)

    # Process Business Outcome keys
    for key in combined_items_keys:
        if key in business_outcome_keys:
            new_row = row.copy()
            new_row['Linked Program Key'] = ''
            new_row['Linked Project Key'] = ''
            new_row['Linked Business Outcome Key'] = key
            all_rows.append(new_row)
            linked = True
        else:
            failed_business_outcomes.append(key)

    # If no keys matched, keep the original row
    if not linked:
        all_rows.append(row)

    # Update failed linkages columns
    row['Failed Program Linkages'] = ','.join(failed_programs)
    row['Failed Project Linkages'] = ','.join(failed_projects)
    row['Failed Business Outcome Linkages'] = ','.join(failed_business_outcomes)

# Convert the list of new rows to a DataFrame
final_df = pd.DataFrame(all_rows)

# Output the final dataframe
Alteryx.write(final_df, 1)



import pandas as pd

# Read input data
input_1 = Alteryx.read("#1")
input_2 = Alteryx.read("#2")
input_3 = Alteryx.read("#3")
input_4 = Alteryx.read("#4")

# Function to combine columns based on a keyword
def combine_columns(df, keyword):
    combined = df.filter(regex=keyword).apply(lambda row: ','.join(row.dropna()), axis=1)
    return combined

# Combine Impacted Project(s) and Impacted Items columns
input_1['Impacted Projects_Programs Combined'] = combine_columns(input_1, 'Impacted Project')
input_1['Impacted Items Combined'] = combine_columns(input_1, 'Impacted Items')

# Read the keys from input streams
program_keys = set(input_2['Program JIRA Key'].tolist())
project_keys = set(input_3['Project JIRA Key'].tolist())
business_outcome_keys = set(input_4['Business Outcome JIRA Key'].tolist())

# Initialize list to store all rows
all_rows = []

# Process each RAID record
for index, row in input_1.iterrows():
    combined_program_project_keys = [key.strip() for key in row['Impacted Projects_Programs Combined'].split(',')]
    combined_items_keys = [key.strip() for key in row['Impacted Items Combined'].split(',')]
    
    # Track if the row has been linked
    linked = False

    # Process Program and Project keys
    for key in combined_program_project_keys:
        if key in program_keys:
            new_row = row.copy()
            new_row['Linked Program Key'] = key
            new_row['Linked Project Key'] = ''
            new_row['Linked Business Outcome Key'] = ''
            all_rows.append(new_row)
            linked = True
        elif key in project_keys:
            new_row = row.copy()
            new_row['Linked Program Key'] = ''
            new_row['Linked Project Key'] = key
            new_row['Linked Business Outcome Key'] = ''
            all_rows.append(new_row)
            linked = True

    # Process Business Outcome keys
    for key in combined_items_keys:
        if key in business_outcome_keys:
            new_row = row.copy()
            new_row['Linked Program Key'] = ''
            new_row['Linked Project Key'] = ''
            new_row['Linked Business Outcome Key'] = key
            all_rows.append(new_row)
            linked = True

    # If no keys matched, keep the original row
    if not linked:
        all_rows.append(row)

# Convert the list of new rows to a DataFrame
final_df = pd.DataFrame(all_rows)

# Output the final dataframe
Alteryx.write(final_df, 1)
