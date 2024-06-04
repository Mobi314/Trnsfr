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

# Initialize lists to store new rows for each hierarchy type
all_rows = []

# Helper function to create new rows for linked keys
def create_linked_rows(row, keys, combined_column, key_column):
    rows = []
    combined_keys = row[combined_column].split(',')
    for key in combined_keys:
        if key in keys:
            new_row = row.copy()
            new_row['Linked Program Key'] = ''
            new_row['Linked Project Key'] = ''
            new_row['Linked Business Outcome Key'] = ''
            new_row[key_column] = key
            rows.append(new_row)
    if not rows:
        rows.append(row)
    return rows

# Process each RAID record by checking against hierarchy input streams

# Process Program JIRA Keys
program_keys = input_2['Program JIRA Key'].tolist()
for index, row in input_1.iterrows():
    linked_rows = create_linked_rows(row, program_keys, 'Impacted Projects_Programs Combined', 'Linked Program Key')
    all_rows.extend(linked_rows)

# Convert to DataFrame
temp_df = pd.DataFrame(all_rows)

# Reset all_rows for Project processing
all_rows = []

# Process Project JIRA Keys
project_keys = input_3['Project JIRA Key'].tolist()
for index, row in temp_df.iterrows():
    linked_rows = create_linked_rows(row, project_keys, 'Impacted Projects_Programs Combined', 'Linked Project Key')
    all_rows.extend(linked_rows)

# Convert to DataFrame
temp_df = pd.DataFrame(all_rows)

# Reset all_rows for Business Outcome processing
all_rows = []

# Process Business Outcome JIRA Keys
business_outcome_keys = input_4['Business Outcome JIRA Key'].tolist()
for index, row in temp_df.iterrows():
    linked_rows = create_linked_rows(row, business_outcome_keys, 'Impacted Items Combined', 'Linked Business Outcome Key')
    all_rows.extend(linked_rows)

# Convert the list of new rows to a DataFrame
final_df = pd.DataFrame(all_rows)

# Output the final dataframe
Alteryx.write(final_df, 1)
