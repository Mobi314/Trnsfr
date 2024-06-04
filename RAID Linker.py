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

# Initialize list to store new rows
new_rows = []

# Helper function to create new rows for linked keys
def create_linked_rows(row, keys, combined_column, key_column):
    rows = []
    for key in keys:
        if key in row[combined_column]:
            new_row = row.copy()
            new_row['Linked Program Key'] = ''
            new_row['Linked Project Key'] = ''
            new_row['Linked Business Outcome Key'] = ''
            new_row[key_column] = key
            rows.append(new_row)
    return rows

# Process Program JIRA Keys
program_keys = input_2['Program JIRA Key'].tolist()
for index, row in input_1.iterrows():
    linked_rows = create_linked_rows(row, program_keys, 'Impacted Projects_Programs Combined', 'Linked Program Key')
    if linked_rows:
        new_rows.extend(linked_rows)
    else:
        new_rows.append(row)

# Convert to DataFrame
temp_df = pd.DataFrame(new_rows)

# Reset new_rows for Project processing
new_rows = []

# Process Project JIRA Keys
project_keys = input_3['Project JIRA Key'].tolist()
for index, row in temp_df.iterrows():
    linked_rows = create_linked_rows(row, project_keys, 'Impacted Projects_Programs Combined', 'Linked Project Key')
    if linked_rows:
        new_rows.extend(linked_rows)
    else:
        new_rows.append(row)

# Convert to DataFrame
temp_df = pd.DataFrame(new_rows)

# Reset new_rows for Business Outcome processing
new_rows = []

# Process Business Outcome JIRA Keys
business_outcome_keys = input_4['Business Outcome JIRA Key'].tolist()
for index, row in temp_df.iterrows():
    linked_rows = create_linked_rows(row, business_outcome_keys, 'Impacted Items Combined', 'Linked Business Outcome Key')
    if linked_rows:
        new_rows.extend(linked_rows)
    else:
        new_rows.append(row)

# Convert the list of new rows to a DataFrame
output_df = pd.DataFrame(new_rows)

# Output the final dataframe
Alteryx.write(output_df, 1)
