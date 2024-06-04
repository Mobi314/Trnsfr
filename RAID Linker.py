import pandas as pd
import Alteryx

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

# Helper function to add a new row
def add_new_row(row, linked_program='', linked_project='', linked_outcome=''):
    new_row = row.copy()
    new_row['Linked Program Key'] = linked_program
    new_row['Linked Project Key'] = linked_project
    new_row['Linked Business Outcome Key'] = linked_outcome
    new_rows.append(new_row)

# Process the rows for each type of JIRA key
def process_rows(input_df, jira_keys, combined_column, key_column):
    new_rows = []
    for index, row in input_df.iterrows():
        linked = False
        for key in jira_keys:
            if key in row[combined_column]:
                new_row = row.copy()
                new_row[key_column] = key
                new_rows.append(new_row)
                linked = True
        if not linked:
            new_rows.append(row)
    return new_rows

# Link Program JIRA Keys
program_keys = input_2['Program JIRA Key'].tolist()
temp_rows = process_rows(input_1, program_keys, 'Impacted Projects_Programs Combined', 'Linked Program Key')

# Link Project JIRA Keys
project_keys = input_3['Project JIRA Key'].tolist()
temp_rows_2 = []
for row in temp_rows:
    temp_rows_2.extend(process_rows(pd.DataFrame([row]), project_keys, 'Impacted Projects_Programs Combined', 'Linked Project Key'))

# Link Business Outcome JIRA Keys
business_outcome_keys = input_4['Business Outcome JIRA Key'].tolist()
final_rows = []
for row in temp_rows_2:
    final_rows.extend(process_rows(pd.DataFrame([row]), business_outcome_keys, 'Impacted Items Combined', 'Linked Business Outcome Key'))

# Convert the list of new rows to a DataFrame
output_df = pd.DataFrame(final_rows)

# Output the final dataframe
Alteryx.write(output_df, 1)
