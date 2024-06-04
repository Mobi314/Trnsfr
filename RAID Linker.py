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

# Helper function to add a new row
def add_new_row(row, linked_program='', linked_project='', linked_outcome=''):
    new_row = row.copy()
    new_row['Linked Program Key'] = linked_program
    new_row['Linked Project Key'] = linked_project
    new_row['Linked Business Outcome Key'] = linked_outcome
    new_rows.append(new_row)

# Link Program JIRA Keys
program_keys = input_2['Program JIRA Key'].tolist()
for index, row in input_1.iterrows():
    linked = False
    for key in program_keys:
        if key in row['Impacted Projects_Programs Combined']:
            add_new_row(row, linked_program=key)
            linked = True
    if not linked:
        add_new_row(row)

# Link Project JIRA Keys
project_keys = input_3['Project JIRA Key'].tolist()
temp_rows = new_rows.copy()
new_rows = []
for row in temp_rows:
    linked = False
    for key in project_keys:
        if key in row['Impacted Projects_Programs Combined']:
            add_new_row(row, linked_project=key)
            linked = True
    if not linked:
        new_rows.append(row)

# Link Business Outcome JIRA Keys
business_outcome_keys = input_4['Business Outcome JIRA Key'].tolist()
temp_rows = new_rows.copy()
new_rows = []
for row in temp_rows:
    linked = False
    for key in business_outcome_keys:
        if key in row['Impacted Items Combined']:
            add_new_row(row, linked_outcome=key)
            linked = True
    if not linked:
        new_rows.append(row)

# Convert the list of new rows to a DataFrame
output_df = pd.DataFrame(new_rows)

# Output the final dataframe
Alteryx.write(output_df, 1)
