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

# Initialize lists to store new rows
new_rows = []

# Link Program JIRA Keys
program_keys = input_2['Program JIRA Key'].tolist()
for index, row in input_1.iterrows():
    linked = False
    for key in program_keys:
        if key in row['Impacted Projects_Programs Combined']:
            new_row = row.copy()
            new_row['Linked Program Key'] = key
            new_row['Linked Project Key'] = ''
            new_row['Linked Business Outcome Key'] = ''
            new_rows.append(new_row)
            linked = True
            break
    if not linked:
        new_rows.append(row)

# Link Project JIRA Keys
project_keys = input_3['Project JIRA Key'].tolist()
temp_rows = []
for row in new_rows:
    linked = False
    for key in project_keys:
        if key in row['Impacted Projects_Programs Combined']:
            new_row = row.copy()
            new_row['Linked Program Key'] = ''
            new_row['Linked Project Key'] = key
            new_row['Linked Business Outcome Key'] = ''
            temp_rows.append(new_row)
            linked = True
            break
    if not linked:
        temp_rows.append(row)

# Link Business Outcome JIRA Keys
business_outcome_keys = input_4['Business Outcome JIRA Key'].tolist()
final_rows = []
for row in temp_rows:
    linked = False
    for key in business_outcome_keys:
        if key in row['Impacted Items Combined']:
            new_row = row.copy()
            new_row['Linked Program Key'] = ''
            new_row['Linked Project Key'] = ''
            new_row['Linked Business Outcome Key'] = key
            final_rows.append(new_row)
            linked = True
            break
    if not linked:
        final_rows.append(row)

# Convert the list of new rows to a DataFrame
output_df = pd.DataFrame(final_rows)

# Output the final dataframe
Alteryx.write(output_df, 1)
