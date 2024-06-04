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

# Initialize new columns
input_1['Linked Program Key'] = ''
input_1['Linked Project Key'] = ''
input_1['Linked Business Outcome Key'] = ''

# Function to link JIRA keys
def link_jira_keys(row, key_list, column_name):
    for key in key_list:
        if key in row[column_name]:
            return key
    return ''

# Link Program JIRA Keys
program_keys = input_2['Program JIRA Key'].tolist()
input_1['Linked Program Key'] = input_1.apply(lambda row: link_jira_keys(row, program_keys, 'Impacted Projects_Programs Combined'), axis=1)

# Link Project JIRA Keys
project_keys = input_3['Project JIRA Key'].tolist()
input_1['Linked Project Key'] = input_1.apply(lambda row: link_jira_keys(row, project_keys, 'Impacted Projects_Programs Combined'), axis=1)

# Link Business Outcome JIRA Keys
business_outcome_keys = input_4['Business Outcome JIRA Key'].tolist()
input_1['Linked Business Outcome Key'] = input_1.apply(lambda row: link_jira_keys(row, business_outcome_keys, 'Impacted Items Combined'), axis=1)

# Output the final dataframe
Alteryx.write(input_1, 1)
