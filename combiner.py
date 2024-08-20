import pandas as pd

# Constants for hierarchy levels
HIERARCHY = ["Program", "Project", "Business Outcome"]
RELEVANT_TYPES = set(HIERARCHY + ["Key Deliverable"])

# Function to parse links
def parse_links(links):
    return [link.split(":") for link in links.split(",") if link]

# Function to recursively find hierarchy
def find_hierarchy(item, all_data, hierarchy_level, parent_chain=[]):
    current_type = hierarchy_level[item]
    parent_chain.append(item)
    
    if current_type == "Program":
        return [parent_chain]
    
    results = []
    parents = all_data[current_type].get(item, {}).get('parents', [])
    
    for parent_type, parent_key in parents:
        if parent_type in RELEVANT_TYPES and HIERARCHY.index(parent_type) < HIERARCHY.index(current_type):
            results.extend(find_hierarchy(parent_key, all_data, hierarchy_level, parent_chain.copy()))
    
    if not results:
        return [parent_chain]
    
    return results

# Read in data
programs = pd.read_csv('Programs.csv')
projects = pd.read_csv('Projects.csv')
business_outcomes = pd.read_csv('BusinessOutcomes.csv')

# Prepare data dictionaries
all_data = {
    "Program": {row['key']: {'parents': parse_links(row['All Parent Links']), 'children': parse_links(row['All Child Links'])} for _, row in programs.iterrows()},
    "Project": {row['key']: {'parents': parse_links(row['All Parent Links']), 'children': parse_links(row['All Child Links'])} for _, row in projects.iterrows()},
    "Business Outcome": {row['key']: {'parents': parse_links(row['All Parent Links']), 'children': parse_links(row['All Child Links'])} for _, row in business_outcomes.iterrows()}
}

# Create a unified dictionary for quick type lookup
hierarchy_level = {}
for key, type_dict in all_data.items():
    for item in type_dict:
        hierarchy_level[item] = key

# Process business outcomes as starting point
final_records = []

for bo_key in all_data['Business Outcome']:
    hierarchies = find_hierarchy(bo_key, all_data, hierarchy_level)
    for hierarchy in hierarchies:
        record = {
            "Program Jira Key": '',
            "Project Jira Key": '',
            "Business Outcome Jira Key": ''
        }
        for item in hierarchy:
            item_type = hierarchy_level[item]
            record[f"{item_type} Jira Key"] = item
        
        final_records.append(record)

# Convert to DataFrame for output
output_df = pd.DataFrame(final_records)
output_df.to_csv('output.csv', index=False)

print("Process Complete. Output saved as 'output.csv'.")
