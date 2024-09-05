import pandas as pd

# Step 1: Read the input data from Alteryx input stream #1
df_input = Alteryx.read("#1")  # Reading input table from Alteryx (Input 1)

# Step 2: Define functions for the binary search and recursive linkage process

def binary_search(dataframe, key):
    """Binary search for a key in a sorted dataframe based on the 'key' field."""
    low, high = 0, len(dataframe) - 1
    while low <= high:
        mid = (low + high) // 2
        mid_key = dataframe.iloc[mid]['key']
        if mid_key == key:
            return dataframe.iloc[mid]  # Found the key, return the record
        elif mid_key < key:
            low = mid + 1
        else:
            high = mid - 1
    return None  # Key not found

def validate_link(parent_record, child_key):
    """Check if the child_key exists in the parent's 'All Child Links' field."""
    child_links = parent_record['All Child Links'].split(',')
    for link in child_links:
        _, linked_key = link.split(':')  # Extract the key from the 'Type:key' format
        if linked_key == child_key:
            return True
    return False

def process_hierarchy(record, hierarchy, input_table):
    """Recursively process parent links to build the full hierarchy."""
    parent_links = record['All Parent Links'].split(',')
    for link in parent_links:
        parent_type, parent_key = link.split(':')  # Extract parent key and type
        parent_record = binary_search(input_table, parent_key)  # Find parent using binary search
        
        if parent_record is not None and validate_link(parent_record, record['key']):
            # Add the valid link to the hierarchy
            hierarchy.append((parent_record['key'], record['key']))  # Store as tuple of parent-child
            if parent_record['Type'] != 'Program':  # If the parent is not a Program, continue upwards
                process_hierarchy(parent_record, hierarchy, input_table)
                
    return hierarchy

# Step 3: Iterate through the input table and build the hierarchy linkages

output_rows = []
for idx, record in df_input.iterrows():
    hierarchy = []
    
    # If the record has non-empty parent links, we process them recursively
    if pd.notna(record['All Parent Links']) and record['All Parent Links'].strip():
        hierarchy = process_hierarchy(record, hierarchy, df_input)
    
    # Add to output table based on the hierarchy built
    for parent_key, child_key in hierarchy:
        parent_record = binary_search(df_input, parent_key)
        if parent_record is not None:  # Fix ambiguous Series truth value issue
            if parent_record['Type'] == 'Program':
                output_rows.append([parent_key, None, None, child_key])  # Program to Business Outcome/Project
            elif parent_record['Type'] == 'Project':
                output_rows.append([None, parent_key, None, child_key])  # Project to Business Outcome
            elif parent_record['Type'] == 'Business Outcome':
                output_rows.append([None, None, parent_key, child_key])  # Business Outcome to Business Outcome

# Step 4: Create a new dataframe for the output table
df_output = pd.DataFrame(output_rows, columns=["Program Jira Key", "Project Jira Key", "Business Outcome Key", "Child Jira Key"])

# Step 5: Write the output dataframe to Alteryx output stream
Alteryx.write(df_output, 1)
