import pandas as pd

# Step 1: Read the input data from Alteryx input stream #1
df_input = Alteryx.read("#1")  # Reading input table from Alteryx (Input 1)

# Step 2: Define functions for binary search and recursive linkage process

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
        if ':' in link:  # Ensure link is in 'Type:Key' format
            _, linked_key = link.split(':')  # Extract the key from the 'Type:key' format
            if linked_key == child_key:
                return True
    return False

def process_hierarchy(record, hierarchy, input_table, program=None, project=None):
    """Recursively process parent links to build the full hierarchy.
       Each path generates a new record, and nested items create duplicate entries."""
    
    parent_links = record['All Parent Links'].split(',')
    record_type = record['Type']
    
    if record_type == 'Program':
        program = record['key']  # Mark this as the Program for hierarchy
        
    if record_type == 'Project':
        project = record['key']  # Mark this as the Project for hierarchy
        
    if record_type == 'Business Outcome':
        # Add BO records at the lowest level of the hierarchy
        hierarchy.append([program, project, record['key']])  # Program, Project, Business Outcome
    
    # Process all parent links
    for link in parent_links:
        if ':' not in link:
            continue  # Skip malformed links
        
        parent_type, parent_key = link.split(':', 1)  # Split the parent link into type and key
        parent_record = binary_search(input_table, parent_key)  # Find the parent record via binary search
        
        if parent_record is not None and validate_link(parent_record, record['key']):
            # Recursive call to process the parent's hierarchy
            process_hierarchy(parent_record, hierarchy, input_table, program, project)
    
    return hierarchy

# Step 3: Iterate through the input table and build the hierarchy linkages

output_rows = []
for idx, record in df_input.iterrows():
    hierarchy = []
    
    # Recursively process parent-child relationships
    if pd.notna(record['All Parent Links']) and record['All Parent Links'].strip():
        hierarchy = process_hierarchy(record, hierarchy, df_input)
    else:
        # Orphaned records without parents
        if record['Type'] == 'Program':
            output_rows.append([record['key'], None, None])  # Orphaned Program
        elif record['Type'] == 'Project':
            output_rows.append([None, record['key'], None])  # Orphaned Project
        elif record['Type'] == 'Business Outcome':
            output_rows.append([None, None, record['key']])  # Orphaned Business Outcome
    
    # For each path found in the hierarchy, add to output
    for program, project, business_outcome in hierarchy:
        output_rows.append([program, project, business_outcome])

# Step 4: Create a new dataframe for the output table
df_output = pd.DataFrame(output_rows, columns=["Program Jira Key", "Project Jira Key", "Business Outcome Jira Key"])

# Step 5: Write the output dataframe to Alteryx output stream
Alteryx.write(df_output, 1)
