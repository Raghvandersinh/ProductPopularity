from TRI_data_extraction import batch_extraction as be
import pandas as pd 


def true_false_to_boolean(df, column):
    if df[column].dtype == 'int':
        df[column] = df[column].map({1:True, 0:False})
    elif df[column].dtype == 'str':
        df[column] = df[column].map({'1': True, '0': False})
    else:
        print("Warning Unsupported Data Type")
    print(df[column].dtype)
    return df

def map_classification_to_enum(df, column_name='classification'):
    classification_enum_map = {
        'TRI': 0,
        'PBT': 1,
        'Dioxin': 2}
    if column_name in df.columns:
        df[column_name] = df[column_name].fillna('TRI')
        df[column_name] = df[column_name].map(classification_enum_map)
    
    else: 
        print(f"Warning {column_name} is missing assinging default value")
    return df

def map_metal_ind_to_enum(df, column_name='metal_ind'):
    """Maps the 'metal_ind' column to appropriate numeric categories."""
    # Mapping for metal categories
    map_metal_ind = {
        "Not_Metal": "0",
        "Parents_Metal": "1",
        "Listed_Metal": "2",
        "Barium": "3",
        "Qualified_Metal": "4"
    }

    if column_name in df.columns:
        # Fill NaN values with default category if needed (e.g., "Not_Metal")
        df[column_name] = df[column_name].fillna('Not_Metal')  
        
        # Map values using the map dictionary
        df[column_name] = df[column_name].map(map_metal_ind)
        
        # If there are any unmatched values, fill them with the default ("0")
        df[column_name] = df[column_name].fillna('0')  # Ensures no NaN values remain

        # Optionally convert the column to string or numeric as needed
        df[column_name] = df[column_name].astype(str)  # Ensuring it's a string column
    else:
        print(f"Warning: '{column_name}' column is missing. Assigning default value '0'.")
        df[column_name] = "0"  # Default to "0" if the column is missing

    return df

def transform_tri_chem_info(raw_data):
    """
    Transforms the raw JSON data from the TRI chemical information table into a
    pandas DataFrame with the appropriate data types and structure for
    database insertion."
    """
    try:
        df = pd.DataFrame(raw_data)
        
        print(f"DataFrame Shape: {df.shape} \n")
        print(f'DataFrame Columns: {df.columns.to_list()},\n')
        df = map_classification_to_enum(df, column_name='classification')
        print(df.head())
        df = map_metal_ind_to_enum(df, column_name="metal_ind")

        # Convert data types as needed, for example:
        df['tri_chem_id'] = df['tri_chem_id'].astype(int)
        df = true_false_to_boolean(df, 'caac_ind')
        df = true_false_to_boolean(df, 'carc_ind')
        df = true_false_to_boolean(df, 'feds_ind')
        df = true_false_to_boolean(df, 'pbt_ind')
        df = true_false_to_boolean(df, 'pfas_ind')
        df = true_false_to_boolean(df, 'r3350_ind')
        df['unit_of_measure'] = df['unit_of_measure'].astype(str)
        df['srs_id'] = df['srs_id'].astype(int)
        return df
    
    except Exception as e:
        print(f"Error during transformation: {e}")
        import traceback; traceback.print_exc();    
        return None


if __name__ == "__main__":
    for raw_data in be(table = 'tri_chem_info/',start = 1, end = 5, increment = 5, loop_count = 1):        
        df = transform_tri_chem_info(raw_data)
        if df is not None:
            print(df.head())
            
            
            
            