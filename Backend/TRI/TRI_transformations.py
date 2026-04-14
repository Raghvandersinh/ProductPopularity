from TRI_data_extraction import batch_extraction as be
import pandas as pd 


def true_false_to_boolean(df, column):
    df[column] = df[column].map({'1': True, '0': False})
    return df

def transform_tri_chem_info(raw_data):
    """
    Transforms the raw JSON data from the TRI chemical information table into a
    pandas DataFrame with the appropriate data types and structure for
    database insertion."
    """
    try:
        df = pd.DataFrame(raw_data)
        # Convert data types as needed, for example:
        df['tri_chem_id'] = int(df['tri_chem_id']).astype(int)
        df = true_false_to_boolean(df, 'caac_ind')
        df = true_false_to_boolean(df, 'carc_ind')
        df = true_false_to_boolean(df, 'feds_ind')
        df['classify'] = df['classify'].astype('category')
        df['metal_ind'] = df['metal_ind'].astype('category')
        df = true_false_to_boolean(df, 'pbt_ind')
        df = true_false_to_boolean(df, 'pfas_ind')
        df = true_false_to_boolean(df, 'r3350_ind')
        df['srs_id'] = int(df['srs_id']).astype(int)
        df['units_of_measure'] = df['units_of_measure'].astype('category')
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