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

def classification_map():
    classification_map = {
        0:"TRI",
        1:"PBT",
        2:"Dioxin"
    }
    return classification_map

def metal_ind_map():
    metal_ind_map = {
        "0": "Not Metal",
        "1": "Parent Metal",
        "2": "Individually Listed Metal",
        "3": "Barium",
        "4": "Metal with Qualifiers"
    }
    return metal_ind_map

def transform_tri_chem_info(raw_data):
    """
    Transforms the raw JSON data from the TRI chemical information table into a
    pandas DataFrame with the appropriate data types and structure for
    database insertion."
    """
    try:
        df = pd.DataFrame(raw_data)
        df['classification'] = df['classification'].apply(lambda x: classification_map().get(x))
        df['metal_ind'] = df['metal_ind'].apply(lambda x: metal_ind_map().get(x))
        # Convert data types as needed, for example:
        df['tri_chem_id'] = df['tri_chem_id'].astype(str)
        df = true_false_to_boolean(df, 'caac_ind')
        df = true_false_to_boolean(df, 'carc_ind')
        df = true_false_to_boolean(df, 'feds_ind')
        df = true_false_to_boolean(df, 'pbt_ind')
        df = true_false_to_boolean(df, 'pfas_ind')
        df = true_false_to_boolean(df, 'r3350_ind')
        df['unit_of_measure'] = df['unit_of_measure'].astype(str)
        df['srs_id'] = df['srs_id'].astype(str)
        return df
    
    except Exception as e:
        print(f"Error during transformation: {e}")
        import traceback; traceback.print_exc();    
        return None

def transform_tri_chem_activity(raw_data):
    try:
        df = pd.DataFrame(raw_data)
        df['doc_ctrl_num'] = df['doc_ctrl_num'].astype(str)
        df = true_false_to_boolean(df,column='ancillary')
        df = true_false_to_boolean(df,column='article_component')
        df = true_false_to_boolean(df,column='byproduct')
        df = true_false_to_boolean(df,column='chem_processing_aid')
        df = true_false_to_boolean(df,column='formulation_component')
        df = true_false_to_boolean(df,column='imported')
        df = true_false_to_boolean(df,column='manufacture_aid')
        df = true_false_to_boolean(df,column='manufacture_impurity')
        df = true_false_to_boolean(df,column='process_impurity')
        df = true_false_to_boolean(df,column='processed_recycling')
        df = true_false_to_boolean(df,column='produce')
        df = true_false_to_boolean(df,column='reactant')
        df = true_false_to_boolean(df,column='repackaging')
        df = true_false_to_boolean(df,column='sale_distribution')
        df = true_false_to_boolean(df,column='used_processed')
    
        return df

    except Exception as e:
        print(f"Error has occured during Transformations {e}")
        import traceback; traceback.print_exc();
        return None 

if __name__ == "__main__":
    end = 1000
    for raw_data in be(table = 'tri_chem_activity/', start = 1, end = end, increment=end, loop_count = 1):        
        df = transform_tri_chem_activity(raw_data)
        if df is not None:
            print(df.tail())
            
            
            