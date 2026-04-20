from TRI_data_extraction import batch_extraction as be
import pandas as pd 
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(os.getenv('DATABASE_URL'))

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

def transform_tri_facility_history(raw_data):
    try:
        df = pd.DataFrame(raw_data)
        df['tri_facility_id'] = df['tri_facility_id'].astype(str)
        #facility_name in DB
        df['parent_name'] = df['parent_name'].astype(str)
        df['name'] = df['name'].astype(str)
        df['city'] = df['city'].astype(str)
        df['county'] = df['county'].astype(str)
        df['state'] = df['state'].astype(str)
        df['epa_standardized_foreign_parent'] = df['epa_standardized_foreign_parent'].astype(str)
        df['epa_standardized_parent'] = df['epa_standardized_parent'].astype(str)
        df['primary_naics'] = df['primary_naics'].astype(str)

        return df
        
    except Exception as e:
        print(f"Error has occured during Transformations {e}")
        import traceback; traceback.print_exc();
        return None 

def transform_tri_form_total(raw_data):
    
    try:
        df = pd.DataFrame(raw_data)
        df['doc_ctrl_num'] = df['doc_ctrl_num'].astype(str)
        df['total_air_release'] = df['total_air_release'].astype(str)
        df['total_land_release'] = df['total_land_release'].astype(str)
        df['total_offsite_release'] = df['total_offsite_release'].astype(str)
        df['total_prod_waste'] = df['total_prod_waste'].astype(str)
        df['total_recovery_transfer'] = df['total_recovery_transfer'].astype(str)
        df['total_recycling_transfer'] = df['total_recycling_transfer'].astype(str)
        df['total_water_release'] = df['total_water_release'].astype(str)
        df['number_of_streams'] = df['number_of_streams'].astype(str)
        return df
    
    except Exception as e:
        print(f'Transformation Failed {e}')
        import traceback; traceback.print_exc();
        return None

def tranform_main(table, start, end, loop_count, df =pd.DataFrame):
    try:
        temp = []
        for raw_data in be(table = table, start = start, end = end, increment=end, loop_count=loop_count):        
            temp.append(raw_data)
        
        result = [record for batch in temp for record in batch]    
        df = df(result)
        if df is not None:
            print(df.tail())
        df.to_sql(table = table[:-1], con=engine, if_exists='replace')
        
    except Exception as e:
        print(f"Error Occured during Transformation or Insertion{e}")
        import traceback; traceback.print_exc();
        
            
if __name__ == "__main__":
    tranform_main(table='tri_form_totals/', start = 1, end = 5, loop_count=1, df = transform_tri_form_total)
            
            