from TRI_data_extraction import batch_extraction as be
import pandas as pd 
import numpy as np
from sqlalchemy import create_engine, text, inspect, MetaData
from dotenv import load_dotenv
import os
from TRI_model import Classifications, Metal_Indicator
from sqlalchemy.dialects.postgresql import insert
load_dotenv()

engine = create_engine(os.getenv('DATABASE_URL'))
        
def true_false_to_boolean(df, column):
    if df[column].dtype == 'int':
        df[column] = df[column].map({1:True, 0:False})
    elif df[column].dtype == 'str':
        df[column] = df[column].map({'1': True, '0': False})
    else:
        print("Warning Unsupported Data Type")
    return df

def transform_tri_chem_info(raw_data):
    """
    Transforms the raw JSON data from the TRI chemical information table into a
    pandas DataFrame with the appropriate data types and structure for
    database insertion."
    """
    
    try:
        df = pd.DataFrame(raw_data)        
        df['classification'] = df['classification'].apply(lambda x: Classifications(x).name)
        df['metal_ind'] = df['metal_ind'].apply(lambda x: Metal_Indicator(x).name)
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
        df['create_date'] = pd.to_datetime(df['create_date'])
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

def get_table_object(table_name):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return metadata.tables[table_name]

def upsert_helper(conn,db_table, df, pk_columns):
    if df.empty:
        return 
    records = df.to_dict('records')
    insert_stmt = insert(db_table)
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=pk_columns,
        set_={col: insert_stmt.excluded[col] for col in df.columns}   
    )
    conn.execute(upsert_stmt, records)    
    conn.commit()
    
def transform_main(table, start, end, increment, loop_count, db_table, df):
    
    inspector = inspect(engine)
    table_col = [col['name'] for col in inspector.get_columns(db_table)]
    pk_columns = inspector.get_pk_constraint(db_table)['constrained_columns']
    db_table_obj = get_table_object(db_table)
    
    try:
        with engine.connect() as conn:
            for i,raw_data in enumerate(be(table = table, start = start, end = end, increment=increment, loop_count=loop_count)):                    
                print(f"Length: {len(raw_data)}")           
                transformed_df = df(raw_data)
                
                if transformed_df.empty:
                    print("DataFrame is empty. Skipping")
                    continue
                if transformed_df is not None:
                    print("Success")
            
                filtred_df = transformed_df[table_col]
                
                upsert_helper(conn, db_table_obj, filtred_df, pk_columns)                

    except Exception as e:
        print(f"Error Occured during Transformation or Insertion{e}")
        import traceback; traceback.print_exc();
                    
if __name__ == "__main__":
    #transform_main(db_table='tri_chem_info',table='tri_chem_info/', start = 1, end = 1000, increment=0, loop_count=1,df = transform_tri_chem_info)
    transform_main(db_table='tri_facility_history',table = 'tri_facility_history_2/', start = 0, end = 10000, increment=10000, loop_count=1, df=transform_tri_facility_history)
    # transform_main(db_table='tri_form_total',table='tri_form_total/', start = 1, end = 1000, loop_count=10, df = transform_tri_form_total)
    # transform_main(db_table='tri_chem_activity',table='tri_chem_activity/', start = 1, end = 100, loop_count=1, df = transform_tri_chem_activity)        
