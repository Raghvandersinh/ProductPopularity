from sqlalchemy import insert, create_engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv
import os
import TRI_table_creation as TRItc
import TRI_transformations as TRIt
import pandas as pd
load_dotenv()


engine = create_engine(os.getenv('DATABASE_URL'))


if __name__ == '__main__':
    
    load_tri_chem_activity = TRIt.tranform_main(table = 'tri_chem_activity/', start = 0, end = 1000, loop_count=1, df=TRIt.transform_tri_chem_activity)
    load_tri_chem_activity.to_sql('tri_chem_activity',con=engine,if_exists='append',index=False)
    