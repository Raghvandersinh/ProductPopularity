import altair as alt
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time
import os 


start_time = time.time()

load_dotenv()

engine = create_engine(os.getenv('DATABASE_URL'))


total_waste_in_2000s_query = """
WITH release_totals AS (
    SELECT 
        trf.tri_facility_id,
        SUM(tft.total_offsite_release::NUMERIC + tft.total_onsite_release::NUMERIC) AS Total_Release
    FROM tri_form_total tft
    JOIN tri_reporting_form trf ON tft.doc_ctrl_num = trf.doc_ctrl_num
    GROUP BY trf.tri_facility_id
)
SELECT 
    rt.tri_facility_id,
    MAX(tfh.name) AS name,
    ROUND(rt.Total_Release, 2) AS Total_Release
FROM release_totals rt
JOIN tri_facility_history tfh ON rt.tri_facility_id = tfh.tri_facility_id
GROUP BY rt.tri_facility_id, rt.Total_Release
ORDER BY rt.Total_Release DESC;
"""



total_waste_from_2000s_df = pd.read_sql(total_waste_in_2000s_query, con=engine)


print(total_waste_from_2000s_df.head())

end_time = time.time()

print(f"Runtime {end_time - start_time} Seconds.")

