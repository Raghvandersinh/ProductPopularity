import altair as alt
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time
import os 
import json

alt.renderers.enable("mimetype")
start_time = time.time()

load_dotenv()

engine = create_engine(os.getenv('DATABASE_URL'))
with open('queries.json', 'r') as f:
    queries = json.load(f)

total_waste_throught_2000s_df = pd.read_sql(queries["Total_Waste_Throughout"], con=engine)

print(total_waste_throught_2000s_df.columns.to_list())

total_waste_chart = alt.Chart(total_waste_throught_2000s_df).mark_line().encode(
    x = 'create_date:T',
    y = 'total_release:Q',
    color = 'name:N'
)

total_waste_chart.show()
total_waste_chart.save("chart.png")
end_time = time.time()
print(f"Runtime {end_time - start_time} Seconds.")
