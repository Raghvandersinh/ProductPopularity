import altair as alt
from vega_datasets import data
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time
import os 
import json
from us import states
import requests
from io import StringIO
from census import Census

start_time = time.time()

alt.renderers.enable("mimetype")

load_dotenv()

engine = create_engine(os.getenv('DATABASE_URL'))
with open('queries.json', 'r') as f:
    queries = json.load(f)

def total_waste_througout_from_top_10_facility_chart_generator():
    total_waste_throught_from_top_10_df = pd.read_sql(queries["Total_Waste_Throughout_top_10"], con=engine)

    print(total_waste_throught_from_top_10_df.columns.to_list())
    print(f"DataFrame shape: {total_waste_throught_from_top_10_df.shape}")
    print(f"DataFrame memory usage: {total_waste_throught_from_top_10_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print(f"Unique names: {total_waste_throught_from_top_10_df['name'].nunique()}")

    total_waste_throughout_top_10_chart = alt.Chart(total_waste_throught_from_top_10_df).mark_line(
        strokeWidth=2,
        point=False  # Remove points for cleaner lines
    ).encode(
        x=alt.X('create_month:T', 
                axis=alt.Axis(format='%b %Y', labelAngle=-45, title='Date'),
                title=None),
        y=alt.Y('total_release:Q', 
                axis=alt.Axis(format=',.0f', title='Total Release (lbs)'),
                scale=alt.Scale(zero=False)),  # Don't force y-axis to start at 0
        color=alt.Color('name:N', 
                        legend=alt.Legend(title='Facility', orient='bottom', columns=2),
                        sort='-y')  # Sort legend by highest values
    ).properties(
        title={
            'text': 'Top 10 Facilities - Waste Release Trends',
            'subtitle': 'Monthly aggregated releases in pounds',
            'fontSize': 16,
            'anchor': 'start'
        },
        width=800,
        height=400
    ).configure_axis(
        grid=True,
        gridColor='lightgray',
        gridOpacity=0.5
    ).configure_view(
        strokeWidth=0  
    )

    total_waste_throughout_top_10_chart.save('Frontend/total_waste_throughout_top_10.png')


def total_waste_by_location_throughout_or_After_2020(choice = ""):
    if choice == 'After':
        total_waste_df = pd.read_sql(queries['Waste_By_Location_2020s'], con=engine) 
    else:
        total_waste_df = pd.read_sql(queries["Waste_By_Location"], con=engine)

    
    print("Sample of data:")
    print(total_waste_df[['state', 'county']].head(10))
    print(f"\nTotal rows: {len(total_waste_df)}")
    
    print(total_waste_df.head(10))    
    states  = alt.topo_feature(data.us_10m.url, feature = 'states')
    state_waste = total_waste_df.groupby('state', as_index=False)['total_release'].sum()
    print('\nState Waste:', state_waste.head())
    # Use the us library to get state names and map IDs
    def get_state_info(abbrev):
        state = states.lookup(abbrev)
        if state:
            return pd.Series({
                'state_name': state.name,
                # The topo feature uses the FIPS code as the ID (but as string without leading zero for single digits)
                'id': int(state.fips)  # Convert FIPS to int to match topo feature format
            })
        return pd.Series({'state_name': abbrev, 'id': None})
    
    # Apply the mapping
    state_info = state_waste['state'].apply(get_state_info)
    print(f'\nState Info: {state_info}')
    state_waste = pd.concat([state_waste, state_info], axis=1)
    print(f'\nState Waste Updated: {state_waste}')

    # Check for unmapped states
    unmapped = state_waste[state_waste['id'].isna()]
    if len(unmapped) > 0:
        print(f"Warning: Could not map IDs for states: {unmapped['state'].tolist()}")
        # Remove unmapped states
        state_waste = state_waste.dropna(subset=['id'])
    
    # Convert id to integer for proper matching
    state_waste['id'] = state_waste['id'].astype(int)
    
    background = alt.Chart(states).mark_geoshape(
        fill = 'lightgray',
        stroke = 'white'
    ).project('albersUsa').properties(
        width = 700,
        height = 500
    )
    
    waste_map = alt.Chart(states).mark_geoshape(
        stroke = 'white'
    ).project(
        'albersUsa'
    ).encode(
        color = alt.Color('total_release:Q',
                          scale = alt.Scale(scheme = 'reds', type = 'log'),
                          title='Total Waste Released' if choice != 'After' else 'Total Waste Released 2020s'),
        tooltip=[
            alt.Tooltip('state_name:N', title='State'),
            alt.Tooltip('total_release:Q', title='Total Waste', format=',.0f')
        ]
    ).transform_lookup(
        lookup = 'id',
        from_=alt.LookupData(state_waste, 'id', ['total_release', 'state_name'])
    ).properties(
        width = 700,
        height=500,
        title = 'Waste Release By State' if choice != 'After' else "Waste Release By State 2020s"
    )
    
    chart = background + waste_map
    if choice == 'After':
        chart.save('Frontend/chart/total_waste_By_States_2020s.png')
    else:
        chart.save('Frontend/chart/total_waste_By_States.png')
    
    return chart

def total_waste_by_counties_throughout_or_After_2020(choice = ""):
    if choice == 'After':
        total_waste_df = pd.read_sql(queries['Waste_By_Location_2020s'], con=engine) 
    else:
        total_waste_df = pd.read_sql(queries["Waste_By_Location"], con=engine)

    counties = alt.topo_feature(data.us_10m.url, feature = 'counties')
    counties_waste = total_waste_df.groupby('county', as_index=False)['total_release'].sum()
    
    background = alt.Chart(counties).mark_geoshape(
        fill = 'lightgray',
        stroke = 'white'
    ).project('albersUsa').properties(
        width = 700,
        height = 500
    )
    
    background.save('County.png')
    


def map_db_counties_to_fips_code():
    location_db = pd.read_sql(queries['Waste_By_Location'], con=engine)
    def safe_state_lookup(abbr):
        result = states.lookup(abbr)
        return result.name if result is not None else pd.NA 
    location_db['state_name'] = location_db['state'].apply(safe_state_lookup)
    
    
    
# Run it
#total_waste_by_location_throughout_or_After_2020(choice = 'After')
#total_waste_by_counties_throughout_or_After_2020()
map_db_counties_to_fips_code()
end_time = time.time()
print(f"Runtime {end_time - start_time} Seconds.")
