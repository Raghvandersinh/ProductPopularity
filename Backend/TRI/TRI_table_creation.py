import enum
from sqlalchemy import create_engine, text ,BOOLEAN, VARCHAR, INTEGER, SMALLINT, Table, Column, Enum
from sqlalchemy.orm import registry
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(os.getenv('DATABASE_URL'))

class Classifications(enum.Enum):
    # TRI classifications
    zero = 0
    # PBT classifications
    one = 1
    # Dioxin classifications
    two = 2

class metal_indicator(enum.Enum):
    # Chemical is NOT a Metal or Metal Compound
    zero = 0
    # Parent Metals and Metal Compound Categories
    one = 1
    #  Individually Listed Metal Compounds,
    two = 2
    # Barium and Barium Compounds
    three = 3 
    # Metals with Qualifiers
    four = 4

class mesurements(enum.Enum):
    # Pounds
    lbs = 'pounds'
    # Grams
    g = 'grams'
try:
    with engine.connect() as connection:
        connection.execute(text('SELECT 1'))
    print('connection successful')
except Exception as e:
    print(f'connection failed: {e}')

mapped_registry = registry()

chem_info_table = Table(
    'tri_chem_info',
    mapped_registry.metadata,
    Column('tri_chem_id', INTEGER, primary_key=True),
    Column('caac_ind',BOOLEAN),
    Column('carc_ind',BOOLEAN),
    Column('feds_ind',BOOLEAN),
    Column('classify', Enum(Classifications)),
    Column('metal_ind', Enum(metal_indicator)),
    Column('pbt_ind',BOOLEAN),
    Column('pfas_ind',BOOLEAN),
    Column('r3350_ind',BOOLEAN),
    Column('srs_id', INTEGER),
    Column('units_of_measure', Enum(mesurements))
)

tri_chem_activity = Table(
    'tri_chem_activity',
    mapped_registry.metadata,
    Column('doc_ctrl_num', VARCHAR(13), primary_key=True),
    Column('ancillary',BOOLEAN),
    Column('article_component',BOOLEAN),
    Column('byproduct',BOOLEAN),
    Column('chem_processing_aid',BOOLEAN),
    Column('formulation_components',BOOLEAN),
    Column('imported', BOOLEAN),
    Column('manufacture_aid', BOOLEAN),
    Column('manufacture_impurity' ,BOOLEAN),
    Column('process_impurity' ,BOOLEAN),
    Column('processed_recycle' ,BOOLEAN),
    Column('produce' ,BOOLEAN),
    Column('reactant' ,BOOLEAN),
    Column('repackaging' ,BOOLEAN),
    Column('sales_distribution' ,BOOLEAN),
    Column('used_process' ,BOOLEAN)   
)

