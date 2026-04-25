from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from TRI_model import meta 


load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))
conn = engine.connect()

for connection in conn:
    print()
    
meta.create_all(engine)

