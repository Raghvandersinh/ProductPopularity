import psycopg2
from dotenv import load_dotenv
import os 
from sqlalchemy import create_engine, text
load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))
print(engine.url)
with engine.connect() as connection:
    result = connection.execute(text("Select version();"))
    print(result.fetchone())