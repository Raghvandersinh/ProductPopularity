from sqlalchemy import create_engine,text
from dotenv import load_dotenv
import os
from TRI_model import Base
from sqlalchemy.orm import Session

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

with Session(engine) as session:
    result = session.execute(text("Select 1;"))
    for row in result:
        print(row)

Base.metadata.create_all(engine)

