import psycopg2
from dotenv import load_dotenv
conn = psycopg2.connect(
    host="localhost",
    database="FraudTransactions",
    user="RagoSauce"
)