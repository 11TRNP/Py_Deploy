import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = os.getenv("DATABASE_URL")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD")

engine = create_engine(DATABASE_URL, fast_executemany=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

print("DATABASE_URL:", DATABASE_URL)