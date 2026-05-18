from sqlalchemy import create_engine

DATABASE_URL = "postgresql://postgres:car.gemera@localhost:5432/ecommerce_db"

engine = create_engine(DATABASE_URL)