import asyncio
import os
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql
from app.models import Base, DeadLetterQueue

# Load environment variables from .env
load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/notification_db"
)

async def main():
    # 1. Print compiled CREATE TABLE DDL (offline compilation showing defaults)
    print("--- SQLAlchemy Compiled DDL (showing defaults) ---")
    ddl = CreateTable(DeadLetterQueue.__table__).compile(dialect=postgresql.dialect())
    print(ddl)
    print("--------------------------------------------------\n")

    print(f"Connecting to database at {DATABASE_URL}...")
    engine = create_async_engine(DATABASE_URL, echo=True)
    
    # 2. Check if table already exists
    async with engine.connect() as conn:
        res = await conn.execute(text(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'dead_letter_queue');"
        ))
        table_exists = res.scalar()
        
    if table_exists:
        print("Table 'dead_letter_queue' already exists. Running ALTER COLUMN migrations...")
        async with engine.begin() as conn:
            # Apply ALTER TABLE migrations for defaults
            await conn.execute(text(
                "ALTER TABLE dead_letter_queue ALTER COLUMN id SET DEFAULT gen_random_uuid();"
            ))
            await conn.execute(text(
                "ALTER TABLE dead_letter_queue ALTER COLUMN replay_count SET DEFAULT 0;"
            ))
            await conn.execute(text(
                "ALTER TABLE dead_letter_queue ALTER COLUMN created_at SET DEFAULT now();"
            ))
        print("ALTER migrations applied successfully!")
    else:
        print("Table 'dead_letter_queue' does not exist. Creating table...")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        print("Table created successfully!")
        
    # 3. Print information_schema to verify database-level defaults
    async with engine.connect() as conn:
        res = await conn.execute(text(
            "SELECT column_name, column_default, is_nullable, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = 'dead_letter_queue' "
            "ORDER BY ordinal_position;"
        ))
        rows = res.fetchall()
        print("\n--- Verified Postgres Database Defaults ---")
        for row in rows:
            print(f"Column: {row[0]:<15} | Default: {str(row[1]):<30} | Nullable: {row[2]:<5} | Type: {row[3]}")
        print("--------------------------------------------\n")
        
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
