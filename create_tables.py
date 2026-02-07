import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration de la connexion DB
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

def create_schema():
    print("--- 🏗️ Création des tables de la base de données ---")
    
    sql_commands = """
    -- 1. Table des Secteurs (Agriculture, Élevage, Pêche)
    CREATE TABLE IF NOT EXISTS sectors (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) UNIQUE NOT NULL
    );

    -- 2. Table des Sous-secteurs (Cacao, Maïs, Bovins...)
    CREATE TABLE IF NOT EXISTS sub_sectors (
        id SERIAL PRIMARY KEY,
        sector_id INTEGER REFERENCES sectors(id) ON DELETE CASCADE,
        name VARCHAR(100) NOT NULL,
        color VARCHAR(20),
        UNIQUE(sector_id, name)
    );

    -- 3. Table des Statistiques de Production
    CREATE TABLE IF NOT EXISTS production_stats (
        id SERIAL PRIMARY KEY,
        sub_sector_id INTEGER REFERENCES sub_sectors(id) ON DELETE CASCADE,
        zone_code VARCHAR(50), 
        volume NUMERIC(15,2),
        unit VARCHAR(20),
        year INTEGER DEFAULT 2023,
        surface_area NUMERIC(15,2) DEFAULT 0,
        yield NUMERIC(10,2) DEFAULT 0,
        producer_count INTEGER DEFAULT 0,
        average_price NUMERIC(15,2) DEFAULT 0,
        description TEXT,
        CONSTRAINT unique_production_entry UNIQUE (sub_sector_id, zone_code, year)
    );
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(sql_commands))
            conn.commit()
            print("✅ Tables créées avec succès (sectors, sub_sectors, production_stats).")
    except Exception as e:
        print(f"❌ Erreur lors de la création des tables : {e}")

if __name__ == "__main__":
    create_schema()