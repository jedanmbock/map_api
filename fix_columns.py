import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Configuration de la connexion DB
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

def fix_columns():
    print("--- 🔧 Réparation de la table administrative_zones ---")
    
    try:
        with engine.connect() as conn:
            # Ajout de la colonne 'code' si elle manque
            conn.execute(text("ALTER TABLE administrative_zones ADD COLUMN IF NOT EXISTS code VARCHAR(50);"))
            conn.commit()
            print("✅ Colonne 'code' ajoutée avec succès.")
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    fix_columns()