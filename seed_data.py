import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration DB
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)

# Couleurs pour l'affichage sur la carte
COLORS = {
    'Cacao': '#8B4513', 'Café': '#6F4E37', 'Coton': '#ECF0F1', 'Maïs': '#F1C40F',
    'Manioc': '#2ECC71', 'Banane': '#F39C12', 'Pomme de terre': '#D35400',
    'Bovins': '#E74C3C', 'Volailles': '#E91E63', 'Porcins': '#FF99CC', 'Petits Ruminants': '#A52A2A',
    'Pêche Maritime': '#3498DB', 'Pêche Fluviale': '#1ABC9C', 'Pêche Continentale': '#2980B9', 'Pêche Indu./Artisa.': '#5DADE2',
    'Riz': '#95A5A6', 'Igname': '#D4AC0D', 'Oignon': '#E67E22', 'Palmier à huile': '#808000', 'Aquaculture': '#00CED1',
    'Hévéa': '#2E8B57', 'Ananas': '#FFD700'
}

# Mapping pour corriger les différences de noms entre CSV et DB PostGIS
NAME_MAPPING = {
    "Extreme-Nord": "Extrême-Nord",
    "Adamaoua": "Adamaoua",
    "Centre": "Centre",
    "Est": "Est",
    "Littoral": "Littoral",
    "Nord": "Nord",
    "Nord-Ouest": "Nord-Ouest",
    "Ouest": "Ouest",
    "Sud": "Sud",
    "Sud-Ouest": "Sud-Ouest",
    "Lekié": "Lekie",
    "Mbam-et-Inoubou": "Mbam-Et-Inoubou",
    "Bénoué": "Benoue",
    "Mfoundi": "Mfoundi",
    "Vina": "Vina",
    "Mbéré": "Mbere",
    "Djérem": "Djerem",
    "Haute-Sanaga": "Haute-Sanaga",
    "Nyong-et-So'o": "Nyong-Et-So'o",
    "Kadey": "Kadey",
    "Lom-et-Djérem": "Lom-Et-Djerem",
    "Diamaré": "Diamare",
    "Logone-et-Chari": "Logone-Et-Chari",
    "Mayo-Danay": "Mayo-Danay",
    "Wouri": "Wouri",
    "Sanaga-Maritime": "Sanaga-Maritime",
    "Moungo": "Moungo",
    "Mayo-Rey": "Mayo-Rey",
    "Mezam": "Mezam",
    "Bui": "Bui",
    "Bamboutos": "Bamboutos",
    "Mifi": "Mifi",
    "Menoua": "Menoua",
    "Dja-et-Lobo": "Dja-Et-Lobo",
    "Océan": "Ocean",
    "Vallée-du-Ntem": "Vallee-Du-Ntem",
    "Fako": "Fako",
    "Meme": "Meme",
    # Arrondissements (Exemples)
    "Obala": "Obala",
    "Monatélé": "Monatele",
    "Sa'a": "Sa'a",
    "Bafia": "Bafia",
    "Bokito": "Bokito",
    "Garoua I": "Garoua I",
    "Lagdo": "Lagdo",
    "Bibemi": "Bibemi",
    "Yaoundé I": "Yaounde I",
    "Yaoundé II": "Yaounde II",
    "Yaoundé III": "Yaounde III",
    "Ngaoundéré I": "Ngaoundere I",
    "Ngaoundéré II": "Ngaoundere II",
    "Meiganga": "Meiganga",
    "Bertoua I": "Bertoua I",
    "Garoua-Boulaï": "Garoua-Boulai",
    "Maroua I": "Maroua I",
    "Gazawa": "Gazawa",
    "Douala IV": "Douala IV",
    "Manoka": "Manoka",
    "Njombé-Penja": "Njombe-Penja",
    "Bafoussam I": "Bafoussam I",
    "Mbouda": "Mbouda",
    "Galim": "Galim",
    "Kribi I": "Kribi I",
    "Campo": "Campo"
}

def update_table_structure():
    """Met à jour la structure de la table production_stats pour ajouter les nouvelles colonnes"""
    with engine.connect() as conn:
        print("--- Mise à jour de la structure de la base de données ---")

        # 1. Ajouter les colonnes si elles n'existent pas
        columns_to_add = [
            ("surface_area", "numeric(15,2)"),
            ("yield", "numeric(10,2)"),
            ("producer_count", "integer"),
            ("average_price", "numeric(15,2)"),
            ("year", "integer DEFAULT 2023")
        ]

        for col_name, col_type in columns_to_add:
            try:
                conn.execute(text(f"ALTER TABLE production_stats ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
            except Exception:
                pass

        # 2. Mettre à jour la contrainte d'unicité pour inclure l'année
        try:
            conn.execute(text("ALTER TABLE production_stats DROP CONSTRAINT IF EXISTS unique_production_entry"))
            conn.execute(text("ALTER TABLE production_stats ADD CONSTRAINT unique_production_entry UNIQUE (sub_sector_id, zone_code, year)"))
        except Exception as e:
            print(f"Info contrainte: {e}")

        conn.commit()
        print("✅ Structure de la table vérifiée.")

def seed_database():
    # D'abord, on met à jour la structure
    update_table_structure()

    csv_path = "./data/production_data.csv"
    if not os.path.exists(csv_path):
        print(f"❌ Erreur: Fichier {csv_path} introuvable. Lancez 'python3 generate_full_data.py' d'abord.")
        return

    print("Lecture du fichier CSV...")
    df = pd.read_csv(csv_path)

    # Remplacer les valeurs NaN
    df = df.fillna({
        'surface_area': 0, 'yield': 0, 'producer_count': 0,
        'average_price': 0, 'description': '', 'parent_pcode': ''
    })

    with engine.connect() as conn:
        print("--- 1. Nettoyage des tables de statistiques ---")
        conn.execute(text("TRUNCATE production_stats, sub_sectors, sectors RESTART IDENTITY CASCADE;"))

        print("--- 2. Synchronisation des Zones Géographiques ---")

        processed_zones = set()

        # Récupération des IDs existants
        existing_zones = pd.read_sql("SELECT code, id FROM administrative_zones", conn)
        zone_id_map = dict(zip(existing_zones['code'], existing_zones['id']))

        # On parcourt le CSV pour mettre à jour les zones
        for _, row in df.iterrows():
            pcode = row['pcode']
            if pcode in processed_zones: continue

            zone_name = row['zone_name']
            level = row['level']
            parent_pcode = row['parent_pcode']
            db_name = NAME_MAPPING.get(zone_name, zone_name)

            # Trouver l'ID du parent
            parent_id = None
            if parent_pcode and parent_pcode in zone_id_map:
                parent_id = zone_id_map[parent_pcode]

            # Mise à jour de la zone (Code, Level, Parent)
            res = conn.execute(text("""
                UPDATE administrative_zones
                SET code = :code, level = :level, parent_id = :pid
                WHERE code = :code OR name ILIKE :name OR name ILIKE :mapped_name
            """), {
                "code": pcode,
                "level": level,
                "pid": str(parent_id) if parent_id else None,
                "name": zone_name,
                "mapped_name": db_name
            })

            processed_zones.add(pcode)

        print("--- 3. Insertion des Secteurs & Sous-Secteurs ---")
        unique_sectors = df['sector'].unique()
        sector_map = {}

        for sec_name in unique_sectors:
            res = conn.execute(text("INSERT INTO sectors (name) VALUES (:name) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id"), {"name": sec_name})
            sector_map[sec_name] = res.fetchone()[0]

        unique_subs = df[['sector', 'sub_sector']].drop_duplicates()
        sub_sector_map = {}

        for _, row in unique_subs.iterrows():
            sec_name = row['sector']
            sub_name = row['sub_sector']
            color = COLORS.get(sub_name, '#7F8C8D')

            res = conn.execute(
                text("""
                    INSERT INTO sub_sectors (sector_id, name, color)
                    VALUES (:sid, :name, :color)
                    ON CONFLICT (sector_id, name) DO UPDATE SET color=EXCLUDED.color
                    RETURNING id
                """),
                {"sid": sector_map[sec_name], "name": sub_name, "color": color}
            )
            sub_sector_map[(sec_name, sub_name)] = res.fetchone()[0]

        print(f"--- 4. Insertion des Statistiques ({len(df)} lignes) ---")

        data_to_insert = []
        for _, row in df.iterrows():
            sub_id = sub_sector_map.get((row['sector'], row['sub_sector']))
            if sub_id:
                data_to_insert.append({
                    "sid": sub_id,
                    "code": row['pcode'],
                    "vol": row['volume'],
                    "unit": row['unit'],
                    "desc": row['description'],
                    "year": row['year'],
                    "surf": row['surface_area'],
                    "yield": row['yield'],
                    "prod_count": row['producer_count'],
                    "price": row['average_price']
                })

        if data_to_insert:
            # Insertion par lot (batch)
            conn.execute(
                text("""
                    INSERT INTO production_stats (
                        sub_sector_id, zone_code, volume, unit, description, year,
                        surface_area, yield, producer_count, average_price
                    )
                    VALUES (:sid, :code, :vol, :unit, :desc, :year, :surf, :yield, :prod_count, :price)
                    ON CONFLICT (sub_sector_id, zone_code, year)
                    DO UPDATE SET
                        volume = EXCLUDED.volume,
                        description = EXCLUDED.description,
                        surface_area = EXCLUDED.surface_area,
                        yield = EXCLUDED.yield,
                        producer_count = EXCLUDED.producer_count,
                        average_price = EXCLUDED.average_price
                """),
                data_to_insert
            )

        conn.commit()
        print(f"✅ Terminé ! Base de données peuplée avec succès.")

if __name__ == "__main__":
    seed_database()
