import os
import geopandas as gpd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from shapely.geometry import Polygon, MultiPolygon

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
engine = create_engine(DB_URL)

def reset_database():
    print("--- 🧹 Nettoyage ---")
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS administrative_zones CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS temp_departements;"))
        conn.execute(text("DROP TABLE IF EXISTS temp_arrondissements;"))
        conn.commit()

def prepare_gdf(gdf):
    if gdf.crs is None: gdf = gdf.set_crs("EPSG:4326")
    else: gdf = gdf.to_crs("EPSG:4326")

    def force_multi(geom):
        return MultiPolygon([geom]) if isinstance(geom, Polygon) else geom

    gdf["geometry"] = gdf["geometry"].apply(force_multi)
    return gdf

def ingest_all():
    try:
        # 1. PAYS
        print("--- 1. Pays ---")
        gdf_0 = gpd.read_file("shapes/gadm41_CMR_0.shp", encoding="utf-8")
        gdf_0 = prepare_gdf(gdf_0)
        col_0 = 'COUNTRY' if 'COUNTRY' in gdf_0.columns else 'NAME_0'
        gdf_0 = gdf_0.rename(columns={col_0: 'name'})[['name', 'geometry']]
        gdf_0['level'] = 'COUNTRY'
        gdf_0['parent_id'] = None
        gdf_0.to_postgis('administrative_zones', engine, if_exists='replace', index=False)

        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE administrative_zones ADD COLUMN id SERIAL PRIMARY KEY;"))
            conn.commit()

        # 2. REGIONS
        print("--- 2. Régions ---")
        with engine.connect() as conn:
            cid = conn.execute(text("SELECT id FROM administrative_zones WHERE level='COUNTRY' LIMIT 1")).scalar()

        gdf_1 = gpd.read_file("shapes/gadm41_CMR_1.shp", encoding="utf-8")
        gdf_1 = prepare_gdf(gdf_1)
        gdf_1 = gdf_1.rename(columns={'NAME_1': 'name'})[['name', 'geometry']]
        gdf_1['level'] = 'REGION'
        gdf_1['parent_id'] = cid
        gdf_1.to_postgis('administrative_zones', engine, if_exists='append', index=False)

        # 3. DEPARTEMENTS (Liaison robuste avec LOWER())
        print("--- 3. Départements ---")
        gdf_2 = gpd.read_file("shapes/gadm41_CMR_2.shp", encoding="utf-8")
        gdf_2 = prepare_gdf(gdf_2)
        gdf_2 = gdf_2.rename(columns={'NAME_2': 'name', 'NAME_1': 'p_name'})[['name', 'p_name', 'geometry']]
        gdf_2['level'] = 'DEPARTEMENT'
        gdf_2.to_postgis('temp_departements', engine, if_exists='replace')

        with engine.connect() as conn:
            # On utilise LOWER() et TRIM() pour être sûr que "CENTRE" matche avec "Centre"
            conn.execute(text("""
                INSERT INTO administrative_zones (name, level, geometry, parent_id)
                SELECT t.name, 'DEPARTEMENT', t.geometry, p.id
                FROM temp_departements t
                JOIN administrative_zones p ON LOWER(TRIM(t.p_name)) = LOWER(TRIM(p.name))
                WHERE p.level = 'REGION'
            """))
            conn.commit()

        # 4. ARRONDISSEMENTS (Liaison robuste avec LOWER())
        print("--- 4. Arrondissements ---")
        gdf_3 = gpd.read_file("shapes/gadm41_CMR_3.shp", encoding="utf-8")
        gdf_3 = prepare_gdf(gdf_3)
        gdf_3 = gdf_3.rename(columns={'NAME_3': 'name', 'NAME_2': 'p_name'})[['name', 'p_name', 'geometry']]
        gdf_3['level'] = 'ARRONDISSEMENT'
        gdf_3.to_postgis('temp_arrondissements', engine, if_exists='replace')

        with engine.connect() as conn:
            # On utilise LOWER() et TRIM() pour être sûr que "MFOUNDI" matche avec "Mfoundi"
            conn.execute(text("""
                INSERT INTO administrative_zones (name, level, geometry, parent_id)
                SELECT t.name, 'ARRONDISSEMENT', t.geometry, p.id
                FROM temp_arrondissements t
                JOIN administrative_zones p ON LOWER(TRIM(t.p_name)) = LOWER(TRIM(p.name))
                WHERE p.level = 'DEPARTEMENT'
            """))
            conn.commit()

            # Vérification du nombre d'arrondissements insérés
            count = conn.execute(text("SELECT count(*) FROM administrative_zones WHERE level='ARRONDISSEMENT'")).scalar()
            print(f"✅ {count} Arrondissements insérés avec succès !")

            conn.execute(text("DROP TABLE temp_departements; DROP TABLE temp_arrondissements;"))

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    reset_database()
    ingest_all()
