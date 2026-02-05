import csv
import random
import os

# --- 1. STRUCTURE ADMINISTRATIVE (Région -> Départements -> Arrondissements) ---
# Pour alléger le script, je mets les principaux. Le script générera des données pour eux.
ADMIN_STRUCTURE = {
    "Adamaoua": {
        "code": "CM001",
        "depts": {
            "Djérem": ["Tibati", "Ngaoundal"],
            "Faro-et-Déo": ["Tignère", "Galim-Tignère"],
            "Mayo-Banyo": ["Banyo", "Bankim"],
            "Mbéré": ["Meiganga", "Djohong"],
            "Vina": ["Ngaoundéré I", "Ngaoundéré II", "Ngaoundéré III", "Belel"]
        }
    },
    "Centre": {
        "code": "CM002",
        "depts": {
            "Haute-Sanaga": ["Nanga-Eboko", "Minta"],
            "Lekié": ["Monatélé", "Obala", "Sa'a", "Ebebda", "Okola"],
            "Mbam-et-Inoubou": ["Bafia", "Bokito", "Ombessa"],
            "Mbam-et-Kim": ["Ntui", "Ngambé-Tikar"],
            "Mefou-et-Afamba": ["Mfou", "Awaé"],
            "Mefou-et-Akono": ["Ngoumou", "Akono"],
            "Mfoundi": ["Yaoundé I", "Yaoundé II", "Yaoundé III", "Yaoundé IV"],
            "Nyong-et-Kéllé": ["Eséka", "Makak"],
            "Nyong-et-Mfoumou": ["Akonolinga", "Endom"],
            "Nyong-et-So'o": ["Mbalmayo", "Ngomedzap"]
        }
    },
    "Est": {
        "code": "CM003",
        "depts": {
            "Boumba-et-Ngoko": ["Yokadouma", "Gari-Gombo"],
            "Haut-Nyong": ["Abong-Mbang", "Doumaintang", "Lomié"],
            "Kadey": ["Batouri", "Kette"],
            "Lom-et-Djérem": ["Bertoua I", "Bertoua II", "Garoua-Boulaï"]
        }
    },
    "Extrême-Nord": {
        "code": "CM004",
        "depts": {
            "Diamaré": ["Maroua I", "Maroua II", "Maroua III", "Gazawa"],
            "Logone-et-Chari": ["Kousseri", "Makary"],
            "Mayo-Danay": ["Yagoua", "Kaélé"],
            "Mayo-Kani": ["Kaélé", "Guidiguis"],
            "Mayo-Sava": ["Mora", "Tokombéré"],
            "Mayo-Tsanaga": ["Mokolo", "Bourha"]
        }
    },
    "Littoral": {
        "code": "CM005",
        "depts": {
            "Moungo": ["Nkongsamba I", "Mbanga", "Njombé-Penja", "Loum"],
            "Nkam": ["Yabassi", "Yingui"],
            "Sanaga-Maritime": ["Edéa I", "Dizangué", "Mouanko"],
            "Wouri": ["Douala I", "Douala II", "Douala III", "Douala IV", "Manoka"]
        }
    },
    "Nord": {
        "code": "CM006",
        "depts": {
            "Bénoué": ["Garoua I", "Garoua II", "Lagdo", "Bibemi", "Pitoa"],
            "Faro": ["Poli", "Beka"],
            "Mayo-Louti": ["Guider", "Figuil"],
            "Mayo-Rey": ["Tcholliré", "Touboro"]
        }
    },
    "Nord-Ouest": {
        "code": "CM007",
        "depts": {
            "Boyo": ["Fundong"],
            "Bui": ["Kumbo", "Jakiri"],
            "Donga-Mantung": ["Nkambé"],
            "Menchum": ["Wum"],
            "Mezam": ["Bamenda I", "Bamenda II", "Santa", "Bali"],
            "Momo": ["Mbengwi"],
            "Ngo-Ketunjia": ["Ndop"]
        }
    },
    "Ouest": {
        "code": "CM008",
        "depts": {
            "Bamboutos": ["Mbouda", "Galim", "Batcham"],
            "Haut-Nkam": ["Bafang", "Bana"],
            "Hauts-Plateaux": ["Baham"],
            "Koung-Khi": ["Bandjoun"],
            "Menoua": ["Dschang", "Santchou", "Nkong-Ni"],
            "Mifi": ["Bafoussam I", "Bafoussam II"],
            "Ndé": ["Bangangté"],
            "Noun": ["Foumban", "Foumbot", "Koutaba"]
        }
    },
    "Sud": {
        "code": "CM009",
        "depts": {
            "Dja-et-Lobo": ["Sangmélima", "Djoum"],
            "Mvila": ["Ebolowa I", "Ebolowa II", "Mengong"],
            "Océan": ["Kribi I", "Kribi II", "Campo", "Lolodorf"],
            "Vallée-du-Ntem": ["Ambam", "Kyé-Ossi"]
        }
    },
    "Sud-Ouest": {
        "code": "CM010",
        "depts": {
            "Fako": ["Limbe I", "Limbe II", "Buea", "Tiko", "Muyuka"],
            "Koupé-Manengouba": ["Bangem"],
            "Lebialem": ["Menji"],
            "Manyu": ["Mamfé"],
            "Meme": ["Kumba I", "Kumba II", "Mbonge"],
            "Ndian": ["Mundemba"]
        }
    }
}

# --- 2. CONFIGURATION DES FILIÈRES ---
# (Secteur, Sous-secteur, Unité, Prix Base, Rendement Base, Régions de prédilection)
FILIERES = [
    # AGRICULTURE
    ("Agriculture", "Cacao", "Tonnes", 1500, 0.5, ["Centre", "Sud", "Est", "Sud-Ouest", "Littoral"]),
    ("Agriculture", "Café", "Tonnes", 1200, 0.4, ["Ouest", "Nord-Ouest", "Est", "Littoral", "Centre"]),
    ("Agriculture", "Coton", "Tonnes", 350, 1.2, ["Extrême-Nord", "Nord", "Adamaoua"]),
    ("Agriculture", "Maïs", "Tonnes", 250, 2.5, ["Ouest", "Nord-Ouest", "Adamaoua", "Centre", "Nord", "Extrême-Nord"]),
    ("Agriculture", "Manioc", "Tonnes", 150, 12.0, ["Centre", "Sud", "Est", "Littoral", "Adamaoua", "Sud-Ouest"]),
    ("Agriculture", "Banane", "Tonnes", 200, 15.0, ["Sud-Ouest", "Littoral", "Ouest", "Centre"]),
    ("Agriculture", "Pomme de terre", "Tonnes", 400, 10.0, ["Ouest", "Nord-Ouest", "Adamaoua"]),
    ("Agriculture", "Riz", "Tonnes", 450, 3.0, ["Extrême-Nord", "Nord", "Nord-Ouest", "Ouest"]),
    ("Agriculture", "Sorgho", "Tonnes", 200, 1.5, ["Extrême-Nord", "Nord", "Adamaoua"]),
    ("Agriculture", "Oignon", "Tonnes", 300, 15.0, ["Nord", "Extrême-Nord"]),
    ("Agriculture", "Arachide", "Tonnes", 500, 1.2, ["Nord", "Extrême-Nord", "Centre", "Est", "Adamaoua"]),
    ("Agriculture", "Palmier à huile", "Tonnes", 90, 8.0, ["Littoral", "Sud-Ouest", "Centre", "Sud"]),
    ("Agriculture", "Hévéa", "Tonnes", 600, 1.5, ["Sud", "Littoral", "Sud-Ouest"]),
    ("Agriculture", "Ananas", "Tonnes", 150, 20.0, ["Centre", "Littoral"]),

    # ELEVAGE
    ("Elevage", "Bovins", "Têtes", 400000, 0, ["Adamaoua", "Nord", "Extrême-Nord", "Nord-Ouest", "Est"]),
    ("Elevage", "Volailles", "Têtes", 3500, 0, ["Ouest", "Centre", "Littoral", "Nord-Ouest", "Sud-Ouest"]),
    ("Elevage", "Porcins", "Têtes", 50000, 0, ["Ouest", "Centre", "Littoral", "Sud", "Nord-Ouest"]),
    ("Elevage", "Petits Ruminants", "Têtes", 40000, 0, ["Extrême-Nord", "Nord", "Adamaoua", "Nord-Ouest"]),

    # PECHE
    ("Peche", "Pêche Maritime", "Tonnes", 2000, 0, ["Littoral", "Sud", "Sud-Ouest"]),
    ("Peche", "Pêche Continentale", "Tonnes", 1500, 0, ["Nord", "Extrême-Nord", "Centre", "Est"]),
    ("Peche", "Aquaculture", "Tonnes", 2500, 0, ["Ouest", "Centre", "Littoral", "Nord-Ouest"]),
]

YEARS = [2021, 2022, 2023, 2024]

def generate_pcode(parent_code, index):
    """Génère un code unique basé sur le parent"""
    return f"{parent_code}{index:02d}"

def generate_full_dataset():
    data_rows = []
    header = ["year", "pcode", "zone_name", "level", "parent_pcode", "sector", "sub_sector", "volume", "unit", "surface_area", "yield", "producer_count", "average_price", "description"]

    # 1. Construction de la liste des zones à plat
    zones_list = [] # (pcode, name, level, parent_pcode, region_name)

    # PAYS
    zones_list.append(("CMR", "Cameroun", "COUNTRY", "", "Cameroun"))

    for reg_name, reg_data in ADMIN_STRUCTURE.items():
        reg_code = reg_data["code"]
        zones_list.append((reg_code, reg_name, "REGION", "CMR", reg_name))

        dept_index = 1
        for dept_name, arros in reg_data["depts"].items():
            dept_code = generate_pcode(reg_code, dept_index)
            zones_list.append((dept_code, dept_name, "DEPARTEMENT", reg_code, reg_name))

            arro_index = 1
            for arro_name in arros:
                arro_code = generate_pcode(dept_code, arro_index)
                zones_list.append((arro_code, arro_name, "ARRONDISSEMENT", dept_code, reg_name))
                arro_index += 1

            dept_index += 1

    print(f"Structure géographique générée : {len(zones_list)} zones.")
    print("Génération des statistiques de production...")

    # 2. Génération des données
    for year in YEARS:
        for pcode, name, level, parent_pcode, region_name in zones_list:

            for sector, sub_sector, unit, base_price, base_yield, regions_predilection in FILIERES:

                # RÈGLE D'OR : On ne génère une ligne QUE si la région est une zone de production
                # pour cette filière. Sinon, on passe (pas de ligne à 0).

                is_production_zone = False

                # Le pays produit tout
                if level == "COUNTRY":
                    is_production_zone = True
                # Pour les régions/départements/arrondissements, on vérifie la liste de prédilection
                elif region_name in regions_predilection:
                    is_production_zone = True

                if not is_production_zone:
                    continue # On saute cette itération, donc pas de ligne dans le CSV

                # Calcul des volumes (décroissant selon le niveau)
                base_vol = 0
                if level == "COUNTRY": base_vol = 500000
                elif level == "REGION": base_vol = 80000
                elif level == "DEPARTEMENT": base_vol = 15000
                elif level == "ARRONDISSEMENT": base_vol = 3000

                # Facteurs de variation
                year_factor = 1.0
                if year == 2022: year_factor = 1.05
                if year == 2023: year_factor = 0.90
                if year == 2024: year_factor = 1.15

                random_factor = random.uniform(0.7, 1.3) # Variation locale

                volume = int(base_vol * year_factor * random_factor)

                # Sécurité : pas de volume négatif ou nul si c'est une zone de production
                if volume <= 0: volume = 100

                # Calculs dérivés
                surface = 0
                yield_val = 0
                if base_yield > 0:
                    yield_val = round(base_yield * random.uniform(0.9, 1.1), 2)
                    surface = int(volume / yield_val)

                price = int(base_price * year_factor * random.uniform(0.95, 1.05))

                prod_ratio = 2 if unit == "Tonnes" else 10
                producer_count = int(volume / prod_ratio * random.uniform(0.8, 1.2))
                if producer_count < 10: producer_count = 10

                desc = f"Production de {sub_sector} en {year}"

                data_rows.append([
                    year, pcode, name, level, parent_pcode,
                    sector, sub_sector, volume, unit,
                    surface, yield_val, producer_count, price, desc
                ])

    # Écriture du CSV
    os.makedirs("data", exist_ok=True)
    file_path = "data/production_data.csv"
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data_rows)

    print(f"✅ Fichier généré : {file_path} ({len(data_rows)} lignes)")
    print("Vous pouvez maintenant lancer 'python3 seed_data.py'")

if __name__ == "__main__":
    generate_full_dataset()
