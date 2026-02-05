from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')
    )
    return conn

# ... [Les routes existantes get_zones, get_filters, get_map_data restent identiques] ...
# Je remets get_zones et get_filters pour la complétude, suivi des nouvelles routes.

@app.route('/api/gis/zones', methods=['GET'])
def get_zones():
    level = request.args.get('level', 'REGION').upper()
    parent_id = request.args.get('parent_id')
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = "SELECT id, name, level, parent_id, code, ST_AsGeoJSON(geometry)::json as geometry FROM administrative_zones WHERE level = %s"
        params = [level]
        if parent_id and parent_id != 'null' and parent_id != 'undefined':
            query += " AND parent_id = %s"
            params.append(str(parent_id))
        cur.execute(query, params)
        rows = cur.fetchall()
        features = []
        for row in rows:
            features.append({
                "type": "Feature",
                "properties": row,
                "geometry": row['geometry']
            })
        return jsonify({"type": "FeatureCollection", "features": features})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/filters', methods=['GET'])
def get_filters():
    parent_id = request.args.get('parent_id')
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if not parent_id or parent_id == 'null' or parent_id == 'undefined':
            where_clause = "1=1"
            params = []
        else:
            where_clause = """
                ps.zone_code IN (
                    WITH RECURSIVE zone_tree AS (
                        SELECT code, id FROM administrative_zones WHERE id = %s
                        UNION ALL
                        SELECT az.code, az.id FROM administrative_zones az
                        JOIN zone_tree zt ON az.parent_id = zt.id::text
                    )
                    SELECT code FROM zone_tree
                )
            """
            params = [int(parent_id)]
        query = f"""
            SELECT DISTINCT ss.id, ss.name, ss.color, s.name as category
            FROM sub_sectors ss
            JOIN sectors s ON ss.sector_id = s.id
            JOIN production_stats ps ON ps.sub_sector_id = ss.id
            WHERE {where_clause}
            ORDER BY s.name, ss.name
        """
        cur.execute(query, params)
        rows = cur.fetchall()
        grouped = {}
        for row in rows:
            cat = row['category']
            if cat not in grouped: grouped[cat] = []
            grouped[cat].append(row)
        return jsonify(grouped)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/map/data', methods=['GET'])
def get_map_data():
    sub_sector_id = request.args.get('sector_id')
    level = request.args.get('level', 'REGION').upper()
    parent_id = request.args.get('parent_id')
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT ss.name, ss.color, s.name as category FROM sub_sectors ss JOIN sectors s ON ss.sector_id = s.id WHERE ss.id = %s", (sub_sector_id,))
        sector_info = cur.fetchone()

        zones_query = "SELECT id, name, level, parent_id, code, ST_AsGeoJSON(geometry)::json as geometry FROM administrative_zones WHERE level = %s"
        zones_params = [level]
        if parent_id and parent_id != 'null' and parent_id != 'undefined':
            zones_query += " AND parent_id = %s"
            zones_params.append(str(parent_id))
        cur.execute(zones_query, zones_params)
        zones = cur.fetchall()

        features = []
        total_global_volume = 0
        global_unit = ""

        for zone in zones:
            stats_query = """
                WITH RECURSIVE zone_descendants AS (
                    SELECT code, id FROM administrative_zones WHERE id = %s
                    UNION ALL
                    SELECT az.code, az.id FROM administrative_zones az
                    JOIN zone_descendants zd ON az.parent_id = zd.id::text
                )
                SELECT SUM(volume) as total, MAX(unit) as unit
                FROM production_stats
                WHERE sub_sector_id = %s AND zone_code IN (SELECT code FROM zone_descendants)
            """
            cur.execute(stats_query, (zone['id'], sub_sector_id))
            stat = cur.fetchone()
            vol = float(stat['total']) if stat and stat['total'] else 0
            unit = stat['unit'] if stat and stat['unit'] else ""
            if unit: global_unit = unit
            total_global_volume += vol

            features.append({
                "type": "Feature",
                "properties": {**zone, "value": vol, "unit": unit},
                "geometry": zone['geometry']
            })

        return jsonify({
            "geojson": { "type": "FeatureCollection", "features": features },
            "stats": { "total": total_global_volume, "unit": global_unit },
            "sector": sector_info
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/zone/stats', methods=['GET'])
def get_zone_stats():
    zone_id = request.args.get('zone_id')
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = """
            WITH RECURSIVE zone_tree AS (
                SELECT code, id FROM administrative_zones WHERE id = %s
                UNION ALL
                SELECT az.code, az.id FROM administrative_zones az
                JOIN zone_tree zt ON az.parent_id = zt.id::text
            )
            SELECT ss.name as sector, s.name as category, SUM(ps.volume) as volume, MAX(ps.unit) as unit
            FROM production_stats ps
            JOIN sub_sectors ss ON ps.sub_sector_id = ss.id
            JOIN sectors s ON ss.sector_id = s.id
            WHERE ps.zone_code IN (SELECT code FROM zone_tree)
            GROUP BY ss.name, s.name
            ORDER BY volume DESC
        """
        cur.execute(query, (int(zone_id),))
        rows = cur.fetchall()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --- NOUVELLES ROUTES POUR LE PANNEAU DROIT ---

@app.route('/api/stats/global', methods=['GET'])
def get_global_zone_stats():
    """Stats globales d'une zone (Top productions, Total producteurs)"""
    zone_id = request.args.get('zone_id')
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Top 5 productions
        query_top = """
            WITH RECURSIVE zone_tree AS (
                SELECT code, id FROM administrative_zones WHERE id = %s
                UNION ALL
                SELECT az.code, az.id FROM administrative_zones az
                JOIN zone_tree zt ON az.parent_id = zt.id::text
            )
            SELECT ss.name, SUM(ps.volume) as volume, MAX(ps.unit) as unit
            FROM production_stats ps
            JOIN sub_sectors ss ON ps.sub_sector_id = ss.id
            WHERE ps.zone_code IN (SELECT code FROM zone_tree)
            GROUP BY ss.name
            ORDER BY volume DESC
            LIMIT 5
        """
        cur.execute(query_top, (int(zone_id),))
        top_products = cur.fetchall()

        # Total producteurs
        query_producers = """
            WITH RECURSIVE zone_tree AS (
                SELECT code, id FROM administrative_zones WHERE id = %s
                UNION ALL
                SELECT az.code, az.id FROM administrative_zones az
                JOIN zone_tree zt ON az.parent_id = zt.id::text
            )
            SELECT SUM(producer_count) as total_producers
            FROM production_stats ps
            WHERE ps.zone_code IN (SELECT code FROM zone_tree)
        """
        cur.execute(query_producers, (int(zone_id),))
        producers = cur.fetchone()

        return jsonify({
            "top_products": top_products,
            "total_producers": producers['total_producers'] if producers else 0
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/stats/evolution', methods=['GET'])
def get_evolution_stats():
    """Evolution temporelle (2021-2024) pour les top produits de la zone"""
    zone_id = request.args.get('zone_id')
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = """
            WITH RECURSIVE zone_tree AS (
                SELECT code, id FROM administrative_zones WHERE id = %s
                UNION ALL
                SELECT az.code, az.id FROM administrative_zones az
                JOIN zone_tree zt ON az.parent_id = zt.id::text
            )
            SELECT ps.year, ss.name as sector, SUM(ps.volume) as volume
            FROM production_stats ps
            JOIN sub_sectors ss ON ps.sub_sector_id = ss.id
            WHERE ps.zone_code IN (SELECT code FROM zone_tree)
            AND ps.year BETWEEN 2021 AND 2024
            GROUP BY ps.year, ss.name
            ORDER BY ps.year ASC
        """
        cur.execute(query, (int(zone_id),))
        rows = cur.fetchall()

        # Restructuration pour le frontend (Recharts)
        # { year: 2021, Cacao: 100, Coton: 200 }
        data_by_year = {}
        sectors = set()
        for row in rows:
            year = row['year']
            if year not in data_by_year: data_by_year[year] = {"year": year}
            data_by_year[year][row['sector']] = float(row['volume'])
            sectors.add(row['sector'])

        return jsonify({"data": list(data_by_year.values()), "sectors": list(sectors)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/stats/comparison', methods=['GET'])
def get_comparison_stats():
    """Comparaison des enfants directs (ex: Départements d'une Région)"""
    zone_id = request.args.get('zone_id')
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Trouver les enfants directs
        query = """
            SELECT id, name, code FROM administrative_zones WHERE parent_id = %s
        """
        cur.execute(query, (str(zone_id),))
        children = cur.fetchall()

        comparison_data = []

        for child in children:
            # Somme totale de production pour cet enfant (toutes filières confondues - attention aux unités différentes,
            # idéalement on filtre par filière principale, ici on fait une somme globale brute pour l'exemple ou on prend le top produit)

            # Pour l'exemple, on prend le volume total du produit le plus important de la zone parente
            # 1. Trouver le top produit de la zone parente
            cur.execute("""
                WITH RECURSIVE zone_tree AS (
                    SELECT code, id FROM administrative_zones WHERE id = %s
                    UNION ALL
                    SELECT az.code, az.id FROM administrative_zones az
                    JOIN zone_tree zt ON az.parent_id = zt.id::text
                )
                SELECT sub_sector_id FROM production_stats ps
                WHERE zone_code IN (SELECT code FROM zone_tree)
                GROUP BY sub_sector_id ORDER BY SUM(volume) DESC LIMIT 1
            """, (int(zone_id),))
            top_sector = cur.fetchone()

            if top_sector:
                sid = top_sector['sub_sector_id']
                # 2. Calculer le volume de ce produit pour l'enfant (récursif)
                cur.execute("""
                    WITH RECURSIVE child_tree AS (
                        SELECT code, id FROM administrative_zones WHERE id = %s
                        UNION ALL
                        SELECT az.code, az.id FROM administrative_zones az
                        JOIN child_tree zt ON az.parent_id = zt.id::text
                    )
                    SELECT SUM(volume) as total FROM production_stats
                    WHERE sub_sector_id = %s AND zone_code IN (SELECT code FROM child_tree)
                """, (child['id'], sid))
                res = cur.fetchone()
                vol = float(res['total']) if res and res['total'] else 0
                comparison_data.append({"name": child['name'], "value": vol})

        return jsonify(comparison_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
