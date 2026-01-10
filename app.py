from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
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

@app.route('/api/gis/zones', methods=['GET'])
def get_zones():
    # Par défaut, si rien n'est demandé, on renvoie le PAYS (Niveau 0)
    level = request.args.get('level', 'COUNTRY').upper()
    parent_id = request.args.get('parent_id')

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Note: GeoPandas to_postgis crée la colonne 'geometry' par défaut
        query = """
            SELECT
                id,
                name,
                level,
                parent_id,
                ST_AsGeoJSON(geometry)::json as geometry
            FROM administrative_zones
            WHERE level = %s
        """
        params = [level]

        if parent_id:
            query += " AND parent_id = %s"
            params.append(parent_id)

        cur.execute(query, params)
        rows = cur.fetchall()

        features = []
        for row in rows:
            features.append({
                "type": "Feature",
                "id": row['id'],
                "properties": {
                    "id": row['id'],
                    "name": row['name'],
                    "level": row['level'],
                    "parent_id": row['parent_id']
                },
                "geometry": row['geometry']
            })

        response = {
            "type": "FeatureCollection",
            "features": features
        }

        return jsonify(response)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Erreur serveur"}), 500
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
