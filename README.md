# api pour la gestion des donnees cartographiques du projet

## Configuration du projet

### Acces au projet

```bash
git clone 

cd map_api
```

### creation et activation de l'environnement virtuel
```bash
python3 -m venv venv

# Activation sous linux
source venv/bin/activate

# Activation sous Windows
venv\Scripts\Activate

# Desactiver
deactivate
```

### Installation des dependances du projet

```bash
pip install -r requirements.txt
```

### Configuration de la base de donnees
```bash
cp .env.example .env
```

## injection des donnees geographiques du cameroun dans la base de donnees
```bash
python3 ingest_data.py
```

## Generation des donnees associer au bassin de production
```bash 
python3 generate_full_data.py
```

## Injection des donnees des bassins de production dans la base de donnees 
```bash
python3 seed_data.py
```

## Demarrage de l'api
```bash
python3 app.py

# demarrage sur le port 5000
```
