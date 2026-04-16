# Projet Data Pipelines DSIA

## Objectif
Plateforme de pipelines de données e-commerce pour l'ingestion en temps réel et le traitement batch.

## Stack Technique
- **Ingestion**: Kafka (Stream Producer & Anomaly Detector)
- **Stockage**: PostgreSQL & MinIO
- **Orchestration**: Airflow
- **Analyse**: Marimo / Jupyter
- **Déploiement**: Docker Compose

## Structure du Projet

```
projet_data_pipeline/
├── docker-compose.yml       # Configuration Docker Compose
├── Dockerfile               
├── requirements.txt           # Configuration des dépendances
├── .env                     # Variables d'environnement
├── airflow/                    # Répertoire des DAGs
│   └── dags 
        └── ecommerce_medallion_etl.py # DAG d'exemple de remonter des données dans bronze minio
    └── notebooks.py  
        └── analysis.py      # traitement données bronze pour faire du silver et du gold 
├── consumers/                    # Répertoire des DAGs 
        └── anomaly_detector.py # traitement des anomalies
        └── database_sink.py  # chargement des commandes dans la base potgres
├── logs/                    # Logs Airflow (créé automatiquement)
├── plugins/                 # Plugins Airflow (créé automatiquement)
└── data/                    # Données

```

## Prérequis

- Docker
- Docker Compose
- uv (pour le développement local, optionnel)


### 3. Démarrer la stack

```bash
docker compose up -d
```

La première fois, cela va :
- Construire l'image Docker Airflow personnalisée
- Initialiser la base de données PostgreSQL
- Créer l'utilisateur admin Airflow
- Démarrer le webserver et le scheduler
- Construire l'image Docker kafka
- Construire l'image Docker marimo
- Construire l'image Docker minio

### 4. Accéder aux interfaces web des différents images

Accés Airflow : http://localhost:8080
    **Identifiants par défaut :**
    - Username: `airflow`
    - Password: `airflow`

Accés Airflow : http://localhost:9001
    **Identifiants par défaut :**
    - Username: `minioadmin`
    - Password: `minioadmin123`

Accés marimo : http://localhost:7860

Accés kafka : http://localhost:8081


Le DAG `ecommerce_medallion_etl.py` illustre un workflow simple en 2 étapes :

### Architecture du DAG

1. **telecharger_donnees** (BashOperator)
   - Télécharge des données le repertoire : data/data.csv
   
2. **traiter_donnees**
   - Exécute une fonction Python simple
   - chargement données dans le bronze minio

## Tester le DAG

### Via l'interface web

1. Connectez-vous à http://localhost:8080
2. Activez le DAG en cliquant sur le toggle
3. Cliquez sur le bouton "Play" pour déclencher une exécution manuelle
4. Consultez les logs de chaque tâche

### Via la ligne de commande

```bash
# Lister les DAGs
docker compose exec airflow-webserver airflow dags list

# Tester un DAG
docker compose exec airflow-webserver airflow dags test first_dag 2024-01-01

# Tester une tâche spécifique
docker compose exec airflow-webserver airflow tasks test first_dag traiter_donnees 2024-01-01
```

## Arrêter la Stack

```bash
# Arrêter les services
docker compose down

# Arrêter et supprimer les volumes (attention : supprime les données)
docker compose down -v
```