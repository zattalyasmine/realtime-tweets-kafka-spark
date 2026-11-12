# Analyse de tweets en temps réel (Kafka + Spark)

Pipeline Big Data de traitement de tweets en temps réel : un producteur Kafka envoie un flux de tweets simulés, analysé par Spark Structured Streaming.

## Architecture
1. **Producteur** (`producer.py`) : génère un tweet simulé toutes les 2 secondes (id, texte, horodatage) et l'envoie dans le topic Kafka `tweets`.
2. **Traitement** (`spark_consumer.py`) : lit le flux Kafka avec Spark Structured Streaming (lots de 5 secondes) et attribue un sentiment (POSITIVE / NEGATIVE / NEUTRAL) par méthode lexicale.
3. **Stockage** : les résultats sont enregistrés dans une base SQLite (`tweets.db`).
4. **Visualisation** (`dashboard.py`) : tableau de bord des résultats.

## Technologies
Python, Apache Kafka, Apache Spark 3.5 (Structured Streaming), SQLite, pandas

## Lancer le projet
1. Démarrer Zookeeper puis Kafka (`localhost:9092`).
2. Créer le topic : `kafka-topics --create --topic tweets --bootstrap-server localhost:9092`
3. Installer les dépendances : `pip install -r requirements.txt`
4. Lancer le consumer : `spark-submit spark_consumer.py`
5. Lancer le producteur : `python producer.py`
6. Lancer le dashboard : `python dashboard.py`

## Remarques
- Les tweets sont simulés : aucune clé d'API Twitter/X n'est nécessaire.
- L'analyse des sentiments repose sur des listes de mots positifs et négatifs.

## Auteure
Yasmine ZATTAL – [LinkedIn](https://www.linkedin.com/in/yasmine-zattal-048b7b206/)
