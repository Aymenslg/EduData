# Backend - API et moteur de données

Le dossier `backend/` reçoit les demandes du front, vérifie les opérations et exécute les lectures autorisées.

## Parcours d'une requête

1. `api.py` expose les routes HTTP et valide les entrées.
2. `orchestrator.py` découvre les schémas et appelle le planificateur dans `ia/`.
3. `query_engine.py` vérifie chaque source, objet, colonne, filtre, jointure et agrégat.
4. Les connecteurs lisent les sources en mode contrôlé.
5. pandas effectue les jointures, regroupements et calculs ; le moteur renvoie lignes, provenance et avertissements.

## Sous-dossiers

- `connectors/` : accès SQLite, MongoDB, Neo4j et lecture/inventaire des fichiers.
- `sources/` : registre local des sources enregistrées.
- `ingestion/` : lecture de fichiers tabulaires et traitement d'import.
- `processing/` : statistiques, normalisation, fusion et provenance.
- `retrieval/` : utilitaires de récupération de documents ; l'interrogation de PDF dans l'assistant n'est pas activée.
- `visualization/` : utilitaires de génération de configuration de graphiques.

SQLite est interrogé en lecture seule. Le moteur n'exécute pas de requêtes d'écriture générées par l'IA.
