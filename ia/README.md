# IA - planification en langage naturel

Le dossier `ia/` contient le composant d'intelligence artificielle. Il transforme une question en français en un plan JSON contrôlé.

## Fichiers

- `ollama_client.py` communique avec le modèle local Ollama et vérifie sa disponibilité.
- `query_planner.py` définit le format du plan, les champs autorisés et les consignes envoyées au modèle.
- `fallback_planner.py` propose quelques plans déterministes pour les cas fréquents si la réponse du modèle est vide.

## Ce que fait l'IA

Elle repère les opérations, objets, colonnes, filtres, jointures et mesures utiles à partir de la question et du schéma reçu. Le modèle par défaut est `qwen2.5:1.5b`.

## Ce que l'IA ne fait pas

- Elle ne se connecte pas aux bases et ne lit pas les lignes directement.
- Elle ne reçoit pas les URI de connexion, chemins locaux ou valeurs des enregistrements dans le prompt de planification.
- Elle ne produit pas de SQL/Cypher libre qui serait exécuté tel quel.
- Elle ne dessine pas d'images génératives. Les graphiques analytiques sont créés par `front/` avec Plotly à partir des vrais résultats.

Le plan proposé doit être validé et exécuté par `backend/` avant d'être affiché.
