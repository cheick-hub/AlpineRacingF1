## Configuration du conteneur pour alimenter Redis

1. Depuis le répertoire `perfo_fastapi`, exécutez la commande suivante pour construire le conteneur :
   ```bash
   docker build -f .docker/Dockerfile-fill_redis -t fill_redis:latest .
   ```

2. Toujours dans le répertoire `perfo_fastapi`, lancez les conteneurs avec cette commande :
    ```bash
    docker compose -f .docker/docker-compose_fill_redis.yml up -d
    ```

3. (Optionnel) Plusieurs containeur seront lancés (un par compétition). Pour accéder à l'instance, utilisez la commande suivante :
    ```bash
    docker exec -it <container_name> /bin/bash
    ```

4. (Optionnel) Pour trouver l'IP de votre machine sur ubuntu :
    ```bash
    hostname -I | cut -d' ' -f1
    ```

Ce conteneur permet d'alimenter la base de données Redis avec des snn récemment traités. Chaque jour, un fichier de `logs` est généré dans le répertoire logs. 
Vous trouverez également dans ce répertoire le fichier `last_executed_date.txt` qui enregistre les dates d'exécution du script. 
Si ce fichier est supprimé, le script s'exécutera depuis l'instant présent plutôt que depuis la dernière date d'exécution enregistrée.