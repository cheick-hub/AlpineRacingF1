# Image de redis avec les fonctionnalités d'indexation, meme si elles ne sont pas utilisé ici
FROM redis/redis-stack-server:latest

# emplacement ou redis stocke les données
WORKDIR /data

# ajout d'un volume pour pouvoir concerver les données du cache
VOLUME /data

# Exposer le port de redis
EXPOSE 6379

# On ajoute la conf
COPY redis.conf /data/redis.conf

# lancer redis
CMD ["redis-server", "--bind", "0.0.0.0", "--protected-mode", "no"]
