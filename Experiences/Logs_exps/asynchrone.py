import asyncio

# Définir une fonction asynchrone qui simule une tâche (comme un appel réseau)
async def ma_tache(tache_num, temps_dattente):
    print(f"Début de la tâche {tache_num}")
    await asyncio.sleep(temps_dattente)  # Simule une opération bloquante avec un délai
    print(f"Fin de la tâche {tache_num}")
    

# Fonction principale
async def main():
    # Créer plusieurs tâches asynchrones
    tache_1 = asyncio.create_task(ma_tache(1, 2))
    tache_2 = asyncio.create_task(ma_tache(2, 1))
    print("Moi je continue tranquillement mon chemin")
    # Attendre que toutes les tâches soient terminées
    await tache_1
    print("je continue mon chemin")
    await tache_2
    print("je continue mon chemin encore")
    print("Toutes les tâches sont terminées, je peux m'arrêter")
# Exécuter l'événement principal asyncio
asyncio.run(main())
