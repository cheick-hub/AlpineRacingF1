#!/bin/sh

# Fonction pour afficher du texte en couleur
echo_color() {
  color_code="$1"
  text="$2"
  echo "\033[0;${color_code}m${text}\033[0m"
}

# Obtenir le chemin absolu du script
script_path=$(realpath "$0")

# Obtenir le répertoire contenant le script
script_dir=$(dirname "$script_path")

# Définir le chemin vers le fichier YAML
yaml_file="$script_dir/.docker/docker-compose_fastAPI_prod.yml"

# Afficher le chemin complet vers le fichier YAML
echo_color "34" "Fichier Compose : $yaml_file"

echo_color "34" "docker pull alpineracing.azurecr.io/alr/catana_fastapi:prod"

docker pull alpineracing.azurecr.io/alr/catana_fastapi:prod

echo_color "34" "docker compose -f $yaml_file --project-directory $script_dir up -d"


docker compose -f "$yaml_file" --project-directory "$script_dir" up -d
