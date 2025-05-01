# Utiliser une image de base Java 17 ou 11 (selon la version requise par ton JAR)
# Pour build le jar : ./gradlew build -x test --no-daemon
FROM openjdk:17-jdk-alpine

# Créer un dossier pour l'application
WORKDIR /app

# Copier le fichier JAR dans le conteneur
COPY NostrRelay/build/libs/NostrRelay.jar /app/app.jar

# Exposer le port si nécessaire (exemple : port 8080)
EXPOSE 8081

# Définir la commande de démarrage du conteneur pour exécuter le fichier JAR
CMD ["java", "-jar", "/app/app.jar"]
