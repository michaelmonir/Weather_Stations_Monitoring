
FROM openjdk:11-jre-slim
LABEL authors="ahmed"


ARG JAR_FILE=target/WeatherStation-1.0-SNAPSHOT.jar

ARG arg1

WORKDIR /app

COPY target/WeatherStation-1.0-SNAPSHOT.jar  /app/WeatherStation.jar


EXPOSE 8081


CMD ["java", "-jar", "WeatherStation.jar"]

