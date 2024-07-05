##FROM openjdk:21-jdk-slim
#FROM openjdk:22-rc
#WORKDIR /app
#
#COPY target/bitcask.jar /app/bitcask.jar
#EXPOSE 8080
#
#CMD ["java", "-jar", "bitcask.jar"]

FROM openjdk:22-rc
WORKDIR /app
COPY target/bitcask.jar /app/bitcask.jar
EXPOSE 8080 12345
CMD ["java", "-jar", "bitcask.jar"]

