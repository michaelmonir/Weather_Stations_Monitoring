FROM openjdk:22-oraclelinux9
EXPOSE 8080
ADD target/producer-jar.jar producer-jar.jar
ENTRYPOINT ["java", "-jar", "producer-jar.jar"]



## Stage 1: Download dependencies
##FROM maven:3.8.4-jdk-11-slim AS dependencies
#FROM maven AS dependencies
#WORKDIR /app
#COPY pom.xml .
#RUN mvn dependency:go-offline
#
## Stage 2: Build the application
#FROM dependencies AS builder
#COPY src /app/src
#RUN mvn package
#
## Stage 3: Create the final image with only necessary artifacts
#FROM openjdk:22-oraclelinux9
#EXPOSE 8080
#COPY --from=builder /app/target/producer-jar.jar /producer-jar.jar
#ENTRYPOINT ["java", "-jar", "producer-jar.jar"]
