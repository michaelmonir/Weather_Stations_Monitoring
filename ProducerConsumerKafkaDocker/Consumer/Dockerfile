FROM openjdk:22-oraclelinux9
EXPOSE 8080
ADD target/consumer-jar.jar consumer-jar.jar
ENTRYPOINT ["java", "-jar", "consumer-jar.jar"]

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
#COPY --from=builder /app/target/consumer-jar.jar /consumer-jar.jar
#ENTRYPOINT ["java", "-jar", "consumer-jar.jar"]

#FROM openjdk:22-oraclelinux9 AS build
#
## Install Maven
#RUN yum install -y maven
#
#WORKDIR /app
#COPY . .
#
#RUN mvn package
#
#FROM openjdk:22-oraclelinux9
#EXPOSE 8080
#
#COPY --from=build /app/target/consumer-jar.jar /consumer-jar.jar
#
#ENTRYPOINT ["java", "-jar", "consumer-jar.jar"]

