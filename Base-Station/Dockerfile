FROM openjdk:11-jre-slim
LABEL authors="ahmed"

# Set the working directory inside the container
WORKDIR /app

# Copy the JAR file (and any other necessary files) into the container
COPY target/Base-Station-1.0-SNAPSHOT.jar /app/Base-Station.jar

# Create a volume for persistent data
VOLUME /app/data

# Expose the port that your application listens on (if applicable)
EXPOSE 8080

# Command to run your application when the container starts
CMD ["java", "-jar", "Base-Station.jar"]
