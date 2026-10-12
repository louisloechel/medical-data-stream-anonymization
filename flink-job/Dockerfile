# Stage 1: Build the Flink job
FROM maven:3.9-eclipse-temurin-11 AS builder

# Set the working directory
WORKDIR /flink-job

# Copy the Maven project files
COPY pom.xml .
COPY src ./src

# Build the Maven project (the Maven repository is cached between builds)
RUN --mount=type=cache,target=/root/.m2 mvn -B -q clean package

# Stage 2: Create the final image
FROM flink:1.19
# Set the working directory
WORKDIR /flink-job

# Copy the JAR file from the builder stage
COPY --from=builder /flink-job/target/flink-job-1.0-SNAPSHOT.jar /flink-job/flink-job.jar

# docker-compose overrides the entrypoint with scripts/flink-submit.sh (waits for
# the JobManager and submits detached); this default is for standalone use.
ENTRYPOINT ["flink", "run", "-m", "flink-jobmanager:8081", "/flink-job/flink-job.jar"]
