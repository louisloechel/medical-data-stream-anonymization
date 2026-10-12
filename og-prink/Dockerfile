# Pinned build image: the pom targets Java 8 bytecode; Temurin 11 builds it and runs the Flink 1.19 build tooling.
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
COPY profiles ./profiles
# Build the application using Maven (tests are not compiled or run in the image build;
# the Maven repository is cached between builds)
RUN --mount=type=cache,target=/root/.m2 mvn -B -q clean package -Dmaven.test.skip=true

FROM flink:1.19

COPY --from=build /app/target/prink-*.jar /opt/flink/usrlib/artifacts/1/

# docker-compose overrides the entrypoint with scripts/flink-submit.sh (waits for
# the JobManager and submits detached); this default is for standalone use.
ENTRYPOINT ["flink", "run", "-m", "flink-jobmanager:8081", "/opt/flink/usrlib/artifacts/1/prink-0.6.jar"]
