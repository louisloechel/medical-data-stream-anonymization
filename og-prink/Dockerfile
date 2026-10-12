# Pinned build image: the pom targets Java 8 bytecode; an unpinned `maven` image may ship a JDK that no
# longer supports that target. Temurin 11 builds Java 8 bytecode and runs the Flink 1.15 build tooling.
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
COPY profiles ./profiles
# Build the application using Maven
RUN mvn clean package -DskipTests

FROM flink:1.19

COPY --from=build /app/target/prink-*.jar /opt/flink/usrlib/artifacts/1/

# CMD [ "--job-classname", "ganges.GangesEvaluation" ]
ENTRYPOINT ["flink", "run", "-m", "flink-jobmanager:8081", "/opt/flink/usrlib/artifacts/1/prink-0.6.jar"]