# --- Stage 1: Build ---
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests

# --- Stage 2: Runtime ---
FROM flink:1.18

COPY --from=builder /build/target/flink-job-1.0-SNAPSHOT.jar /opt/flink/usrlib/flink-job-1.0-SNAPSHOT.jar