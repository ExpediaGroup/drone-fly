# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

Drone Fly is a distributed Hive metastore (HMS) event forwarder. It reads Hive metastore events from a Kafka topic (published by the [Apiary Kafka Metastore Listener](https://github.com/ExpediaGroup/apiary-extensions)) and forwards them to `MetaStoreEventListener` implementations running in a separate JVM context — decoupling listeners from the HMS process itself.

## Build commands

```bash
# Build all modules (skips tests by default per pom.xml skip.tests=true)
mvn package

# Build and run unit tests
mvn package -Dskip.tests=false

# Run unit tests only (no package)
mvn test -Dskip.tests=false

# Run a single test class
mvn test -Dskip.tests=false -pl drone-fly-app -Dtest=DroneFlyNotificationServiceTest

# Run integration tests (in drone-fly-integration-tests module)
mvn verify -Dskip.tests=false -pl drone-fly-integration-tests

# Format code (Google Java Format via Spotless)
mvn spotless:apply

# Check formatting without applying
mvn spotless:check
```

> Note: `.mvn/jvm.config` adds `--add-exports` flags required by google-java-format on JDK 17+. These are applied automatically by Maven.

## Module structure

| Module | Purpose |
|--------|---------|
| `drone-fly-core` | Shared core (currently minimal — `DroneFlyCore` placeholder, `DroneFlyException`) |
| `drone-fly-app` | Spring Boot application — contains all runtime logic |
| `drone-fly-integration-tests` | Integration tests using embedded Kafka (`spring-kafka-test`) |

## Application architecture

The main processing loop in `DroneFlyRunner` (Spring `ApplicationRunner`) calls `DroneFlyNotificationService.notifyListeners()` in a tight loop:

1. **`MessageReaderAdapter`** — reads a deserialized `ApiaryListenerEvent` from Kafka via `kafka-metastore-receiver`
2. **`HiveEventConverterService`** — converts the Apiary event to a Hive `ListenerEvent` (e.g. `CreateTableEvent`, `AlterTableEvent`, etc.)
3. **`MetaStoreListenerNotifier.notifyEvent()`** — dispatches to all loaded `MetaStoreEventListener` instances
4. **`ListenerCatalog`** — holds the list of loaded listeners; populated by `ListenerCatalogFactory` from `apiary.listener.list` config property

## Key configuration properties

| Property | Default | Description |
|----------|---------|-------------|
| `apiary.bootstrap.servers` | (required) | Kafka bootstrap servers |
| `apiary.kafka.topic.name` | (required) | Kafka topic for HMS events |
| `apiary.listener.list` | `LoggingMetastoreListener` | Comma-separated FQCNs of HMS listeners to load |
| `instance.name` | `drone-fly` | Used as Kafka consumer group ID |
| `endpoint.port` | `8008` | Spring Boot server port |

Additional Kafka consumer properties can be passed with the prefix `apiary.messaging.consumer.*`.

## Hive compatibility shims (important)

The `drone-fly-app` module contains shim classes under `src/main/java/org/apache/hadoop/hive/metastore/` that shadow Hive 4.x classes at runtime. These exist because `apiary-hive-events:8.1.15` was compiled against Hive 3.x and has binary incompatibilities with Hive 4.0.1 (the version used at runtime):

- **`org/apache/hadoop/hive/metastore/HiveMetaStore.java`** — re-introduces `HiveMetaStore` with an inner `HMSHandler` extending the Hive 4.x top-level `HMSHandler`
- **`org/apache/hadoop/hive/metastore/events/CreateTableEvent.java`** — provides both 3-arg (Hive 3.x) and 4-arg (Hive 4.x) constructors
- **9 `ColumnStatisticsData` shims** (`api/` package) — remove conflicting `ByteBuffer` setters that cause Jackson `InvalidDefinitionException` during Kafka event deserialization

Do not delete these shims. They are load-order sensitive: project classes must appear on the classpath before Hive jars.

## Java 21 / runtime notes

- Project targets Java 21 (`maven.compiler.release=21`), Spring Boot 3.2.x, Hive 4.0.1
- Surefire is configured with `--add-opens` flags for Hadoop/Hive/Mockito reflection — already in `pom.xml`
- The uber jar is produced with classifier `exec`: `drone-fly-app-<version>-exec.jar`
- Run it with: `java -Dloader.path=lib/ -jar drone-fly-app-<version>-exec.jar`

## Code style

Google Java Format (via Spotless). Run `mvn spotless:apply` before committing. Import ordering and unused import removal are enforced automatically.
