# Java 21 Migration — Change Log

This document describes all changes made to migrate drone-fly from Java 8 to Java 21
and fix the integration tests that broke as a result of the Hive 4.x upgrade.

---

## Branch

`Upgrade_Java_21` (personal fork) / `feature/EGDL_7736_Upgrade_Java_21` (upstream)

---

## Commit 1 — `33b6f95` Migrate project to Java 21

### Dependencies upgraded

| Dependency | Before | After |
|---|---|---|
| Java / JDK | 8 | 21 |
| Spring Boot | 2.7.10 | 3.2.12 (Spring Framework 6.1) |
| Hive Metastore | 2.3.9 | 4.0.1 |
| Apiary Extensions (`kafka-metastore-receiver`) | 6.0.2 | 8.1.15 |
| Hadoop client runtime | — | 3.3.6 (added; provides shaded WoodStox XML parser) |
| Guava | 27.1-jre | 33.4.0-jre |
| MSK IAM Auth | 1.1.9 | 2.2.0 |
| Spotless Maven plugin | 2.4.1 | 2.43.0 (google-java-format 1.19.2, Java 21 compatible) |
| JaCoCo | 0.8.6 | 0.8.12 |
| Surefire | 3.0.0-M5 | 3.2.5 |
| Docker base image | `openjdk:8-jdk` | `amazoncorretto:21` |

Dropped explicit version pins for Logback, Log4j, JUnit, Mockito, AssertJ, and
Dropwizard — these are now managed by the Spring Boot BOM.

### Build / tooling changes

- `pom.xml`: set `jdk.version=21`, `maven.compiler.release=21`
- `.mvn/jvm.config`: added `--add-exports` flags required by google-java-format
  on JDK 17+
- Surefire `argLine`: added `--add-opens` for Hadoop / Hive / Mockito runtime
  reflection
- Jib container config: added `--add-opens` JVM flags for Hadoop / Hive runtime
- GitHub Actions workflows (`.github/workflows/*.yml`): upgraded to Java 21,
  `corretto` distribution, `actions/checkout@v4`, `actions/setup-java@v4`
- `DataSourceAutoConfiguration` excluded to suppress spurious JDBC auto-config
  pulled in by Hive transitive dependencies

### Source code changes

| File | Change |
|---|---|
| `DroneFly.java` | No logic change; re-formatted |
| `DroneFlyRunner.java` | `javax.annotation.PreDestroy` → `jakarta.annotation.PreDestroy` |
| `CommonBeans.java` | `javax.annotation.PreDestroy` → `jakarta.annotation.PreDestroy` |
| `HMSHandlerFactory.java` | Import updated: `HiveMetaStore.HMSHandler` → `HMSHandler` (Hive 4.x moved it to a top-level class) |
| `HiveEventConverterService.java` | Updated all event constructors for Hive 4.x API changes (see table below) |
| `ListenerCatalog.java` | `JavaUtils` package rename in Hive 4.x |
| `ListenerCatalogFactory.java` | `commons-lang` → `commons-lang3` (`StringUtils`) |
| `LoggingMetastoreListener.java` | Re-formatted |
| `DroneFlyIntegrationTest.java` | `org.awaitility.Duration` → `java.time.Duration`; import ordering |
| `DroneFlyIntegrationTestUtils.java` | Re-formatted |
| `DummyListener.java` (integration) | Re-formatted |

### Hive 4.x event constructor changes in `HiveEventConverterService`

| Event | Hive 3.x constructor | Hive 4.x constructor |
|---|---|---|
| `CreateTableEvent` | `(Table, boolean, HMSHandler)` | `(Table, boolean, IHMSHandler, boolean isReplicated)` |
| `AlterTableEvent` | `(oldTable, newTable, isTruncateOp, status, handler)` | `(oldTable, newTable, isTruncateOp, status, writeId, handler, isReplicated)` |
| `AlterPartitionEvent` | shorter signature | `(oldPart, newPart, table, status, isTruncateOp, writeId, handler)` |
| `DropTableEvent` | `(table, status, deleteData, handler)` | `(table, status, deleteData, handler, isReplicated)` |
| `InsertEvent` | shorter signature | `(dbName, catName, tableName, partVals, insertData, status, handler)` |

---

## Commit 2 — `91cdcca` Fix integration tests: add Hive 3.x/4.x compatibility shims

### Problem

`apiary-hive-events:8.1.15` (consumed via `kafka-metastore-receiver`) was compiled
against Hive 3.x. At runtime with `hive-metastore:4.0.1` three layers of binary
incompatibility caused the integration test to fail.

### Layer 1 — `NoSuchMethodError`: missing 3-arg `CreateTableEvent` constructor

**Root cause:**
`JsonMetaStoreEventSerDe$HeplerApiaryListenerEvent` has a static initializer that
calls `new CreateTableEvent(null, false, (HiveMetaStore.HMSHandler) null)`.
Two things are wrong in Hive 4.x:

1. `HiveMetaStore.HMSHandler` (inner class) no longer exists — `HMSHandler` is now
   a standalone top-level class.
2. The constructor signature changed to
   `CreateTableEvent(Table, boolean, IHMSHandler, boolean isReplicated)`.

**Fix — two shim classes added to `drone-fly-app/src/main/java/`:**

`org/apache/hadoop/hive/metastore/HiveMetaStore.java`
- Re-introduces `HiveMetaStore` with an inner `HMSHandler` class that extends the
  new standalone `org.apache.hadoop.hive.metastore.HMSHandler`.

`org/apache/hadoop/hive/metastore/events/CreateTableEvent.java`
- Provides both constructors:
  - 3-arg (Hive 3.x): `(Table, boolean, HiveMetaStore.HMSHandler)` — satisfies the
    apiary static initializer.
  - 4-arg (Hive 4.x): `(Table, boolean, IHMSHandler, boolean)` — used by
    `HiveEventConverterService`.
- Extends `ListenerEvent` directly and implements `getTable()` / `isReplicated()`.

Because these classes live in `drone-fly-app/target/classes/`, they appear on the
classpath before the Hive jars and shadow the Hive versions at runtime.

### Layer 2 — Jackson `InvalidDefinitionException`: conflicting `ByteBuffer` setters

**Root cause:**
Hive 4.x added an inline `colStats: ColumnStatistics` field to `Table`. When Jackson
builds a deserializer for `Table` (triggered during Kafka event deserialization), it
recursively introspects `ColumnStatistics` → `ColumnStatisticsData` → all 8 column
stats variant types. Each of these Thrift-generated Hive 4.x classes exposes both a
`setX(byte[])` and a `setX(ByteBuffer)` setter for binary properties (`bitVectors`,
`histogram`, `unscaled`). Jackson treats these as conflicting definitions for the same
property and throws `InvalidDefinitionException` before any data is read.

**Fix — 9 shim classes added to `drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/`:**

| Shim class | Conflicting fields removed |
|---|---|
| `BooleanColumnStatsData` | `setBitVectors(ByteBuffer)` |
| `LongColumnStatsData` | `setBitVectors(ByteBuffer)`, `setHistogram(ByteBuffer)` |
| `DoubleColumnStatsData` | `setBitVectors(ByteBuffer)`, `setHistogram(ByteBuffer)` |
| `StringColumnStatsData` | `setBitVectors(ByteBuffer)` |
| `BinaryColumnStatsData` | `setBitVectors(ByteBuffer)` |
| `DecimalColumnStatsData` | `setBitVectors(ByteBuffer)`, `setHistogram(ByteBuffer)` |
| `DateColumnStatsData` | `setBitVectors(ByteBuffer)`, `setHistogram(ByteBuffer)` |
| `TimestampColumnStatsData` | `setBitVectors(ByteBuffer)`, `setHistogram(ByteBuffer)` |
| `Decimal` | `setUnscaled(ByteBuffer)` |

Each shim is a minimal plain Java bean (`Serializable`, no-arg constructor,
`byte[]`-only setters) that shadows the Hive 4.x Thrift-generated class on the
classpath. drone-fly only deserializes these objects from JSON via Jackson; it never
performs Thrift I/O on them, so the omission of Thrift plumbing is safe.

### Layer 3 — Assertion failure: new Hive 4.x `Partition` fields

**Root cause:**
Hive 4.x `Partition` gained two new optional fields: `writeId` (default `-1`) and
`isStatsCompliant` (default `false`). These fields are serialized to JSON by
`KafkaMetaStoreEventListener` and deserialized back by drone-fly, setting the Thrift
`isSet` flags to `true`. The expected `Partition` built by `buildPartition()` had the
same values but with `isSet = false`, causing `Partition.equals()` to return `false`.

**Fix:**
`DroneFlyIntegrationTestUtils.buildPartition()` now explicitly calls
`partition.setWriteId(-1)` and `partition.setIsStatsCompliant(false)` so the expected
object's `isSet` flags match the deserialized object.

---

## Files changed (summary)

```
.github/workflows/build.yml
.github/workflows/main.yml
.github/workflows/release.yml
.mvn/jvm.config                                              [new]
drone-fly-app/pom.xml
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/DroneFly.java
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/DroneFlyRunner.java
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/context/CommonBeans.java
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/messaging/MessageReaderAdapter.java
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/service/DroneFlyNotificationService.java
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/service/HiveEventConverterService.java
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/service/ListenerCatalog.java
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/service/factory/HMSHandlerFactory.java
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/service/factory/ListenerCatalogFactory.java
drone-fly-app/src/main/java/com/expediagroup/dataplatform/dronefly/app/service/listener/LoggingMetastoreListener.java
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java             [new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/BinaryColumnStatsData.java [new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/BooleanColumnStatsData.java[new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/DateColumnStatsData.java   [new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/Decimal.java               [new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/DecimalColumnStatsData.java[new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/DoubleColumnStatsData.java [new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/LongColumnStatsData.java   [new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/StringColumnStatsData.java [new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/api/TimestampColumnStatsData.java[new - compat shim]
drone-fly-app/src/main/java/org/apache/hadoop/hive/metastore/events/CreateTableEvent.java   [new - compat shim]
drone-fly-app/src/test/java/...  (unit tests updated for Hive 4.x API)
drone-fly-core/src/main/java/... (re-formatted)
drone-fly-integration-tests/pom.xml
drone-fly-integration-tests/src/test/java/.../DroneFlyIntegrationTest.java
drone-fly-integration-tests/src/test/java/.../DroneFlyIntegrationTestUtils.java
drone-fly-integration-tests/src/test/java/.../DummyListener.java
pom.xml
```
