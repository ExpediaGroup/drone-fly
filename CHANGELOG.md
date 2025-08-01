# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [1.0.8] - 2025-07-22
###
* Update to parent 3.0.1
* Upgrade parent pom to accomodate for new sonatype deployment.
* set drone-fly-app-exec.jar executable flag to false as it breaks deployment. (This jar is not used inside the docker image).

## YANKED (incomplete release) [1.0.7] - 2025-07-22

## YANKED (incomplete release) [1.0.6] - 2025-07-22

## [1.0.5] - 2025-06-06
###
* Upgrade `hive` version from `2.3.7` to `2.3.9`.
* Hive Metrics can be sent now.

## [1.0.2] - 2024-11-08
### Added
* Support for consumer properties allowing connecting to Kafka cloud provider.

## [1.0.0] - 2023-04-27
### Changed
* Upgrade `Springboot` version from `2.3.3.RELEASE` to `2.7.10`.
* Upgrade `Springframework` version from `5.2.8.RELEASE` to `5.3.25`.
* Upgrade `Mockito` version from `2.25.1` to `3.12.4`.

## [0.0.3] - 2021-12-14
### Changed
* Updated log4j version to 2.16.0 because of zero day vulnerability.

## [0.0.2] - 2021-10-13
### Added
* Integration tests.

### Changed
* Updated `springframework.boot.version` to `2.3.3.RELEASE` (was `2.1.3.RELEASE`).
* Updated `springframework.version` to `5.2.8.RELEASE` (was `5.1.5.RELEASE`).

## [0.0.1] - 2020-08-03
### Added
* First Release.
