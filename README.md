# Drone Fly
A service which allows Hive metastore (HMS) `MetaStoreEventListener` implementations to be deployed in a separate context to the metastore's own.

# Overview
Drone Fly is a distributed Hive metastore events forwarder service that allows users to deploy metastore listeners outside the Hive metastore service.

With the advent of event driven systems, the number of listeners that a user needs to install in the metastore is ever increasing. These listeners can be both internal or can be provided by third party tools for integration purposes. More and more processing is being added to these listeners to address various business use cases.

Adding these listeners directly on the classpath of your Hive metastore couples them with it and can lead to performance degradation or in the worst case, it could take down the entire metastore (e.g. by running out memory, thread starvation etc.) Drone Fly decouples your HMS from the event listeners by providing a virtual Hive context. The event listeners can be provided on the Drone Fly's classpath and it then forwards the events received from [Kafka metastore Listener](https://github.com/ExpediaGroup/apiary-extensions/tree/master/apiary-metastore-events/kafka-metastore-events/kafka-metastore-listener) on to the respective listeners.

## System architecture

The diagram below shows a typical Hive metastore setup without using Drone Fly. In this example there are a number of HiveMetastoreListeners installed which send Hive events to other systems like Apache Atlas, AWS SNS, Apache Kafka and other custom implementations.

![Hive Metastore setup without Drone Fly.](drone-fly-before.png "Multiple Hive metastore listeners are deployed in HMS context.")

With Drone Fly, the setup gets modified as shown in the diagram below. The only listener installed in the Hive metastore context is the [Apiary Kafka Listener](https://github.com/ExpediaGroup/apiary-extensions/tree/master/apiary-metastore-events/kafka-metastore-events/kafka-metastore-listener). This forwards the Hive events on to Kafka from which Drone Fly can retrieve them. The other listeners are moved out into separate contexts and receive the messages from Drone Fly which forwards them on as if they were Hive events so the listener code doesn't need to change at all.

![Hive Metastore setup with Drone Fly.](drone-fly-after.png "Only one Hive metastore listener is deployed in HMS context and others are deployed in Drone Fly context")

Drone Fly can be set up to run in dockerized containers where each instance is initiated with one listener to get even further decoupling.

# Using with Docker

The Drone Fly image will be used as a base image by downstream projects which need a Hive Listener.

Drone Fly uses the Jib plugin which will build a docker image during the `package` phase. The image can also be built directly:

    mvn compile jib:dockerBuild -pl drone-fly-app
    
# Running DroneFly

	java -Dloader.path=lib/ -jar drone-fly-app-<version>-exec.jar \
		--apiary.bootstrap.servers=localhost:9092 \
		--apiary.kafka.topic.name=apiary \
		--apiary.listener.list="com.expediagroup.sampleListener1,com.expediagroup.sampleListener2"
	
# Running DroneFly Docker image

	docker run --env APIARY_BOOTSTRAP_SERVERS="localhost:9092" \
			   --env APIARY_LISTENER_LIST="com.expediagroup.sampleListener1,com.expediagroup.sampleListener2" \
			   --env APIARY_KAFKA_TOPIC_NAME="dronefly" \
			   expediagroup/drone-fly-app:<image-version>
	
The properties `instance.name`, `apiary.bootstrap.servers`, `apiary.kafka.topic.name` and `apiary.listener.list` can also be provided in the spring properties file.
	
	java -Dloader.path=lib/ -jar drone-fly-app-<version>-exec.jar --spring.config.location=file:///dronefly.properties

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2020 Expedia, Inc.


