if [[ ! -z $DRONEFLY_CONFIG ]]; then
  echo "$DRONEFLY_CONFIG"|base64 -d > ./conf/dronefly-config.yml
  java -Dloader.path=/app/lib/ -jar /app/libs/drone-fly-app-$APP_VERSION-exec.jar --config=./conf/dronefly-config.yml
else
  java -Dloader.path=/app/lib/ -jar /app/libs/drone-fly-app-$APP_VERSION-exec.jar --apiary.bootstrapservers=$KAFKA_BOOTSTRAP_SERVERS --apiary.kafka.topicname=$KAFKA_TOPIC_NAME
fi