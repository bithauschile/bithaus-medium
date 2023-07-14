#!/bin/sh
# 
# Bithaus Medium - Topic Replicator App
# Jul 2023
# jmakuc@bithaus.cl
#

# TOPIC REPLICATOR APP
# Use this app to manually messages from one topic to another
# Params: sourceTopic targetTopic partition initialOffset [endOffset]


# JVM options
CONF=conf-example/replicator

SOURCE_MSGSVC_CONFIG_FILE=$CONF/source-service-kafka.properties
TARGET_MSGSVC_CONFIG_FILE=$CONF/target-service-kafka.properties

JVM_OPT=-Djava.util.logging.config.file=conf-example/logger_unitTests.properties

JAR=target/bithaus-medium-1.2.10-jar-with-dependencies.jar
MAIN=cl.bithaus.medium.utils.TopicReplicator

echo ""
echo "BITHAUS MEDIUM  (c) 2022"
echo "Topic Replicator                                  Bithaus Software Chile"
echo "========================================================================"
echo ""

# check if parameters 1, 2 and 3 are present
if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]  
then
    echo "Usage: $0 <sourceTopic> <targetTopic> <partition> <initialOffset> [endOffset]"
    exit 1
fi


echo "java $JVM_OPT -cp $JAR $MAIN $SOURCE_MSGSVC_CONFIG_FILE $TARGET_MSGSVC_CONFIG_FILE $1 $2 $3 $4"
java $JVM_OPT -cp $JAR $MAIN $SOURCE_MSGSVC_CONFIG_FILE $TARGET_MSGSVC_CONFIG_FILE $1 $2 $3 $4