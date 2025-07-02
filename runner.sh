#!/bin/bash

if [ -z "$2" ]; then
    echo "Error: Provide src and trg properties filepath as arguments."
    echo "Usage: $0 <src_filepath> <trg_filepath>"
    exit 1
fi

dbzPath=$(dirname "$(realpath "$0")")
echo $dbzPath

# -Xmx128g
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$dbzPath/debezium-dataLoad-1.0.jar:$dbzPath/libs/*:$dbzPath/custom/*:" cdc.Runner "$1" "$2"
