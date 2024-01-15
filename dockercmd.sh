#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 [build|run|copy|remove] [path|container_name]"
    exit 1
fi

case $1 in
    build)
        docker build -t data_commons .
        ;;
    run)
        if [ "$#" -ne 2 ]; then
            echo "To use 'run', specify the script's path: $0 run [script_path]"
            exit 1
        fi
        script_path=$2
        script_name=$(basename -- "$script_path")
        script_name="${script_name%.*}"
        container_name="data_commons-$script_name"
        docker run --name $container_name -it data_commons $script_path
        ;;
    copy)
        if [ "$#" -ne 2 ]; then
            echo "To use 'copy', specify the container name: $0 copy [container_name]"
            exit 1
        fi
        container_name=$2

        script_name=${container_name#data_commons-}

        timestamp=$(date +%s)
        destination="./downloads/${script_name}/${timestamp}/"
        mkdir -p "$destination"

        docker cp ${container_name}:/app/downloads/. "$destination"
        ;;
    remove)
        if [ "$#" -ne 2 ]; then
            echo "To use 'remove', specify the container name: $0 remove [container_name]"
            exit 1
        fi
        container_name=$2
        echo "Stopping and removing container '${container_name}'..."
        docker stop $container_name
        docker rm $container_name
        ;;
    *)
        echo "Invalid argument: $1. Use 'build', 'run', 'copy', or 'remove'."
        exit 1
        ;;
esac
