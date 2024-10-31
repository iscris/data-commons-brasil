!/bin/bash

set -e

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 [download|process|import] <source>"
    exit 1
fi

output_dir=$PWD/output
downloader_output=$output_dir/downloader
processor_output=$output_dir/processor
importer_output=$output_dir/importer

case $1 in
    download)
        flags="-d --rm"
        container_name=downloader_$2
        volumes="-v $downloader_output:/app/output/downloader"
        image=ghcr.io/iscris/datatools:latest
        command="download --source $2"
        docker run $flags --name $container_name $volumes $image $command
        ;;
    process)
        source_output=$downloader_output/$2
        last_download=$(ls -td $source_output/* | tail -1)

        flags="-d --rm"
        container_name=downloader_$2
        volumes="-v $last_download:/app/output/downloader/$2 -v $PWD/output/processor:/app/output/processor"
        image=ghcr.io/iscris/datatools:latest
        command="process --source $2 --input=/app/output/downloader/$2"
        docker run $flags --name $container_name $volumes $image $command
        ;;
    import)
        flags="-d --rm"
        container_name=importer_$2
        volumes="-v $processor_output/$2:/app/output/processor/$2 -v $importer_output:/app/output/importer"
        image=ghcr.io/iscris/dc-brasil-importer:latest
        env_vars="-e GOOGLE_APPLICATION_CREDENTIALS=/gcp/creds.json -e INPUT_DIR=/app/output/processor/$2 -e OUTPUT_DIR=/app/output/importer"
        env_file="--env-file .dc.env"
        docker run $flags --name $container_name $volumes $env_vars $env_file $image
        ;;
    *)
        echo "Invalid argument: $1. Use 'download', 'process', or 'import'."
        exit 1
        ;;
esac
