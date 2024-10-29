# Data Commons Brasil

## How get data from downloading to importing step by step

### Downloading data from given source:

1. Within the `downloader` directory, create a new module and name it according to the chosen source;

2. In the same folder, include the new source in the `downloaders.py` file within the `pick_downloader` function (you don't need to modify anything else there);

3. Finally, to download the data via docker, use the command bellow:

    ``` bash
    $ ./dockercmd.sh download <source> 
    ```
  
The command above will create a container, execute the downloader and the data will be saved at `.output/downloader/<source>/<time-stamp>/`. When this process is done, the container will be automatically stoped and removed from Docker. 

### Processing data from given source:

1. Within the `processor` directory, create a new module and name it according to the chosen source (the same name used for the downloader);

2. In the same folder, include the new source in the `processor.py` file within the `process_source` function; 

3. Finally, to process the data via docker, use the command bellow:

    ``` bash
    $ ./dockercmd.sh process <source>
    ``` 

The command above will process data (from a given source) with the latest timestamp by default. Resulting data will can be found in the path `./output/process/<source>`. 

_If data from the same source is processed more than once, older results will be replaced._

### Creating files to be imported from given source:

1. Simply execute the command bellow:

    ``` bash
    $ ./dockercmd.sh import 
    ```

The command above will create a database formatted specifically to be imported into the data commons environment.

### Example
Using IPEA source:

``` bash
$ ./dockercmd.sh download ipea 

$ ./dockercmd.sh process ipea

$ ./dockercmd.sh import
``` 
The commands listed above download, process and format data from IPEA (assuming that the modules included in the source code work properly). 
