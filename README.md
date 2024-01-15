# Data Commons Brasil
## How to run code in uranus
Build docker image:

``` bash
$ ./dockercmd.sh build 
``` 

Run docker container for a specific file:

``` bash
$ ./dockercmd.sh run <file_path>
``` 

Copy contents from `downloads` (inside the container) to host:

``` bash
$ ./dockercmd.sh copy <container_name> 
``` 

Remove container:

``` bash
$ ./dockercmd.sh remove <container_name> 
``` 

### Example
Using IPEA downloader:

``` bash
$ ./dockercmd.sh build 

$ ./dockercmd.sh run downloaders/ipea/ipea.py

$ ./dockercmd.sh copy data_commons-ipea

$ ./dockercmd.sh remove data_commons-ipea
``` 
The commands listed above download files from IPEA e save them identified by the name of the source and a unix timestamp in the host's `downloads` folder, in this case `downloads/ipea/1704910194/`.
