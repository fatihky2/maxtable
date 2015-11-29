## Defining Machine Configuration for MaxTable ##
Maxtable can use the local system or Kosmos file system (KFS) as its back-end storage engine. If using the KFS as the back-end storage, you need to build the library for the KFS and link it into the binary (startMaster and startRanger).

  * Build library including the client API of DFS
```
Please check the Wiki HowToBuildWithKFSFacer
```

  * Configure for the Master server in the file config/master.conf
```
# Configuration for meta server: 
# Port to open for client connections
port=1959
# KFS meta server location
kfsserver=192.168.0.100
kfsport=20000
```
  * Configure for the Ranger server in the file config/region.conf
```
# Configuration for range server: 
# Meta server location
metaserver=192.168.0.160
metaport=1959
# Ranger address
rangerserver=192.168.180
# Port to open for client connections
rangerport=1949
# Port to transfer the Bigdata to the client
bigdataport=1969
# KFS meta server location
kfsserver=192.168.0.100
kfsport=20000
```

  * Configure for the client in the file config/cli.conf
```
# Configuration for the client: 
# Meta server location
metaserver=192.168.0.160
metaport=1959
```