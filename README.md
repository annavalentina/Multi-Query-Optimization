# Multi-Query-Optimization
Placement of analytic tasks from multiple queries, to heterogeneous geo-distributed edge devices targeting three objectives, namely latency, quality-of-results, and resource utilization. 

## Storm Deployment

### Prerequisites
For the deployment of Apache Storm to a cluster, we use the swarm mode of Docker. Our cluster consisted of 6 nodes, namely
 - anaconda (Leader)
 - panther
 - deer
 - unicorn 
 - pegasus
 - cerberos
 
For each node ssh access is required in order to customize the latency and bandwidth between pair of nodes.

### Storm setup
1. Create a swarm mode, set leader and rest nodes.
2. Create a docker network
```shell
docker network create --driver overlay --attachable stormnet
```
3. Place the files of the stack folder to each node.
  - each supervisor.scheduler.meta.group-id field in supervisor.yaml must be unique (e.g. anaconda => 1, deer => 2, etc).
4. Modify the stack/docker-compose.yml file to your needs 
  - in our case, we set the swarm Leader, i.e. anaconda to also run the zookeeper, nimbus, ui services).
  - modify the directory path in the volumes section (/home/valentina) to point to the directory containing the files from step 3.
5. Install tcconfig (https://github.com/thombashi/tcconfig) to each node of the cluster.

### Deploy and run experiments
1. Place the scripts folder to a machine that has ssh access to each node of the cluster.
2. Modify incl.sh accordingly.
3. Run deploy.sh (sets latency and bandwidth between each pair of cluster nodes, starts Apache Storm and runs experiments).
4. The results will be located in the storm_metrics folder.


