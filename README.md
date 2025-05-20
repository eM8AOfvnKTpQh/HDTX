# HDTX

This work presents HDTX, a high-performance dtxn system that delivers fast dtxn services and supports flexible dtxn scheduling in the DM architecture. 
We first conduct an in-depth analysis of the characteristics of dtxns and one-sided RDMA primitives. Based on our analysis, we propose a fast commit protocol to commit dtxns within minimum network round trips and significantly reduce the latency of dtxns. We further propose an RDMA-enabled offloading mechanism to reduce data transfers across computing and memory nodes by carefully orchestrating different RDMA primitives. In addition, we propose a decentralized priority-based locking mechanism to schedule mission-critical dtxns, thereby significantly reducing the tail latency.

## Preview

Please deploy the ```HDTX_client``` on computing nodes and the ```HDTX_server``` on memory nodes.

## Dependencies

- Hardware
  - Mellanox InfiniBand NIC (e.g., ConnectX-3) that supports RDMA
  - Mellanox InfiniBand Switch
- Software
  - Operating System: Ubuntu 18.04 LTS or CentOS 7
  - Programming Language: C++ 11
  - Compiler: g++ 7.5.0 (at least)
  - OFED driver: MLNX_OFED_LINUX-4** (MLNX_OFED_LINUX-5** is incompatible due to API changes)
  - Libraries: ibverbs, pthread, boost_coroutine, boost_context, boost_system
- Machines
  - At least 3 machines, in which one acts as the computing pool and other two act as the memory pool to maintain a primary-backup replication



## Build

- Clone the Repository:

```sh
$ git clone https://github.com/eM8AOfvnKTpQh/HDTX.git
$ cd HDTX
```

- For each machine in the memory pool: 

```sh 
$ ./build.sh -s
```

- For each machine in the computing pool (boost is required):

```sh 
$ ./build.sh
```


- After running the ```build.sh``` script, cmake will automatically generate a ```build/``` directory in which all the compiled libraries and executable files are stored.
 1. `HDTX_server/build/memory_pool/server/mem_pool` using this file to load the memory pool
 2. `HDTX_client/build/compute_pool/run/run_macro`  using this file to run the macro-benchmarks
 3. `HDTX_client/build/compute_pool/run/run_micro`  using this file to run the micro-benchmark


## Preparations

To run our system, it is necessary to configure and compile the codes according to the experimental setup. For example, when configuring two computing node with ip ```10.192.168.1``` and ```10.192.168.2``` and two memory nodes with ip ```10.192.168.3``` and ```10.192.168.4```, respectively. 

The parameters in ```HDTX_client/config/compute_node_config.json``` should be configured as follow:
```sh 
"machine_num": 2,  #for two computing nodes, at least 1 computing node is required
"machine_id": 0,
"remote_ips": [
      "10.192.168.3",
      "10.192.168.4"
    ],
```

and 
```sh 
"machine_num": 2,  #for two computing nodes, at least 1 computing node is required
"machine_id": 1,
"remote_ips": [
      "10.192.168.3",
      "10.192.168.4"
    ],
```

The parameters in ```HDTX_server/config/memory_node_config.json``` on the memory nodes should be configured as follows respectively:
```sh 
"machine_num": 2,  #for two memory nodes, at least 2 memory nodes are required
"machine_id": 0,
"compute_node_ips": [
      "10.192.168.1",
      "10.192.168.2"
    ],
```    
and
```sh 
"machine_num": 2,  #for two memory nodes, at least 2 memory nodes are required
"machine_id": 1,
"compute_node_ips": [
      "10.192.168.1",
      "10.192.168.2"
    ],
```

When the ```use_pm``` in ```HDTX_server/config/memory_node_config.json``` is set to ```0```, memory nodes will build the datastore using DRAM.

To use PM on the memory node, the ```use_pm``` should be set to ```1``` and the ```pm_root``` is set to the path where PM is mounted, e.g., ```/dev/dax2.0```. To mount PM as the ```chardev```, please refer to https://stevescargall.com/blog/2019/07/how-to-extend-volatile-system-memory-ram-using-persistent-memory-on-linux/.

The above shows the case of 2 computing nodes and 2 memory nodes, the system can be scaled to a multi-server environment through similar parameter modifications.

## Performance Test 

HDTX implements several comparisons. To test them, some macro definitions in file `hdtx/core/flag.h` need to be specified. The following list shows the macro definitions and the corresponding comparisons.

| Comparisons   | Macro Definition  |
|  ----  | ----  |
| FCP-enabled HDTX  | #define USE_VALIDATE_COMMIT 1  |
| FCP-disabled HDTX  | #define USE_VALIDATE_COMMIT 0 |
|  ----  | ----  |
| offloading-enabled HDTX  | #define USE_CPU 1  |
| offloading-disabled HDTX  | #define USE_CPU 0 |
|  ----  | ----  |
| RDMA CAS-based locking mechanism  | #define USE_CAS 0  |
| RDMA FAA-based locking mechanism  | #define USE_CAS 1 // #define USE_PRIORITY 0 |
| Priority-Based Locking of HDTX  | #define USE_CAS 1 // #define USE_PRIORITY 1 |

To run different workloads, users need to modify the ```workload``` value in the ```HDTX/config/memory_node_config.json``` file on all nodes to the target workload (```TPCC```/```SmallBank```/```TATP```/```MICRO```).


## Run

For each machine in the memory pool: Start server to load tables and wait for the confirmation message indicating that the database has been successfully built.

```sh
$ cd HDTX
$ cd ./build/memory_pool/server
$ sudo ./mem_pool
```

For each machine in the computing pool: After loading database tables in the memory pool, we run a benchmark. 

- Regarding a macro benchmark:

```sh
$ cd HDTX
$ cd ./build/compute_pool/run
$ ./run_macro <Workload> <SystemName> <Threads> <Coroutines>
```
 Workload: ```tpcc```/```smallbank```/```tatp```
 SystemName: ```hdtx```/```farm```/```ford```
 Threads: any numbers greater than 0 (e.g. ```1```/```8```/```16```)
 Coroutines: any numbers greater than 1 (e.g. ```2```/```8```/```16```)

e.g. ```./run_macro smallbank hdtx 16 8``` to run HDTX under the SmallBank with 16 threads and each thread spawns 8 coroutines.

- Regarding a micro benchmark:

The parameters in ```HDTX_client/config/compute_node_config.json``` on the computing nodes should be configured as follows respectively:

```sh
"thread_num_per_machine": 8,  # the numbers of threads, any numbers greater than 0 (e.g. ```1```/```8```/```16```)
"coroutine_num": 8,   # the numbers of coroutines per thread, any numbers greater than 1 (e.g. ```2```/```8```/```16```)
"txn_system": 4,  # set to 4 to evaluate HDTX system
```

```sh
$ cd HDTX
$ cd ./build/compute_pool/run
$ ./run_micro <Distribution>-<WriteRates>
```
 Distribution: ```s``` and ```u``` for skew and uniform distributions, respectively.
 WriteRates: any number from 0 to 100 (e.g. ```0```/```50```/```100```)

e.g. ```./run_micro s-75``` to run HDTX with 75 percents write rates for the skewed access distribution under the micro-benchmark.

## Acknowledgments

This project is built upon the excellent RDMA communication framework from FORD [FAST'22]. We sincerely thank the authors for their groundbreaking work and for open-sourcing their implementation.

Reference: Ming Zhang, Yu Hua, Pengfei Zuo, and Lurong Liu. FORD: Fast One-sided RDMA-based Distributed Transactions for Disaggregated Persistent Memory. In Proceedings of the 20th USENIX Conference on File and Storage Technologies (FAST’22), pages 51–68, 2022.
