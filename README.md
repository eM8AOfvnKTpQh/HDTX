# HDTX


## Preview

Please deploy the ```hdtx_client``` on compute nodes and the ```hdtx_server``` on memory nodes.

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
  - At least 3 machines, in which one acts as the compute pool and other two act as the memory pool to maintain a primary-backup replication



## Build

- Clone the Repository:

```sh
$ git clone https://github.com/eM8AOfvnKTpQh/HDTX.git
$ cd hdtx
```

- For each machine in the memory pool: 

```sh 
$ ./build.sh -s
```

- For each machine in the compute pool (boost is required):

```sh 
$ ./build.sh
```

After running the ```build.sh``` script, cmake will automatically generate a ```build/``` directory in which all the compiled libraries and executable files are stored.

## Preparations

To run our system, it is necessary to configure and compile the codes according to the experimental setup. For example, when configuring a compute node with ip 10.192.168.119 and two memory nodes with ip 10.192.168.120 and 10.192.168.122, respectively, the parameters in ```HDTX_client/config/compute_node_config.json``` should be configured as follow:
```sh 
"machine_num": 1,
"machine_id": 0,
"remote_ips": [
      "10.192.168.120".
      "10.192.168.122"
    ],
```

The parameters in ```HDTX_server/config/memory_node_config.json``` on the memory nodes should be configured as follows respectively:
```sh 
"machine_num": 2,
"machine_id": 0,
"compute_node_ips": [
      "10.192.168.119"
    ],
```    
and
```sh 
"machine_num": 2,
"machine_id": 1,
"compute_node_ips": [
      "10.192.168.119"
    ],
```
To use PM on the memory node, the ```use_pm``` in ```HDTX_server/config/memory_node_config.json``` should be set to ```1``` and the ```pm_root``` is set to the path where PM is mounted, e.g., ```/dev/dax2.0```. To mount PM as chardev, please refer to https://stevescargall.com/blog/2019/07/how-to-extend-volatile-system-memory-ram-using-persistent-memory-on-linux/.


## Run

- For each machine in the memory pool: Start server to load tables.

```sh
$ cd hdtx
$ cd ./build/memory_pool/server
$ sudo ./mem_pool
```

- For each machine in the compute pool: After loading database tables in the memory pool, we run a benchmark, e.g., SmallBank.

```sh
$ cd hdtx
$ cd ./build/compute_pool/run
$ ./run smallbank hdtx 16 8 # run hdtx with 16 threads and each thread spawns 8 coroutines
```

## Acknowledgments

This project is built upon the excellent RDMA communication framework from FORD [FAST'22]. We sincerely thank the authors for their groundbreaking work and for open-sourcing their implementation.

Reference: Ming Zhang, Yu Hua, Pengfei Zuo, and Lurong Liu. FORD: Fast One-sided RDMA-based Distributed Transactions for Disaggregated Persistent Memory. In Proceedings of the 20th USENIX Conference on File and Storage Technologies (FAST’22), pages 51–68, 2022.
