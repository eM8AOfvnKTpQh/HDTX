# HDTX


## Dependencies

- Hardware
  - Mellanox InfiniBand NIC (e.g., ConnectX-3) that supports RDMA
  - Mellanox InfiniBand Switch
- Software
  - Operating System: Ubuntu 18.04 LTS or CentOS 7
  - Programming Language: C++ 11
  - Compiler: g++ 7.5.0 (at least)
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

