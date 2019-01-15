# GraphD: Fast Out-Of-Core Pregel-Like Graph Processing

GraphD is the right tool if you find that your graph runs out-of-memory in existing in-memory Pregel-like system on your cluster. As an illutration, on our 15-machine cluster, it takes merely 250 seconds in each superstep to compute PageRank on the ClueWeb graph with 978,408,098 nodes and 42,574,107,469 links.

GraphD adopts a novel distributed semi-streaming model, where each machine only holds its portion of the vertex states in main memory, and edges and messages are streamed on local disk efficiently.

Unlike other disk-based systems, we handle sparse data access (i.e., when only a few vertices participate in computation) to avoid scanning the whole graph in each superstep.

We use multithreading to perform computation (i.e., message generation) and communication (i.e. message transmission) in parallel, by caching unsent messages on local disks. For Gigabit Ethernet, GraphD is comparable to the in-memory Pregel+ system since the disk streaming cost is totally hidden in that of message transmission; while if the network is very fast, the performance is limited by the sequential disk-IO bandwidth.

When combiner is applicable, one may run our code in recoded mode, which performs both sender-side and receiver-side message combining in main memories, and thus avoids caching incoming messages to local disks and can often beat Pregel+ when Gigabit Ethernet is used.

## Project Website
[http://www.cse.cuhk.edu.hk/systems/graphd/](http://www.cse.cuhk.edu.hk/systems/graphd/index.html)

## License

Copyright 2018 Husky Data Lab, CUHK
