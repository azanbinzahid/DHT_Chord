Spring 2019

### Distributed Hash Table based File Sharing System

A fundamental problem that confronts peer-to-peer applications is to efficiently locate the node that stores a particular data item. To solve this problem, Chord a distributed lookup protocol can be used. Chord provides support for just one operation: given a key, it maps the key onto a node. Data location can be easily implemented on top of Chord by associating a key with each data item, and storing the key/data item pair at the node to which the key maps. Chord adapts efficiently as nodes join and leave the system, and can answer queries even if the system is continuously changing. Behind this simple interface, Chord distributes objects over a dynamic network of nodes, and implements a protocol for finding these objects once they have been placed in the network. Every node in this network is a server capable of looking up keys for client applications, but also participates as key store. Hence, Chord is a decentralized system in which no particular node is necessarily a performance bottleneck or a single point of failure (if the system is implemented to be fault-tolerant). 

**Keys**
Every key (based on the name of a file) inserted into the DHT must be hashed to fit into the keyspace supported by the particular implementation of Chord. The hashed value of the key will take the form of an m bit unsigned integer. Thus, the keyspace (the range of possible hashes) for the DHT resides between 0 and 2^m-1 inclusive. 

**The Ring** 
Just as each key that is inserted into the DHT has hash value, each node in the system also has a hash value in the keyspace of the DHT. To get this hash value, we could simply give each node a distinct name (or use the combination of IP and port) and then take the hash of the name, using the same hashing algorithm we use to hash keys inserted into the DHT. Once each node has a hash value, we are able to give the nodes an ordering based on those hashes. Chord orders the node in a circular fashion, in which each node's successor is the node with the next highest hash. The node with the largest hash, however, has the node with the smallest hash as its successor. It is easy to imagine the nodes placed in a ring, where each node's successor is the node after it when following a clockwise rotation. 

**The Overlay**
As the paper in which Chord was introduced states, it would be possible to look up any particular key by sending an iterative request around the ring. Each node would determine whether its successor is the owner of the key, and if so perform the request at its successor. Otherwise, the node asks its successor to find the successor of the key and the same process is repeated. Unfortunately, this method of finding successors would be incredibly slow on a large network of nodes. For this reason, Chord employs a clever overlay network that, when the topology of the network is stable, routes requests to the successor of a particular key in log(n) time, where n is the number of nodes in the network. 
To locate the node at which a particular key-value pair is stored, one need only find the successor to the hash value of the key. 
This optimized search for successors is made possible by maintaining a finger table at each node. The number of entries in the finger table is equal to m, where m is the number of bits representing a hash in the keyspace of the DHT. Entry i in the table, with 0 <= i < m, is the node which the owner of the table believes is the successor for the hash h+2^i (h is the current node's hash). When node A services a request to find the successor of the key k, it first determines whether its own successor is the owner of k (the successor is simply entry 0 in the finger table). If it is, then A returns its successor in response to the request. Otherwise, node A finds node B in its finger table such that B has the largest hash smaller than the hash k, and forwards the request to B.

# Assignment 3: DC++

## Introduction

You must be aware of the movie sharing application widely used in LUMS known as DC++. In this assignment, you will be required to design something similar using a DHT Chord.

## Bootstrap (30)

Start off with only one node with its finger table containing references to itself. Whenever another node wants to join the ring, it asks any existing node to find the successor of the hash of the joining node. The states updates as illustrated in the following diagram.

## Leaving (15)

In case a node wants to leave the chord, it should properly inform its predecessor and successor, and transfer all the files to its successor before going offline.

## Failure Resilience (15)

Every node should periodically contact its successor to know if it’s still online, and in case of three non- replies assume that it’s no longer online and update its successor. This is achieved by maintaining successor lists, rather than a single successor so that the failure of a few nodes is not enough to send the system into disrepair.

## Downloading (20)

Client should be able to connect with any node and enquire about a file by its name. The node will compute the key of the file. If the node does not have the file itself it will contact its successor or contacting the node in its finger table that has the largest hash smaller than the key, and forwards the request to it. Make sure that you your implementation supports resuming download in case of failure of with of the receiver or sender.(more details in Overlay)

## Storing (20)

Storing is done similarly to downloading. Given a file a key is computed based on the file name and the request is either kept locally or is forwarded to the node with the largest hash smaller than the key. You have to make sure that the data is replicated at more than one nodes so in case of a node failure the data is still available to users.

## Bonus(10)

Make a Graphical User Interface for this application. Good design and complete functionality have equal weightage in this part.

**Submission:**

1. The deadline for this assignment is 21st April 2019.
2. This assignment needs to be completed individually.
3. Make a zip file called “*rollnumber*_assignment_3.zip” containing only your “node.py” code files.

**Note:**
1. Design the assignment structure thoroughly on paper before starting the code.
2. START ON TIME
3. Divide the assignment into parts and test each part to make sure it is working correctly before moving on to the next part.


