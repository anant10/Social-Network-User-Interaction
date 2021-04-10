# Social Network User Interaction - Hadoop MapReduce

## Project Overview:
1. Problem Statement
We as a team want to verify that given any user “A”, how fast can he/she communicate with other users in a given social network.
“A” can directly communicate with another user “B” or via other user(s). The vital aspect of this situation is - which interaction happens the fastest/quickest, i.e., which interaction takes the shortest time of completion (assuming that each interaction happening in this social network is of equal length, for instance: we can assume that every user interacting with one another passes over information of comparable size). This is implemented using Single Source Shortest Path Algorithm.
We further want to learn the Circle of all reachable nodes from a given node and fastest reachable nodes from that given node i.e., for every node “k” in a graph, we can get the circle of nodes influenced by node k using any threshold. To solve problems like this, we are trying to implement All Pairs Shortest Path Algorithm.

2. Goal
Our goal is to learn and implement Dijkstra’s algorithm (in a way such that it is suitable for parallel processing of data): implying that we have used Breadth First Search (BFS) for computing Single source shortest path in a given social network and implement map reduce algorithm to find All-pairs shortest path.

Fall 2020

Code author
-----------
1. Anant Vasant Shanbhag
2. Archita Sundaray
3. Pooja Krishnanath Shanbhag

Installation
------------
These components are installed:
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/home/joe/tools/hadoop/hadoop-2.9.1
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination
