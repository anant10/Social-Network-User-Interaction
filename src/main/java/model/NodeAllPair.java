package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * model.NodeAllPair represents a node in a graph. model.NodeAllPair has a nodeId,
 * adjacencyList which is a Pair of neighbouring nodeId and its edgeWeight, and boolean isVertex.
 * This is an intermediate value object for All pair shortest path
 */
public class NodeAllPair implements Writable {
    private List<Pair> adjacencyList = new ArrayList<>();
    private String nodeId;
    private boolean isVertex;


    private NodeAllPair() {

    }

    public NodeAllPair(String nodeId, Pair adjacentNode) {
        this.nodeId = nodeId;
        adjacencyList.add(adjacentNode);
        isVertex = false;
    }

    public NodeAllPair(String nodeId, Pair adjacentNode, boolean isVertex) {
        this.nodeId = nodeId;
        adjacencyList.add(adjacentNode);
        this.isVertex = isVertex;
    }

    public boolean isVertex() {
        return isVertex;
    }

    public List<Pair> getAdjacencyList() {
        return adjacencyList;
    }

    public String getNodeId() {
        return nodeId;
    }


    public static NodeAllPair createNode(String[] split) {
        NodeAllPair node = new NodeAllPair();
        node.nodeId = split[0];
        if (split.length > 1) {
            String[] adj = split[1].split(",");
            for (String neighbor : adj) {
                String[] s = neighbor.split(":");
                Pair pair = new Pair(s[0], Double.parseDouble(s[1]));
                node.adjacencyList.add(pair);
            }
        }
        node.isVertex = Boolean.parseBoolean(split[2]);
        return node;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(nodeId.trim());
        StringBuilder accumulate = new StringBuilder();
        for (Pair neighbor : adjacencyList)
            accumulate.append(neighbor.getNodeId()).append(":").append(neighbor.getEdgeWeight()).append(",");
        if (accumulate.length() > 0) {
            String adj = accumulate.substring(0, accumulate.length() - 1);
            byte[] b = adj.getBytes();
            out.writeInt(b.length);
            out.write(b);
        } else {
            out.writeInt(0);
        }
        out.writeBoolean(isVertex);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        nodeId = in.readUTF().trim();
        int byteArrSize = in.readInt();
        adjacencyList = new ArrayList<>();
        if (byteArrSize > 0) {
            byte[] content = new byte[byteArrSize];
            in.readFully(content);
            String adjList = new String(content);
            if (adjList.length() == 0) {
                return;
            }
            String[] adj = adjList.split(",");
            for (String neighbor : adj) {
                String[] split = neighbor.split(":");
                Pair pair = new Pair(split[0], Double.parseDouble(split[1]));
                adjacencyList.add(pair);
            }
        }
        isVertex = in.readBoolean();
    }

    @Override
    public String toString() {
        StringBuilder adj = new StringBuilder();
        for (Pair p : adjacencyList) {
            adj.append(p.getNodeId()).append(":").append(p.getEdgeWeight()).append(",");
        }
        return nodeId + " " + isVertex + " " + adj.substring(0, adj.length() - 1);
    }
}

