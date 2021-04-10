package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * model.NoNodeSSSP represents a node in a graph. model.NodeSSSP has a nodeId, distance which is the minDist of the node from src node,
 * adjacencyList which is a Pair of neighbouring nodeId and its edgeWeight, and booleans isActive and isVertex.
 * This is an intermediate value object for Single source shortest path.
 */
public class NodeSSSP implements Writable {
    private Double distance;
    private List<Pair> adjacencyList = new ArrayList<>();
    private String nodeId;
    private boolean isVertex;
    private boolean isActive;

    private NodeSSSP() {

    }

    public NodeSSSP(String nodeId, Double distance) {
        this.nodeId = nodeId;
        this.distance = distance;
    }

    public NodeSSSP(NodeSSSP p) {
        nodeId = p.nodeId;
        distance = p.distance;
        isActive = p.isActive;
        isVertex = p.isVertex;
        adjacencyList = createNewAdj(p.adjacencyList);
    }

    private List<Pair> createNewAdj(List<Pair> adjacencyList) {
        List<Pair> newList = new ArrayList<>();
        for (Pair p : adjacencyList) {
            newList.add(new Pair(p));
        }
        return newList;
    }

    public Double getDistance() {
        return distance;
    }

    public List<Pair> getAdjacencyList() {
        return adjacencyList;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isVertex() {
        return isVertex;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public static NodeSSSP createNode(String[] split) {
        NodeSSSP node = new NodeSSSP();
        node.nodeId = split[0];
        node.distance = Double.parseDouble(split[1]);
        node.isVertex = Boolean.parseBoolean(split[2]);
        node.isActive = Boolean.parseBoolean(split[3]);
        if (split.length > 4) {
            String[] adj = split[4].split(",");
            for (String neighbor : adj) {
                String[] s = neighbor.split(":");
                Pair pair = new Pair(s[0], Double.parseDouble(s[1]));
                node.adjacencyList.add(pair);
            }
        }
        return node;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(nodeId.trim());
        out.writeDouble(distance);
        out.writeBoolean(isVertex);
        out.writeBoolean(isActive);
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
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        nodeId = in.readUTF().trim();
        distance = in.readDouble();
        isVertex = in.readBoolean();
        isActive = in.readBoolean();
        int byteArrSize = in.readInt();
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
    }

}

