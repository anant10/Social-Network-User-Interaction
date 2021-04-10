package model;

/**
 * Pair represent a pair of nodeId and its weight.
 */
public class Pair {
    private final String nodeId;
    private final Double edgeWeight;

    public Pair(String nodeId, Double edgeWeight) {
        this.nodeId = nodeId;
        this.edgeWeight = edgeWeight;
    }

    public Pair(Pair pair) {
        nodeId = pair.nodeId;
        edgeWeight = pair.edgeWeight;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Double getEdgeWeight() {
        return edgeWeight;
    }
}
