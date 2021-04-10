package model;

/**
 * PairOfDoubleBoolean is an object whose key is Double and value is Boolean.
 * This is used in All pair shortest path for in mapper combining
 */
public class PairOfDoubleBoolean {
    private double key;
    private boolean value;

    public PairOfDoubleBoolean(double key, boolean value) {
        this.key = key;
        this.value = value;
    }

    public double getKey() {
        return key;
    }

    public void setKey(double key) {
        this.key = key;
    }

    public boolean isValue() {
        return value;
    }

    public void setValue(boolean value) {
        this.value = value;
    }
}
