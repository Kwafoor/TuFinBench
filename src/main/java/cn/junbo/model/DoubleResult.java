package cn.junbo.model;

import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;

public class DoubleResult implements Comparable<DoubleResult> {

    public BigInteger id;
    public Double value;

    public DoubleResult(BigInteger id, Double loan) {
        this.id = id;
        this.value = loan;
    }

    @Override
    public String toString() {
        return String.format("%s|%.2f", id, value);
    }

    @Override
    public int compareTo(@NotNull DoubleResult o) {
        return this.id.compareTo(o.id);
    }
}
