package cn.junbo;

import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;

public class Result implements Comparable<Result> {

    public BigInteger id;
    public Double value;

    public Result(BigInteger id, Double loan) {
        this.id = id;
        this.value = loan;
    }

    @Override
    public String toString() {
        return String.format("%s,%s", id, value);
    }

    @Override
    public int compareTo(@NotNull Result o) {
        return this.id.compareTo(o.id);
    }
}
