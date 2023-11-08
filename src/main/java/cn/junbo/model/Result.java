package cn.junbo.model;

import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;

public class Result implements Comparable<Result> {

    public BigInteger id;
    public Number value;

    public Result(BigInteger id, Double loan) {
        this.id = id;
        this.value = loan;
    }

    public Result(BigInteger id, Long loan) {
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
