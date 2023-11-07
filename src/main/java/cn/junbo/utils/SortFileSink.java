package cn.junbo.utils;

import com.antgroup.geaflow.example.function.FileSink;

import java.util.ArrayList;
import java.util.Collections;

public class SortFileSink<OUT extends Comparable<? super OUT>> extends FileSink<OUT> {
    private ArrayList<Comparable> list = new ArrayList<>();

    @Override
    public void write(OUT out) throws Exception {
        list.add(out);
    }

    @Override
    public void close() {
        Collections.sort(list);
        list.forEach(s-> {
            try {
                super.write((OUT) s);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        super.close();
    }
}
