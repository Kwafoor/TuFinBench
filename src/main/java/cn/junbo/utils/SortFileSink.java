package cn.junbo.utils;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.ResultValidator;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;

public class SortFileSink<OUT extends Comparable<? super OUT>> extends RichFunction implements SinkFunction<OUT> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SortFileSink.class);
    public static final String OUTPUT_DIR = "output.dir";
    public static final String TASK_ID = "task.id";
    public static final String FILE_OUTPUT_APPEND_ENABLE = "file.append.enable";
    private File file;
    private ArrayList<Comparable> list = new ArrayList<>();

    public SortFileSink() {
    }

    public void write(OUT out) throws Exception {
        list.add(out);
    }

    public void open(RuntimeContext runtimeContext) {
        String filePath = String.format("%sresult%s.csv",
                runtimeContext.getConfiguration().getString(OUTPUT_DIR),
                runtimeContext.getConfiguration().getString(TASK_ID));
        ResultValidator.cleanResult(filePath);
        LOGGER.info("sink file name {}", filePath);
        boolean append = runtimeContext.getConfiguration().getBoolean(new ConfigKey("file.append.enable", true));
        this.file = new File(filePath);

        try {
            if (!append && this.file.exists()) {
                try {
                    FileUtils.forceDelete(this.file);
                } catch (Exception var5) {
                }
            }

            if (!this.file.exists()) {
                if (!this.file.getParentFile().exists()) {
                    this.file.getParentFile().mkdirs();
                }

                this.file.createNewFile();
            }

        } catch (IOException ex) {
            throw new GeaflowRuntimeException(ex);
        }

    }

    public void close() {
        Collections.sort(list);
        try {
            FileUtils.write(this.file, "id|value" + "\n", Charset.defaultCharset(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        list.forEach(s -> {
            try {
                FileUtils.write(this.file, s + "\n", Charset.defaultCharset(), true);
            } catch (IOException var3) {
                throw new RuntimeException(var3);
            }
        });
    }
}
