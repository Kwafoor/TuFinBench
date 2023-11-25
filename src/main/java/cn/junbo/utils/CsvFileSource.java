package cn.junbo.utils;

import com.antgroup.geaflow.example.function.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class CsvFileSource<OUT> extends FileSource<OUT> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvFileSource.class);
    public static final String SOURCE_DIR = "source.dir";


    public CsvFileSource(String filePath, FileLineParser<OUT> parser) {
        super(filePath, parser);
    }

    @Override
    public void init(int parallel, int index) {
        this.records = this.readFileLines(this.filePath);
        if (parallel != 1) {
            List<OUT> allRecords = this.records;
            this.records = new ArrayList();

            for (int i = 0; i < allRecords.size(); ++i) {
                if (i % parallel == index) {
                    this.records.add(allRecords.get(i));
                }
            }
        }

    }

    private List<OUT> readFileLines(String filePath) {
        String dataPath = runtimeContext.getConfiguration().getString(SOURCE_DIR);
        Path file = Paths.get(dataPath + filePath);
        try (Stream<String> lines = Files.lines(file)) {
            List<OUT> result = new ArrayList<>();
            boolean[] isFirstLine = {true}; // 使用数组以便在 lambda 表达式中修改此变量
            lines.forEach(line -> {
                if (isFirstLine[0]) {
                    // 打印schema信息，并将isFirstLine设为false
                    LOGGER.info("CSV Schema: " + line);
                    isFirstLine[0] = false;
                } else {
                    if (!line.trim().isEmpty()) {
                        Collection<OUT> collection = parser.parse(line);
                        result.addAll(collection);
                    }
                }
            });

            return result;
        } catch (IOException ex) {
            throw new RuntimeException("error in read resource file: " + filePath, ex);
        }
    }


}
