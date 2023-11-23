package cn.junbo;

import cn.junbo.task1.LoanSum;
import cn.junbo.task2.LoopTransfer;
import cn.junbo.task3.QuickTransfer;
import cn.junbo.task4.GuartaanteeSum;
import cn.junbo.utils.SortFileSink;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class TaskSubmit {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskSubmit.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/";
    public static final String SOURCE_FILE_PATH = "finBench/";
    public static final String SOURCE_DIR = "source.dir";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(new String[]{"{}"});
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(SOURCE_DIR, args[0]);
        envConfig.put(SortFileSink.OUTPUT_DIR, args[1]);
        envConfig.put(SortFileSink.TASK_ID, args[2]);
        switch (envConfig.getString(SortFileSink.TASK_ID)) {
            case "1": {
                IPipelineResult result = LoanSum.submit(environment);
                PipelineResultCollect.get(result);
                break;
            }
            case "2": {
                IPipelineResult result = LoopTransfer.submit(environment);
                PipelineResultCollect.get(result);
                break;
            }
            case "3": {
                IPipelineResult result = QuickTransfer.submit(environment);
                PipelineResultCollect.get(result);
                break;
            }
            case "4": {
                IPipelineResult result = GuartaanteeSum.submit(environment);
                PipelineResultCollect.get(result);
            }
        }
        environment.shutdown();
    }

}
