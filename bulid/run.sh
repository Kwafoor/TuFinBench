#!/bin/bash

# 获取数据集目录和结果目录作为输入参数
DATASET_DIR=$1
RESULT_DIR=$2

# 检查参数
if [ -z "${DATASET_DIR}" ]; then
   echo "Please specify the dataset directory."
   exit 1
fi

if [ -z "${RESULT_DIR}" ]; then
   echo "Please specify the result directory."
   exit 1
fi

# 运行java程序并将其置于后台
java -cp TuFinBench-1.0-SNAPSHOT-jar-with-dependencies.jar cn.junbo.TaskSubmit ${DATASET_DIR} ${RESULT_DIR} 1 &
java -cp TuFinBench-1.0-SNAPSHOT-jar-with-dependencies.jar cn.junbo.TaskSubmit ${DATASET_DIR} ${RESULT_DIR} 2 &
java -cp TuFinBench-1.0-SNAPSHOT-jar-with-dependencies.jar cn.junbo.TaskSubmit ${DATASET_DIR} ${RESULT_DIR} 3 &
java -cp TuFinBench-1.0-SNAPSHOT-jar-with-dependencies.jar cn.junbo.TaskSubmit ${DATASET_DIR} ${RESULT_DIR} 4 &

# 等待所有后台任务完成
wait