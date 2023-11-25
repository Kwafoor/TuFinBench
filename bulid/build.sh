cd ../
mvn package assembly:single
cd ./build
cp ../target/TuFinBench-1.0-SNAPSHOT-jar-with-dependencies.jar ./
zip wang_cn.zip TuFinBench-1.0-SNAPSHOT-jar-with-dependencies.jar run.sh