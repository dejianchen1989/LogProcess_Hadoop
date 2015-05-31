
### run hadoop

hdfs namenode -format
start-dfs.sh

### 
hdfs dfs -mkdir /input
hdfs dfs -put ./logs /input
hadoop jar logprocess.jar LogProcess /input/logs /output
