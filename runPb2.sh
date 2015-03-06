mvn package
hadoop fs -rmr /user/hadoop/hw1/output_tmp
hadoop fs -rmr /user/hadoop/hw1/output
hadoop jar target/hadoop-student-homework-1.0.0-SNAPSHOT.jar homework2part2 /user/hadoop/hw1/input /user/hadoop/hw1/output
hadoop fs -cat /user/hadoop/hw1/output/part-r-00000
