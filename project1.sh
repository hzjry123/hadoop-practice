#generate data
#hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.5.jar randomtextwriter -Dmapreduce.randomtextwriter.totalbytes=10240 /data/ngram/test_big
hadoop fs -rm -r /output/ngram/*
hadoop fs -rm -r /output/loganalyze/*
hadoop fs -rm -r /data/ngram/test_small/*

#run ngram 2 define means ngram,default is 2
#q3
#create test data

export TEST_PATH=~/project1/test_data_1
echo "test ngram" > $TEST_PATH
hadoop fs -put $TEST_PATH /data/ngram/test_small/

echo "********************************"
echo "****************p3**************"
echo "********************************"
hadoop jar ~/project1/project1.jar com.project1.ngram 3 /data/ngram/test_small /output/ngram/test
hadoop jar ~/project1/project1.jar com.project1.ngram 3 /data/ngram/test_big/part-m-00000 /output/ngram/big_data


echo "********************************"
echo "****************p4**************"
echo "********************************"
#q4 1,2
echo "********************************"
echo "****************q1 2**************"
echo "********************************"
hadoop jar ~/project1/project1.jar com.project1.Analyzelog1 /data/loganalyze/access_log /output/loganalyze/q2_1
hadoop jar ~/project1/project1.jar com.project1.Analyzelog2 /data/loganalyze/access_log /output/loganalyze/q2_2
#q4 3
#map doc
echo "********************************"
echo "****************q3**************"
echo "********************************"
hadoop jar ~/project1/project1.jar com.project1.MapDoc /data/loganalyze/access_log /output/loganalyze/doc
#sort
hadoop jar ~/project1/project1.jar com.project1.Sort /output/loganalyze/doc/part-r-00000 /output/loganalyze/q2_3

#q4 4
echo "********************************"
echo "****************q4**************"
echo "********************************"
hadoop jar ~/project1/project1.jar com.project1.MapIp /data/loganalyze/access_log /output/loganalyze/ip
#map Ip
#sort
hadoop jar ~/project1/project1.jar com.project1.Sort /output/loganalyze/ip/part-r-00000 /output/loganalyze/q2_4

echo "********************************"
echo "****************p3_answer**************"
echo "********************************"

echo ""
echo "********************************"
echo "****************p3 big**************"
echo "********************************"
echo ""

hadoop fs -cat /output/ngram/big_data/part-r-00000 
echo ""
echo "********************************"
echo "****************p3 test**************"
echo "********************************"
echo ""
hadoop fs -cat /output/ngram/test/part-r-00000


echo ""
echo "********************************"
echo "****************q4 1 **************"
echo "********************************"
echo ""

hadoop fs -cat /output/loganalyze/q2_1/part-r-00000
echo ""
echo "********************************"
echo "****************q4 2**************"
echo "********************************"
echo ""
hadoop fs -cat /output/loganalyze/q2_2/part-r-00000

echo ""
echo "********************************"
echo "****************q4 3**************"
echo "********************************"
echo ""
hadoop fs -cat /output/loganalyze/q2_3/part-r-00000 | head -n 1

echo "********************************"
echo "****************q4 4**************"
echo "********************************"
echo ""
hadoop fs -cat /output/loganalyze/q2_4/part-r-00000 | head -n 1


