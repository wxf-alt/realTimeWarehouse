redis：
    su - root
    cd /usr/local/redis
	redis-server /usr/local/redis/redis.conf
	redis-cli -h nn1.hadoop -p 6379
zookeeper：
     ./ssh_all.sh /usr/local/zookeeper/bin/zkServer.sh start
kafka：
	./ssh_all.sh "nohup /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties > /tmp/kafka_logs 2>&1 &"
kafka-manager：
	nohup ./kafka-manager -Dconfig.file=/usr/local/kafka-manager/conf/application.conf -Dhttp.port=9999 > /usr/local/kafka-manager/logs/kafka-manager.log 2>&1 &
es：
    ./ssh_all.sh "nohup /usr/local/elasticsearch-6.3.1/bin/elasticsearch > /usr/local/elasticsearch-6.3.1/logs/esStart.log 2>&1 &"
    nohup /usr/local/kibana-6.3.1/bin/kibana > /usr/local/kibana-6.3.1/kibanaStart.log 2>&1 &