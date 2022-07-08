kafka-topics --bootstrap-server localhost:29092 --create --topic youtube --partitions 3

kafka-console-consumer --bootstrap-server localhost:29092 --topic youtube