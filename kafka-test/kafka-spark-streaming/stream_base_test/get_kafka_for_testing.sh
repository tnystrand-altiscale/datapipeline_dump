rm -r kafka
mkdir kafka
wget http://apache.mirrors.spacedump.net/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz
tar zxvf kafka_2.11-0.10.2.0.tgz -C kafka --strip-components 1
rm kafka_2.11-0.10.2.0.tgz
