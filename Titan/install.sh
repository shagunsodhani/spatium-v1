
mkdir -p ~/Devsetup/
wget -P ~/Downloads/ http://mirror.tcpdiag.net/apache/cassandra/2.1.8/apache-cassandra-2.1.8-bin.tar.gz
tar -xvzf ~/Downloads/apache-cassandra-2.1.8-bin.tar.gz -C ~/Devsetup/
wget -P ~/Downloads/ https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-3.0.4.tgz
tar -zxvf ~/Downloads/mongodb-linux-x86_64-3.0.4.tgz -C ~/Devsetup/
mv ~/Devsetup/apache-cassandra-2.1.8/ ~/Devsetup/cassandra
mv ~/Devsetup/mongodb-linux-x86_64-3.0.4/ ~/Devsetup/mongo
echo $"export PATH=~/Devsetup/mongo/bin:~/Devsetup/cassandra/bin:$PATH" >>~/.bashrc
