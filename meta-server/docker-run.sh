
sudo docker rm -f meta-server

sudo docker rmi -f meta-server

sudo docker build -t meta-server .

sudo docker run --publish 1883:1883 --name meta-server --rm meta-server -v /data/logs/meta/:/data/logs/meta/
