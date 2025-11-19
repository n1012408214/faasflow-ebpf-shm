# install docker
apt-get update
apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker.io python3-pip
# install python packages
pip3 install -r requirements.txt
# install redis
docker pull swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/python:3.7-slim-bullseye
docker tag  swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/python:3.7-slim-bullseye  python:3.7
docker pull swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/redis:7.0.14
docker tag  swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/redis:7.0.14  redis
docker run -itd -p 6379:6379 --name redis redis
bash image_setup.bash
