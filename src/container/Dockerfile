FROM python:3.9-slim
ENV PYTHONUNBUFFERED=1
#COPY pip.conf /etc/pip.conf
ENV http_proxy=http://10.17.43.86:10809
ENV https_proxy=http://10.17.43.86:10809
ENV HTTP_PROXY=http://10.17.43.86:10809
ENV HTTPS_PROXY=http://10.17.43.86:10809
RUN sed -i "s@http://deb.debian.org@http://mirrors.ustc.edu.cn@g" /etc/apt/sources.list || true
RUN sed -i "s@http://security.debian.org@http://mirrors.ustc.edu.cn@g" /etc/apt/sources.list || true
RUN apt-get update && apt-get install -y sed
#RUN sed -i "s@http://deb.debian.org@http://mirror.sjtu.edu.cn@g" /etc/apt/sources.list
#RUN sed -i "s@http://security.debian.org@http://mirror.sjtu.edu.cn@g" /etc/apt/sources.list
RUN apt install -y iproute2
RUN pip3 config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip3 config set global.trusted-host pypi.tuna.tsinghua.edu.cn
RUN pip3 install gevent flask requests couchdb redis gipc kafka-python confluent_kafka
RUN mkdir /proxy
RUN mkdir /proxy/mnt
RUN mkdir /proxy/scripts
COPY block.py container_config.py proxy.py store.py bypass_store.py /proxy/
COPY ./shm_utils.py /proxy/
COPY ./container_shm.py /proxy/
COPY ./config.py /proxy/
COPY ./simple_perf_logger.py /proxy/
COPY ./optimized_container_shm.py /proxy/
COPY scripts /proxy/scripts
WORKDIR /proxy
EXPOSE 5000

#RUN apt clean
#RUN apt update

CMD ["python3", "/proxy/proxy.py"]