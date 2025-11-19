FROM codeless_base
RUN pip3 install  tensorflow keras pillow
COPY config.py /proxy
RUN mkdir /blocks
COPY blocks /blocks
COPY resnet50_final_violence.h5 /proxy/resnet50_final_violence.h5