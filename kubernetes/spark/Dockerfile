FROM neylsoncrepalde/spark-operator:v3.1.1-hadoop3-aws-kafka

# using root
USER root:root

# create directory for applications
RUN mkdir -p /app 

COPY ./requirements.txt ./

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt

#set main work directory
WORKDIR /app

#user
USER root