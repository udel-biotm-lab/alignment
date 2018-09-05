FROM ubuntu:16.04
MAINTAINER Jia Ren <renj@udel.edu>

RUN apt-get update && apt-get upgrade -y

RUN apt-get install build-essential -y

RUN apt-get install python python-pip -y

RUN pip install glog
RUN mkdir /align_workdir

COPY ./alignment /alignment

ENTRYPOINT ["python", "/alignment/align_entity.py", "/align_workdir/origin_file.json", "/align_workdir/result_file.json", "/align_workdir/output_file.json"]
