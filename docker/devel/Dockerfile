FROM ubuntu:xenial
RUN apt-get update
RUN apt-get -y install apt-utils
RUN apt-get -y install sudo
RUN apt-get install -y git cmake build-essential autoconf curl libtool python-dev python-numpy python-pip libboost-all-dev unzip graphviz
RUN pip install ipython typing funcsigs subprocess32 protobuf==3.0.0a2 colorama graphviz cloudpickle
RUN adduser --gecos --ingroup ray --disabled-login --gecos ray
RUN adduser ray sudo
RUN sed -i "s|%sudo\tALL=(ALL:ALL) ALL|%sudo\tALL=NOPASSWD: ALL|" /etc/sudoers
USER ray
RUN echo '\n# Ray environment\nsource /home/ray/ray/setup-env.sh' >> /home/ray/.bashrc
WORKDIR /home/ray
ENTRYPOINT bash
