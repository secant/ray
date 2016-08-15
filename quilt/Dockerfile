FROM vfang/ray:deploy
RUN apt-get -y install vim
WORKDIR /bin
Copy run .
#RUN mkdir /root/.ssh
#Copy ./keys /root/.ssh
#RUN cd /root/.ssh && \
#    cat id_rsa.pub >> authorized_keys && \
#    chmod 600 *
#Copy sshd_config /etc/ssh
Copy scripts/ /root/ray/scripts
WORKDIR /root/ray
ENV PYTHONPATH="/root/ray/lib/python/:/root/ray/thirdparty/numbuf/build"
ENTRYPOINT []
