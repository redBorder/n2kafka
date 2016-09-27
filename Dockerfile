FROM centos:7
MAINTAINER redBorder

RUN yum install -y epel-release
RUN rpm -ivh http://repo.redborder.com/redborder-repo-1.0.0-1.el7.rb.noarch.rpm; \
  yum install -y \
    librd0          \
    libev           \
    libmicrohttpd   \
    librdkafka1     \
    jansson         \
    libcurl         \
    yajl;           \
  yum clean all

WORKDIR /app

COPY n2kafka /app/
COPY consul-template /app/
COPY config.template /app/

CMD ["./consul-template", "-consul", "consul",  "-template", "config.template:config.json:./n2kafka config.json" ]
