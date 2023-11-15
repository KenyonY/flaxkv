FROM python:3.10-alpine
LABEL maintainer="K.Y"
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV TZ=Asia/Shanghai
RUN apk update && \
    apk add tzdata --no-cache && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    apk del tzdata && \
    rm -rf /var/cache/apk/*

COPY . /home/flaxkv
WORKDIR /home/flaxkv
#musl-dev
RUN apk add patch g++ libstdc++ linux-headers leveldb-dev --no-cache && pip install -e . --no-cache-dir && apk del g++ gcc && rm -rf /var/cache/apk/*

EXPOSE 8000
ENTRYPOINT ["python", "-m", "flaxkv.__main__", "run"]
