FROM golang:1.14-alpine3.11

ENV GO111MODULE on
# ENV GOPROXY https://goproxy.cn,direct

WORKDIR /src/app

# RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apk/repositories

COPY go.mod go.sum ./
RUN go mod download
RUN set -ex; \
    apk update; \
    apk add --no-cache gcc libc-dev

COPY . .




