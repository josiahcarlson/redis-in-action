FROM golang:1.14-alpine3.11

ENV GO111MODULE on
# ENV GOPROXY https://goproxy.cn,direct

WORKDIR /src/app

ENV CGO_ENABLED=0

COPY go.mod go.sum ./
RUN go mod download

COPY . .




