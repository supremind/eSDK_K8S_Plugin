FROM golang:alpine AS builder

WORKDIR /src
COPY . /src/
RUN go build -o bin/huawei-csi ./src/csi

FROM alpine
COPY --from=builder /src/bin/huawei-csi /huawei-csi
RUN apk add --no-cache e2fsprogs
ENTRYPOINT [ "/huawei-csi" ]
