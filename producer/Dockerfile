# producer/Dockerfile
FROM golang:1.19-alpine

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN go build -o producer main.go

CMD ["./producer"]
