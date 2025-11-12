# consumer/Dockerfile
FROM golang:1.19-alpine

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN go build -o consumer main.go

CMD ["./consumer"]
