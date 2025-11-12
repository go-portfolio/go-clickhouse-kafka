# Use Go 1.24 image instead of Go 1.19
FROM golang:1.24-alpine

# Set the current working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download Go dependencies (go mod download)
RUN go mod download

# Copy the rest of your application code
COPY . .

# Build the Go app
RUN go build -o consumer main.go
