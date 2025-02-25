FROM golang:1.22

# Set the working directory
WORKDIR /go/src/app

# Copy the local code to the container
COPY . .

# Build the application
RUN go mod tidy && go build -o /usr/local/bin/dbsync2

CMD ["dbsync2"]
