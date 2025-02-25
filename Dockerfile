FROM golang:1.22

WORKDIR /go/src/app

# Clone the repository directly to the GOPATH
RUN git clone https://bitbucket.org/modima/dbsync2.git $GOPATH/src/bitbucket.org/modima/dbsync2

# Set the working directory to the cloned repository
WORKDIR $GOPATH/src/bitbucket.org/modima/dbsync2

# Ensure Go modules are enabled and build the application
RUN go mod tidy && go build -o /usr/local/bin/dbsync2

CMD ["dbsync2"]