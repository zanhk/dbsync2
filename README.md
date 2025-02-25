# Sync Client V2

The Sync Client can be used to transfer all updates on contacts to an SQL database or a web service.

## Activation

To be able to use the Sync Client, activate the corresponding connector in the campaign settings.
After successful activation, a token is displayed below the connectors. It is required to use the client.
**The token is only visible to the user who activated the connector.**

![Alternativtext wenn Bild nicht gefunden](images/dbsync1.png)

## Installation

### Download the latest version of the binary
* [Linux/Windows/Mac](https://bitbucket.org/modima/dbsync2/downloads/)

### Installation of the bindary from our Personal Packet Archive (PPA)

**Add archive as package source**

```shell
sudo add-apt-repository ppa:cloud-it/ppa && sudo apt update
```

**Install Sync Client**

```shell
sudo apt install dbsync2
```

#### Alternative Installation

**Add archive as package source**

```shell
sudo echo "deb http://ppa.launchpad.net/cloud-it/ppa/ubuntu bionic main" >> /etc/apt/sources.list
```
```shell
sudo echo "deb-src http://ppa.launchpad.net/cloud-it/ppa/ubuntu bionic main" >> /etc/apt/sources.list
```

**Import signature key**

```shell
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 441418A3042CED3E4CEAB3679F70A4F921F821F6
```

**Install Sync Client**

```shell
sudo apt-get update && apt-get install dbsync2
```

### Download and compile source code

**Install Git**

```shell
sudo apt-get install git
```

**Install Google Go**

```shell
sudo apt-get install golang-go
```

For the further steps it is necessary that the environment variable **GOPATH** is set.
A description of how to set the **GOPATH** environment variable correctly can be found [here](https://golang.org/doc/code.html#GOPATH).

**Clone the repository**

```shell
go get bitbucket.org/modima/dbsync2
```

**Install all dependencies**
```shell
cd $GOPATH/src/bitbucket.org/modima/dbsync && godep restore
```

**Compile the source code**

* Target platform Linux
    ```shell
    cd $GOPATH/src/bitbucket.org/modima/dbsync2/ && go build
    ```

* Target platform Windows
    ```shell
    cd $GOPATH/src/bitbucket.org/modima/dbsync2/ && GOOS=windows GOARCH=amd64 go build -o dbsync2.exe
    ```

* Target platform Mac
    ```shell
    cd $GOPATH/src/bitbucket.org/modima/dbsync2/ && GOOS=darwin GOARCH=amd64 go build -o dbsync2_mac
    ```

## How it works

All updates on contacts are loaded every minute and then transferred directly to the web service or database.

## Usage

### SQL database

The client currently supports the following database systems:

- **MySQL / MariaDB**
- **PostgreSQL**
- **Microsoft SQL Server**

Before you can use the client with a database, the corresponding database must be created.

The client creates the following 4 tables within that database:

- **contacts**
    - Contains all **$ fields**, as well as the first 100 custom fields of the campaign.
- **transactions**
    - Contains all transactions and the foreign key **contact_id** on the corresponding contact.
- **connections**
    - Contains all connections of the transaction and the foreign key **transaction_id** to the corresponding transaction.
- **recordings**
    - Contains all call recordings of the connection and the foreign key **connection_id** to the corresponding connection.
- **inbound_calls**
    - Contains all inbound calls and the foreign key **contact_id** to the corresponding contact

![Alternativtext wenn Bild nicht gefunden](images/dbsync2.png)

### Database Connection URL Schema
#### MySQL / MariaDB:
```shell
mysql://username:password@localhost:3306/database?useSSL=false
```
#### PostgreSQL:
```shell
postgres://username:password@localhost:5432/database?sslmode=disable
```
#### Microsoft SQL Server:
```shell
sqlserver://username:password@localhost:1433/instance/database
```

#### Example

Transfer all transactions from **01 February 2018** in the campaign **MY_CAMPAIGN** to a local running instance of **Microsoft SQL Server**. Only updates that begin with the **prefix 'fc_' or 'qc_'** in campaign stages and have been performed by a **user** are to be transferred.

```shell
dbsync2 --a db_sync --fm hi_updates_only --fp 'fc_,qc_' --c MY_CAMPAIGN_ID --ct MY_CAMPAIGN_TOKEN --s 2018-02-01 --url sqlserver://my_user:my_password@localhost:1433/sql_server_instance/my_database
```

### Web service

As an alternative to a database, the transactions can be forwarded to a web service.
The service must accept **POST** requests and answer the successful receipt of the data with a status code between **200** and **299**. Otherwise, the data will be resent (maximum 10 attempts).

The sent payload has the following **JSON format**:
```json
{
    "contact":...,      
    "transaction":...,  
    "state":...,
}
```

- **contact**
    - contains the contact details
- **transaction**
    - Contains the corresponding transaction
- **state**
    - new ... for a new transaction
    - updated ... When the transaction is updated (for example, the connection data is added later).

#### Example

Transfer all **future** transactions in the campaign **MY_CAMPAIGN** to a **Webservice**.

```shell
./dbsync --a webhook --c MY_CAMPAIGN_ID --ct MY_CAMPAIGN_TOKEN --url 'https://example.com/api/transactions/'
```

### (Error) protocol

- All error messages are written directly to the console (stdout).
- All log messages are written to ```/var/log/dbsync/{MY_CAMPAIGN_ID}_{MODE}_{TIMESTAMP}.log```.
- If the directory **/var/log/** is locked, then the messages appear under ```$HOME/.dbsync/logs/{MY_CAMPAIGN_ID}_{MODE}_{TIMESTAMP}.log```.

### Command line options

**An overview of all options can be obtained with the following command.**

```shell
dbsync --help
```