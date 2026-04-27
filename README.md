# Distributed Replication System

A Java project that simulates distributed data replication using **RabbitMQ** as the message broker. Data written by a client is automatically broadcast to 3 independent replicas. A majority-vote reader can detect and filter out inconsistencies across replicas.

```

---


### 1 — Start the three replicas (each in its own terminal)

```bash
java -cp "out:lib/*" replication.Replica 1
java -cp "out:lib/*" replication.Replica 2
java -cp "out:lib/*" replication.Replica 3
```

Each replica creates a `replicaN/data.txt` file and listens for write and read commands.

### 2 — Write data

```bash
java -cp "out:lib/*" replication.ClientWriter "1 Hello "
java -cp "out:lib/*" replication.ClientWriter "2 anything else"
```


### 3 — Read (fast)

```bash
java -cp "out:lib/*" replication.ClientReader
```

Sends `READ_LAST` to all replicas and prints whichever replies first.

### 4 — Read with majority vote

```bash
java -cp "out:lib/*" replication.ClientReaderV2
```


### 5 i added a  Web dashboard 

```bash
java -cp "out:lib/*" replication.DashboardServer
```

Open [http://localhost:8080](http://localhost:8080) 


