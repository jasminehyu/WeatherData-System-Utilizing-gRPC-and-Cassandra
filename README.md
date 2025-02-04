# Weather Data Management System with Cassandra and gRPC

## Project Overview

This project involves the development of a server using gRPC to manage and query weather data stored in a Cassandra database. 
The system is designed to handle data insertion and querying under various network conditions, demonstrating the balance between read and write availability.


## Key Features:

- **Cassandra Database Integration:** Implements a robust schema with partition keys and cluster keys to efficiently manage weather data.
- **Data Handling with Spark:** Utilizes Apache Spark for preprocessing data before insertion into Cassandra, showcasing efficient data management techniques.
- **gRPC for Data Interaction:** Uses gRPC for server-client communication, allowing for effective querying and data management.
- **Fault Tolerance and Availability:** Explores trade-offs between read and write availability, ensuring that data insertion is prioritized even under potential network failures.


## Technical Implementation
1. **Server Setup:**
    - Utilizes Docker containers to simulate a real-world distributed database system with multiple nodes.
    - Sets up a Cassandra cluster within Docker to handle large-scale data management.
      
2. **Data Schema and RPC Communication:**

    - Designs and implements a Cassandra schema to store weather station data.
    - Develops gRPC services to enable data insertion and retrieval, demonstrating methods to handle different types of data interactions.

3. **Data Processing with Spark:**

    - Integrates Spark to preprocess large datasets efficiently.
    - Implements data extraction and transformation tasks to prepare data for database insertion.

4. **Consistency and Availability:**

    - Configures Cassandra’s consistency levels to ensure data availability and consistency across distributed systems.
    - Implements error handling to manage various failure scenarios effectively.


## Installation and Usage
To deploy the Weather Data Management System:

1. **Build the Docker Image:**
    ```
    docker build . -t p6-base
    ```
2. **Start the System:**
   ```
   docker compose up -d
   ```
   Note: It will start three containers ('p6-db-1', 'p6-db-2', 'p6-db-3').\
   It generally takes around 1 to 2 minutes for the Cassandra cluster to be ready.
   

   Run the following command:
   
   ```
   docker exec p6-db-1 nodetool status
   ```
   and if the cluster is ready, it will produce an output like this:

   ```sh
    Datacenter: datacenter1
    =======================
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address     Load       Tokens  Owns (effective)  Host ID                               Rack 
    UN  172.27.0.4  70.28 KiB  16      64.1%             90d9e6d3-6632-4721-a78b-75d65c673db1  rack1
    UN  172.27.0.3  70.26 KiB  16      65.9%             635d1361-5675-4399-89fa-f5624df4a960  rack1
    UN  172.27.0.2  70.28 KiB  16      70.0%             8936a80e-c6b2-42ef-b54d-4160ff08857d  rack1
    ```
    If the cluster is not ready it will generally show an error. If this
    occurs then wait a little bit and rerun the command and keep doing so
    until you see that the cluster is ready.

    
3. **Compile the proto files:**
    ```
    docker exec -w /src p6-db-1 sh -c "python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. station.proto "
    ```
   Run the grpc_tools.protoc tool in p6-db-1 to generate stub code for our clients and servicer code for your server.
    
## Interacting with the System

- **Initialize the Server:**
  ```
  docker exec -it p6-db-1 python3 /src/server.py
  ```

- **Query Data:**
  Use provided client scripts to interact with the system and retrieve weather data.

  Examples:
  1. **RPC - StationSchema:** Use ClientStationSchema.py to make a client call
     ```
     docker exec -w /src p6-db-1 python3 ClientStationSchema.py
     ```
     
     Result:
     ```
        CREATE TABLE weather.stations (
            id text,
            date date,
            name text static,
            record station_record,
            PRIMARY KEY (id, date)
        ) WITH CLUSTERING ORDER BY (date ASC)
            AND additional_write_policy = '99p'
            AND bloom_filter_fp_chance = 0.01
            AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
            AND cdc = false
            ...
     ```
     
  2. **RPC - StationName:** The function should execute a Cassandra query and parse the result to obtain the name of a station for a specific station id.
  The station id is stored in request.station (refer to station.proto).
     ```
        docker exec -w /src p6-db-1 python3 ClientStationName.py US1WIMR0003
     ```
     --> It will print out "AMBERG 1.3 SW".
     
  4. **RPC - RecordTemps:** RecordTemps function in StationService class. It receives temperature data and writes it to weather.stations.
     The ClientRecordTemps.py pulls its data from src/weather.parquet. You can run it as follows:
     ```
     docker exec -w /src p6-db-1 python3 ClientRecordTemps.py
     ```
     --> It will print out a list of: Inserted {station_id} on {date} with tmin={tmin} and tmax={tmax}.

  5. **RPC - StationMax:** StationMax RPC in server.py, which will return the maximum tmax ever seen for the given station.
     Then, you can use ClientStationMax.py to make a client call:
     ```
     docker exec -w /src p6-db-1 python3 ClientStationMax.py USR0000WDDG
     ```
     -->  It will print out 344.
## Conclusion
This project showcases the integration of modern technologies such as Cassandra, Docker, gRPC, and Spark to create a resilient and scalable weather data management system. It demonstrates my ability to design and implement complex data handling solutions that are robust and efficient under varied conditions.
  
We provide the Dockerfile and docker-compose.yml for this project. You can run the following:
