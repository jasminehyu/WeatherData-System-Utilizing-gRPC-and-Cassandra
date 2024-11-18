import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import ConsistencyLevel
from cassandra import Unavailable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr



class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        # TODO: create schema for weather data;
        cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.sess = cluster.connect()
        
        self.sess.execute("DROP KEYSPACE IF EXISTS weather")

        self.sess.execute("""
        CREATE KEYSPACE weather
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '3'}
        """)
        self.sess.execute("USE weather")
        
        self.sess.execute("""
        create type station_record(tmax int, tmin int)
        """)
        self.sess.execute("""
        create table stations(
        id text,
        name text STATIC,
        date date,
        record station_record,
        PRIMARY KEY(id,date)
        )
        """)
        self.spark = SparkSession.builder.appName("p6").getOrCreate()
        df = (
        self.spark.read.text("ghcnd-stations.txt")
        .withColumn("ID", expr("substring(value, 1, 11)"))
        .withColumn("STATE", expr("substring(value, 39, 2)"))
        .withColumn("NAME", expr("substring(value, 42, 30)"))
        .select("ID", "STATE", "NAME")
        .filter(col("STATE") == "WI")
        .collect()
        )
    

        for row in df:
            station_id=row.ID
            station_name=row.NAME
            self.sess.execute("""
            INSERT INTO weather.stations(id,name)
            VALUES(%s, %s)
            """,(station_id, station_name)
            )
        
        
        # TODO: load station data from ghcnd-stations.txt; 

        # ============ Server Stated Successfully =============
        print("Server started") # Don't delete this line!


    def StationSchema(self, request, context):
        result= self.sess.execute("DESCRIBE TABLE weather.stations").one().create_statement
        return station_pb2.StationSchemaReply(schema=result, error="")


    def StationName(self, request, context):
        station_id=request.station
        result=self.sess.execute("""
        select name
        from weather.stations
        where id= %s
        """,(station_id,))
        row=result.one()
        station_name=row.name
        #print(f"Type of station_name: {type(station_name)}, Value: {station_name}")

        return station_pb2.StationNameReply(name=station_name, error="")

    def RecordTemps(self, request, context):
        stat_id=request.station
        stat_date=request.date
        stat_tmax=request.tmax
        stat_tmin=request.tmin

        try:
            prepared=self.sess.prepare("""
            INSERT INTO weather.stations(id, date, record)
            VALUES(?, ?, {tmax: ?, tmin: ?})
            """)
            prepared.consistency_level = ConsistencyLevel.ONE
           # print(f"Debug: stat_id={stat_id}, stat_date={stat_date}, stat_tmax={stat_tmax}, stat_tmin={stat_tmin}")
            
            self.sess.execute(prepared,(stat_id, stat_date,stat_tmax,stat_tmin))
            
            
            return station_pb2.RecordTempsReply(error="")

        except Unavailable:

            return station_pb2.RecordTempsReply(error="unavailable")
        except NoHostAvailable:
            return station_pb2.RecordTempsReply(error="unavailable")
        except Exception as e:
            return station_pb2.RecordTempsReply(error=f"error: {str(e)}")


    def StationMax(self, request, context):
        stat_id=request.station

        try:
            prepared=self.sess.prepare("""
            select MAX(record.tmax) as max_temp
            from weather.stations
            where id= ?
            """)
            prepared.consistency_level = ConsistencyLevel.THREE
            result=self.sess.execute(prepared,(stat_id,))
           
            row=result.one()
            station_maxtemp=row.max_temp
        
            return station_pb2.StationMaxReply(tmax=station_maxtemp, error="")
        
        except Unavailable:

            return station_pb2.StationMaxReply(error="unavailable")
        except NoHostAvailable:
            return station_pb2.StationMaxReply(error="unavailable")
        except Exception as e:
            return station_pb2.StationMaxReply(error=f"error: {str(e)}")
    

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
