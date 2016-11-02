import pyspark
import sys
import pandas
import math
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import explode
from pyspark.sql.functions import lit



def distance(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 3959 # miles

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d


def startInterval(starttime):
    time1 = starttime - datetime.timedelta(minutes=10)
    time2 = starttime
    time_points = [time1]
    while True:
        if time1 <= time2:
            time1 = time1 + datetime.timedelta(seconds=1)
            time_points.append(time1)
        else:
            break
    return time_points
    

def endInterval(stoptime):
    time1 = stoptime
    time2 = stoptime + datetime.timedelta(minutes=10)
    time_points = [time1]
    while True:
        if time1 <= time2:
            time1 = time1 + datetime.timedelta(seconds=1)
            time_points.append(time1)
        else:
            break
    return time_points

def getLocation(latitude,longitude):
    return (latitude,longitude)


if __name__=='__main__':
    if len(sys.argv)<2:
        print "Please enter station name: <station_name>"
        sys.exit(-1)
    station_name = sys.argv[1];

    sc = pyspark.SparkContext()

    spark = SparkSession.builder\
            .appName('BDM_quiz')\
            .config('spark.some.config.option','some value')\
            .getOrCreate()


    startInterval = udf(startInterval,ArrayType(TimestampType()))
    endInterval = udf(endInterval,ArrayType(TimestampType()))
    distance = udf(distance, DoubleType())
    getLocation = udf(getLocation,ArrayType(DoubleType()))


    bikes = spark.read.csv('feb2014_citibike.csv',header=True,inferSchema=True,mode="DROPMALFORMED")
    raw_start_bikes = bikes.filter(bikes[r'start station name'] == station_name)
    raw_end_bikes = bikes.filter(bikes[r'end station name'] == station_name)
    start_bikes = raw_start_bikes.select(raw_start_bikes[r'start station name'].alias('Station_name'),startInterval(raw_start_bikes[r'starttime']).alias('time_points'),raw_start_bikes[r'start station latitude'].alias('latitude'),raw_start_bikes[r'start station longitude'].alias('longitude'))    
    end_bikes = raw_end_bikes.select(raw_end_bikes[r'end station name'].alias('Station_name'),endInterval(raw_end_bikes[r'stoptime']).alias('time_points'),raw_end_bikes[r'end station latitude'].alias('latitude'),raw_end_bikes[r'end station longitude'].alias('longitude'))   
    target_bike_trips = start_bikes.unionAll(end_bikes)
    location = target_bike_trips.agg({'latitude':'avg','longitude':'avg'})
    latitude = location.first()[0]
    longitude = location.first()[1]
    target_location = sc.broadcast([latitude,longitude])
    start_trip_time = start_bikes.select(explode(start_bikes.time_points).alias('time')).dropDuplicates().orderBy('time')
    end_trip_time = end_bikes.select(explode(end_bikes.time_points).alias('time')).dropDuplicates().orderBy('time')

    taxi = spark.read.csv('feb2014.csv',header=True,inferSchema=True,mode="DROPMALFORMED")
    taxi_locations = taxi.select(getLocation(taxi['pickup_latitude'],taxi['pickup_longitude']).alias('pickup_location'),taxi['pickup_datetime'],getLocation(taxi['dropoff_latitude'],taxi['dropoff_longitude']).alias('dropoff_location'),taxi['dropoff_datetime'])
    taxi_locations = taxi_locations.withColumn('target_latitude',lit(target_location.value[0]))
    taxi_locations = taxi_locations.withColumn('target_longitude',lit(target_location.value[1]))
    taxi_locations = taxi_locations.select('pickup_location','pickup_datetime','dropoff_location','dropoff_datetime',getLocation(taxi_locations['target_latitude'],taxi_locations['target_longitude']).alias('target_location'))
    taxi_distance = taxi_locations.select(distance(taxi_locations['target_location'],taxi_locations['pickup_location']).alias('pickup_distance'),taxi_locations['pickup_datetime'],distance(taxi_locations['target_location'],taxi_locations['dropoff_location']).alias('dropoff_distance'),taxi_locations['dropoff_datetime'])
    pickup_taxi = taxi_distance.filter(taxi_distance['pickup_distance']<=0.25).select(taxi_distance['pickup_datetime'].alias('time')).orderBy('time')    
    dropoff_taxi = taxi_distance.filter(taxi_distance['dropoff_distance']<=0.25).select(taxi_distance['dropoff_datetime'].alias('time')).orderBy('time')
    


    pickup_taxi_result = end_trip_time.join(pickup_taxi,end_trip_time.time == pickup_taxi.time)
    dropoff_taxi_result = start_trip_time.join(dropoff_taxi,start_trip_time.time == dropoff_taxi.time)
    trip_count = sc.broadcast(pickup_taxi_result.count() + dropoff_taxi_result.count())

    print trip_count.value






