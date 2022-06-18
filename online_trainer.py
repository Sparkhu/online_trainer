import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import torch
from torch import nn
from hdfs import InsecureClient
import configparser
from sklearn.preprocessing import MinMaxScaler
from pyspark.ml.feature import MinMaxScalerModel
from influxdb import InfluxDBClient
import time
from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import timezone, timedelta

# hyperparameter
TIMESTEP = 60
N_OUT = 20
N_BATCH = 64
N_EPOCH=100
N_HIDDEN = 60
N_LAYER = 1
learning_rate = 0.01
train_ratio = 0.8
smoothing_factor = 0.6
margin_of_step = 1
# Parse Args
import argparse
import datetime
parser = argparse.ArgumentParser()

# Target Feature
parser.add_argument("db")
parser.add_argument("table")
parser.add_argument("feature")

parser.add_argument("--timestep", type=int)
parser.add_argument("--num_out", type=int)
args = parser.parse_args()

DB = args.db
TABLE = args.table
FEATURE = args.feature


###
KAFKA_BOOTSTRAP_SERVERS = ['']
PROFILE = "dev"
STREAM_WINDOW_WATERMARK = "90 minutes"
STREAM_WINDOW_GROUP = "90 minutes"
STREAM_WINDOW_TRIGGER = "5 minutes"
PREDICTIVE_PERIOD = timedelta(minutes=1)
INFLUX_HOST = ''
INFLUX_PORT = 0
INFLUX_USER = ''
INFLUX_PASSWORD = ''
TIME_ZONE = "Asia/Seoul"
###

if args.timestep:
    TIMESTEP = args.timestep
if args.num_out:
    N_OUT = args.num_out

findspark.init()



today=datetime.datetime.now().strftime("%Y-%m-%d")
conf = SparkConf().setAppName(f"online_trainer_{DB}_{TABLE}_{FEATURE}_{today}")\
    .setMaster('local[*]')\
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3")
spark = SparkContext(conf=conf)
sql_context = SQLContext(spark)




# 설정값

class Net(nn.Module):
    def __init__(self, input_dim, hidden_dim, seq_len, output_dim, layers):
        super(Net, self).__init__()
        self.hidden_dim = hidden_dim
        self.seq_len = seq_len
        self.output_dim = output_dim
        self.layers = layers

        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers=layers,
                            # dropout = 0.1,
                            batch_first=True)
        self.fc = nn.Linear(hidden_dim, output_dim, bias=True)


    def reset_hidden_state(self):
        self.hidden = (
            torch.randn(self.layers, self.seq_len, self.hidden_dim, requires_grad=True),
            torch.randn(self.layers, self.seq_len, self.hidden_dim, requires_grad=True))

    def forward(self, x):
        x, _status = self.lstm(x)
        x = self.fc(x[:, -1])
        return x

# Get model module (please refactoring)


HDFS_PATH = f'/modelDir/offline/{DB}_{TABLE}_{FEATURE}.pt'
LOCAL_PATH =  f'/tmp/offlineModel_{DB}_{TABLE}_{FEATURE}.pt'
def download_model_from_hdfs():
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    client = InsecureClient(config['WEBHDFS']['URL'], config['WEBHDFS']['USER'])
    client.download(hdfs_path=HDFS_PATH, local_path=LOCAL_PATH, overwrite=True)

download_model_from_hdfs()
model = Net(1, N_HIDDEN, TIMESTEP, N_OUT, N_LAYER)
model.load_state_dict(torch.load(LOCAL_PATH))
model.eval()

criterion  = nn.MSELoss()
adam = torch.optim.Adam(model.parameters(), learning_rate)

kafka_df = sql_context.readStream. \
  format("kafka").\
  option("subscribe", 'AMPds2_tests_electricity').\
  option("kafka.bootstrap.servers", ",".join(KAFKA_BOOTSTRAP_SERVERS)).\
  option("startingOffsetsByTimestampStrategy", "latest").\
  load()
kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

minMaxModelPath = 'hdfs:///modelDir/ampds2_electricity_V/minmaxModel'
loadedMMModel = MinMaxScalerModel.load(minMaxModelPath)
minmax = [loadedMMModel.originalMin.toArray(), loadedMMModel.originalMax.toArray()]
scaler = MinMaxScaler()
def process(row):
    x = preprocessData(row)
    if x is None:
        return
    y_hat_scaled = predict(x)
    y_hat = descale(y_hat_scaled)
    updatePrediction(y_hat, last_timestamp=row['time'][-1], period=PREDICTIVE_PERIOD)


def descale(scaled):
    scaler.fit(minmax)
    return scaler.inverse_transform(scaled)


def preprocessData(row):
    if row["count"] < (TIMESTEP - margin_of_step) or row["count"] > (TIMESTEP - margin_of_step):
        print("Input count: ", row["count"])
        return None
    print("Input count: ", row["count"], " success")
    _row = list(map(lambda x: [x], row[FEATURE]))
    x = torch.tensor([_row[row["count"] - TIMESTEP:]])
    return x

def predict(x):
    result = model(x)
    return result.tolist()

def generateTimestampSeries(last_timestamp, period, num):
    floor_timestamp = last_timestamp.astimezone(timezone.utc) + timedelta(minutes = 1)
    floor_timestamp = floor_timestamp.replace(second=0)
    return [floor_timestamp + x*period for x in range(num)]

def updatePrediction(y_hat, last_timestamp, period=timedelta(minutes=1)):
    timestampSeries = generateTimestampSeries(last_timestamp, period, N_OUT)
    queryResult = getPreviousPrediction(timestampSeries[0], timestampSeries[-1])
    predictions = simpleExponentialSmoothing(y_hat, timestampSeries, queryResult)
    update(predictions)
    return





def getPreviousPrediction(start, end):
    try:
        influx_client = InfluxDBClient(INFLUX_HOST, INFLUX_PORT, INFLUX_USER, INFLUX_PASSWORD)
        dbs = influx_client.get_list_database()
        find = False
        for ele in dbs:
            if DB in ele.values():
                find = True
                break
        if not find:
            influx_cleint.create_database(DB)
        influx_client.switch_database(DB)
        start_str = start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        end_str = end.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        # select_query = f'SELECT {FEATURE} FROM "{TABLE}" WHERE time >= {start} and time <= {end} tz("{TIME_ZONE}");'
        select_query = f"SELECT {FEATURE} FROM {TABLE} WHERE time >= '{start_str}' AND time < '{end_str}';"
        print(select_query)
        result = influx_client.query(select_query)
        influx_client.close()
    except Exception:
        print("Error Occur in influx select query")
        return
    return result





def simpleExponentialSmoothing(y_hat, timestampSeries, queryResult):
    values = []
    times = []
    if (queryResult is not None and len(queryResult.raw['series']) > 0):
        values = queryResult.raw['series'][0]['values']
        times = [x[0] for x in values]
    predictions = []
    St = 0.0
    for idx, timestamp in enumerate(timestampSeries):
        if timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") in times:
            St_1 = values[times.index(timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"))][1]
            St = smoothing_factor * y_hat[0][idx] + (1 - smoothing_factor) * St_1
        else:
            St = y_hat[0][idx]
        predictions.append([int(time.mktime(timestamp.timetuple())) + 60 * 60 * 9, St])

    return predictions

def update(predictions):
    try:
        influx_client = InfluxDBClient(INFLUX_HOST, INFLUX_PORT, INFLUX_USER, INFLUX_PASSWORD)
        influx_client.switch_database(DB)
        data = []
        for prediction in predictions:
            update_query = f"{TABLE} {FEATURE}={prediction[1]} {prediction[0]}"
            data.append(update_query)
        influx_client.write_points(data, database=DB, time_precision='s', batch_size=5000, protocol='line')
        influx_client.close()
    except Exception:
        print("Error Occur in influx update query: ")
    return


schema = StructType([
    StructField("payload", StructType([
        StructField("time", TimestampType()),
        StructField(FEATURE, FloatType())
    ]))
])

feature_df = kafka_df.selectExpr("CAST(value AS STRING)").withColumn(FEATURE, F.from_json(F.col("value"), schema)).select('.'.join([FEATURE, 'payload', '*']))
sliding_window = feature_df.withWatermark("time", STREAM_WINDOW_WATERMARK)\
    .groupBy(
        F.window(F.col("time"), STREAM_WINDOW_GROUP, STREAM_WINDOW_TRIGGER)
    ).agg(F.collect_list(F.col(FEATURE)).alias(FEATURE),
         F.collect_list(F.col("time")).alias("time"),
         F.count("time").alias("count"))

query = sliding_window.writeStream.outputMode('update').foreach(process).start()
query.awaitTermination()

spark.stop()
