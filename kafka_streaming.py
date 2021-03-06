from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import numpy as np
import time


def frequency_time_window(list_timestamp, time_window):
    if len(list_timestamp) == 1:
        return 1
    from collections import deque
    frequency_window = [0]
    q = deque()
    for timestamp in list_timestamp:
        q.append(timestamp)
        while q[-1] - q[0] > time_window:
            q.popleft()
            if frequency_window[-1]:
                frequency_window.append(0)
        frequency_window[-1] += 1
    return max(frequency_window)


def get_gaussian(frequency_list):
    return np.mean(frequency_list), np.std(frequency_list)


def is_fraud(frequency, gaussian_model, tolerance_lev):
    m, std = gaussian_model[0], gaussian_model[1]
    # if frequency <= m:
    #     return False
    # np.exp(- (frequency - m) ** 2 / (2.0 * std ** 2)) / (std * np.sqrt(2 * np.pi))
    return True if frequency >= m + tolerance_lev * std else False


def write_to_redis(rdd):
    import redis
    redis_server = redis.Redis("localhost")
    for rec in rdd.collect():
        try:
            print('X' * 100, json.dumps(rec), 'X' * 100)
            redis_server.rpush('anomalies', json.dumps(rec))
        except Exception as ex:
            pass


# def mean_stdv(rdd):
#     import redis
#     redis_server = redis.Redis("localhost")
#     mean_v = 0
#     count = 0
#     recs = rdd.collect()
#     for rec in recs:
#         count += 1
#         mean_v += rec[1]
#     mean_result = mean_v / count
#     stdv = 0
#     for rec in recs:
#         stdv += (rec[1] - mean_result) ** 2
#     stdv = (stdv / count) ** 0.5
#     redis_server.set('mean', mean_result)
#     redis_server.set('standard_deviation', stdv)


if __name__ == "__main__":
    sc = SparkContext(appName="Athena-Fraud-Detector")
    slice_duration = 5
    ssc = StreamingContext(sc, slice_duration)

    # bid_stream = KafkaUtils.createStream(ssc, 'localhost:2181', 'athena', {'bids': 1})
    bid_stream = KafkaUtils.createDirectStream(ssc, ['bids'], {'metadata.broker.list': 'localhost:9092'})

    t = time.time()
    path = '/var/athena/data/windows/window-%d' % t

    # athena_rdd = sc.textFile('/var/athena/data/windows')
    # athena_old = athena_rdd.map(json.loads).map(lambda x: (x[0], x[1]))

    window = bid_stream \
        .map(lambda x: json.loads(x[1])) \
        .map(lambda x: (x.get('uuid'), [x.get('timestamp')])) \
        .reduceByKey(lambda x, y: x + y)
    # if window.count() > 0:
    #     window\
    #     .map(json.dumps)\
    #     .saveAsTextFile(path)

    ssc.checkpoint('/var/athena/data/')

    reduced_window = window.reduceByKeyAndWindow(lambda x, y: x + y, None, 10, slice_duration)
    # reduced_window.pprint()
    # try:
    #     reduced_window.foreachRDD(lambda frame: frame.filter(lambda x: len(x[1]) > 1).saveAsTextFile(path))
    # except:
    #     pass

    # def update_function(new_values, running_value):
    #     return max(new_values, running_value if running_value else new_values)
    #
    # id_frequency = reduced_window.map(lambda x: (x[0], frequency_time_window(x[1], 1)))
    # running_id_freq = id_frequency.updateStateByKey(update_function)
    id_frequency = reduced_window.foreachRDD(lambda frame: frame.map(lambda x: (x[0], frequency_time_window(x[1], 10))))

    # id_frequency.map(lambda x: x[1]).foreachRDD(mean_stdv)

    # gaussian_model = get_gaussian(id_frequency.map(lambda x: x[1]).collect())
    tolerance_lev = 2

    id_frequency \
        .filter(lambda x: is_fraud(x[1], (2, 0.5), tolerance_lev)) \
        .map(lambda x: x[0]) \
        .foreachRDD(write_to_redis)

    # print fraud_id

    # try:
    # athena_new = sc.textFile('%s*' % path).map(json.loads).map(lambda x: (x[0], x[1]))
    # athena_old.join(athena_new).map(json.dumps).saveAsTextFiles('/var/athena/data/global_window')
    # athena_old.join(window).saveAsTextFiles('/var/athena/data/global_window')
    # window.join(athena_old).saveAsTextFiles('/var/athena/data/global_window')
    # except Exception as ex:
    #     print 'X'*100, ex.message, 'X'*100


    ssc.start()
    ssc.awaitTermination()
