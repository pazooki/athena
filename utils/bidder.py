import optparse
import json
import time
from uuid import uuid4
from kafka_handler import publish
import random
import redis

redis_server = redis.Redis("localhost")

def bid_record(uuid, timestamp):
    record = {
        'uuid': uuid,
        'timestamp': timestamp,
    }
    return json.dumps(record)

total_records = []
unique_records = []
unique_users = []
anomaly_users = []
anomaly_records = []

def generate(volume, anomalies_percentage, window, anomalies_threshold, anomalies_interval):
    timer = time.time()
    anomalies_ratio = 0.0
    while True:
        if ((time.time() - timer)/window) < (float(len(total_records))/float(volume)):
            continue
        window_is_closed = ((time.time()-timer) > window)
        volume_satisfied = (len(total_records) == volume)
        if window_is_closed or volume_satisfied:
            break
        uuid = str(uuid4())
        timestamp = int(time.time() * 1000000)
        if random.randrange(100) < anomalies_percentage:
            if random.randrange(2) == 1:
                try:
                    uuid = anomaly_users[len(anomaly_users) / 3]
                except IndexError as ie:
                    pass
                for i in xrange(1, anomalies_threshold + 1):
                    record = bid_record(uuid=uuid, timestamp=timestamp + (i * anomalies_interval))
                    total_records.append(record)
                    anomaly_records.append(record)
                    yield record
            anomaly_users.append(uuid)
        else:
            record = bid_record(uuid=uuid, timestamp=timestamp)
            total_records.append(record)
            unique_records.append(record)
            unique_users.append(uuid)
            yield record


def fire(records):
    for record in records:
        publish(record)
        redis_server.rpush('bids', record)

def main(opts):
    started = int(time.time())
    bid_generator = generate(opts.volume, opts.anomalies_percentage, opts.window, opts.anomalies_threshold, opts.anomalies_interval)
    fire(bid_generator)
    print opts.__dict__
    print 'Started: %d - Ended: %d - Total Records: %d - Unique Records: %d - Anomalies: %d - Unique users: %d - Anomaly users: %d' % (
        started, int(time.time()), len(total_records), len(unique_records), len(anomaly_records), len(unique_users), len(anomaly_users))


if __name__ == '__main__':
    usage = """
    """
    parser = optparse.OptionParser(usage)
    parser.add_option('--volume', dest='volume', type=int, default=100, help='Volume of bids per time window')
    parser.add_option('--window', dest='window', type=int, default=1, help='Time window for volume in seconds')
    parser.add_option('--anomalies-pct', dest='anomalies_percentage', type=int, default=10, help='Anomalies ratio in %')
    parser.add_option('--anomalies-interval', dest='anomalies_interval', type=int, default=1, help='Anomalies onterval in milliseconds')
    parser.add_option('--anomalies-threshold', dest='anomalies_threshold', type=int, default=10, help='Anomalies threshold')
    options, remainder = parser.parse_args()
    main(options)