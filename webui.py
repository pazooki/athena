import tornado.escape
import tornado.ioloop
import tornado.web
import ujson
import redis

redis_server = redis.Redis("localhost")

def takeout_bids(n):
    for i in xrange(n):
        yield redis_server.rpop('bids')

def takeout_anomalies(n):
    for i in xrange(n):
        yield redis_server.rpop('anomalies')


class BidsHandler(tornado.web.RequestHandler):
    def get(self):
        try:
            response = {
                'total_bids': [ujson.loads(rec) for rec in takeout_bids(100)],
                'anomalies': [ujson.loads(rec) for rec in takeout_anomalies(100) if rec]
            }
        except Exception as ex:
            print ex.message
        self.write(response)


application = tornado.web.Application([
    (r"/bids", BidsHandler)
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()

