#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API
access_token = 'MkeqjMDbMOgUkhxwN2W5JKyy4'
access_token_secret = 'FIwxuICnVNYwkoNDKNogYKyug7GhAmcbvvxgMeclSnPOaMfEhd'
consumer_key = '3287930315-sQdNkOJVrdk9R4nyIV8rm0I50lUkTu3XAXOyPal'
consumer_secret = 'tobkhrxYRYU7bdcFEApqom6wbximb3yl0U1CRyWbWyuS5'


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    #This line filter Twitter Streams to capture data by the keywords: 'TorontoApacheSpark', 'MehrdadPyCon'
    stream.filter(track=['#TorontoApacheSpark', '#MehrdadPyCon'])