from django.shortcuts import render
import tweepy
import json
from textwrap import TextWrapper
import time
from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_GET
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import urllib3
from queue import Queue

flag = False
tweetQueue = Queue(maxsize=10)



consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)


es = Elasticsearch()
host = ''
awsauth = AWS4Auth('' , '','us-west-2', 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
print(es.info())

class StreamListener(tweepy.StreamListener):
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def __init__(self, time_limit=10):
        self.start_time = time.time()
        self.limit = time_limit
        super(StreamListener, self).__init__()


    # def on_status(self, status):
    #
    #     if (time.time() - self.start_time) < self.limit:
    #         #print 'n%s %s' % (status.author.screen_name, status.created_at)
    #             tweets = status._json
    #             if (tweets['place']):
    #                 print(tweets['text'])
    #                 self.append_record(tweets)
    #                 return True
    #             else:
    #                 pass
    #     else:
    #         return False
    #
    # def on_error(self, status_code):
    #     print("response: %s" % status_code)
    #     if status_code == 420:
    #         return False
    #
    # def append_record(self, record):
    #     with open('my_file_1', 'a') as f:
    #         json.dump(record, f)
    #         f.write(os.linesep)
    #
    # def on_timeout(self):
    #     return False
global count
count = 0
@csrf_exempt
def init_index(request):
    return render(request, 'index.html')


@csrf_exempt
def notifications(request):
    body = json.loads(request.body.decode("utf-8"))
    print(type(body))
    hdr = body['Type']

    if hdr == 'SubscriptionConfirmation':
        url = body['SubscribeURL']
        print("Subscription Confirmation - Visiting URL : " + url)
        http = urllib3.PoolManager()
        r = http.request('GET', url)
        print(r.status)

    if hdr == 'Notification':
        print("SNS Notification")
        tweet = json.loads(body['Message'])
        es.index(index='stweets', doc_type='tweet', id=tweet["id"], body=tweet)
        count += 1

        if not tweetQueue.full() and flag == False:
            tweetQueue.put(tweet)
            print("Tweet in queue")

    return HttpResponse(status=200)



#streamer = tweepy.Stream(auth=auth, listener=StreamListener())
@csrf_protect
def filter(request):
    if request.method == "POST":
        #print(request.POST)
        if 'searchname' in request.POST:
            query = str(request.POST.get('searchname', ''))

            if query is None:
                elasticQuery = {
                    'match_all': {}
                }
            else:
                elasticQuery = {
                    "query_string": {
                        "query": query.lower()
                    }
                }

            searchResult = es.search(index="stweets", body={
                "sort": [{"id": {"order": "desc"}}],
                "size": 500,
                "query": elasticQuery
            })

            # print(searchResult)

            features= []


            try:
                for entry in searchResult['hits']['hits']:
                    result = entry['_source']
                    if 'query' not in str(result):
                        features.append(result)

            except KeyError:
                print("No Results found")

            # print(features)

            pass_list = json.dumps(features)
            print(pass_list)

            return render(request, 'map.html', {
                        "mydata": pass_list,
                     })


            #print(query)
            # try:
            #     l = StreamListener(time_limit=10)
            #     auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            #     auth.set_access_token(access_token, access_secret)
            #     stream = tweepy.Stream(auth, l)
            #     stream.timeout=10
            #     try:
            #         stream.filter(track=[query])
            #     except:
            #         pass
            #
            #     if not os.path.exists('my_file_1'):
            #         return render(request, 'map.html', {
            #             "mydata": []
            #         })
            #     res = file_read()
            #     tweets = []
            #     #print(res)
            #     for i in res:
            #         tweets.append(es.get(index="kunal", doc_type='tweet', id=i))
            #     #print(tweets)
            #     pass_list = json.dumps(tweets)
            #     print(pass_list)
            #     if os.path.exists('my_file_1'):
            #         os.remove('my_file_1')
            #     return render(request, 'map.html', {
            #         "mydata" : pass_list
            #     })
            # except (AttributeError, ValueError) as v:
            #     print(v)
            #     return HttpResponse('Error')
            #

#Renders the index.html on startup


# #Read the file consisting of tweets  and index it using elasticsearch
# def file_read():
#     with open('my_file_1') as f:
#         result = []
#         for line in f:
#             try:
#                 data = json.loads(line)
#                 place = data.get('place')
#                 coordinates = (place.get('bounding_box').get('coordinates')[0])[0]
#                 lat = coordinates[0]
#                 lng = coordinates[1]
#                 lng = Decimal(("%0.5f" % lng))
#                 lat = Decimal(("%0.5f" % lat))
#                 doc = {
#                 "timestamp":datetime.now(),
#                 "location":{
#                     "lat":lng,
#                     "lon":lat
#                 } ,
#                 "title":data.get('text')
#                 }
#               #  print(data)
#                 es.index(index="kunal", doc_type='tweet', id=data.get('id'), body=doc)
#                 result.append(data.get('id'))
#             except:
#                 #print('Error Here')
#                 pass
#     return result
