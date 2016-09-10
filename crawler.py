#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'puxama'

import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
#from twitter.config import ckey, csecret, atoken, asecret
import codecs
import os
import time
import sys



class Listener(StreamListener):


    def on_data(self, data):

        print data
        try:
            decoded = json.loads(data)
        except Exception as e:
            print e
            return True

        if decoded.get('geo') is not None:
            location = decoded.get('geo').get('coordinates')
        else:
            location = '[,]'

        text = decoded['text'].replace('\n',' ')
        user = '@' + decoded.get('user').get('screen_name')
        created = decoded.get('created_at')
        timestamp = decoded.get('timestamp_ms')
        source = decoded.get('source')
        tweet = '%s|%s|%s|%s|%s|%s\n' % (user, location, created, text, timestamp, source)
        print tweet
        file.write(tweet)

        return True

    def on_error(self, status):
        print status


def start_stream():
    while True:
        try:
            twitterStream = Stream(auth, Listener())
            twitterStream.filter(track=['ClaroPeru', 'MovistarPeru', 'soportemovistar', 'EntelPeru', 'bitelperu'])
        except:
            print 'El proceso se durmio 5m'
            time.sleep(300)
            continue

if __name__ == '__main__':
    print 'Empezando...'
    #file = codecs.open(str(sys.argv[1]), 'a', 'utf-8')
    file = codecs.open('tweets.txt', 'a', 'utf-8')
    tweet = '%s|%s|%s|%s|%s|%s\n' % ('user', 'location', 'created', 'text', 'timestamp', 'source')
    auth = OAuthHandler(os.environ['CKEY'], os.environ['CSECRET'])
    auth.set_access_token(os.environ['ATOKEN'], os.environ['ASECRET'])

    start_stream()



