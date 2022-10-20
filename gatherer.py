import argparse
import sys
import os
import time
from pathlib import Path

import tweepy
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger("tweepy")


def setup_log(args):
    path = args.path + "/logs"
    Path(path).mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s", "%Y-%m-%d %H:%M:%S")
    # file_handler = logging.FileHandler(filename="logs/twit.log")
    # file_handler = RotatingFileHandler(filename=path + "/twit.log", maxBytes=16777216, backupCount=6000)
    # file_handler.setLevel(logging.INFO)
    # file_handler.setFormatter(formatter)
    # logger.addHandler(file_handler)
    shell_handler = logging.StreamHandler()
    shell_handler.setFormatter(formatter)
    shell_handler.setLevel(logging.INFO)
    logger.addHandler(shell_handler)


def build_api(args):
    consumer_key = args.consumer_key
    consumer_secret = args.consumer_secret
    access_token = args.access_token
    access_token_secret = args.access_token_secret
    auth = tweepy.OAuth1UserHandler(
        consumer_key, consumer_secret, access_token, access_token_secret
    )

    return tweepy.API(auth)


appended = []


def write_appended(data_file):
    with open(data_file, "a") as f:
        f.write("\n".join(appended) + "\n")
        appended.clear()


def append_to_dataset(tweet, data_file):
    s = "\t".join(
        ["none" if k is None else str(k).replace("\n", " ").replace("\t", " ").replace("\r", " ").replace("  ", " ") for
         k in tweet])
    appended.append(s)
    if len(appended) > 100:
        write_appended(data_file)


def extract_from_tweet(item, archive=True):
    if archive:
        return [
            item.author.created_at,
            item.author.description,
            item.author.favourites_count,
            item.author.followers_count,
            item.author.friends_count,
            item.author.id,
            item.author.location,
            item.author.name,
            item.created_at,
            None if (item.entities is None or item.entities["hashtags"] is None) else ",".join(
                [h["text"] for h in item.entities["hashtags"]]),
            item.full_text,
            item.id,
            item.lang,
            item.in_reply_to_user_id,
            item.retweet_count,
            item.source,
            item.geo,
            item.retweeted_status.id if hasattr(item, "retweeted_status") else None
        ]
    else:
        return [
            item.id,
            item.author_id,
            item.created_at,
            item.lang,
            item.source,
            item.text
        ]


def set_path(args):
    path = args.path + "/data"
    Path(path).mkdir(parents=True, exist_ok=True)
    data_file = path + "/dataset_{}.csv".format("archive" if args.archive else "stream")
    logger.info("data path = {}".format(data_file))
    return data_file


def archive(args):
    q = "(#mahsa_amini OR #mahsaamini OR #مهسا_امینی) AND (-filter:retweets AND -filter:replies)"
    data_file = set_path(args)
    last_id = args.last_id
    if last_id is None:
        last_id = sys.maxsize
    logger.info("Start with last_id = {}".format(last_id))
    i = 1
    j = 0
    api = build_api(args)
    while True:
        logger.info("Retrieve new set with last_id = {}".format(last_id))
        try:
            for item in tweepy.Cursor(api.search_tweets, q, max_id=last_id, tweet_mode='extended').items(
                    args.page_size):
                last_id = min(last_id, item.id - 1)
                logger.info("Retrieve item {}, iteration = {}, id = {}".format(j, i, item.id))
                logger.debug("Retrieved item is {}".format(item))
                try:
                    tweet = extract_from_tweet(item)
                    append_to_dataset(tweet, data_file)
                except Exception as ex:
                    logger.error("Error {} occurred for item {}".format(ex, item))
                j += 1
        except tweepy.errors.TooManyRequests:
            logger.error("Too many requests error occurred at iteration {}".format(i))
        except Exception as ex:
            logger.error("Error {} occurred at iteration {}".format(ex, i))
            logger.info("Finish, last_id = {}".format(last_id))
        i = i + 1
        if args.page_count is not None and i > args.page_count:
            logger.info("Finish, last_id = {}".format(last_id))
            break

        logger.info("Sleep iteration = {} by {} seconds, last_id = {}".format(i, args.sleep, last_id))
        time.sleep(args.sleep)


def stream(args):
    data_file = set_path(args)

    class MyStreamListener(tweepy.StreamingClient):
        def __init__(self, bearer_token):
            super().__init__(bearer_token)
            self.i = 0
        def on_tweet(self, item):
            try:
                tweet = extract_from_tweet(item, False)
                append_to_dataset(tweet, data_file)
            except Exception as ex:
                logger.error("Error {} occurred for item {}".format(ex, item))
            self.i += 1
            logger.info("Stream i = {}, id = {}".format(self.i, item.id))

        def on_errors(self, status_code):
            logger.error("Error {} occurred".format(status_code))
            return False

    # stream = MyStreamListener(args.consumer_key, args.consumer_secret, args.access_token, args.access_token_secret)
    # stream.filter(track=["#mahsa_amini", "#mahsaamini", "#مهسا_امینی"])
    # print(stream.sample())

    client = MyStreamListener(args.bearer)
    client.add_rules([tweepy.StreamRule("#mahsa_amini OR #mahsaamini OR #مهسا_امینی")])
    client.filter(threaded=True, tweet_fields=["author_id", "created_at","source","id","lang","text"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--last_id', dest='last_id', type=int, help='Last tweet ID')
    parser.add_argument('--page_size', dest='page_size', type=int, help='Number of tweets in each request',
                        default=2500)
    parser.add_argument('--page_count', dest='page_count', type=int, help='Number of pages')
    parser.add_argument('--sleep', dest='sleep', type=int, help='Time to sleep between iterations (in second)',
                        default=300)
    parser.add_argument('--consumer_key', dest='consumer_key', type=str, help='API Auth consumer_key',
                        default=os.getenv("consumer_key"))
    parser.add_argument('--consumer_secret', dest='consumer_secret', type=str, help='API Auth consumer_secret',
                        default=os.getenv("consumer_secret"))
    parser.add_argument('--access_token', dest='access_token', type=str, help='API Auth access_token',
                        default=os.getenv("access_token"))
    parser.add_argument('--access_token_secret', dest='access_token_secret', type=str,
                        help='API Auth access_token_secret', default=os.getenv("access_token_secret"))
    parser.add_argument('--path', dest='path', type=str, help='Output Path', default=".")
    parser.add_argument('-archive', dest='archive', type=str, nargs="?", help='Archive', default=False, const=True)
    parser.add_argument('--bearer', dest='bearer', type=str, help='Bearer Token', default=os.getenv("bearer"))

    args = parser.parse_args()

    setup_log(args)

    logger.info("args = {}".format(args))

    if args.archive:
        archive(args)
    else:
        stream(args)
