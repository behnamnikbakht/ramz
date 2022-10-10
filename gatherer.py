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
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s", "%Y-%m-%d %H:%M:%S")
    #file_handler = logging.FileHandler(filename="logs/twit.log")
    file_handler = RotatingFileHandler(filename=path + "/twit.log", maxBytes=67108864, backupCount=1500)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
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




def append_to_dataset(tweet, data_file):
    s = "\t".join(["none" if k is None else str(k).replace("\n", " ").replace("\t", " ").replace("\r", " ").replace("  ", " ") for k in tweet])
    with open(data_file, "a") as f:
        f.write(s + "\n")


def extract_from_tweet(item):
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
        item.geo
    ]


def retrieve(args):
    path = args.path + "/data"
    Path(path).mkdir(parents=True, exist_ok=True)
    data_file = path + "/dataset.csv"
    logger.info("data path = {}".format(data_file))
    last_id = args.last_id
    if last_id is None:
        last_id = sys.maxsize
    logger.info("Start with last_id = {}".format(last_id))
    q = "#mahsa_amini OR #mahsaamini OR #مهسا_امینی"
    i = 1
    j = 0
    api = build_api(args)
    while True:
        has_item = False
        logger.info("Retrieve new set with last_id = {}".format(last_id))
        try:
            for item in tweepy.Cursor(api.search_tweets, q, max_id=last_id, tweet_mode='extended').items(
                    args.page_size):
                last_id = min(last_id, item.id - 1)
                has_item = True
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--last_id', dest='last_id', type=int, help='Last tweet ID')
    parser.add_argument('--page_size', dest='page_size', type=int, help='Number of tweets in each request', default=2500)
    parser.add_argument('--page_count', dest='page_count', type=int, help='Number of pages')
    parser.add_argument('--sleep', dest='sleep', type=int, help='Time to sleep between iterations (in second)', default=900)
    parser.add_argument('--consumer_key', dest='consumer_key', type=str, help='API Auth consumer_key',
                        default=os.getenv("consumer_key"))
    parser.add_argument('--consumer_secret', dest='consumer_secret', type=str, help='API Auth consumer_secret',
                        default=os.getenv("consumer_secret"))
    parser.add_argument('--access_token', dest='access_token', type=str, help='API Auth access_token',
                        default=os.getenv("access_token"))
    parser.add_argument('--access_token_secret', dest='access_token_secret', type=str,
                        help='API Auth access_token_secret', default=os.getenv("access_token_secret"))
    parser.add_argument('--path', dest='path', type=str, help='Output Path', default=".")
    args = parser.parse_args()

    setup_log(args)

    logger.info("args = {}".format(args))

    retrieve(args)
