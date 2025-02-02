import argparse
import json
import sys
import time
import traceback
import html
import re
import yaml
import pika
from kafka import KafkaProducer

import clickhouse_driver.dbapi.cursor
from clickhouse_driver import connect as ClickhouseConnect
import psycopg2
from psycopg2.extras import execute_batch
from .downloader import YoutubeCommentDownloader, SORT_BY_POPULAR, SORT_BY_RECENT

INDENT = 4

comments_table = "comments2"
timedRE = re.compile("\d?\d:\d\d(:\d\d)?")
config_path = "comment_downloader_config.yml"
comments_parsed_bytes_queue = "comments_parsed_bytes"
commenters_queue = "commenters"
commenters_topic = "commenters"


def to_json(comment, indent=None):
    comment_str = json.dumps(comment, ensure_ascii=False, indent=indent)
    if indent is None:
        return comment_str
    padding = ' ' * (2 * indent) if indent else ''
    return ''.join(padding + line for line in comment_str.splitlines(True))


def insert_comments(comments, cur):
    execute_batch(cur, """
    INSERT INTO comments_partitioned
        (id,video_youtube_id,commentator_youtube_id,text,votes_number,heart,reply,channel_id,timed)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO UPDATE
    SET text=EXCLUDED.text,
        votes=EXCLUDED.votes,heart=EXCLUDED.heart,timed=EXCLUDED.timed,
        reply=EXCLUDED.reply,channel_id=EXCLUDED.channel_id,
        parsed=NOW(),votes_number=EXCLUDED.votes_number
    """, comments, 1000)


def insert_comments_clickhouse(comments, cur: clickhouse_driver.dbapi.cursor.Cursor):
    query = 'INSERT INTO  comments_grn_1024_test2 (id,video_youtube_id,commenter_youtube_id,text,votes_number,heart,reply,channel_id,timed) VALUES'

    cur.executemany(query, comments)


def enqueue_commenters(commenters, rabbit_channel):
    msg = json.dumps(commenters)
    rabbit_channel.basic_publish(exchange='',
                                 routing_key=commenters_queue,
                                 body=msg,
                                 properties=pika.BasicProperties(
                                     delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                                 ))

def kafka_send_commenters(commenters, producer):
    msg = json.dumps(commenters).encode('utf-8')
    producer.send(commenters_topic, msg)

# def insert_commentators(commentators, cur):
#     commentators_list = []
#     for k in commentators:
#         commentators_list.append(commentators[k])
#
#     # print(commentators_list)
#
#     execute_batch(cur, """
#     INSERT INTO commentators_partitioned
#         (youtube_id,name_id,name,avatar_url)
#     VALUES (%s,%s,'',%s)
#     ON CONFLICT (youtube_id) DO NOTHING
#     """, commentators_list, 1000)


def parse_votes(votes):
    multiply = False
    if votes[-1] == "K":
        votes = votes[:-1]
        multiply = True

    if multiply:
        split = votes.split(".")
        if len(split) == 1:
            out = int(votes) * 1000
        else:
            out = int(split[0] + split[1]) * 100
    else:
        out = int(votes)

    return out


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False,
                                     description=('Download Youtube comments without using the Youtube API'))
    parser.add_argument('--help', '-h', action='help', default=argparse.SUPPRESS,
                        help='Show this help message and exit')
    parser.add_argument('--youtubeid', '-y', help='ID of Youtube video for which to download the comments')
    parser.add_argument('--url', '-u', help='Youtube URL for which to download the comments')
    parser.add_argument('--limit', '-l', type=int, help='Limit the number of comments')
    parser.add_argument('--language', '-a', type=str, default=None,
                        help='Language for Youtube generated text (e.g. en)')
    parser.add_argument('--sort', '-s', type=int, default=SORT_BY_RECENT,
                        help='Whether to download popular (0) or recent comments (1). Defaults to 1')
    parser.add_argument('--channel_id', '-c', type=str,
                        help='channel id')

    cfg = yaml.safe_load(open(config_path))

    while True:
        try:
            kafka_p = KafkaProducer(bootstrap_servers=cfg["kafka"]["brokers"],
                                    sasl_plain_username=cfg["kafka"]["user"],
                                    sasl_plain_password=cfg["kafka"]["pass"],
                                    security_protocol="SASL_PLAINTEXT",
                                    sasl_mechanism="PLAIN")

            break
        except Exception as e:
            print('kafka_connect_error:', str(e))
            time.sleep(120)

    while True:
        try:
            url_str = "clickhouse://{0}:{1}@{2}:{3}".format(
                cfg["clickhouse"]["user"],
                cfg["clickhouse"]["pass"],
                cfg["clickhouse"]["host"],
                cfg["clickhouse"]["port"]
            )
            clickhouse_conn = ClickhouseConnect(url_str)
            clickhouseCursor = clickhouse_conn.cursor()

            break
        except Exception as e:
            print('clickhouse_connect_error:', str(e))
            time.sleep(120)

    while True:
        try:
            rabbit_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    heartbeat=600,
                    blocked_connection_timeout=300,
                    host=cfg["rabbit"]["host"],
                    port=cfg["rabbit"]["port"],
                    credentials=pika.PlainCredentials(cfg["rabbit"]["user"],
                                                      cfg["rabbit"]["pass"])))
            rabbit_channel = rabbit_connection.channel()
            break
        except Exception as exp:
            print('rabbit_connect_error:', str(exp))
            time.sleep(120)

    try:
        args = parser.parse_args() if argv is None else parser.parse_args(argv)

        youtube_id = args.youtubeid
        youtube_url = args.url
        limit = args.limit
        channel_id = args.channel_id

        if not youtube_id and not youtube_url:
            parser.print_usage()
            raise ValueError('you need to specify a Youtube ID/URL')

        downloader = YoutubeCommentDownloader()
        generator = (
            downloader.get_comments(youtube_id, args.sort, args.language)
            if youtube_id
            else downloader.get_comments_from_url(youtube_url, args.sort, args.language)
        )

        count = 1
        # start_time = time.time()

        video_id = args.youtubeid

        comments_batch = []
        # commentators_batch = {}
        commentators_batch_enqueue = {}
        commentators_batch_size = 1000
        comments_batch_size = 20000
        comment = next(generator, None)
        bytes_sum = 0
        while comment:
            # comment_str = to_json(comment, indent=INDENT if pretty else None)
            comment = None if limit and count >= limit else next(generator,
                                                                 None)  # Note that this is the next comment
            # sys.stdout.write('Downloaded %d comment(s)\r' % count)
            # sys.stdout.flush()

            if comment is not None:
                count += 1
                bytes_sum += len(comment["text"].encode('utf-8'))

                if len(commentators_batch_enqueue) < commentators_batch_size:
                    # commentators_batch[comment["channel"]] = (
                    #     comment["channel"],
                    #     comment["author"],
                    #     comment["photo"],
                    # )
                    commentators_batch_enqueue[comment["channel"]] = {
                        "commenter_id": comment["channel"],
                        "name_id": comment["author"],
                        "photo_url": comment["photo"],
                    }
                else:
                    enqueue_commenters(commentators_batch_enqueue, rabbit_channel)
                    kafka_send_commenters(commentators_batch_enqueue, kafka_p)
                    commentators_batch_enqueue = {}
                if len(comments_batch) < comments_batch_size:
                    timed_res = timedRE.match(comment["text"])
                    timed = False
                    if timed_res:
                        timed = True

                    comments_batch.append((
                        comment["cid"],
                        video_id,
                        comment["channel"],
                        html.unescape(comment["text"]),
                        parse_votes(comment["votes"]),
                        comment["heart"],
                        comment["reply"],
                        channel_id,
                        timed,
                    ))
                else:
                    insert_comments_clickhouse(comments_batch, clickhouseCursor)
                    comments_batch = []

        if len(comments_batch) > 0:
            insert_comments_clickhouse(comments_batch, clickhouseCursor)
        # if len(commentators_batch) > 0:
            # insert_commentators(commentators_batch, cursor)
            # enqueue_commenters(commentators_batch_enqueue, rabbit_channel)
        if len(commentators_batch_enqueue) > 0:
            enqueue_commenters(commentators_batch_enqueue, rabbit_channel)
            kafka_send_commenters(commentators_batch_enqueue, kafka_p)

        msg = json.dumps({'bytes_sum': bytes_sum})
        rabbit_channel.basic_publish(exchange='',
                                     routing_key=comments_parsed_bytes_queue,
                                     body=msg)

        # print('\n[{:.2f} seconds] Done!'.format(time.time() - start_time))

        rabbit_channel.close()
        rabbit_connection.close()

        clickhouseCursor.close()
        clickhouse_conn.close()
    except Exception as e:
        print(traceback.format_exc())
        print('python_error:', str(e))
        sys.exit(1)
