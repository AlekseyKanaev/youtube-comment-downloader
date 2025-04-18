import json
import sys
import time
import traceback
import html
import re
import yaml
import pika
# from kafka import KafkaProducer
from polyglot.detect import Detector

# import clickhouse_driver.dbapi.cursor
# from clickhouse_driver import connect as ClickhouseConnect
from .downloader import YoutubeCommentDownloader, SORT_BY_POPULAR, SORT_BY_RECENT

INDENT = 4

comments_table = "comments2"
timedRE = re.compile("\d?\d:\d\d(:\d\d)?")
config_path = "comment_downloader_config.yml"
comments_parsed_bytes_queue = "comments_parsed_bytes"
commenters_queue = "commenters"
comments_queue = "comments"
video_crawler_jobs_queue = "videos_crawler_jobs"
video_parsed_queue = "video_parsed"

commenters_topic = "commenters"


def to_json(comment, indent=None):
    comment_str = json.dumps(comment, ensure_ascii=False, indent=indent)
    if indent is None:
        return comment_str
    padding = ' ' * (2 * indent) if indent else ''
    return ''.join(padding + line for line in comment_str.splitlines(True))


# def insert_comments_clickhouse(comments, cur: clickhouse_driver.dbapi.cursor.Cursor):
#     query = 'INSERT INTO  comments_grn_1024_test2 (id,video_youtube_id,commenter_youtube_id,text,votes_number,heart,reply,channel_id,timed,lang) VALUES'
#
#     cur.executemany(query, comments)


def enqueue_comments(comments, rabbit_channel):
    msg = json.dumps(comments)
    rabbit_channel.basic_publish(exchange='',
                                 routing_key=comments_queue,
                                 body=msg)
    del msg


def enqueue_commenters(commenters, rabbit_channel):
    msg = json.dumps(commenters)
    rabbit_channel.basic_publish(exchange='',
                                 routing_key=commenters_queue,
                                 body=msg)
    # properties=pika.BasicProperties(
    #     delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
    # ))
    del msg


def kafka_send_commenters(commenters, producer):
    msg = json.dumps(commenters).encode('utf-8')
    producer.send(commenters_topic, msg)
    del msg


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


def get_lang_code(text: str) -> str:
    try:
        languages = Detector(text).languages
        if len(languages) > 0:
            return languages[0].code

    except Exception as e:
        # print('short_comment_error:', str(e))
        # print(text)

        return ""

    return ""


def get_rabbit_connection(cfg: dict) -> pika.BlockingConnection:
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
            return rabbit_connection
        except Exception as exp:
            print('rabbit_connect_error:', str(exp))
            time.sleep(120)


def get_kafka_connection(cfg: dict):
    while True:
        try:
            # kafka_p = KafkaProducer(bootstrap_servers=cfg["kafka"]["brokers"],
            #                         sasl_plain_username=cfg["kafka"]["user"],
            #                         sasl_plain_password=cfg["kafka"]["pass"],
            #                         security_protocol="SASL_PLAINTEXT",
            #                         sasl_mechanism="PLAIN")

            break
        except Exception as e:
            print('kafka_connect_error:', str(e))
            time.sleep(120)


def download_comments(video_id: str, channel_id: str, sort: str, language: str, host: str, rabbit_channel):
    downloader = YoutubeCommentDownloader()
    generator = downloader.get_comments(video_id, sort, language)

    count = 1


    comments_batch = []
    commentators_batch_enqueue = {}
    commentators_batch_size = 1000
    comments_batch_size = 20000
    comment = next(generator, None)
    bytes_sum = 0
    while comment:
        # comment_str = to_json(comment, indent=INDENT if pretty else None)
        comment = next(generator, None)  # Note that this is the next comment
        # sys.stdout.write('Downloaded %d comment(s)\r' % count)
        # sys.stdout.flush()

        if comment is not None:
            count += 1
            bytes_sum += len(comment["text"].encode('utf-8'))

            commentators_batch_enqueue[comment["channel"]] = {
                "commenter_id": comment["channel"],
                "name_id": comment["author"],
                "photo_url": comment["photo"],
            }
            if len(commentators_batch_enqueue) == commentators_batch_size:
                enqueue_commenters(commentators_batch_enqueue, rabbit_channel)
                del commentators_batch_enqueue
                commentators_batch_enqueue = {}

            timed_res = timedRE.match(comment["text"])
            timed = False
            if timed_res:
                timed = True

            unescp_text = html.unescape(comment["text"])
            comments_batch.append({
                "id": comment["cid"],
                "video_id": video_id,
                "commenter_id": comment["channel"],
                "text": unescp_text,
                "votes": parse_votes(comment["votes"]),
                "hearted": comment["heart"],
                "reply": comment["reply"],
                "channel_id": channel_id,
                "timed": timed,
                "lang": get_lang_code(unescp_text)
            })
            if len(comments_batch) == comments_batch_size:
                enqueue_comments(comments_batch, rabbit_channel)
                del comments_batch
                comments_batch = []

    if len(comments_batch) > 0:
        enqueue_comments(comments_batch, rabbit_channel)
    if len(commentators_batch_enqueue) > 0:
        enqueue_commenters(commentators_batch_enqueue, rabbit_channel)

    msg = json.dumps({'bytes_sum': bytes_sum, 'host': host})
    rabbit_channel.basic_publish(exchange='',
                                 routing_key=comments_parsed_bytes_queue,
                                 body=msg.encode())

    rabbit_channel.basic_publish(exchange='',
                                 routing_key=video_parsed_queue,
                                 body=video_id)


def msg_handler_closure(host):
    def msg_handler(ch, method, properties, body):
        try:
            video_parse = json.loads(body.decode('utf-8'))

            eprint(video_parse)


            # youtube_id = args.youtubeid
            # channel_id = args.channel_id
            # host = args.host

            download_comments(video_parse['video_id'],
                              video_parse['channel_id'],
                              video_parse['sort'],
                              video_parse['lang'],
                              host,
                              ch
                              )

            ch.basic_publish(exchange='',
                             routing_key=video_parsed_queue,
                             body=video_parse['youtube_id'])
        except Exception as e:
            # todo: log
            print(traceback.format_exc())
            print('python_error:', str(e))

            ch.basic_publish(exchange='',
                             routing_key=video_crawler_jobs_queue,
                             body=body)

    return msg_handler


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main(argv=None):
    cfg = yaml.safe_load(open(config_path))

    rabbit_connection = get_rabbit_connection(cfg)
    rabbit_channel = rabbit_connection.channel()

    rabbit_channel.basic_consume(queue=video_crawler_jobs_queue, on_message_callback=msg_handler_closure(cfg['host']),
                                 auto_ack=False)

    eprint(' [*] Waiting for messages. To exit press CTRL+C')
    rabbit_channel.start_consuming()

    rabbit_channel.close()
    rabbit_connection.close()

