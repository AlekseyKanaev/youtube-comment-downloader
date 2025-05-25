import json
import os
import sys
import time
import traceback
import html
import re
import pika
from polyglot.detect import Detector

from .downloader import YoutubeCommentDownloader, SORT_BY_POPULAR, SORT_BY_RECENT, YOUTUBE_VIDEO_URL

INDENT = 4

comments_table = "comments2"
timedRE = re.compile(r'\d?\d:\d\d(:\d\d)?')
comments_parsed_bytes_metrics_queue = "comments_parsed_bytes_metrics"
comments_parsed_metrics_queue = "comments_parsed_metrics"
commenters_queue = "commenters"
comments_queue = "comments"
video_crawler_jobs_queue = "videos_crawler_jobs"
video_parsed_queue = "video_parsed"

commenters_topic = "commenters"

ENV_RABBIT_USER = "RABBIT_USER"
ENV_RABBIT_PASS = "RABBIT_PASSWORD"
ENV_RABBIT_HOST = "RABBIT_HOST"
ENV_RABBIT_PORT = "RABBIT_PORT"

ENV_APP_INSTANCE = "APP_INSTANCE"


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
    # dt = datetime.now()
    #
    # # getting the timestamp
    # ts = datetime.timestamp(dt)
    #
    # eprint(str(votes), ts)
    # print(votes)

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


def get_rabbit_connection() -> pika.BlockingConnection:
    while True:
        try:
            rabbit_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    heartbeat=600,
                    blocked_connection_timeout=300,
                    host=os.getenv(ENV_RABBIT_HOST),
                    port=int(os.getenv(ENV_RABBIT_PORT)),
                    credentials=pika.PlainCredentials(os.getenv(ENV_RABBIT_USER),
                                                      os.getenv(ENV_RABBIT_PASS))))
            return rabbit_connection
        except Exception as exp:
            eprint('rabbit_connect_error:', str(exp))
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
            eprint('kafka_connect_error:', str(e))
            time.sleep(120)


def download_comments(video_id: str, channel_id: str, sort: str, language: str, host: str, rabbit_channel):
    downloader = YoutubeCommentDownloader()
    # generator = downloader.get_comments(video_id)
    generator = downloader.get_comments_from_url(YOUTUBE_VIDEO_URL.format(youtube_id=video_id),
                                                 language="en")

    comments_batch = []
    commentators_batch_enqueue = {}
    commentators_batch_size = 100
    comments_batch_size = 100
    comment = next(generator, None)
    bytes_sum = 0
    comments_sum = 1
    while comment:
        # comment_str = to_json(comment, indent=INDENT if pretty else None)
        comment = next(generator, None)  # Note that this is the next comment
        # sys.stdout.write('Downloaded %d comment(s)\r' % count)
        # sys.stdout.flush()

        if comment is not None:
            comments_sum += 1
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
                                 routing_key=comments_parsed_bytes_metrics_queue,
                                 body=msg.encode())

    msg2 = json.dumps({'comments_sum': comments_sum, 'host': host})
    rabbit_channel.basic_publish(exchange='',
                                 routing_key=comments_parsed_metrics_queue,
                                 body=msg2.encode())

    rabbit_channel.basic_publish(exchange='',
                                 routing_key=video_parsed_queue,
                                 body=video_id)


def msg_handler_closure(host):
    def msg_handler(ch, method, properties, body):
        try:
            video_parse = json.loads(body.decode('utf-8'))

            eprint(video_parse)

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


def parse_video(chan, host, body):
    try:
        video_parse = json.loads(body.decode('utf-8'))

        eprint("parsing video... ", video_parse['video_id'])

        download_comments(video_parse['video_id'],
                          video_parse['channel_id'],
                          video_parse['sort'],
                          video_parse['lang'],
                          host,
                          chan
                          )

        chan.basic_publish(exchange='',
                           routing_key=video_parsed_queue,
                           body=video_parse['video_id'])
    except Exception as e:
        # todo: log
        eprint(traceback.format_exc())
        eprint('python_error:', str(e))

        chan.basic_publish(exchange='',
                           routing_key=video_crawler_jobs_queue,
                           body=body)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main(argv=None):
    # cfg = yaml.safe_load(open(config_path))

    rabbit_connection = get_rabbit_connection()
    rabbit_channel = rabbit_connection.channel()
    rabbit_channel.basic_qos(prefetch_count=1)

    app_instance = os.getenv(ENV_APP_INSTANCE)

    for method_frame, properties, body in rabbit_channel.consume(video_crawler_jobs_queue):
        rabbit_channel.basic_ack(method_frame.delivery_tag)

        parse_video(rabbit_channel, app_instance, body)

    rabbit_channel.close()
    rabbit_connection.close()
