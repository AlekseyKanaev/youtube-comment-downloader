import argparse
import io
import json
import os
import sys
import time
import traceback

from clickhouse_driver import Client
import psycopg2
from psycopg2.extras import execute_batch
from .downloader import YoutubeCommentDownloader, SORT_BY_POPULAR, SORT_BY_RECENT

INDENT = 4

table = "comments_grn_1024"

def to_json(comment, indent=None):
    comment_str = json.dumps(comment, ensure_ascii=False, indent=indent)
    if indent is None:
        return comment_str
    padding = ' ' * (2 * indent) if indent else ''
    return ''.join(padding + line for line in comment_str.splitlines(True))


def insert_comments(comments, cur):
    execute_batch(cur, """INSERT INTO comments_partitioned
                                  (id,video_youtube_id,commentator_youtube_id,text,votes,votes_number,heart,reply)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (id) DO UPDATE
                            SET text=EXCLUDED.text,
                            votes=EXCLUDED.votes,heart=EXCLUDED.heart,reply=EXCLUDED.reply,
                            parsed=NOW(),votes_number=EXCLUDED.votes_number
                             """, comments, 1000)


def insert_comments_clickhouse(comments, cur):
    execute_batch(cur, """INSERT INTO comments_partitioned
                                  (id,video_youtube_id,commentator_youtube_id,text,votes,votes_number,heart,reply)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (id) DO UPDATE
                            SET text=EXCLUDED.text,
                            votes=EXCLUDED.votes,heart=EXCLUDED.heart,reply=EXCLUDED.reply,
                            parsed=NOW(),votes_number=EXCLUDED.votes_number
                             """, comments, 1000)


def insert_commentators(commentators, cur):
    execute_batch(cur, """ 
                            INSERT INTO commentators_partitioned
                        (youtube_id,name_id,name,avatar_url)
                        VALUES (%s,%s,'',%s)
                        ON CONFLICT (youtube_id) DO NOTHING
                             """, commentators, 1000)


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
    parser.add_argument('--output', '-o', help='Output filename (output format is line delimited JSON)')
    parser.add_argument('--pretty', '-p', action='store_true', help='Change the output format to indented JSON')
    parser.add_argument('--limit', '-l', type=int, help='Limit the number of comments')
    parser.add_argument('--language', '-a', type=str, default=None,
                        help='Language for Youtube generated text (e.g. en)')
    parser.add_argument('--sort', '-s', type=int, default=SORT_BY_RECENT,
                        help='Whether to download popular (0) or recent comments (1). Defaults to 1')

    while True:
        try:
            conn = psycopg2.connect(dbname='youtube_28_04', user='admin',
                                    password='123456', host='192.168.0.151')
            conn.autocommit = True
            cursor = conn.cursor()

            break
        except Exception as e:
            print('Error:', str(e))
            time.sleep(120)

    try:
        args = parser.parse_args() if argv is None else parser.parse_args(argv)

        youtube_id = args.youtubeid
        youtube_url = args.url
        output = args.output
        limit = args.limit
        pretty = args.pretty

        if (not youtube_id and not youtube_url) or not output:
            parser.print_usage()
            raise ValueError('you need to specify a Youtube ID/URL and an output filename')

        if os.sep in output:
            outdir = os.path.dirname(output)
            if not os.path.exists(outdir):
                os.makedirs(outdir)

        print('Downloading Youtube comments for', youtube_id or youtube_url)
        downloader = YoutubeCommentDownloader()
        generator = (
            downloader.get_comments(youtube_id, args.sort, args.language)
            if youtube_id
            else downloader.get_comments_from_url(youtube_url, args.sort, args.language)
        )

        count = 1
        with io.open(output, 'w', encoding='utf8') as fp:
            sys.stdout.write('Downloaded %d comment(s)\r' % count)
            sys.stdout.flush()
            start_time = time.time()

            if pretty:
                fp.write('{\n' + ' ' * INDENT + '"comments": [\n')

                # this approach is used to return an id on conflict, otherwise nothing is returned
            cursor.execute("""
                    INSERT INTO videos
                        (youtube_id)
                        VALUES (%s)
                        ON CONFLICT (youtube_id)
                        DO NOTHING;
                             """, (
                args.youtubeid,
            ))
            # cursor.execute("SELECT id FROM videos WHERE youtube_video_id=%s;", (
            #     args.youtubeid,
            # ))  # todo: can be removed because videos insert is done golang
            # video_id = cursor.fetchone()[0]
            # fetch = cursor.fetchone()
            video_id = args.youtubeid
            # if fetch is None:
            #     print("@@@@@@@@@@@@@@@@@@@@@@@@")
            #     print(args.youtubeid)
            #     print("@@@@@@@@@@@@@@@@@@@@@@@@")
            #     # conn.commit()

            comments_batch = []
            commentators_batch = []
            commentators_batch_size = 1000
            comments_batch_size = 1000
            comment = next(generator, None)
            while comment:
                comment_str = to_json(comment, indent=INDENT if pretty else None)
                comment = None if limit and count >= limit else next(generator,
                                                                     None)  # Note that this is the next comment
                comment_str = comment_str + ',' if pretty and comment is not None else comment_str
                print(comment_str.decode('utf-8') if isinstance(comment_str, bytes) else comment_str, file=fp)
                sys.stdout.write('Downloaded %d comment(s)\r' % count)
                sys.stdout.flush()
                count += 1

                if comment is not None:
                    if len(commentators_batch) < commentators_batch_size:
                        commentators_batch.append((
                            comment["channel"],
                            comment["author"],
                            comment["photo"],
                        ))
                    else:
                        insert_commentators(commentators_batch, cursor)
                        commentators_batch = []

                    # this approach is used to return an id on conflict, otherwise nothing is returned
                    # cursor.execute("""
                    #     INSERT INTO commentators_partitioned
                    #     (youtube_id,name_id,name,avatar_url)
                    #     VALUES (%s,%s,'',%s)
                    #     ON CONFLICT (youtube_id) DO NOTHING
                    # """, (
                    #     comment["channel"],
                    #     comment["author"],
                    #     comment["photo"],
                    # ))
                    # cursor.execute("SELECT id FROM commentators_partitioned2 WHERE youtube_id=%s;", (
                    #     comment["channel"],
                    # ))    # todo: do this before insert to avoid unnecessary INSERT
                    #  conn.commit()
                    # fetch = cursor.fetchone()

                    # if fetch is None:
                    # print("!!!!!!!!!!!!!!!!!!!!!!!!")
                    # print(comment["channel"])
                    # print(comment["author"])
                    # print(args.youtubeid)
                    # print("!!!!!!!!!!!!!!!!!!!!!!!!")

                    # if fetch:
                    #     commentator_id = fetch[0]
                    # else:
                    #     commentator_id = None

                    if len(comments_batch) < comments_batch_size:
                        comments_batch.append((
                            comment["cid"],
                            video_id,
                            comment["channel"],
                            comment["text"],
                            comment["votes"],
                            parse_votes(comment["votes"]),
                            comment["heart"],
                            comment["reply"],
                        ))
                    else:
                        insert_comments(comments_batch, cursor)
                        comments_batch = []

                    # cursor.execute("""
                    # INSERT INTO comments_partitioned
                    #       (id,video_youtube_id,commentator_youtube_id,text,votes,votes_number,heart,reply)
                    # VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    # ON CONFLICT (id) DO UPDATE
                    # SET text=EXCLUDED.text,
                    # votes=EXCLUDED.votes,heart=EXCLUDED.heart,reply=EXCLUDED.reply,
                    # parsed=NOW(),votes_number=EXCLUDED.votes_number
                    #  """, (
                    #     comment["cid"],
                    #     video_id,
                    #     comment["channel"],
                    #     comment["text"],
                    #     comment["votes"],
                    #     parse_votes(comment["votes"]),
                    #     comment["heart"],
                    #     comment["reply"],
                    # ))
                    #
                    # conn.commit()
            if len(comments_batch) > 0:
                insert_comments(comments_batch, cursor)
            if len(commentators_batch) > 0:
                insert_commentators(commentators_batch, cursor)

            if pretty:
                fp.write(' ' * INDENT + ']\n}')
        print('\n[{:.2f} seconds] Done!'.format(time.time() - start_time))

        cursor.close()
        conn.close()
    except Exception as e:
        print(traceback.format_exc())
        print('Error:', str(e))
        sys.exit(1)
