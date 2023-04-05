#pylint: disable=invalid-name
"""Connects to museum Kafka cluster and cleans incoming kiosk data."""
import json
from datetime import datetime
from argparse import ArgumentParser, Namespace
from collections import OrderedDict
from confluent_kafka import Consumer
from dotenv import dotenv_values
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection


def collect_arguments() -> Namespace:
    """Collects command line arguments."""
    desc = "Returns flag if user wants to log data errors."
    arg_parser = ArgumentParser(description=desc)
    arg_parser.add_argument("--l", action='store_true',
                            help='logs errors to error-log.txt')
    arg_parser.add_argument("--e", action='store_true',
                            help='sets auto.offset.reset to earliest')
    arg_parser.add_argument("--n", default=-1,
                            help='number of messages to retrieve')
    arg_parser.add_argument("--t", default='lmnh',
                            help='name of kafka cluster topic')
    return arg_parser.parse_args()

def get_db_connection(env_variables: OrderedDict) -> connection:
    """Establishes connection to database."""
    return psycopg2.connect(user = env_variables["DATABASE_USERNAME"],
                            password = env_variables["DATABASE_PASSWORD"],
                            host = env_variables["DATABASE_IP"],
                            port = env_variables["DATABASE_PORT"],
                            database = env_variables["DATABASE_NAME"])

def log_invalid_message(invalid_message: dict, reason: str, write_to_log: bool) -> None:
    """Either records data errors in txt file or prints them to terminal."""
    print(f"MESSAGE ERROR: {reason}")
    if write_to_log:
        with open('./error-log.txt', 'a+', encoding='utf-8') as log:
            log.write(f"MESSAGE: {invalid_message} \nMESSAGE ERROR: {reason}\n")

def consume_messages(consumer: Consumer, topic: str, conn: connection, 
                     write_to_log: bool, limit: int) -> None:
    consumer.subscribe([topic])
    count = 0
    while count < limit or limit == -1:
            msg = consumer.poll(timeout=1.0)
            count += 1

            if msg is None:
                continue

            value = json.loads(msg.value().decode('utf-8'))
            print(value)
            # Validate time
            try:
                time = datetime.strptime(value["at"], "%Y-%m-%dT%H:%M:%S.%f%z")
                if time.hour < 9 or time.hour > 18:
                    raise ValueError

            except KeyError:
                log_invalid_message(value, 'Missing at key.', write_to_log)
                continue
            except ValueError:
                log_invalid_message(value, 'Museum closed, go home.', write_to_log)
                continue
            except TypeError:
                log_invalid_message(value, 'Invalid time format.', write_to_log)
                continue
            # Validate exhibition id
            try:
                exhibition_id = int(value["site"])
                if exhibition_id not in range(6):
                    raise ValueError

            except KeyError:
                log_invalid_message(value, 'Missing site key.', write_to_log)
                continue
            except ValueError:
                log_invalid_message(value, 'Exhibition id must be between 0 and 5.', write_to_log)
                continue
            except TypeError:
                log_invalid_message(value, 'Invalid exhibition id.', write_to_log)
                continue
            # Validate rating
            try:
                rating = int(value["val"])
                if rating not in range(-1, 5):
                    raise ValueError

            except KeyError:
                log_invalid_message(value, 'Missing val key.', write_to_log)
                continue
            except ValueError:
                log_invalid_message(value, 'Val must be an integer between -1 and 4.', write_to_log)
                continue
            except TypeError:
                log_invalid_message(value, 'Invalid val.', write_to_log)
                continue
            # Validate type
            if rating == -1:
                try:
                    assistance_type_id = int(value["type"])
                    if assistance_type_id not in range(0, 2):
                        raise ValueError

                except ValueError:
                    log_invalid_message(value, 'Type must be either 0 or 1.', write_to_log)
                    continue
                except KeyError:
                    log_invalid_message(value, 'Missing type key.', write_to_log)
                    continue
                except TypeError:
                    log_invalid_message(value, 'Invalid type.', write_to_log)
                    continue
                # Insert call for assistance data
                with conn.cursor(cursor_factory=RealDictCursor) as curs:
                    query = """INSERT INTO exhibition_help (exhibition_id, type_id, called_at)
                                VALUES (%s, %s, %s);"""
                    params = (exhibition_id, assistance_type_id, time)
                    curs.execute(query, params)
                    conn.commit()
                continue
            # Insert exhibition rating data
            with conn.cursor(cursor_factory=RealDictCursor) as curs:
                query = """INSERT INTO exhibition_rating (exhibition_id, value, rated_at)
                            VALUES (%s, %s, %s);"""
                params = (exhibition_id, rating, time)
                curs.execute(query, params)
                conn.commit()

if __name__ == "__main__":
    args = collect_arguments()

    config = dotenv_values()
    offset = 'earliest' if args.e else 'latest'
    kafka_config = {
        'bootstrap.servers': config["BOOTSTRAP_SERVERS"],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': config["SASL_USERNAME"],
        'sasl.password': config["SASL_PASSWORD"],
        'group.id': 'qwerty',
        'auto.offset.reset': offset
    }
    cons = Consumer(kafka_config)

    conn = get_db_connection(config)

    try:
        consume_messages(cons, args.t, conn, args.l, args.n)
    except KeyboardInterrupt:
        print(" Thank goodness that's over")
    finally:
        cons.close()
        conn.close()
