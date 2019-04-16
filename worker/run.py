#!/usr/bin/python2
"""
Monitor queue and download pdf file to local disk, then compress it and convert to png
"""
import argparse
import datetime
import time
from azure.storage.queue import QueueService

def print_with_time(message):
    print "[{0}]{1}".format(datetime.datetime.now(), message)

def handle_message(message):
    print(message.content)

def run(account, key):
    queue_service = QueueService(account_name=account, account_key=key)
    queue_service.create_queue('taskqueue', fail_on_exist=False)
    messages = queue_service.get_messages('taskqueue', num_messages=4, visibility_timeout=20*60)
    for message in messages:
        try:
            handle_message(message)
            queue_service.delete_message('taskqueue', message.id, message.pop_receipt)
        except Exception as e:
            print_with_time("ERROR:{0}".format(e.message))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", help="Azure storage account")
    parser.add_argument("--key", help="Azure storage key")
    args = parser.parse_args()

    while True:
        try:
            run(args.account, args.key)
            time.sleep(5)
        except Exception as e:
            print_with_time("ERROR:{0}".format(e.message))

if __name__ == "__main__":
    main()