import os
import random
import time
from datetime import datetime

# modify to convert the ts to seconds
def convert_to_seconds(ts):
    # print(ts)
    if len(ts) < 2: # Some empty line
        return 0
        
    date_time = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    # print(date_time)

    timedelta = date_time - datetime(1900, 1, 1)
    seconds = timedelta.total_seconds()

    # print(seconds)
    return seconds


# modify this function, the sleep time should based on the time in the data
def random_sleep(ts):
    # t = random.randint(1, 4)  # modify this line
    time.sleep(ts)


def main():
    last_ts = None
    with open('new_tweets.txt', 'rb') as f:
        for line in f:

            # split the text and timestamp
            parts = line.rstrip().split(',')
            text = ' '.join(parts[:-1])
            ts = parts[-1]

            ts = convert_to_seconds(ts)
            
            # text_ls = [x for x in text.split() if x.startswith('#')]
            # text = ' '.join(text_ls)

            cmd = 'echo "' + text + '" | /usr/hdp/2.6.5.0-292/kafka/bin/kafka-console-producer.sh --broker-list dicvmd7.ie.cuhk.edu.hk:6667 --topic 983-ft'

            os.system(cmd)
            # print(text)

            if last_ts is not None:
                ts_delta = ts - last_ts
                # print(ts_delta)
                random_sleep(ts_delta)

            last_ts = ts


if __name__ == '__main__':
    main()