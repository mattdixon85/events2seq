
import sys
from itertools import groupby


class Stream:
    def __init__(self, centricity, config):
        self.centricity = centricity
        self.config = config

    def run(self, data):
        for cid, events in groupby(data, lambda x : x.strip().split('\t')[0]):
            centricity = self.centricity(cid, self.config)
            centricity.process_events(events)


class Centricity:
    def __init__(self, cid, config):
        self.cid = cid
        self.config = config
  
    def emit_seq(self):
        # concatenate events and generate a tab-separated tuple of id, time and sequence
        seq_str = ' '.join(self.seq)
	seq_len = len(self.seq)
	seq_end = self.last_ts
	seq_time = seq_end - self.seq_start
        print '\t'.join(map(str, [self.cid, self.seq_start, seq_end, seq_time, seq_len, seq_str]))
    
    def process_events(self, events):
        self.seq = list()
        self.seq_start = None

        for event in events:
            cid, ts, event = event.strip().split('\t')
	    ts = int(ts)
            num_events = len(self.seq)

            if self.seq_start == None:
                self.seq_start = ts
                time_passed = 0
            else:
                time_passed = ts - self.seq_start

            if num_events + 1 > self.config['max_events'] or time_passed > self.config['max_time']:
                # bundle events into seq and emit
                self.emit_seq()
                # reset variables
                self.seq = [event]
                self.seq_start = ts
		self.last_ts = ts
            else:
                # append and continue
                self.seq.append(event)
		self.last_ts = ts

        # end of stream - emit outstanding data
        self.emit_seq()


def main():
    max_events = int(sys.argv[1])
    max_time = int(sys.argv[2])
    config = dict(max_events = max_events, max_time = max_time)
    stream = Stream(Centricity, config)
    stream.run(sys.stdin)

if __name__ == '__main__':
    main()
  
