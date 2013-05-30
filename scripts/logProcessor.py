#!/usr/bin/python

import sys
import re

def main(argv):
	regex = re.compile(('(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)'))
	line = sys.stdin.readline()
	try:
		while line:
			fields = regex.match(line)
			if (fields != None):
			    # In our example, we use the Hadoop Aggregate package for the reduction part of our computation.
			    # Hadoop aggregate package provides reducer and combiner implementations for simple aggregate
			    # operations such as sum, max, unique value count, and histogram. When used with Hadoop Streaming,
			    # the mapper outputs must specify the type of aggregation operation of the current computation as a
			    # prefix to the output key, which is "LongValueSum" in our example.
			    # See more at: http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/lib/aggregate/ValueAggregator.html
				print "LongValueSum:" + fields.group(1) + "\t" + fields.group(7);
			line = sys.stdin.readline();
	except "end of file":
		return None

if __name__ == "__main__":
	main(sys.argv)

