#!/usr/bin/env python

import sys

for line in sys.stdin:
  line = line.strip()
  words = line.split()

  for word in words:
    # This will be the input for the reduce script
    print (word, 1)