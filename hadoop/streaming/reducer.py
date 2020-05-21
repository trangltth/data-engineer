import sys

words = {}

# Gets something like
# word1 1
# word1 1
# word2 1
# word3 1
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)[0], len(line.split('\t', 1)[0])
    #   print(len(line.split('\t', 1)[0]))
    try:
        words[word] = count
    except ValueError:
        print('text is invalid: ', ValueError)

    for (word, count) in words.items():
        print (word, count)