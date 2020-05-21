-- for mapreduce mode
-- %default INPUT  '/user/trang/input/test.txt';
-- %default OUTPUT '/user/trang/output';

-- for local mode
%default INPUT '/home/trang/input/test.txt';
%default OUTPUT '/home/trang/output'

records = LOAD '$INPUT';

terms = FOREACH records GENERATE FLATTEN(TOKENIZE((chararray) $0)) AS word;

grouped_terms = GROUP terms BY word;

word_counts = FOREACH grouped_terms GENERATE COUNT(terms), group;

STORE word_counts INTO '$OUTPUT';
