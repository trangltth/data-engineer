from mrjob.job import MRJob

class MRWordFrequenceCount(MRJob):
    def mapper(self,_,line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "line", len(line.split('\n'))

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    MRWordFrequenceCount.run()