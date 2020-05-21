import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes

class Mapper(api.Mapper):
    def map(self, context):
        for word in context.values.split():
            w = context.emit(word, 1)
            print("word is: ", word)
            print("context emit value: ", w)

class Reducer(api.Reducer):
    def reducer(self, context):
        context.emit(context.key, sum(context.values))

_factory = pipes.Factory(Mapper, reducer_class=Reducer)

def main():
    pipes.run_task(_factory)

if __name__ == '__main__':
    main()