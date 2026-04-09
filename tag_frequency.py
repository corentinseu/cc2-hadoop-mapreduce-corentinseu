from mrjob.job import MRJob

class TagFrequency(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return
            tag = ','.join(parts[2:-1]).strip().lower()
            if tag:
                yield tag, 1
        except Exception:
            pass

    def reducer(self, tag, counts):
        yield tag, sum(counts)

if __name__ == '__main__':
    TagFrequency.run()
