from mrjob.job import MRJob

class TagsPerUser(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return
            userId = parts[0]
            yield userId, 1
        except Exception:
            pass

    def reducer(self, userId, counts):
        yield userId, sum(counts)

if __name__ == '__main__':
    TagsPerUser.run()
