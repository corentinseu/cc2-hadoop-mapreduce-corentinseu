from mrjob.job import MRJob

class TagsPerMovieUser(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':
                return
            userId = parts[0]
            movieId = parts[1]
            yield '%s_%s' % (movieId, userId), 1
        except Exception:
            pass

    def reducer(self, key, counts):
        yield key, sum(counts)

if __name__ == '__main__':
    TagsPerMovieUser.run()
