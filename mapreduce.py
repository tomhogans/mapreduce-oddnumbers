# Tom Hogans - CS 491 Big Data
# Final Exam
# Problem #3, Map Reduce

import mrjob.job
import re


WORD_RE = re.compile(r"[\w']+")


class MROddNumberCounter(mrjob.job.MRJob):
    """ Counts frequency of occurrence of integers, and returns the integers
    with an odd frequency. """

    def freq_mapper(self, key, value):
        """ In first step, key is null, value is line content """
        for number in WORD_RE.findall(value):
            yield (int(number), 1)

    def freq_combiner(self, number, counts):
        yield (number, sum(counts))

    def freq_reducer(self, number, counts):
        yield (number, sum(counts))

    def odd_mapper(self, key, value):
        if value % 2 != 0:
            yield (key, value)

    def steps(self):
        return [
            self.mr(mapper=self.freq_mapper,
                    combiner=self.freq_combiner,
                    reducer=self.freq_reducer),
            self.mr(mapper=self.odd_mapper),
        ]

if __name__ == '__main__':
    MROddNumberCounter.run()
