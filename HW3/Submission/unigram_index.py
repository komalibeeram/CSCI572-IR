from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import re
from collections import defaultdict

class MRUnigramIndex(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_counts),
            MRStep(reducer=self.reducer_output)
        ]

    def mapper(self, _, line):
        #clean data and split words
        if '\t' in line:
            docID, content = line.strip().split('\t', 1)
            content = re.sub(r'[^a-z\s]', ' ', content.lower())
            words = content.split()
            for word in words:
                yield (word, docID), 1

    def reducer_counts(self, key, values):
        word, docID = key
        yield word, (docID, sum(values))

    def reducer_output(self, word, doc_counts):
        #sort the doc ids and output to file
        count_dict = defaultdict(int)
        for docID, count in doc_counts:
            count_dict[docID] += count
        
        sorted_postings = sorted(count_dict.items())
        postings_str = ', '.join(f"{doc}:{count}" for doc, count in sorted_postings)
        yield None, f"{word} -> {postings_str}"

MRUnigramIndex.run()
