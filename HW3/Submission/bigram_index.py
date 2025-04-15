from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import re
from collections import defaultdict

SELECTED_BIGRAMS = {"computer science", "information retrieval","power politics", "los angeles", "bruce willis"}

class MRBigramIndex(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,reducer=self.reducer_counts),
            MRStep(reducer=self.reducer_output)
        ]

    def mapper(self, _, line):
        if '\t' in line:
            docID, content = line.strip().split('\t', 1)
            content = re.sub(r'[^a-z\s]', ' ', content.lower())
            words = content.split()
            for i in range(len(words) - 1):
                bigram = f"{words[i]} {words[i+1]}"
                if bigram in SELECTED_BIGRAMS:
                    yield (bigram, docID), 1

    def reducer_counts(self, key, values):
        bigram, docID = key
        yield bigram, (docID, sum(values))

    def reducer_output(self, bigram, doc_counts):
        count_dict = defaultdict(int)
        for docID, count in doc_counts:
            count_dict[docID] += count
        
        sorted_postings = sorted(count_dict.items())
        postings_str = ', '.join(f"{doc}:{count}" for doc, count in sorted_postings)
        yield None, f"{bigram} -> {postings_str}"

MRBigramIndex.run()
