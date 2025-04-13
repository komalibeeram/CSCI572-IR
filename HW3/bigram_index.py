import os
import re
from collections import defaultdict

TARGET_BIGRAMS = {"computer science", "information retrieval","power politics", "los angeles", "bruce willis"}

#cleaning input data files
def clean_text(text):
    text = re.sub(r'[^a-z\s]', ' ', text.lower())
    return text

#reading input files
def parse_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            if '\t' in line:
                docID, content = line.strip().split('\t', 1)
                yield docID, clean_text(content)

#creating bigrams
def selected_bigram_index(data_folder):
    index = defaultdict(lambda: defaultdict(int))
    
    for filename in os.listdir(data_folder):
        filepath = os.path.join(data_folder, filename)
        print("scanning", filepath)
        for docID, text in parse_file(filepath):
            words = text.split()
            bigrams = zip(words, words[1:])
            for w1, w2 in bigrams:
                bigram = f"{w1} {w2}"
                if bigram in TARGET_BIGRAMS:
                    index[bigram][docID] += 1
        print("scanned", filepath)

    #printing all bigrams in the output file
    os.makedirs('HW3/output', exist_ok=True)
    with open('HW3/output/selected_bigram_index.txt', 'w') as f:
        for bigram in sorted(index):
            postings = [f"{doc}:{count}" for doc, count in sorted(index[bigram].items())]
            f.write(f"{bigram} -> {', '.join(postings)}\n")

selected_bigram_index('HW3/data/devdata')
