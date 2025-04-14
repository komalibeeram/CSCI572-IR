import os
import re
from collections import defaultdict

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

#creating unigrams 
def unigram_index(data_folder):
    index = defaultdict(lambda: defaultdict(int))
    
    for filename in os.listdir(data_folder):
        filepath = os.path.join(data_folder, filename)
        print("scanning", filepath)
        for docID, text in parse_file(filepath):
            for word in text.split():
                index[word][docID] += 1
        print("scanned", filepath)
    
    #printing all unigrams to output file
    os.makedirs('HW3/output', exist_ok=True)
    with open('HW3/output/unigram_index.txt', 'w') as f:
        for word in sorted(index):
            postings = [f"{doc}:{count}" for doc, count in sorted(index[word].items())]
            f.write(f"{word} -> {', '.join(postings)}\n")


unigram_index('HW3/data/fulldata')
