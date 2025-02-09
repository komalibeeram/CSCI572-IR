import csv
from bs4 import BeautifulSoup
import time
import requests
from random import randint
from html.parser import HTMLParser
import json

USER_AGENT = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

#DEFAULT SEARCH ENGINE DETAILS
SEARCHING_URL = "https://www.duckduckgo.com/html/?q="
SEARCH_SELECTOR = "a"
SEARCH_ATTRS = {"class": "result__a"}

#INPUT FILE PATHS
QUERIES_PATH = "HW1/100QueriesSet4.txt"
GOOGLE_JSON = "HW1/Google_Result4.json"

#OUTPUT FILE NAMES
OUTPUT_JSON = "HW1/hw1.json"
OUTPUT_CSV = "HW1/hw1.csv"
OUTPUT_TXT = "HW1/hw1.txt"

class SearchEngine:

    @staticmethod
    def search(query, sleep=True):
        # Prevents loading too many pages too soon
        if sleep: 
            time.sleep(randint(10, 100))
        #for adding + between words for the query
        temp_url = '+'.join(query.split()) 
        url = SEARCHING_URL + temp_url
        soup = BeautifulSoup(requests.get(url, headers=USER_AGENT).text,"html.parser")
        new_results = SearchEngine.scrape_search_result(soup)
        return new_results
    
    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all(SEARCH_SELECTOR,SEARCH_ATTRS)
        results = []
        result_count = 0
        #implement a check to get only 10 results
        for result in raw_results:
            result_count +=1
            if result_count > 10:
                break
            link = result.get('href')
            results.append(link)
        return results


def calculate_overlap_and_ranks():

    overlaps_ranks_data = []    
    search_engine_results = json.loads(open(OUTPUT_JSON).read())
    google_engine_results = json.loads(open(GOOGLE_JSON).read())

    for query in search_engine_results.keys():
        search_engine_response = search_engine_results[query]
        google_engine_response = google_engine_results[query]

        overlap = 0
        google_rank = []
        search_engine_rank = []
        for i in range(0, len(google_engine_response)):
            for j in range(0, len(search_engine_response)):
                if google_engine_response[i] == search_engine_response[j]:
                    overlap += 1
                    google_rank.append(i)
                    search_engine_rank.append(j)

        overlap_rank_data = {
            "query": query,
            "google_rank": google_rank,
            "search_engine_rank": search_engine_rank,
            "overlap": overlap
        }

        overlaps_ranks_data.append(overlap_rank_data)
    return overlaps_ranks_data     
 

def calculate_spearman_coefficient(overlaps_ranks_data):

    statistics_data = []
    total_overlap = 0
    total_spearman = 0
    index=0
    for overlap_rank_data in overlaps_ranks_data:
        index+=1
        statistic_data = {}
        google_rank = overlap_rank_data["google_rank"]
        search_engine_rank = overlap_rank_data["search_engine_rank"]
        number_of_overlaps = overlap_rank_data["overlap"]

        spearman_coefficient = 0
        if number_of_overlaps == 0:
            spearman_coefficient = 0
        elif number_of_overlaps == 1:
            if google_rank[0] != search_engine_rank[0]:
                spearman_coefficient = 0
            else:
                spearman_coefficient = 1
        else:
            difference_squared = 0
            for i in range(0, len(google_rank)):
                difference = google_rank[i] - search_engine_rank[i]
                difference_squared += pow(difference, 2)
            spearman_coefficient = 1 - ((6 * difference_squared) / (number_of_overlaps * (pow(number_of_overlaps, 2) - 1)))

        statistic_data = {
            "queries": f"Query {index}",
            "overlapping_results": number_of_overlaps,
            "percentage_overlap": (number_of_overlaps / 10) * 100.0,
            "spearman_coefficient": spearman_coefficient
        }
        total_overlap += number_of_overlaps
        total_spearman += spearman_coefficient

        statistics_data.append(statistic_data)

    return statistics_data, (total_overlap / 100.0)*10, total_spearman / 100.0

#############Driver code############
# reading the query list dataset
with open(QUERIES_PATH) as file:
    query_list = [line.rstrip() for line in file]

response_json = {}
for query in query_list:
    result = SearchEngine.search(query)
    response_json[query] = result


# to write the respon as a json file
with open(OUTPUT_JSON, "w") as file:
    json.dump(response_json, file)

# to calculate overlap and ranks
overlaps_ranks_data = calculate_overlap_and_ranks()

# call for calculating spearman coefficient
data, average_percentage_overlap, average_spearman = calculate_spearman_coefficient(overlaps_ranks_data)

# to store the data in csv file
with open(OUTPUT_CSV, "w") as file:
    write = csv.writer(file)
    write.writerow(["Queries", "Number of Overlapping Results", "Percentage Overlap", "Spearman Coefficient"])
    for d in data:
        write.writerow(
            [d["queries"], d["overlapping_results"], d["percentage_overlap"], d["spearman_coefficient"]])
     
# txt file for describing the performance
description = f"Baseline search engine: Google\nAssigned search engine for comparison: DuckDuckGo\n \n" \
              f"Average percentage overlap: {average_percentage_overlap}%\n" \
              f"Average spearman coefficient: {average_spearman}\n\n" 

if average_spearman < 0:
    description += f"The results show that DuckDuckGo and Google rank search results very differently. From the above average percent overlap and average Spearman coefficient, it can be clearly seen that the similarity between the search results obtained by DuckDuckGo and Google's results is very low. The average percentage overlap " \
                   f"indicate that most of the results from DuckDuckGo do not appear in Google's top rankings possibly due to the search engine algorithms" \
                   f" and their page rankings system and the huge database google has. The negative average spearman coefficient also say that the rankings are more often in reverse order when compared to Google's."

with open(OUTPUT_TXT,"w") as f:
    f.write(description)

####################################