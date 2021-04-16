import re
import json
import requests
import urllib
import pandas as pd
import datetime
import time
import pickle
import multiprocessing as mp
from tqdm import tqdm

#######################################
# Helper function for error handling
#######################################

def call_with_maxretry(fn, arg, max_retries=12):
    count = 0
    error = None
    while count < max_retries:
        try:
            return {"result": fn(arg), "success": True}
        except requests.exceptions.SSLError as e:
            time.sleep(5) # retry after 5 seconds
            error = e
            count += 1
    return {"result": str(error), "success": False}

#######################################
# Google Search: Query
#######################################

# Search on the "Google Patent" page for company patent information:
# - patent title
# - patent publication date
# - patent publication number
# - pdf for the patent
# - url for google patent page

GOOGLE_QUERY_URL = "https://patents.google.com/xhr/query"
GOOGLE_PATENT_PAGE_URL = "https://patents.google.com/patent/"
GOOGLE_PATENT_PDF_URL = "https://patentimages.storage.googleapis.com/"
QUERY_NUM_PER_PAGE = 20

def get_query_url(params):
    """Return the URL for querying patent information from Google Patent"""
    url = GOOGLE_QUERY_URL + "?"
    for key, value in params.items():
        # encode special characters for url parameters
        value = str(value)
        value = value.replace("&", "%26")
        value = value.replace("'", "%27")
        value = re.sub(r'\s+', '+', value)
        value = urllib.parse.quote(value, safe='/')
        # add query parameters
        url += "{}={}%26".format(key, value)
    url = url[:-3] # trim training '%26' ('&')
    return url

def query(company, start_date=None, end_date=None, page_num=None):
    """
    Return the COMPANY patent information between START_DATE and END_DATE
    on PAGE_NUM of the results page
    """
    # compute query parameters
    params = {
        "url": "assignee={}".format(company),
        "num": QUERY_NUM_PER_PAGE
    }
    if page_num:
        params["page"] = page_num
    if start_date:
        start_date = start_date.replace("-", "")
        params["after"] = "publication:" + start_date
    if end_date:
        end_date = end_date.replace("-", "")
        params["before"] = "publication:" + end_date

    # parse Google patent query responses
    response = call_with_maxretry(requests.get, get_query_url(params))
    if response["success"]:
        response = response["result"]
    else:
        print("Failed to fetch url {}\nError: {}".format(get_query_url(params),
                                                         response["result"]))
        return

    if response.status_code != 200:
        print("Error: " + str(response.status_code))
        print("Reason: " + response.reason)
        print("URL: " + response.url)
        return
    response = json.loads(response.text)
    result = {
        "total_num_pages": response["results"]["total_num_pages"],
        "num_page": response["results"]["num_page"],
        "results": []
    }
    if len(response["results"]["cluster"]) > 0:
        if len(response["results"]["cluster"][0]) > 0:
            result["results"] = response["results"]["cluster"][0]["result"]

    return result

def parse_result(item):
    """Return the json response of one single result returned by Google"""
    result = {
        "title": item["patent"]["title"],
        "publication_date": item["patent"]["publication_date"],
        "publication_number": item["patent"]["publication_number"],
        "pdf": "",
        "url": ""
    }
    if len(item["patent"]["pdf"].strip()) > 0:
        result["pdf"] = GOOGLE_PATENT_PDF_URL + item["patent"]["pdf"]
    if len(item["patent"]["publication_number"].strip()) > 0:
        result["url"] = GOOGLE_PATENT_PAGE_URL + item["patent"]["publication_number"]
    return result

#######################################
# Google Patent Page Scraping
#######################################

# Scrape Google patent page for:
# - citation counts
# - inventor names

CITATION_PREFIXES = [
    "Patent Citations",
    "Non-Patent Citations",
    "Cited By",
    "Families Citing this family",
    "Family Cites Families",
]

CITATION_PATTERNS = [
    re.compile(r'{}\s+\((\d+)\)'.format(prefix))
    for prefix in CITATION_PREFIXES
]

INVENTOR_PATTERN = re.compile(r'<meta[^>]+content="([^"]+)"[^>]+scheme="inventor">')

def get_html(url):
    """Return the HTML source for URL"""
    resp = call_with_maxretry(requests.get, url)
    if resp["success"]:
        resp = resp["result"]
    else:
        print("Failed to fetch url {}\nError: {}".format(url, resp["result"]))
        return ""
    resp.encoding= 'utf-8'
    if resp.status_code == 200:
        return str(resp.text)
    return ""

def get_inventors(html):
    """Parse inventor information from HTML source"""
    return INVENTOR_PATTERN.findall(html)

def get_citation_counts(html):
    """Parse citation counts from HTML source"""
    total = 0
    for pattern in CITATION_PATTERNS:
        match = pattern.search(html)
        if match:
            total += int(match.groups(0)[0])
    return total

#######################################
# Pipeline
#######################################

def collect_patent_information(company_name,
                               anndate_3yrsago=None,
                               anndate=None,
                               deal_number=''):
    """
    The main pipeline for collecting all patent information of COMPANY_NAME
    between ANNDATE_3YRSAGO and ANNDATE with M&A DEAL_NUMBER
    """
    # reformat arguments
    if anndate_3yrsago is not None:
        anndate_3yrsago = anndate_3yrsago.strftime("%Y-%m-%d")
    if anndate is not None:
        anndate = anndate.strftime("%Y-%m-%d")
    company_name = company_name.title().strip()

    # get Google query results for the patent
    query_results = []
    total_pages = 1
    page_num = 0
    while page_num < total_pages:
        resp = query(company_name,
                     start_date=anndate_3yrsago,
                     end_date=anndate,
                     page_num=page_num)
        if resp is not None:
            total_pages = resp["total_num_pages"]
            query_results += resp["results"]
        page_num += 1
    query_results = [parse_result(x) for x in query_results]

    # parse results to get target information for collection
    patent_results = []
    for q in query_results:
        html = get_html(q["url"])
        patent_results.append({
            "deal number": deal_number,
            "anndate": anndate,
            "company name": company_name,
            "patent title": q["title"].strip(),
            "publication date": q["publication_date"],
            "publication number": q["publication_number"],
            "citation count": get_citation_counts(html),
            "inventors": ", ".join(get_inventors(html)),
            "url": q["url"],
            "pdf": q["pdf"]
        })
    return patent_results

#######################################
# Main Procedure
#######################################

if __name__ == '__main__':
    # read input excel
    INPUT_FILENAME = "sdc.xls" # or "SDC 2001-2015 AT 05-28-19.xls"
    sdc = pd.read_excel(INPUT_FILENAME)

    # calculate the time of the third years before the M&A deal of each company
    THREE_YEARS = datetime.timedelta(days=3*365)
    sdc['anndate_3yrsago'] = sdc['anndate'] - THREE_YEARS

    # select only features we care about
    sdc = sdc[['comnam_tar', 'anndate_3yrsago', 'anndate', "deal_number"]]

    sdc.head()

    sdc.values[0]

    # Set-up worker to pull patent information multi-threaded

    def worker(arg):
        """The job for the each worker process to run"""
        return collect_patent_information(*arg)

    # feel free to change this to try a smaller number of inputs
    input_values = sdc.values

    # nice progress bar to visualize our scraping process
    NUM_PROCESSES = mp.cpu_count() * 15
    pool = mp.Pool(NUM_PROCESSES)
    print("Set up a multi-process pool of {} workers".format(NUM_PROCESSES))
    print("\nError messges will be printed below, if any:\n")

    # set output file name
    OUTPUT_FILENAME = "sdc_patent_output.csv"

    # start scraping
    print_header = True
    with open(OUTPUT_FILENAME, "w") as file:
        with tqdm(total=len(input_values)) as pbar:
            jobs = pool.imap_unordered(worker, input_values)
            pool.close()
            for results in jobs:
                if len(results) > 0:
                    csv_data = {
                        column_name: [ res[column_name] for res in results]
                        for column_name in list(results[0].keys())
                    }
                    df = pd.DataFrame.from_dict(csv_data)
                    csv = df.to_csv(index=False, encoding='utf-8', header=print_header)
                    print_header = False
                    file.write(csv)
                pbar.update()

