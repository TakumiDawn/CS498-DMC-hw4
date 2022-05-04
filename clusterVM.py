FROM pyhive import hive
import json
import sys

cursor = hive.connect(host="localhost", port=10000, username="kailee9874").cursor()

def results(term):
    cursor.execute("SELECT * FROM searchLog5 WHERE terms[\"searchTerm\"] = \'" + term + "\'")
    term, urls = cursor.fetchall()[0]
    return json.dumps(
        {
            "results": json.loads(urls)
        }
    )

def trends(term):
    cursor.execute("SELECT * FROM searchLog5 WHERE terms[\"searchTerm\"] = \'" + term + "\'")
    term, urls = cursor.fetchall()[0]
    count = 0
    url_count = json.loads(urls)
    for key in url_count:
        count += url_count[key]
    return json.dumps(
        {
            "clicks": count
        }
    )

def popularity(url):
    cursor.execute("SELECT * FROM searchLog5")
    result = cursor.fetchall()
    count = 0
    for row in result:
        term, urls = row
        url_count = json.loads(urls)
        for key in url_count:
            if key == url:
                count += url_count[key]
    return json.dumps(
        {
            "clicks": count
        }
    )

def getBestTerms(website):
    cursor.execute("SELECT * FROM searchLog5")
    result = cursor.fetchall()
    best_terms = []
    total_count = 0
    for row in result:
        term, urls = row
        url_count = json.loads(urls)
        for key in url_count:
            if key == website:
                total_count += url_count[key]
    for row in result:
        term, urls = row
        term = json.loads(term)
        url_count = json.loads(urls)
        for key in url_count:
            if key == website and url_count[key] > 0.05 * total_count:
                best_terms.append(term["searchTerm"])
    return json.dumps(
        {
            "best_terms": best_terms
        }
    )


def main():
    mode = sys.argv[1]
    parameter = sys.argv[2]

    if mode == "results":
        sys.stdout.write(results(parameter))
    elif mode == "trends":
        print(trends(parameter))
    elif mode == "popularity":
        print(popularity(parameter))
    elif mode == "get_best":
        print(getBestTerms(parameter))
    else:
        print("Error: not supported request")

if __name__ == "__main__":
    main()
