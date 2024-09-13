from serpapi import GoogleSearch

params = {
  "engine": "google_news",
  "q": "pizza",
  "gl": "us",
  "hl": "en",
  "api_key": "7e386e8daf17d5a10a64244ae465ccecec3274285a73e7fbf22bd1ef28dbac29"
}

search = GoogleSearch(params)
results = search.get_dict()
news_results = results["news_results"]