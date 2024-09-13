from serpapi import GoogleSearch

params = {
  "q": "Emissões CO2 logística",
  "location": "Brazil",
  "hl": "pt",
  "gl": "br",
  "google_domain": "google.com.br",
  "api_key": "7e386e8daf17d5a10a64244ae465ccecec3274285a73e7fbf22bd1ef28dbac29"
}

search = GoogleSearch(params)
results = search.get_dict()