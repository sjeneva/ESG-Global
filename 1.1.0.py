from pygooglenews import GoogleNews
import pandas as pd
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(filename='news_fetch_errors.log', level=logging.ERROR,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Function to get titles
def get_titles(search, country_code):
    gn = GoogleNews(lang='en', country=country_code)
    stories = []
    try:
        search_result = gn.search(search)
        newsitem = search_result['entries']
        for item in newsitem:
            published = item.get('published', 'Unknown Date')
            publisher = item.get('source', {}).get('title', 'Unknown Publisher')
            story = {
                'title': item.title,
                'link': item.link,
                'publisher': publisher,
                'published': published,
                'keyword': search
            }
            stories.append(story)
    except Exception as e:
        logging.error(f"Error fetching stories for {search} in {country_code}: {e}")
    return stories

# Function to convert the 'published' string into a 'Month Year' format
def convert_to_month_year(date_str):
    try:
        date_obj = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')
        return date_obj.strftime('%B %Y')
    except ValueError:
        return 'Unknown Date'

# Updated Keywords list based on the provided information
keywords = [
    "Logistics CO2 emissions", "Logistics environmental impact", "Logistics green tech", "Logistics land use", "Logistics packaging", "Logistics pollution reduction", "Logistics carbon footprint", "Logistics renewable energy", "Reverse logistics", "Logistics sustainable transport",
    "Board Oversight in Logistics Firms", "Governance in Sustainable Logistics", "Transparent Logistics Management", "Compliance in Logistics Operations", "Corporate Governance in Logistics", "Ethical Practices in Logistics Operations", "Financial Transparency in Logistics", "Regulatory Compliance in Logistics", "Responsible Investment in Logistics", "Risk Management in Logistics",
    "Community Relations in Logistics Hubs", "Logistics Labor Rights", "Employee Training in Logistics", "Ethical Sourcing in Logistics", "Fair Wage Practices in Logistics", "Labor Management in Logistics", "Labor Safety Innovations in Logistics", "Workforce Development in Logistics", "Diversity and Inclusion in Logistics", "Health & Safety Standards in Logistics"
]

# Dictionary of countries with their codes and file names
countries = {
    'US': 'United States',
    'IN': 'India',
    'ID': 'Indonesia',
    'CA': 'Canada',
    'AU': 'Australia',
    'UK': 'United Kingdom',
    'MY': 'Malaysia',
    'SG': 'Singapore',
    'PH': 'Philippines',
    'NZ': 'New Zealand',
    'BW': 'Botswana',
    'ET': 'Ethiopia',
    'GH': 'Ghana',
    'IE': 'Ireland',
    'IL': 'Israel',
    'KE': 'Kenya',
    'LV': 'Latvia',
    'NA': 'Namibia',
    'NG': 'Nigeria',
    'PK': 'Pakistan',
    'ZA': 'South Africa',
    'TZ': 'Tanzania',
    'UG': 'Uganda',
    'ZW': 'Zimbabwe'
}

# Function to fetch and process news articles for a country
def process_country_news(country_code, country_name):
    all_stories = []
    for keyword in keywords:
        print(f"Fetching stories for keyword: {keyword} in {country_name}")
        stories = get_titles(keyword, country_code)
        all_stories.extend(stories)

    df_all_stories = pd.DataFrame(all_stories)
    if not df_all_stories.empty and 'published' in df_all_stories.columns:
        df_all_stories['Month Year'] = df_all_stories['published'].apply(convert_to_month_year)

    if not df_all_stories.empty:
        excel_path = f'combined_esg_logistics_news_{country_code}.xlsx'
        df_all_stories.to_excel(excel_path, index=False)
        print(f"All stories for {country_name} saved to {excel_path}")
    else:
        print(f"No data to save for {country_name}")

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(process_country_news, code, name) for code, name in countries.items()]
    for future in as_completed(futures):
        future.result()
