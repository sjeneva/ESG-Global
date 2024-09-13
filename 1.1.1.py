from pygooglenews import GoogleNews
import pandas as pd
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(filename='news_fetch_errors.log', level=logging.ERROR,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# ESG categories mapping
keyword_categories = {
    "Emissões CO2 logística": "Environment",
    "Impacto ambiental logística": "Environment",
    "Tecnologia verde logística": "Environment",
    "Uso do solo logística": "Environment",
    "Embalagens logística": "Environment",
    "Redução de emissões logística": "Environment",
    "Pegada de carbono logística": "Environment",
    "Energias renováveis logística": "Environment",
    "Logística reversa": "Environment",
    "Transporte sustentável logística": "Environment",

    "Supervisão do conselho nas empresas de logística": "Social",
    "Governança na logística sustentável": "Social",
    "Gestão logística transparente": "Social",
    "Conformidade nas operações logísticas": "Social",
    "Governança corporativa na logística": "Social",
    "Práticas éticas nas operações logísticas": "Social",
    "Transparência financeira na logística": "Social",
    "Conformidade regulatória na logística": "Social",
    "Investimento responsável na logística": "Social",
    "Gestão de riscos na logística": "Social",

    "Relações comunitárias em hubs logísticos": "Governance",
    "Direitos trabalhistas na logística": "Governance",
    "Treinamento de funcionários na logística": "Governance",
    "Abastecimento ético na logística": "Governance",
    "Práticas de salários justos na logística": "Governance",
    "Gestão de trabalho na logística": "Governance",
    "Inovações em segurança do trabalho na logística": "Governance",
    "Desenvolvimento da força de trabalho na logística": "Governance",
    "Diversidade e inclusão na logística": "Governance",
    "Normas de saúde e segurança na logística": "Governance"
}

# Function to get titles
def get_titles(search, country_code):
    gn = GoogleNews(lang='pt', country=country_code)
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
                'keyword': search,
                'category': keyword_categories.get(search, 'Unknown')  # Include the ESG category
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


# Updated Keywords list
# Updated German keywords list
keywords = [
    "Emissões CO2 logística", "Impacto ambiental logística", "Tecnologia verde logística", "Uso do solo logística",
    "Embalagens logística", "Redução de emissões logística", "Pegada de carbono logística",
    "Energias renováveis logística",
    "Logística reversa", "Transporte sustentável logística",
    "Supervisão do conselho nas empresas de logística", "Governança na logística sustentável",
    "Gestão logística transparente",
    "Conformidade nas operações logísticas", "Governança corporativa na logística",
    "Práticas éticas nas operações logísticas", "Transparência financeira na logística",
    "Conformidade regulatória na logística", "Investimento responsável na logística", "Gestão de riscos na logística",
    "Relações comunitárias em hubs logísticos", "Direitos trabalhistas na logística",
    "Treinamento de funcionários na logística",
    "Abastecimento ético na logística", "Práticas de salários justos na logística", "Gestão de trabalho na logística",
    "Inovações em segurança do trabalho na logística", "Desenvolvimento da força de trabalho na logística",
    "Diversidade e inclusão na logística", "Normas de saúde e segurança na logística"
]

# Dictionary of countries with their codes and file names
countries = {
    'PT': 'Portugal',
    'BR': 'Brazil',
    'MZ': 'Mozambique',
    'AO': 'Angola',
    'CV': 'Cape Verde',
    'GW': 'Guinea-Bissau',
    'TL': 'East Timor'
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
