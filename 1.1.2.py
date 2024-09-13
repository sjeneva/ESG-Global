import pandas as pd
import os

# Folder where all your files are stored
folder_path = 'C:/Users/1234/OneDrive - 인하대학교/바탕 화면/LDA_Global/Portuguese'

# List all files in the folder
files = [
    'combined_esg_logistics_news_AO.xlsx',
    'combined_esg_logistics_news_BR.xlsx',
    'combined_esg_logistics_news_CV.xlsx',
    'combined_esg_logistics_news_GW.xlsx',
    'combined_esg_logistics_news_MZ.xlsx',
    'combined_esg_logistics_news_PT.xlsx',
    'combined_esg_logistics_news_TL.xlsx',
]

# Full file paths
file_paths = [os.path.join(folder_path, file) for file in files]

# Load and combine all the dataframes
combined_df = pd.concat([pd.read_excel(file) for file in file_paths])

# Save the combined dataframe to a new Excel file
output_path = os.path.join(folder_path, 'combined_esg_logistics_news_complete.xlsx')
combined_df.to_excel(output_path, index=False)

print(f"Files combined successfully and saved to {output_path}")
