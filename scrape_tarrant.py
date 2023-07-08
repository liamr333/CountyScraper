from bs4 import BeautifulSoup
import pandas as pd
import requests
import random
import time
import csv
import os


# parcels, mapsco, agent, site name, primary building name, site number, stuff in square footage note acres, zip code
# filter out null and single family from data, format large numbers with commas

feature_to_div_class = {
    'Address' : 'col-lg-4 order-2 order-lg-1',
    'Owner Name' : 'col-lg-4',
    'City' : 'col-lg-4 order-2 order-lg-1',
    'Georeference' : 'col-lg-4 order-2 order-lg-1',
    'Neighborhood Code' : 'col-lg-4 order-2 order-lg-1',
    'Latitude' : 'col-lg-4 order-2 order-lg-1',
    'Longitude' : 'col-lg-4 order-2 order-lg-1',
    'Legal Description' : 'col-lg-6',
    'State Code' : 'col-lg-6',
    'Year Built' : 'col-lg-6',
    'Site Class' : 'col-lg-6',
    'Land Sqft' : 'col-lg-6',
    'Land Acres' : 'col-lg-6',
    'Primary Building Type' : 'col-lg-6',
    'Primary Building Name' : 'col-lg-6',
    'Gross Building Area' : 'col-lg-6',
    'Net Leasable Area' : 'col-lg-6',
    'MAPSCO' : 'col-lg-4 order-2 order-lg-1',
    'Parcels' : 'col-lg-6',
    'Site Number' : 'col-lg-6',
    'Site Name' : 'col-lg-6',
    'Agent' : 'col-lg-6'
}

feature_to_div_index = {
    'Address' : 0,
    'Owner Name' : 1,
    'City' : 0,
    'Georeference' : 0,
    'Neighborhood Code' : 0,
    'Latitude' : 0,
    'Longitude' : 0,
    'Legal Description' : 0,
    'State Code' : 0,
    'Year Built' : 0,
    'Site Class' : 1,
    'Land Sqft' : 1,
    'Land Acres' : 1,
    'Primary Building Type' : 1,
    'Primary Building Name' : 1,
    'Gross Building Area' : 1,
    'Net Leasable Area' : 1,
    'MAPSCO' : 0,
    'Parcels' : 1,
    'Site Number' : 1,
    'Site Name' : 1,
    'Agent' : 0
}

feature_to_p_tag_index = {
    'Address' : 0,
    'Owner Name' : 1,
    'City' : 1,
    'Georeference' : 2,
    'Neighborhood Code' : 3,
    'Latitude' : 4,
    'Longitude' : 5,
    'Legal Description' : 0,
    'State Code' : 3,
    'Year Built' : 4,
    'Site Class' : 2,
    'Primary Building Name' : 4,
    'Primary Building Type' : 5,
    'Gross Building Area' : 6,
    'Net Leasable Area' : 7,
    'Land Sqft' : 8,
    'Land Acres' : 9,
    'MAPSCO' : 7,
    'Parcels' : 3,
    'Site Number' : 0,
    'Site Name' : 1,
    'Agent' : 6
}

feature_to_div_or_table = {
    'Address' : 'div',
    'Owner Name' : 'div',
    'City' : 'div',
    'Georeference' : 'div',
    'Neighborhood Code' : 'div',
    'Latitude' : 'div',
    'Longitude' : 'div',
    'Legal Description' : 'div',
    'State Code' : 'div',
    'Year Built' : 'div',
    'Site Class' : 'div',
    'Land Sqft' : 'div',
    'Land Acres' : 'div',
    'Improvement Market' : 'table',
    'Land Market' : 'table',
    'Total Market' : 'table',
    'Total Appraised' : 'table',
    'Primary Building Name' : 'div',
    'Primary Building Type' : 'div',
    'Gross Building Area' : 'div',
    'Net Leasable Area' : 'div',
    'MAPSCO' : 'div',
    'Parcels' : 'div',
    'Site Number' : 'div',
    'Site Name' : 'div',
    'Agent' : 'div'
}

feature_to_td_index = {
    'Improvement Market' : 1,
    'Land Market' : 2,
    'Total Market' : 3,
    'Total Appraised' : 4
}

feature_names = [
    'Property ID',
    'Owner Name',
    'Address',
    'City',
    'Parcels',
    'Georeference',
    'MAPSCO',
    'Neighborhood Code',
    'Latitude',
    'Longitude',
    'Legal Description',
    'State Code',
    'Year Built',
    'Site Number',
    'Site Name',
    'Site Class',
    'Agent',
    'Land Sqft',
    'Land Acres',
    'Improvement Market',
    'Land Market',
    'Total Market',
    'Total Appraised',
    'Primary Building Name',
    'Primary Building Type',
    'Gross Building Area',
    'Net Leasable Area'   
]


def get_soup(prop_id):
    url_stem = 'https://www.tad.org/property.php?pin={}'
    url = url_stem.format(prop_id)
    content = requests.get(url).text
    soup = BeautifulSoup(content, 'html.parser')
    return soup


def get_feature(soup, feature):
    if feature not in str(soup):
        return ''
    if feature_to_div_or_table[feature] == 'div':
        div_class = feature_to_div_class[feature]
        div = soup.find_all('div', class_=div_class)[feature_to_div_index[feature]]
        p_tag = div.find_all('p')[feature_to_p_tag_index[feature]]
        value = p_tag.text
        value = value[value.find(':')+2:].strip()
    else:
        table = soup.find('table', class_='table table-bordered values')
        tr = table.find_all('tr')[2]
        tds = tr.find_all('td')
        value = tds[feature_to_td_index[feature]].text.strip()
    return value


def scrape_property(prop_id):
    soup = get_soup(prop_id)
    row = [prop_id]
    for feature_name in feature_names[1:]:
        try:
            extracted_feature = get_feature(soup, feature_name)
        except:
            extracted_feature = ''
        row.append(extracted_feature)
    return row
        


def main():

    TIME_DELAY=1

    input_file_1 = 'AccountsToScrapeTarrant (All).csv'
    output_file = 'TarrantData2.csv'
    error_ids_file = 'ErrorIDs.txt'
    last_id_file = 'LastID.txt'

    df = pd.read_csv(input_file_1)

    property_ids = (str(id_).replace('_', '') for id_ in list(df['Account_Num']))


    if output_file not in os.listdir():
        with open(output_file, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(feature_names)
        scraped_ids = []
    else:
        output_df = pd.read_csv(output_file, encoding='cp1252')
        scraped_ids = [str(id_).replace('_', '') for id_ in list(output_df['Property ID'])]

    if last_id_file not in os.listdir():
        last_id_logger = open(last_id_file, 'w')
        last_id_logger.write(property_ids[0])
        last_id_logger.close()

    if error_ids_file not in os.listdir():
        error_logger = open(error_ids_file, 'w')
        error_logger.close()

    last_id = open(last_id_file, 'r').read()

    if last_id == '':
        f = open(last_id_file, 'w')
        last_id = scraped_ids[-1]
        f.write(scraped_ids[-1])
        f.close()
    

    for property_id in property_ids:
        if property_id in scraped_ids:
            print('Skipping {}, already scraped'.format(property_id))
            continue
        time.sleep(TIME_DELAY)
        try:
            row = scrape_property(property_id)
            print(row)
            with open(output_file, 'a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(row)
                csv_file.close()
            last_id_logger = open(last_id_file, 'w')
            last_id_logger.write(str(property_id))
            last_id_logger.close()
        except:
            print('Error encountered while scraping. Logging URL')
            error_logger = open(error_ids_file, 'a')
            error_logger.write(str(property_id))
            error_logger.close()
		


if __name__ == '__main__':
	main()	

