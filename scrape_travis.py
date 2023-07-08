import json
import requests
import csv
import time
import re
import os
import sys
import pandas as pd
import locale

TIME_DELAY=0
auth_key = open('Authorization Key.txt', 'r').read()
output_file = sys.argv[1]
scrape_mode = sys.argv[2]  # "normal" means scrape B1 and F1 properties, "linked" means scrape properties linked to properties
# already in output file
input_file = 'TravisData1.csv'
last_id_file = 'LastID.txt'
error_logger_file = 'ErrorIDs.txt'



feature_section_dict = {
    'stateCd' : 'propertyProfile',
    'imprvActualYearBuilt' : 'propertyProfile',
    'imprvTotalArea' : 'propertyProfile',
    'imprvUnits' : 'propertyProfile',
    'imprvCondition' : 'propertyProfile',
    'landSizeSqft' : 'propertyProfile',
    'name' : 'owners',
    'addrDeliveryLine' : 'owners',
    'zip' : 'situses',
    'improvementHSValue' : 'valuations',
    'improvementNHSValue': 'valuations',
    'landHSValue': 'valuations',
    'landNHSValue': 'valuations',
    'structureValue': 'valuations',
    'landValue' : 'valuations',
    'value' : 'valuations',
    'bedrooms' : 'permits',
    'bathrooms' : 'permits',
    'units' : 'permits',
    'landSizeEffectiveDepth': 'propertyProfile',
    'landSizeEffectiveFront': 'propertyProfile',
    'squareFootArea' : 'permits',
    'marketArea' : 'propertyCharacteristics',
    'legalDescription' : 'propertyLegalDescription',
    'mapsco' : 'propertyIdentification',
    'geoID' : 'propertyIdentification',
    'mapID' : 'propertyIdentification',
    'agents' : 'owners'
}

# add legal description (for Dallas as well)
features = [
    'name',
    'propType',
    'linkedPID',
    'agents',
    'stateCd',
    #'squareFootArea',
    'imprvTotalArea',
    'bedrooms',
    'geoID',
    'landSizeSqft',
    'sizeSqft',
    'landHSValue',
    'landNHSValue',
    'improvementHSValue',
    'improvementNHSValue',
    'marketValue',
    'landValue',
    'structureValue',
    'appraisedValue',
    'value',
    'grossLivingArea',
    'imprvActualYearBuilt',
    'mapID',
    'legalDescription',
    'marketArea',
    #'addrDeliveryLine',
    'address',
    'zip',
    'geometry',
    'mapsco',
    'imprvCondition',
    'bathrooms',
    #'units',
    'landSizeEffectiveFront',
    'landSizeEffectiveDepth',
    'imprvUnits']

headers = ['propertyID']

for feature in features:
    headers.append(feature)


def write_json_to_file(property_id, json_content):
    with open(f'json_output/{property_id}.json', 'w+') as output_json_file:
        output_json_file.write(json_content)



def get_json(property_id, year, auth_key):
    try:
        url_stem = 'https://stage-container.trueprodigyapi.com/public/jsonexport/{}/{}'
        url = url_stem.format(property_id, year)
        response = requests.get(url, headers={'Authorization':auth_key})
        if response.status_code != 200:
            return None
        content = requests.get(url, headers={'Authorization':auth_key}).text
        json_obj = json.loads(content)
    except Exception as e:
        print("Exception: {}".format(e))
        print("Exception has type: {}".format(type(e)))
    return json_obj



def get_sub_dict(json_obj, sub_dict_name):
    sub_dict = None
    try:
        sub_dict = json_obj['results'][0][sub_dict_name][0]
    except KeyError:
        pass
    except IndexError:
        pass
    return sub_dict


def format_address(address, zip_code):
    address = ' '.join(address.split()).replace(', ,', ', ').strip().replace(' ,', ',')
    result = address
    if re.search('[0-9]{5}', address[-5:]) is None:
        result = ' '.join(f'{address}, {zip_code}'.split())
    if result[-5:] == ', nan':
        result = result[:-5]
    return result


def get_feature(json_obj, feature_name):
    extracted_feature = ''
    try:
        if feature_name in ['marketValue', 'appraisedValue']:
            extracted_feature = json_obj['results'][0]['owners'][0]['ownerTaxable'][0][feature_name]
        elif feature_name == 'linkedPID':
            extracted_feature = ', '.join([str(d['linkedPID']) for d in json_obj['results'][0]['links']])
        elif feature_name in ['grossLivingArea']:
            extracted_feature = json_obj['results'][0]['valuations'][0]['details']['cost-local']['grossLivingArea']
        elif feature_name == 'geometry':
            coordinates_list = json_obj['results'][0][feature_name]
            coord_1 = re.findall(r'[0-9]{2}.[0-9]*', coordinates_list)[0]
            coord_2 = re.findall(r'[0-9]{2}.[0-9]*', coordinates_list)[1]
            extracted_feature = f'{coord_1}, -{coord_2}'
        elif feature_name == 'propType':
            extracted_feature = json_obj['results'][0][feature_name]
        elif feature_name == 'agents':
            extracted_feature = ', '.join([d['companyName'] for d in json_obj['results'][0]['owners'][0]['agents']])
        elif feature_name == 'units':
            extracted_feature = ', '.join([item['units'] for item in get_sub_dict(json_obj, feature_section_dict[feature_name])])
        elif feature_name == 'sizeSqft':
            extracted_feature = sum([float(item['sizeSqft']) for item in json_obj['results'][0]['valuations'][0]['details']['cost-local']['land']])
        elif feature_name == 'address':
            streetNum = json_obj['results'][0]['situses'][0]['streetNum']
            streetPrefix = json_obj['results'][0]['situses'][0]['streetPrefix']
            streetName = json_obj['results'][0]['situses'][0]['streetName']
            streetSuffix = json_obj['results'][0]['situses'][0]['streetSuffix']
            streetSecondary = json_obj['results'][0]['situses'][0]['streetSecondary']
            city = json_obj['results'][0]['situses'][0]['city']
            state = json_obj['results'][0]['situses'][0]['state']
            zip_ = json_obj['results'][0]['situses'][0]['zip']
            if streetNum is None:
                streetNum = ''
            if streetPrefix is None:
                streetPrefix = ''
            if streetName is None:
                streetName = ''
            if streetSuffix is None:
                streetSuffix = ''
            if city is None:
                city = ''
            if state is None:
                state = ''
            if zip_ is None:
                zip_ = ''
            extracted_feature = f'{streetNum} {streetPrefix} {streetName} {streetSuffix}, {city}, {state} {zip_}'
        else:
            sub_dict = get_sub_dict(json_obj, feature_section_dict[feature_name])
            if sub_dict is not None:
                extracted_feature = sub_dict[feature_name]
                if feature_name in ['geoID', 'mapID'] and extracted_feature is not None:
                    extracted_feature = '_' + extracted_feature
    except KeyError:
        pass
    except IndexError:
        pass
    return extracted_feature



def scrape(property_id, year, auth_key):
    json_obj = get_json(property_id, year, auth_key)
    write_json_to_file(property_id, json.dumps(json_obj, indent=3))
    row = [property_id]
    if json_obj == None:
        return row
    for feature in features:
        try:
            extracted_feature = get_feature(json_obj, feature)
            if extracted_feature is None: 
                row.append('')
            elif type(extracted_feature) == list and len(extracted_feature) == 0:
                row.append('')
            else:
                row.append(extracted_feature)
        except KeyError as k:
            row.append('')
    return row


def write_to_csv(output_file, rows):
    with open(output_file, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(rows)


def main():

    if 'unscrapable_ids.txt' not in os.listdir():
        print('not found')
        with open('unscrapable_ids.txt', 'w+') as bad_ids_file:
            bad_ids_file.write('')

    bad_ids = []

    with open('unscrapable_ids.txt', 'r') as bad_ids_file:
        for line in bad_ids_file:
            bad_ids.append(line.strip())



    print('output file: {}'.format(output_file))
    
    if output_file not in os.listdir():
        with open(output_file, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(headers)
            print('writing')

    df_output = pd.read_csv(output_file)
    df_input = pd.read_csv(input_file)
    props_with_comp_codes = set(df_input[(df_input.state_code=='F1')|(df_input.state_code=='B1')].prop_id)

    time.sleep(2)

    if scrape_mode == 'direct_comps':
        prop_ids_to_scrape = list([prop_id for prop_id in props_with_comp_codes if prop_id not in list(df_output.propertyID)])
    # mode for scraping linked properties
    # presumes all properties in comp set with F1 and B1 codes have already been scraped
    elif scrape_mode == 'linked':
        comp_codes_df = df_output[df_output.propertyID.isin(props_with_comp_codes)]
        prop_ids = []
        for item in comp_codes_df.linkedPID:
            try:
                for sub_item in item.split(','):
                    prop_ids.append(int(sub_item.strip()))
            except:
                pass
        prop_ids = set(prop_ids)
        print(len(prop_ids))
        prop_ids_to_scrape = list([prop_id for prop_id in prop_ids if prop_id not in list(df_output.propertyID)])
        print(len(prop_ids_to_scrape))
    else:
        print('Unknown scrape mode')
        return
    

    last_id = prop_ids_to_scrape[-1]

    buffer = []

    currently_scraped_ids = []


    for unscraped_prop_id in prop_ids_to_scrape:
        if unscraped_prop_id in currently_scraped_ids:
            continue
        if str(unscraped_prop_id) in bad_ids:
            print('Skipping {}, bad id'.format(unscraped_prop_id))
            continue
        time.sleep(TIME_DELAY)
        row = scrape(unscraped_prop_id, 2023, auth_key)
        print(row)
        if len(row) > 1:
            buffer.append(row)
        else:
            with open('unscrapable_ids.txt', 'a+') as bad_ids_file:
                bad_ids_file.write(str(row[0]))
                bad_ids_file.write('\n')
        currently_scraped_ids.append(unscraped_prop_id)
        if len(buffer) == 25 or unscraped_prop_id == last_id:
            print('Writing to disk')
            write_to_csv(output_file, buffer)
            buffer = []



if __name__ == '__main__':
	main()