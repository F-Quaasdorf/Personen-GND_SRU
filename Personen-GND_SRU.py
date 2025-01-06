import requests
import unicodedata
from lxml import etree
from bs4 import BeautifulSoup as soup
import pandas as pd

# Builds the SRU-string and returns the fetched records
def dnb_sru(query):
    base_url = "https://services.dnb.de/sru/authorities"
    params = {
        'recordSchema': 'MARC21-xml',
        'operation': 'searchRetrieve',
        'version': '1.1',
        'maximumRecords': '100',
        'query': query
    }
    
    records = []
    i = 1
    first_request = True
    
    while True:
        params.update({'startRecord': i})
        req = requests.get(base_url, params=params)
        
        # Print SRU-URL
        if first_request:
            print(req.url)
            first_request = False
        
        if req.status_code != 200:
            print(f"Error: Failed to fetch records. Status code {req.status_code}")
            break

        xml = soup(req.content, features="xml")
        new_records = xml.find_all('record', {'type': 'Authority'})
        
        if not new_records:
            break
        
        records.extend(new_records)
        if len(new_records) < 100:
            break
        
        i += 100
    
    return records


# Parse the records and returns a dictionary of the fetched fields and elements
def parse_record(record):
    ns = {"marc": "http://www.loc.gov/MARC21/slim"}
    xml = etree.fromstring(unicodedata.normalize("NFC", str(record)))
            
    # Return first element of a field
    def extract_text(xpath_query):
        elements = xml.xpath(xpath_query, namespaces=ns)
        return elements[0].text if elements else 'N.N.'
    
    # Return multiple elements from a field (e.g. places of publication)    
    def multi_extract_text(xpath_query):
        #return ", ".join([elem.text for elem in xml.xpath(xpath_query, namespaces=ns)]) or "N.N." #Originalcode
        return [elem.text for elem in xml.xpath(xpath_query, namespaces=ns)] or "N.N."
    
    if extract_text("marc:datafield[@tag='075']/marc:subfield[@code='b']") == "p":
        
        meta_dict = {
            "GND-ID": extract_text("marc:controlfield[@tag='001']"),        
            "Name": extract_text("marc:datafield[@tag='100']/marc:subfield[@code='a']"),    
            "Lebensdaten": extract_text("marc:datafield[@tag='100']/marc:subfield[@code='d']"),
            "Zusatzdaten": multi_extract_text("marc:datafield[@tag='548']/marc:subfield[@code='a']"),
            "Zusatzdaten Bezeichnung": multi_extract_text("marc:datafield[@tag='548']/marc:subfield[@code='i']"),
            "Land": multi_extract_text("marc:datafield[@tag='043']/marc:subfield[@code='c']"),       
            "Orte": multi_extract_text("marc:datafield[@tag='551']/marc:subfield[@code='a']"),
            "Orte Bezeichnung": multi_extract_text("marc:datafield[@tag='551']/marc:subfield[@code='i']"),
            "Weitere Angaben": extract_text("marc:datafield[@tag='678']/marc:subfield[@code='b']"),
            "Art": extract_text("marc:datafield[@tag='075']/marc:subfield[@code='b']")
        }
        
        return meta_dict


def to_df(records):
    raw_df = pd.DataFrame(records)
    
    return raw_df


def refine_df(raw_df):
    def concat_column_data(row, data_column, label_column):
        # Retrieve the two columns and ensure they are lists
        data = row[data_column]
        labels = row[label_column]
        
        if not isinstance(data, list):
            data = [data] if pd.notna(data) else []
        if not isinstance(labels, list):
            labels = [labels] if pd.notna(labels) else []
        
        # Ensure both lists have the same length by padding with "N.A."
        max_len = max(len(data), len(labels))
        data = data + ["N.A."] * (max_len - len(data))
        labels = labels + ["N.A."] * (max_len - len(labels))
        
        # Combine corresponding elements
        return [f"{datum} ({label})" for datum, label in zip(data, labels)]
    
    def extract_locations(row):
        # Map "Orte" and "Orte Bezeichnung" into a dictionary
        location_mapping = dict(zip(row["Orte Bezeichnung"], row["Orte"]))
        
        # Extract specific locations
        geburtsort = location_mapping.get("Geburtsort", "s.l.")  # Default to "s.l." if not found
        sterbeort = location_mapping.get("Sterbeort", "s.l.")   # Default to "s.l." if not found
        
        # Extract all "Wirkungsort" entries as a list
        wirkungsorte = [row["Orte"][i] for i, label in enumerate(row["Orte Bezeichnung"]) if label == "Wirkungsort"]
        
        return pd.Series({"Geburtsort": geburtsort, "Sterbeort": sterbeort, "Wirkungsorte": wirkungsorte})
    
    # Apply concat_column_data and expand into multiple columns
    raw_df["Zusatzdaten"] = raw_df.apply(concat_column_data, axis=1, args=("Zusatzdaten", "Zusatzdaten Bezeichnung"))
    expanded_df = pd.DataFrame(raw_df["Zusatzdaten"].tolist(), index=raw_df.index)
    expanded_df.fillna("N.A.", inplace=True)
    expanded_df.columns = [f"Zusatzdaten_{i+1}" for i in range(expanded_df.shape[1])]
    
    # Extract specific locations into separate columns
    location_columns = raw_df.apply(extract_locations, axis=1)
    raw_df = pd.concat([raw_df, location_columns], axis=1)
    
    # Drop unnecessary columns
    raw_df.drop(columns=["Zusatzdaten Bezeichnung", "Orte", "Orte Bezeichnung", "Zusatzdaten"], inplace=True)
    
    # Combine expanded columns with the main DataFrame
    final_df = pd.concat([raw_df, expanded_df], axis=1)
    
    return final_df


records = dnb_sru("idn=118508288") #Beethoven: idn=118508288 Bach: idn=11850553X Satzart Entität bbg=Tp* (Pica: Feld 500)

# Parse records
parsed_records = [record for record in (parse_record(rec) for rec in records) if record is not None]
if not parsed_records:
    print("No results found.")
else:
    raw_df = to_df(parsed_records)
    df = refine_df(raw_df)
    pd.set_option('display.max_columns', None)
    print(df)
    