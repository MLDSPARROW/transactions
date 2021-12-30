import requests
import xml.etree.ElementTree as et
import pandas as pd
import re


def get_transactions(url):
    """
    get and parse all transactions from url
    input:url
    output the dataframe of parsed data
    """
    response = requests.get(url = url)

    tree = et.ElementTree(et.fromstring(response.content))
    root = tree.getroot()
    child_dataset = root.find('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}DataSet')
    generic_namespace = '{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}'
    child_series = child_dataset.find(generic_namespace + 'Series')
    child_obs = child_series.findall(generic_namespace + 'Obs')

    all_generic_data = []
    for child in child_obs:
        all_generic_data.append((child.find(generic_namespace + 'ObsDimension').attrib['value'], child.find(generic_namespace + 'ObsValue').attrib['value']))
    all_generic_data_df = pd.DataFrame(all_generic_data, columns = ["TIME_PERIOD", "OBS_VALUE"])

    identified_value = url.split('/')[-1]
    if identified_value[-1] == '?':
        identified_value = identified_value[:-1]
    identifier = [identified_value] * len(all_generic_data)
    identifier_df = pd.DataFrame(identifier, columns = ['IDENTIFIER'])

    result_df = pd.concat([identifier_df, all_generic_data_df], axis = 1)
    return result_df

def get_formula_data(formula):
    """
    get transaction of formula from right side of the equation
    the output is data frame including right side identifers as columns
    """
    formula  = re.sub('\s+', '', formula)
    right_side = re.split(r"=", formula)[1]
    right_identifiers = re.split(r"-|\+", right_side)
    base_url = "https://sdw-wsrest.ecb.europa.eu/service/data/BP6/"
    result_df = pd.DataFrame()
    for identifier in right_identifiers:
        df = get_transactions(base_url + identifier)
        df = df[["TIME_PERIOD", "OBS_VALUE"]].set_index("TIME_PERIOD").rename(columns={"OBS_VALUE": identifier})
        result_df = pd.concat([result_df, df], axis = 1)

    
    result_df = result_df.replace("NaN", 0).astype(float)
    return result_df

def compute_aggregate(formula):
    """
    
    """
    formula  = re.sub('\s+', '', formula)
    right_side = re.split(r"=", formula)[1]
    left_side = re.split(r"=", formula)[0]
    right_identifiers = re.split(r"-|\+", right_side)

    df = get_formula_data(formula)

    all_column_values = re.findall(r'[+-]?[A-Za-z0-9_.]+', right_side)

    for col in all_column_values:
        if col[0] == '-':
            column = col[1:]
            df[column] = df[column].astype(float).apply(lambda x: -x)
    result_df = pd.DataFrame()        
    result_df[left_side] = df[right_identifiers].sum(axis = 1)
    return result_df

if __name__ == "__main__":
    sample = """Q.N.I8.W1.S1.S1.T.A.FA.D.F._Z.EUR._T._X.N
    = Q.N.I8.W1.S1P.S1.T.A.FA.D.F._Z.EUR._T._X.N - Q.N.I8.W1.S1Q.S1.T.A.FA.D.F._Z.EUR._T._X.N"""
    print(compute_aggregate(sample))