import os
import pandas as pd
import re


def data_cleaning(text):
    """
    Regular Expression is used to remove all @(Reply), #(Hashtag), and URL
    
    Parameters:
        text (str): original text
    
    Return:
        text_cleaned (str): cleaned version of the original text
    """

    # Remove @(Reply)
    text_1 = re.sub(r'@[^\s]*', "", text)
    # Remove #(Hashtag)
    text_3 = re.sub(r'#[^\s]*', "", text_1)
    # Remove URL
    text_cleaned = re.sub(r'[a-zA-Z]+://[^\s]*', "", text_3)

    return text_cleaned


def text_sample_join(folderpath, frac, df):
    """
    Resample all datasets in the folder with a given fraction,
    Join them into one dataset
    
    Parameters:
        folderpath (str): the path of the folder holding all datasets
        frac (float): resampling fraction
        df (DataFrame): the dataset to join all the subsets into
        
    Return:
        df (DataFrame): resampled and joined dataset
    """

    # Traverse all files in the folder
    for file in os.listdir(folderpath):
        
        # Generate the file path
        filepath = os.path.join(folderpath, file)

        # Resample and join to df if the path is assigned a file
        if os.path.isfile(filepath):
            text_data = pd.read_csv(filepath, lineterminator='\n')
            text_sample = text_data.sample(frac=frac, random_state=1216)
            df = pd.concat([df, text_sample])

    return df


def main():

    df = pd.DataFrame(columns=['id', 'text', 'start_time'])
    text_joined = text_sample_join('text_data', 0.5, df)

    text_joined['text_cleaned'] = text_joined['text'].apply(data_cleaning)

    text_joined.drop(['Unnamed: 0', 'withheld'], axis=1).to_csv('text_processed.csv')


if __name__ == "__main__":
    main()
