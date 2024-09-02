"""
This script will parse the pubmed dataset and filter the articles that belong to the psychology journals.
The script will save the filtered articles in batches of 500k rows to avoid memory issues.
To modify the journals list, you can change the 'psy_journals_list.csv' file.

The information about the pubmed tags, which serve as columns in the final data  can be found here:
https://dtd.nlm.nih.gov/ncbi/pubmed/doc/out/180101/index.html

"""

from collections import defaultdict
from time import time

import datasets
import pandas as pd
from tqdm import tqdm


# extract and merge the unique values from the journals_df into a dictionary with combined keys
def extract_unique_journals(journals_df):
    # drop the boolean column that doesn't contain any keys
    jdf = journals_df.drop(columns=['Matched_ISSN'])
    _dict = defaultdict(set)
    # iterate over the columns and update the dictionary with the unique values
    for col in jdf.columns:
        if 'issn' in col.lower():
            _dict['issn'].update(jdf[col].str.split(',').explode().str.strip().str.lower())
        elif 'title' in col.lower():
            _dict['title'].update(jdf[col].str.strip().str.lower())
        elif 'abbr' in col.lower():
            _dict['abbr'].update(jdf[col].str.strip().str.lower())
        elif 'id' in col.lower():
            _dict['id'].update(jdf[col])
    return _dict


if __name__ == '__main__':
    # Start the timer
    start = time()
    # Load the dataset
    dataset = datasets.load_dataset("data/pubmed")

    # Load the journals list
    journals_df = pd.read_csv('psy_journals_list.csv', header=0, sep=',')

    # Extract unique values from the journals_df
    match_dict = extract_unique_journals(journals_df)

    # Create a list (table) to store the filtered articles, and initialize the counters
    table = []
    count = 0
    missing_abstracts = 0
    filtered_articles = 0
    _batch = 0

    # Iterate over the dataset rows and filter the articles that belong to the psychology journals
    for row in tqdm(dataset["train"]):
        medline = row['MedlineCitation']
        pmid = medline['PMID']
        # date_completed = medline['DateCompleted']['Year']
        # date_revised = medline['DateRevised']['Year']

        article = medline['Article']
        journal = article['Journal']
        journal_issue = journal['JournalIssue']

        article_abstract = article['Abstract']['AbstractText']
        article_title = article['ArticleTitle']
        article_language = article['Language']

        journal_title = journal['Title']
        journal_issn = journal['ISSN']
        journal_abbr_iso = journal['ISOAbbreviation']

        journal_year = journal_issue['PubDate']['Year']

        journal_abbr_med = medline['MedlineJournalInfo']['MedlineTA']

        # Check if the article has an abstract
        if article_abstract:
            # Check if the article belongs to the psychology journals
            if pmid in match_dict['id'] or \
                    journal_issn.lower() in match_dict['issn'] or \
                    journal_title.lower() in match_dict['title'] or \
                    journal_abbr_iso.lower() in match_dict['abbr'] or journal_abbr_med.lower() in match_dict['abbr']:
                # Increment the count of the articles
                count += 1
                # If the article belongs to the psychology journals, then append it to the table, as a dictionary.
                # Each dict key will be a column in the DataFrame that will be created later.
                table.append(
                    dict(
                        PMID=pmid,
                        # DateCompleted=date_completed,
                        # DateRevised=date_revised,
                        ArticleTitle=article_title,
                        ArticleLanguage=article_language,
                        AbstractText=article_abstract,
                        JournalTitle=journal_title,
                        JournalISSN=journal_issn,
                        ISOAbbreviation=journal_abbr_iso,
                        PubDate=journal_year,
                        MedlineTA=journal_abbr_med
                    ))
            else:
                # If the article doesn't belong to the psychology journals, then increment the filtered articles count
                filtered_articles += 1
        else:
            # If the article doesn't have an abstract, then increment the missing abstracts count
            missing_abstracts += 1
        # Will save the data in batches of up to 500k rows, to reduce memory consumption. Smaller batch -> less memory.
        if count > 0 and count % 500000 == 0:
            print('Processed:', count)
            # Create a DataFrame from the table
            _df = pd.DataFrame.from_records(table)
            print(f'df info: {_df.info()}')
            print('Saving batch...')
            # Save the DataFrame to a csv file, with the defined separator
            _df.to_csv(f'psy_articles_{_batch}.csv', sep=',' ,index=False)
            print(f'Saved batch {_batch}')
            print('will dump the table to free memory')
            _batch += 1
            # Clear the table to free memory
            table = []
    else:
        # Save the remaining rows
        print('Processed:', count)
        # Create a DataFrame from the table
        _df = pd.DataFrame.from_records(table)
        print(f'df info: {_df.info()}')
        print('Saving the remaining batch...')
        # Save the DataFrame to a csv file, with the defined separator
        _df.to_csv(f'psy_articles_{_batch}.csv', index=False)
        print(f'Saved batch {_batch}')

    print('Total number of articles after filtering:', count)
    print('Missing abstracts:', missing_abstracts)
    print('Dropped articles that had abstract:', filtered_articles)
    _hours = (time() - start) // 3600
    _minutes = (time() - start) % 3600 // 60
    _seconds = (time() - start) % 60
    print(f'The process took: {int(_hours):02d}:{int(_minutes):02d}:{int(_seconds):02d} hours')
