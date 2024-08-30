from collections import defaultdict
from time import time

import datasets
import pandas as pd
from tqdm import tqdm

MISSING_KEY = '*** MISSING ***'


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

    # Iterate over the dataset and create a table with only the matching journals
    table = []
    count = 0
    missing_abstracts = 0
    filtered_articles = 0
    _batch = 0
    for row in tqdm(dataset["train"]):
        medline = row['MedlineCitation']
        pmid = medline['PMID']
        date_completed = medline['DateCompleted']['Year']
        date_revised = medline['DateRevised']['Year']

        article = medline['Article']
        journal = medline['Journal']

        article_abstract = article['Abstract']['AbstractText']
        article_title = article['ArticleTitle']
        article_language = article['Language']

        article_journal = article['Journal']
        article_journal_title = article_journal['Title']
        article_journal_issn = article_journal['ISSN']
        article_journal_abbr_iso = article_journal['ISOAbbreviation']

        if journal:
            journal_issn = journal['ISSN']
            journal_title = journal['Title']
            journal_isoabbr = journal['ISOAbbreviation']
            journal_year = journal['PubDate']['Year']
        else:
            journal_issn = MISSING_KEY
            journal_title = MISSING_KEY
            journal_isoabbr = MISSING_KEY
            journal_year = MISSING_KEY

        journal_abbr_med = medline['MedlineJournalInfo']['MedlineTA']

        if article_abstract:
            if (pmid in match_dict['id']) or \
                    (article_journal_issn.lower() in match_dict['issn'] or
                     journal_issn.lower() in match_dict['issn']) or \
                    (article_journal_title.lower() in match_dict['title']
                     or journal_title.lower() in match_dict['title']) or \
                    (article_journal_abbr_iso.lower() in match_dict['abbr'] or
                     journal_isoabbr.lower() in match_dict['abbr'] or journal_abbr_med.lower() in match_dict['abbr']):
                count += 1
                table.append(
                    dict(pmid=pmid,
                         date_completed=date_completed,
                         date_revised=date_revised,
                         article_title=article_title,
                         article_language=article_language,
                         article_abstract=article_abstract,
                         article_journal_title=article_journal_title,
                         article_journal_issn=article_journal_issn,
                         article_journal_abbr_iso=article_journal_abbr_iso,
                         journal_title=journal_title,
                         journal_issn=journal_issn,
                         journal_isoabbr=journal_isoabbr,
                         journal_year=journal_year,
                         journal_abbr_med=journal_abbr_med))
            else:
                filtered_articles += 1
        else:
            missing_abstracts += 1
        # 1M rows per batch, equals to 1.2GB
        if count > 0 and count % 1000000 == 0:
            print('Processed:', count)
            _df = pd.DataFrame.from_records(table)
            print(f'df info: {_df.info()}')
            print('Saving batch...')
            _df.to_csv(f'psy_articles_{_batch}.csv', index=False)
            print(f'Saved batch {_batch}')
            print('will dump the table to free memory')
            _batch += 1
            table = []
    else:
        # Save the remaining rows
        print('Processed:', count)
        _df = pd.DataFrame.from_records(table)
        print(f'df info: {_df.info()}')
        print('Saving the remaining batch...')
        _df.to_csv(f'psy_articles_{_batch}.csv', index=False)
        print(f'Saved batch {_batch}')

    print('Total number of articles after filtering:', count)
    print('Missing abstracts:', missing_abstracts)
    print('Dropped articles that had abstract:', filtered_articles)
    _hours = (time() - start) // 3600
    _minutes = (time() - start) % 3600 // 60
    _seconds = (time() - start) % 60
    print(f'The process took: {int(_hours):02d}:{int(_minutes):02d}:{int(_seconds):02d} hours')
