import datasets
import pandas as pd

# Load the dataset
dataset = datasets.load_dataset("data/pubmed")

# All 'MedlineCitation' keys:
# ['PMID', 'DateCompleted', 'NumberOfReferences', 'DateRevised', 'Article',
# 'MedlineJournalInfo', 'ChemicalList', 'CitationSubset', 'MeshHeadingList']
# All 'PubmedData' keys:
# ['ArticleIdList', 'PublicationStatus']


# Article keys:
# ['Abstract', 'ArticleTitle', 'AuthorList', 'Language', 'GrantList', 'PublicationTypeList']

journals_df = pd.read_csv('psy_journals_list.csv', header=0, sep=',')


# Iterate over the dataset and create a table
table = []
count = 0
missing_abstracts = 0
_batch = 0
for row in dataset["train"]:
    pmid = row['MedlineCitation']['PMID']
    date_completed = row['MedlineCitation']['DateCompleted']['Year']
    date_revised = row['MedlineCitation']['DateRevised']['Year']

    article = row['MedlineCitation']['Article']

    abstract = article['Abstract']['AbstractText']

    journal = article['Journal']
    journal_title = journal['Title']
    journal_issn = journal['ISSN']
    journal_abbr_iso = journal['ISOAbbreviation']

    journal_abbr_med = row['MedlineCitation']['MedlineJournalInfo']['MedlineTA']

    title = article['ArticleTitle']
    language = article['Language']

    if abstract:
        count += 1
        table.append(
            dict(pmid=pmid,
                 date_completed=date_completed,
                 date_revised=date_revised,
                 title=title,
                 language=language,
                 abstract=abstract,
                 journal_title=journal_title,
                 journal_issn=journal_issn,
                 journal_abbr_iso=journal_abbr_iso,
                 journal_abbr_med=journal_abbr_med))
    else:
        missing_abstracts += 1
    if count % 1000000 == 0:
        print('Processed:', count)
        _df = pd.DataFrame.from_records(table)
        _df.to_csv(f'psy_articles_{_batch}.csv', index=False)
        _batch += 1
        table = []
        print(f'Saved batch {_batch}')

print('Total number of abstracts:', count)
print('Missing abstracts:', missing_abstracts)
