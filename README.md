# PubMedParse

Create a new environment and install the requirements:

```bash
conda create -n pubmedparse python=3.10
conda activate pubmedparse
pip install -r requirements.txt
```
Then run the following command to download and generate the raw data:

```bash
python pubmed.py
```

The raw data will be saved in the `data` directory. The data is in the huggingface dataset format. 
Then filter the data and parse it into a table format:

```bash
python parse_datasaet_to_table.py
```

### Note that the raw data will be downloaded from the PubMed API to the huggingface cache directory. To change the cache directory, set the `HF_HOME` environment variable to the desired directory before running the script.

Some articles were missing a year in the PubDate format and have the value in MedlineDate instead. Where a single year can be easily extracted it was added as PubDate as well, otherwise the date only appears in MedlineDate often in that format: '1977-1978 Winter'.