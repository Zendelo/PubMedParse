# PubMedParse

There are two scripts in this repository. The first script `pubmed.py` downloads the raw data from the PubMed API (43GB)
into the cache directory (defined in the code) and then parses the data into a huggingface dataset format, saved as
arrow (format) files in the `output_dir` directory. The second script `parse_dataset_to_table.py` reads the arrow files
and parses the data into a table format, saved as a csv file in the `output_dir` directory.

Create a new environment and install the requirements:

```bash
conda create -n pubmedparse python=3.10
conda activate pubmedparse
pip install -r requirements.txt
```

Then run the following command to download and generate the raw data:

```bash
python pubmed.py --num_proc 8 --output_dir data/pubmed
```

`num_proc` is the number of processes to use for parsing the data.
`output_dir` is the directory where the data will be saved.

The raw data downloaded from PubMed will be saved in `cache` directory within the `output_dir`. The parsed data in the
huggingface dataset format will be saved in the `output_dir`. A total of ~36.5M (36,555,430) articles, 43GB of data,
will be downloaded and saved in the `cache` directory. The parsed data is 40GB in size.
A log file will be saved in the same directory as the code with the name `pubmed.log`. Useful mostly for debugging
purposes.

Then filter the data and parse it into a table format:

```bash
python parse_datasaet_to_table.py --dataset data/pubmed --output psy_articles.csv --journals_list psy_journals_list.csv
```

`dataset` is the directory where the parsed data is saved, the same as the `output_dir` in the previous step. `output`
is the name of the csv file where the filtered data will be saved. `journals_list` is the name of the csv file that
contains the list of journals to filter the data.
The filtered data contains 490,287 articles, and it's 564MB in size.

### Note that the raw data will be downloaded from the PubMed API to the huggingface cache directory. To change the cache directory, set the

`HF_HOME` environment variable to the desired directory before running the script.

Some articles were missing a year in the PubDate format and have the value in MedlineDate instead. Where a single year
can be easily extracted it was added as PubDate as well, otherwise the date only appears in MedlineDate often in that
format: '1977-1978 Winter'.

## Updating the data for a new year

The data is released every year around December. To update the data for the new year, update the following line in the
`pubmed.py` script:

```python
# The URLs to the data, the data is split in 1219 files so we need to download them all.
_URLs = [f"https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed24n{i:04d}.xml.gz" for i in range(1, 1220)]
```

Note that the data is split into 1219 files, so the range is from 1 to 1220.
The URLS might change, so make sure to check the [PubMed FTP site](https://ftp.ncbi.nlm.nih.gov/pubmed/) baseline
directory for the new URLs.