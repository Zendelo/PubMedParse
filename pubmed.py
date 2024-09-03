# coding=utf-8
# Copyright 2020 The HuggingFace Datasets Authors and the current dataset script contributor.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""MEDLINE/PubMed data."""

"""
The script downloads the pubmed dataset and parses the XML files to extract the articles.
This script was slightly adapted by Oleg Zendel from a Hugging Face dataset script to be used in a separate project.
The official documentation for the datasets library can be found here: https://huggingface.co/docs/datasets/index

Note that the script downloads only the 'baseline' files, which are released annually in December.
The script can be modified to download the 'update' files that are released daily in the following year.
For more details, refer to: https://ftp.ncbi.nlm.nih.gov/pubmed/

The script uses the 'datasets' library to download and parse the XML files.
"""

import copy
import gzip
import logging
import os
import xml.etree.ElementTree as ET
from argparse import ArgumentParser

import datasets

logger = logging.getLogger(__name__)

_CITATION = """\
Courtesy of the U.S. National Library of Medicine.
"""

_DESCRIPTION = """\
NLM produces a baseline set of MEDLINE/PubMed citation records in XML format for download on an annual basis. The annual baseline is released in December of each year. Each day, NLM produces update files that include new, revised and deleted citations. See our documentation page for more information.
"""

_HOMEPAGE = "https://www.nlm.nih.gov/databases/download/pubmed_medline.html"

_LICENSE = ""
# The URLs to the data, the data is split in 1219 files so we need to download them all.
# _URLs = [f"https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed24n{i:04d}.xml.gz" for i in range(1, 1220)]
# Comment out the above line and uncomment the below line to download only 3 random files for testing purposes
_URLs = [f"https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed24n{i:04d}.xml.gz" for i in [9, 16, 100]]

MONTHS = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10,
          'Nov': 11, 'Dec': 12}


# Copyright Ferry Boender, released under the MIT license.
# Modified by @Narsil to handle more oddities
def deepupdate(target, src):
    """Deep update target dict with src
    For each k,v in src: if k doesn't exist in target, it is deep copied from
    src to target. Otherwise, if v is a list, target[k] is extended with
    src[k]. If v is a set, target[k] is updated with v, If v is a dict,
    recursively deep-update it.

    Examples:
    >>> t = {'name': 'Ferry', 'hobbies': ['programming', 'sci-fi']}
    >>> deepupdate(t, {'hobbies': ['gaming']})
    >>> print(t)
    {'name': 'Ferry', 'hobbies': ['programming', 'sci-fi', 'gaming']}
    """
    for k, v in src.items():
        if k in target and isinstance(target[k], int) and isinstance(v, str):
            try:
                v = MONTHS.get(v) if v in MONTHS else int(v)
            except Exception as e:
                logger.warning(e)
                logger.debug(f"Failed to convert {v} to int for v={v} ; k={k} ; target={target}; src={src}")
                pass
        if k in target and type(target[k]) != type(v):
            logger.warning(f"Ignoring field {k} it's a {type(v)} and we expect a {type(target[k])}")
            continue

        if type(v) == list:
            if k not in target:
                target[k] = copy.deepcopy(v)
            elif isinstance(target[k], list):
                target[k].extend(v)
            elif isinstance(target[k], str):
                # Very special case to handle `AbstractText` which sometimes end up
                # being a list.
                new_v = " ".join(el for el in v if isinstance(el, str))
                target[k] = new_v
            else:
                logger.warning(f"Ignoring field {k} it's a {type(v)} and we expect a {type(target[k])}")
        elif type(v) == dict:
            if k not in target:
                target[k] = copy.deepcopy(v)
            elif isinstance(target[k], dict):
                deepupdate(target[k], v)
            else:
                logger.warning(f"Ignoring field {k} it's a {type(v)} and we expect a {type(target[k])}")
        elif type(v) == set:
            if k not in target:
                target[k] = v.copy()
            elif isinstance(target[k], set):
                target[k].update(v.copy())
            else:
                logger.warning(f"Ignoring field {k} it's a {type(v)} and we expect a {type(target[k])}")
        else:
            if isinstance(target[k], (list, tuple, dict)):
                logger.warning(f"Ignoring field {k} it's a {type(v)} and we expect a {type(target[k])}")
                continue

            target[k] = copy.copy(v)


def default_date():
    return {"Year": 0, "Month": 0, "Day": 0}


# the default structure of an article
def default_inline_article():
    return {
        "Abstract": {"AbstractText": ""},
        "ArticleTitle": "",
        # 'Pagination': {'MedlinePgn': datasets.Value('string')},
        # "AuthorList": {"Author": []},
        "Language": "",
        # "GrantList": {
        #     "Grant": [],
        # },
        "PublicationTypeList": {"PublicationType": []},
        "Journal": {
            'ISSN': '',
            'JournalIssue': {'Volume': '', 'Issue': '', 'PubDate': default_date()},
            'Title': '',
            'ISOAbbreviation': ''
        },
    }


# the default structure of a pubmed object that contains an article
def default_article():
    return {
        "MedlineCitation": {
            "PMID": 0,
            "DateCompleted": default_date(),
            "NumberOfReferences": 0,
            "DateRevised": default_date(),
            "Article": default_inline_article(),
            "MedlineJournalInfo": {"Country": "", "MedlineTA": "", "NlmUniqueID": "", "ISSNLinking": ""},
            # "ChemicalList": {"Chemical": []},
            # "CitationSubset": "",
            # "MeshHeadingList": {"MeshHeading": []},
        },
        "PubmedData": {
            "ArticleIdList": [{"ArticleId": []}],
            "PublicationStatus": "",
            "History": {"PubMedPubDate": []},
            # "ReferenceList": [],
        },
    }


class Pubmed(datasets.GeneratorBasedBuilder):
    """Pubmed citations records"""

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="2024", description="The 2024 annual record", version=datasets.Version("4.0.0")),
    ]

    # FILLED automatically from features
    SIMPLE_KEYS = {"PubmedArticleSet"}
    LIST_KEYS = {"PubmedArticle"}
    IGNORE_KEYS = set()

    def fill_keys_from_features(self, features):
        if isinstance(features, dict):
            for key, value in features.items():
                if isinstance(value, datasets.Sequence):
                    self.LIST_KEYS.add(key)
                    self.fill_keys_from_features(value.feature)
                else:
                    self.SIMPLE_KEYS.add(key)
                    self.fill_keys_from_features(value)

    def xml_to_dictionnary(self, parentElement):
        data = {}
        if parentElement.tag in {"AbstractText", "ArticleTitle"}:
            # XXX
            # Very special case, it will contain html leading to having very odd structure
            tag = parentElement.tag
            string = ET.tostring(parentElement).decode("utf-8").strip()
            inner_string = string[len(f"<{tag}>"): -len(f"</{tag}>")]
            return {parentElement.tag: inner_string}

        for child in list(parentElement):
            child.text = child.text if (child.text is not None) else " "
            key = child.tag
            if len(child) == 0:
                value = child.text.strip()
            else:
                value = self.xml_to_dictionnary(child)
                if isinstance(value, dict) and set(value.keys()) == {key}:
                    value = value[key]

            if key in data:
                old_value = data[key]
                if isinstance(old_value, dict):
                    data[key] = [old_value, value]
                elif isinstance(old_value, list):
                    data[key].append(value)
            elif key in self.LIST_KEYS:
                data[key] = [value]
            elif key in self.SIMPLE_KEYS:
                data[key] = value
            elif key in self.IGNORE_KEYS:
                continue
            else:
                logger.info(f"Ignoring key {key} from {parentElement.tag}")
                self.IGNORE_KEYS.add(key)

        # Filling defaults
        # if parentElement.tag == "MeshHeading" and "QualifierName" not in data:
        #     data["QualifierName"] = ""
        # elif parentElement.tag == "Author":
        #     if "ForeName" not in data:
        #         data["ForeName"] = ""
        #     if "Initials" not in data:
        #         data["Initials"] = ""
        #     if "LastName" not in data:
        #         data["LastName"] = ""
        #     if "CollectiveName" not in data:
        #         data["CollectiveName"] = ""
        if parentElement.tag == "JournalIssue":
            if "Volume" not in data:
                data["Volume"] = ""
            if "Issue" not in data:
                data["Issue"] = ""
        if parentElement.tag == "PubDate":
            if "Year" not in data:
                data["Year"] = data.get("MedlineDate", 0)
        # elif parentElement.tag == "Grant" and "GrantID" not in data:
        #     data["GrantID"] = ""

        return {parentElement.tag: data}

    def _info(self):
        Date = {
            "Year": datasets.Value("int32"),
            "Month": datasets.Value("int32"),
            "Day": datasets.Value("int32"),
        }

        # MeshHeading = {"DescriptorName": datasets.Value("string"), "QualifierName": datasets.Value("string")}

        MedlineJournalInfo = {
            "Country": datasets.Value("string"),
            # Too inconsistent
            'MedlineTA': datasets.Value('string'),
            'NlmUniqueID': datasets.Value('string'),
            'ISSNLinking': datasets.Value('string'),
        }
        # Chemical = {
        #     "RegistryNumber": datasets.Value("string"),
        #     "NameOfSubstance": datasets.Value("string"),
        # }
        # Too inconsistent in the data to be used
        Journal = {
            'ISSN': datasets.Value('string'),
            'JournalIssue': {
                'Volume': datasets.Value('string'),
                'Issue': datasets.Value('string'),
                'PubDate': Date,
            },
            'Title': datasets.Value('string'),
            'ISOAbbreviation': datasets.Value('string')
        }
        # JournalTitle = datasets.Value("string"),
        # JournalISOAbbreviation = datasets.Value("string"),
        # Author = {
        #     "LastName": datasets.Value("string"),
        #     "ForeName": datasets.Value("string"),
        #     "Initials": datasets.Value("string"),
        #     "CollectiveName": datasets.Value("string"),
        # }
        # Reference = {
        #     "Citation": datasets.Value("string"),
        #     "CitationId": datasets.Value("int32"),
        # }
        # Grant = {
        #     "GrantID": datasets.Value("string"),
        #     "Agency": datasets.Value("string"),
        #     "Country": datasets.Value("string"),
        # }
        Article = {
            'Journal': Journal,
            "Abstract": {"AbstractText": datasets.Value("string")},
            "ArticleTitle": datasets.Value("string"),
            # Too inconistent
            # 'Pagination': {'MedlinePgn': datasets.Value('string')},
            # "AuthorList": {"Author": datasets.Sequence(Author)},
            "Language": datasets.Value("string"),
            # "GrantList": {
            #     "Grant": datasets.Sequence(Grant),
            # },
            "PublicationTypeList": {"PublicationType": datasets.Sequence(datasets.Value("string"))},
        }
        features = datasets.Features(
            {
                "MedlineCitation": {
                    "PMID": datasets.Value("int32"),
                    "DateCompleted": Date,
                    "NumberOfReferences": datasets.Value("int32"),
                    "DateRevised": Date,
                    "Article": Article,
                    "MedlineJournalInfo": MedlineJournalInfo,
                    # "ChemicalList": {"Chemical": datasets.Sequence(Chemical)},
                    # "CitationSubset": datasets.Value("string"),
                    # "MeshHeadingList": {
                    #     "MeshHeading": datasets.Sequence(MeshHeading),
                    # },
                },
                "PubmedData": {
                    "ArticleIdList": datasets.Sequence({"ArticleId": datasets.Sequence(datasets.Value("string"))}),
                    "PublicationStatus": datasets.Value("string"),
                    "History": {"PubMedPubDate": datasets.Sequence(Date)},
                    # "ReferenceList": datasets.Sequence(Reference),
                },
            }
        )
        self.fill_keys_from_features(features)
        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            features=features,
            homepage=_HOMEPAGE,
            license=_LICENSE,
            citation=_CITATION,
        )

    def _split_generators(self, dl_manager):
        """Returns SplitGenerators."""
        dl_dir = dl_manager.download(_URLs)
        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                gen_kwargs={"filenames": dl_dir},
            ),
        ]

    def update_citation(self, article):
        """
        ArticleId and ArticleIdList are already used field name so we rewrite and
        flatten those as {Citation, CitationId}.
        """
        citations = []
        try:
            list_ = article["PubmedData"]["ReferenceList"]
        except Exception:
            return

        for ref in list_:
            if "Reference" not in ref:
                continue
            for re in ref["Reference"]:
                if "Citation" not in re:
                    continue
                citation = re["Citation"]
                if "ArticleIdList" not in re:
                    continue
                for r in re["ArticleIdList"]:
                    if "ArticleId" not in r:
                        continue
                    for rr in r["ArticleId"]:
                        try:
                            citation = {"Citation": citation, "CitationId": int(rr)}
                        except Exception:
                            continue
                        citations.append(citation)
        article["PubmedData"]["ReferenceList"] = citations

    def _generate_examples(self, filenames):
        """Yields examples."""
        id_ = 0
        for filename in filenames:
            with gzip.open(filename) as f:
                try:
                    tree = ET.parse(f)
                    root = tree.getroot()
                    xmldict = self.xml_to_dictionnary(root)
                except ET.ParseError:
                    logger.warning(f"Ignoring file {filename}, it is malformed")
                    continue

                for article in xmldict["PubmedArticleSet"]["PubmedArticle"]:
                    # self.update_citation(article)
                    new_article = default_article()

                    try:
                        deepupdate(new_article, article)
                    except Exception as e:
                        logger.warning(f"Exception {e}")
                        logger.warning(f"Ignoring article {article}, it is malformed")
                        continue

                    try:
                        _ = self.info.features.encode_example(new_article)
                    except Exception as e:
                        logger.warning(f"Ignore example because {e}")
                        continue
                    yield id_, new_article
                    id_ += 1


def ensure_dir(file_path, create_if_not=True):
    """
    The function ensures the dir exists,
    if it doesn't it creates it and returns the path or raises FileNotFoundError
    In case file_path is an existing file, returns the path of the parent directory
    """
    # tilde expansion
    file_path = os.path.normpath(os.path.expanduser(file_path))
    if os.path.isfile(file_path):
        directory = os.path.dirname(file_path)
    else:
        directory = file_path
    if not os.path.exists(directory):
        if create_if_not:
            try:
                os.makedirs(directory)
            except FileExistsError:
                # This exception was added for multiprocessing, in case multiple processes try to create the directory
                pass
        else:
            raise FileNotFoundError(f"The directory {directory} doesnt exist, create it or pass create_if_not=True")
    return directory


def init_logger(logger, log_file='pubmed.log'):
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)
    return logger


logger = init_logger(logger)

if __name__ == '__main__':
    # add argument parser
    parser = ArgumentParser()
    parser.add_argument("--output_dir", type=str, default="./data/pubmed/")
    parser.add_argument("--num_proc")
    args = parser.parse_args()
    n_proc = args.num_proc
    if n_proc is not None:
        n_proc = int(n_proc)

    output_dir = ensure_dir(args.output_dir)
    cache_dir = ensure_dir(os.path.join(output_dir, "cache"))

    builder = Pubmed()
    dc = datasets.download.DownloadConfig(cache_dir=cache_dir, num_proc=None)
    logger.info(f'Cache dir for raw download files {dc.cache_dir}')
    builder.download_and_prepare(output_dir=output_dir,
                                 download_config=dc,
                                 base_path=None,
                                 file_format="arrow",
                                 num_proc=n_proc,
                                 storage_options=None)
    logger.info(builder.info)
