import logging

from urllib.error import HTTPError

from web_sources import bankrupt_list_sources
from parsers.bankrupt_list import ParserBankrupt

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class BankruptListJob:

    @classmethod
    def run(cls):
        """
        for each source from bankrupt_list_sources
        for each actual year
        in loop:
        extract download link -> download files -> save to db
        """
        parser = ParserBankrupt()
        for source in bankrupt_list_sources:
            logging.info(f"Start parsing source: {source}")
            links = parser.parse_source(source)
            logging.info(f"Extract links to download: {links}")
            for key, value in links.items():
                try:
                    path_to_file = parser.download_file(source=source, year=key, link=value)
                except HTTPError:
                    logging.error("Error when downloading file, details:", exc_info=True)
                    continue
                logging.info(f"Start process to write {path_to_file} content in database...")
                parser.save_data(year=key, source=source, path=path_to_file)
