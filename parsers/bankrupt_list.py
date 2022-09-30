import requests
import logging

from re import search
from bs4 import BeautifulSoup
from urllib.request import urlretrieve
from openpyxl import load_workbook
from sqlite3 import IntegrityError
from requests.exceptions import HTTPError

from db.create_db import conn
from utils import check_text_match_all_keys
from web_sources import years

logging.getLogger().setLevel(logging.INFO)


class ParserBankrupt:

    def parse_source(self, source: str):
        """
        extract download link from source for each actual year
        return dict with items "year": "link"
        """
        resp = requests.get(f"{source}/ru", timeout=30)
        soup = BeautifulSoup(resp.text, "lxml")
        nav = soup.find("a", string="Юридическим лицам")
        nav_elem = nav.findNext('ul')
        download_links = {}

        for y in years:
            year = nav_elem.find('a', string=f"{y} год")
            if year:
                resp = requests.get(f'{source}/ru/depsection/{y}-god', timeout=30)
                target_soup = BeautifulSoup(resp.text, 'lxml')

                link_to_download_page = None
                target_soup = target_soup.find('section', {'id': 'content'})
                links = target_soup.find_all("a")

                for a in links:
                    if check_text_match_all_keys(a.text, ["нформационн", "сообщени"]):
                        if len(a.text) - len("Информационные сообщения") < 10:
                            link_to_download_page = a
                            break

                if link_to_download_page:
                    try:
                        target_page = requests.get(f"{source}{link_to_download_page['href']}", timeout=30)
                    except HTTPError as e:
                        logging.error("Can not open download page", exc_info=True)
                        continue
                    target_soup = BeautifulSoup(target_page.text, "lxml")

                    download_link = self._extract_download_links(target_soup)
                    if download_link:
                        download_links[y] = download_link['href']

        return download_links

    def _extract_download_links(self, soup):
        """
        trying to extract the link by several known possible
        return link
        """
        download_link_keys = ["возбуждени", "дела", "банкротств", "порядке", "заявлени", "требован",
                              "кредиторам", "времен", "управляю"]
        download_link_extract_methods = [self.get_by_link, self.get_by_span, self.get_by_u]
        for method_ in download_link_extract_methods:
            download_link = method_(soup, download_link_keys)
            if download_link:
                return download_link

    @staticmethod
    def get_by_link(soup, keys):
        """
        try extract download link by searching <a> tag
        """
        links = soup.find_all("a")
        for link in links:
            if check_text_match_all_keys(link.text, keys):
                download_link = link
                return download_link

    @staticmethod
    def get_by_span(soup, keys):
        """
        try extract download link by searching <span> tag
        """
        spans = soup.find_all("span", recursive=True)
        for span in spans:
            if check_text_match_all_keys(span.text, keys):
                download_link = span.find_parent('a')
                return download_link

    @staticmethod
    def get_by_u(soup, keys):
        """
        try extract download link by searching <u> tag
        """
        u_list = soup.find_all("u")
        for u in u_list:
            if check_text_match_all_keys(u.text, keys):
                download_link = u.find_parent('a')
                return download_link

    def download_file(self, source: str, year: str, link: str):
        """
        download file with name format "region_year.xlsx"
        return path to file
        """
        filename = f"{search('http://(.+?).kgd.gov.kz', source).group(1)}_{year}.xlsx"
        path = "./downloads/bankrupts/" + filename
        urlretrieve(link, path)
        return path

    def save_data(self, year: str, source: str, path: str):
        """
        parse xlsx file and write data to database
        """
        try:
            doc = load_workbook(path)
            sheet = doc.active
        except Exception as e:
            logging.error("Error when reading file", exc_info=True)
            return

        position = self.get_start_position(sheet)
        if not position:
            logging.error("Can not find data in file")
            return

        region = search('http://(.+?).kgd.gov.kz', source).group(1)
        cursor = conn.cursor()
        self.insert_row(connection=conn, cursor=cursor, position=position,
                        sheet=sheet, year=year, region=region)

    def get_start_position(self, sheet):
        """
        find first cell of data list
        """
        i = 0
        while i < 20:
            i += 1
            value = sheet['B' + str(i)].value
            if value and str(value) == "2":
                return i

    def insert_row(self, connection, cursor, position, sheet, year, region):
        """
        read rows and commit to database
        """
        while True:
            position += 1
            if not sheet["B" + str(position)].value:
                break
            composite_id = str(sheet["B" + str(position)].value) + str(year) + str(sheet["A" + str(position)].value)
            row = (
                composite_id, region, year,
                sheet["A" + str(position)].value,
                sheet["B" + str(position)].value,
                sheet["C" + str(position)].value,
                sheet["D" + str(position)].value,
                sheet["E" + str(position)].value,
                sheet["F" + str(position)].value,
                sheet["G" + str(position)].value,
                sheet["H" + str(position)].value,
                sheet["I" + str(position)].value,
                sheet["G" + str(position)].value,
                sheet["K" + str(position)].value,
                sheet["L" + str(position)].value,
                sheet["M" + str(position)].value,
                sheet["N" + str(position)].value,
            )
            statement = "INSERT INTO bankrupt VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
            try:
                cursor.execute(statement, row)
                connection.commit()
            except IntegrityError:
                logging.warning(f"Ignored duplicate id {composite_id}")
                continue
