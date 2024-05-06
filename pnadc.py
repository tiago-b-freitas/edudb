import os
import zipfile

from bs4 import BeautifulSoup
import requests

from .common import handleDatabase, parse_sas
from .definitions import RAW_FILES_PATH

PATH = 'pnadc'
URL = 'http://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados'
CRITERION = ('[self.url+"/"+a["href"] for a in soup.find_all("a")'
                'if str(self.trimester).zfill(2)+str(self.year) in a["href"]]')
FIRST_YEAR = 2012
LAST_YEAR = 2023
FIRST_TRIMESTER = 1 
LAST_TRIMESTER = 4


class handlePNADc(handleDatabase):

    def __init__(self, year, trimester, medium=requests):
        if (year < FIRST_YEAR
            or (year == FIRST_YEAR and trimester < FIRST_TRIMESTER)
            or year > LAST_YEAR
            or (year == LAST_YEAR and trimester > LAST_TRIMESTER)):
            print_error(f'Não há dados disponíveis para o ano {year} e o trime'
                        f'stre {trimester}.')
            raise ValueError

        super().__init__(medium, year)
        self.trimester = trimester
        self.name = 'PNADc'
        self.path = os.path.join(self.root, PATH)
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.raw_files_path = os.path.join(self.path, RAW_FILES_PATH)
        if not os.path.isdir(self.raw_files_path):
            os.mkdir(self.raw_files_path)
        self.url = f'{URL}/{year}'
        self.is_zipped = True
        self.filename = f'{self.year}-{self.trimester}-PNADc'

    def basic_names(self):
        return [f'Base de dados = "{self.name}"',
                f'Ano = "{self.year}"',
                f'Trimestre = "{self.trimester}"']

    def get_url(self):
        criterion = CRITERION
        file_url = super().get_url(criterion)
        return self.file_url

    def unzip(self):
        if not hasattr(self, 'filepath'):
            self.get_save_raw_database()
        if not hasattr(self, 'database_dict'):
            self.make_database_dict()

        with zipfile.ZipFile(self.filepath, 'r') as zf:
            fns = [fn for fn in zf.namelist()]
            if len(fns) > 1:
                print_error('Mais de um arquivo .txt')
                raise ValueError
            filename = fns[0]
            with zf.open(filename) as f:
                self.df = pd.read_fwf(f,
                                 names=self.database_dict.codigo,
                                 colspecs=self.colspecs,
                                 dtype=self.dtypes)
        return self.df

    def make_database_dict(self):
        url = os.path.join(URL, 'Documentacao') 
        r = self.medium.get(url)   
        soup = BeautifulSoup(r.text, 'html.parser')
        file_url = [a['href'] for a in soup.find_all('a')
                    if 'dicionario' in a['href'].lower()][0]
        filepath = os.path.join(self.raw_files_path, file_url)
        if not os.path.isfile(filepath):
            r = self.medium.get(os.path.join(url, file_url))
            with open(filepath, 'wb') as f:
                f.write(r.content)

        with zipfile.ZipFile(filepath) as zf:
            fp = [fp for fp in zf.filelist()
            with zf.open('input_trimestral.txt') as f:
                parse_sas(self, f, encoding='latin-1')

    def otimize_df(self):
        for col in self.df.select_dtypes('float'):
            self.df[col] = pd.to_numeric(self.df[col], downcast='float')
        for col in self.df.select_dtypes('int'):
            self.df[col] = pd.to_numeric(self.df[col], downcast='unsigned')
        return self.df