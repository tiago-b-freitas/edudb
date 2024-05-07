import os
import zipfile

from bs4 import BeautifulSoup
import pandas as pd
import requests

from .common import handleDatabase, parse_sas, print_info
from .definitions import RAW_FILES_PATH

PATH = 'pnadc'
URL = 'http://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados'
EXPR_FILTER = ('[self.url+"/"+a["href"] for a in soup.find_all("a")'
                'if str(self.trimester).zfill(2)+str(self.year) in a["href"]]')
FIRST_YEAR = 2012
LAST_YEAR = 2023
FIRST_TRIMESTER = 1 
LAST_TRIMESTER = 4
WEIGHTS = 'V1028'

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
        self.path_dict = os.path.join(self.path, f'dicionario') 
        self.raw_files_path = os.path.join(self.path, RAW_FILES_PATH)
        if not os.path.isdir(self.raw_files_path):
            os.mkdir(self.raw_files_path)
        self.url = f'{URL}/{year}'
        self.is_zipped = True
        self.filename = f'{self.year}-{self.trimester}-PNADc'
        self.weight_var = WEIGHTS
        self.is_otimized = True
        self.expr_filter = EXPR_FILTER

    def basic_names(self):
        return [f'Base de dados = "{self.name}"',
                f'Ano = "{self.year}"',
                f'Trimestre = "{self.trimester}"']

    def unzip(self):
        if not hasattr(self, 'filepath'):
            self.filepath = self.get_save_raw_database()
        if not hasattr(self, 'df_dict'):
            self.make_database_dict()
        if not hasattr(self, 'int_vars'):
            self.make_map_dict()

        # Ajustar algumas incorreções que atribuem dtype 'category' para
        #  variáveis que são Int
        for col in self.int_vars:
            self.dtypes[col] = 'string'

        with zipfile.ZipFile(self.filepath, 'r') as zf:
            fns = [fn for fn in zf.namelist()]
            if len(fns) > 1:
                print_error('Mais de um arquivo .txt')
                raise ValueError
            filename = fns[0]
            with zf.open(filename) as f:
                print_info('Descompressão concluída!')
                print_info('Carregando DataFrame...')
                print(self.df_dict)
                self.df = pd.read_fwf(f,
                                 names=self.df_dict.key,
                                 colspecs=self.colspecs,
                                 dtype=self.dtypes)
                print_info('Carregando concluído!')
        return self.df

    def make_database_dict(self):
        url = os.path.join(URL, 'Documentacao') 
        r = self.medium.get(url)   
        soup = BeautifulSoup(r.text, 'html.parser')
        file_url = [a['href'] for a in soup.find_all('a')
                    if 'dicionario' in a['href'].lower()][0]
        self.doc_filepath = os.path.join(self.raw_files_path, file_url)
        if not os.path.isfile(self.doc_filepath):
            r = self.medium.get(os.path.join(url, file_url))
            with open(self.doc_filepath, 'wb') as f:
                f.write(r.content)

        with zipfile.ZipFile(self.doc_filepath) as zf:
            fp = [fp for fp in zf.namelist() if os.path.splitext(fp)[-1] == '.txt'][0]
            with zf.open(fp) as f:
                parse_sas(self, f, encoding='latin-1', ignore='peso replicado')

    def make_map_dict(self):
        if not hasattr(self, 'doc_filepath'):
            self.make_database_dict()
        print_info('Preparando dicionário de mapeamento das variáveis...')
        with zipfile.ZipFile(self.doc_filepath) as zf:
            fp = [fp for fp in zf.namelist() if os.path.splitext(fp)[-1] == '.xls'][0]
            with zf.open(fp) as f_xls:
                df = pd.read_excel(f_xls,
                                   names=['pos', 'size', 'key', 'n', 'desc',
                                          'cod_cat', 'cat', 'period'],
                                   dtype='string', skiprows=3)
        df = (df[~df.desc.str.startswith('Peso replicado')
                .fillna(False)]
                .reset_index(drop=True))
        df = df[['key', 'cod_cat', 'cat', 'desc']]
        df.cat = df.cat.str.strip()
        df.key = df.key.ffill()

        cat_vars = [key for key, value in self.dtypes.items() if value == 'category']
        names = []
        self.int_vars = []
        map_dict_vars = {}
        for key, g in df[df.key.isin(cat_vars)].groupby('key'):
            e = g.cod_cat.iloc[0]
            if isinstance(e, str) and e.isdigit():
                map_dict_vars[key] = {cod_cat: cat for cod_cat, cat
                                      in g[['cod_cat', 'cat']].itertuples(False, None)
                                      if pd.notna(cod_cat)}
                names.append(g.iloc[0].desc)
            elif pd.isna(e) or e != 'código':
                self.int_vars.append(key)
            else: #codigo ver tabelas anexas
                #TODO
                ...

        df = pd.DataFrame({'COD_VAR': [key for key in map_dict_vars.keys()],
                           'NOME_VAR': [name for name in names],
                           'MAP_VAR': [value for value in map_dict_vars.values()]})
        print_info('Preparação concluída!')

        df.to_excel(f'{self.path_dict}.xlsx', index=False)
        df.to_pickle(f'{self.path_dict}.pickle')

        self.map_dict_vars = df
        return df

    def preprocess_df(self):
            if not hasattr(self, 'int_vars'):
                self.make_map_dict()
            for col in self.df.select_dtypes('string').columns.to_list() + self.int_vars:
                self.df[col] = pd.to_numeric(self.df[col], downcast='unsigned')
            self.df.dropna(axis=1, how='all', inplace=True)
            return self.df

    def get_df(self, filetype='parquet', **kwargs):
        return super().get_df(filetype, **kwargs)
