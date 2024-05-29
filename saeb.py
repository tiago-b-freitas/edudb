import os
import zipfile

import pandas as pd
import requests

from .common import handleDatabase
from .definitions import RAW_FILES_PATH, CERT_PATH

NAME = 'saeb'
PATH = 'saeb'
URL_MICRODADOS = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/saeb'
URL_RESULTADOS = 'https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/saeb/resultados/'
EXPR_FILTER_MICRODADOS = '[a["href"] for a in soup.find("div", id="parent-fieldname-text").find_all("a") if str(self.year) in a["href"]]'
EXPR_FILTER_RESULTADOS = '[a["href"] for a in soup.find("div", id="parent-fieldname-text").find_all("a", class_="external-link") if str(self.year) in a["href"] and self.agg_finder in a["href"].lower() and (".xlsx" in a["href"].lower() or ".zip" in a["href"].lower()) and "alfabe" not in a["href"]]'
CERT = 'inep-gov-br-chain.pem'
FIRST_YEAR = 2007 # Há dados desde 1995, mas ainda não implementei.
LAST_YEAR = 2021
AGG_LEVEL = (
    'brasil',
    'uf',
)

COLUMNS_LABELS_HEADER = {
    'brasil': {2011: ['ANO_SAEB', 'ID'],
               2015: ['ID'],},
    'uf': {2011: ['ANO_SAEB', 'CO_UF', 'NO_UF'],
           2015: ['CO_UF', 'NO_UF'],},
}
COLUMNS_LABELS_BODY = {
    2011: [
        'DEPENDENCIA_ADM', 'LOCALIZACAO', 'CAPITAL',
        'MEDIA_5_LP',  'MEDIA_5_MT', 
        'MEDIA_9_LP',  'MEDIA_9_MT',
        'MEDIA_12_LP', 'MEDIA_12_MT'
    ],
    2015: [
        'DEPENDENCIA_ADM', 'LOCALIZACAO',
        'MEDIA_5_LP',  'MEDIA_5_MT',
        'MEDIA_9_LP',  'MEDIA_9_MT',
        'MEDIA_12_LP', 'MEDIA_12_MT',
        'nivel_0_LP5', 'nivel_1_LP5', 'nivel_2_LP5', 'nivel_3_LP5', 'nivel_4_LP5',
        'nivel_5_LP5', 'nivel_6_LP5', 'nivel_7_LP5', 'nivel_8_LP5', 'nivel_9_LP5',
        
        'nivel_0_MT5', 'nivel_1_MT5', 'nivel_2_MT5', 'nivel_3_MT5', 'nivel_4_MT5',
        'nivel_5_MT5', 'nivel_6_MT5', 'nivel_7_MT5', 'nivel_8_MT5', 'nivel_9_MT5',
        'nivel_10_MT5',

        'nivel_0_LP9',  'nivel_1_LP9', 'nivel_2_LP9', 'nivel_3_LP9', 'nivel_4_LP9',
        'nivel_5_LP9',  'nivel_6_LP9', 'nivel_7_LP9', 'nivel_8_LP9',

        'nivel_0_MT9',  'nivel_1_MT9',  'nivel_2_MT9', 'nivel_3_MT9', 'nivel_4_MT9',
        'nivel_5_MT9',  'nivel_6_MT9',  'nivel_7_MT9', 'nivel_8_MT9', 'nivel_9_MT9',

        'nivel_0_LP12', 'nivel_1_LP12', 'nivel_2_LP12', 'nivel_3_LP12', 'nivel_4_LP12',
        'nivel_5_LP12', 'nivel_6_LP12', 'nivel_7_LP12', 'nivel_8_LP12',

        'nivel_0_MT12', 'nivel_1_MT12', 'nivel_2_MT12', 'nivel_3_MT12', 'nivel_4_MT12',
        'nivel_5_MT12', 'nivel_6_MT12', 'nivel_7_MT12', 'nivel_8_MT12', 'nivel_9_MT12',
        'nivel_10_MT12'
   ],
}

COLUMNS_LABELS_UF = {
    2011: [
        'ANO_SAEB',    'CO_UF',     'NO_UF',       'DEPENDENCIA_ADM',
        'LOCALIZACAO', 'CAPITAL',   'MEDIA_5_LP',  'MEDIA_5_MT', 
        'MEDIA_9_LP',  'MEDIA_9_MT','MEDIA_12_LP', 'MEDIA_12_MT'
    ],
    2015: [
        'CO_UF',        'NO_UF',        'DEPENDENCIA_ADM', 'LOCALIZACAO',
        'MEDIA_5_LP',   'MEDIA_5_MT',   'MEDIA_9_LP',   'MEDIA_9_MT',  'MEDIA_12_LP',  
        'MEDIA_12_MT',  'nivel_0_LP5',  'nivel_1_LP5',  'nivel_2_LP5', 'nivel_3_LP5',  
        'nivel_4_LP5',  'nivel_5_LP5',  'nivel_6_LP5',  'nivel_7_LP5', 'nivel_8_LP5',  
        'nivel_9_LP5',  'nivel_0_MT5',  'nivel_1_MT5',  'nivel_2_MT5', 'nivel_3_MT5',  
        'nivel_4_MT5',  'nivel_5_MT5',  'nivel_6_MT5',  'nivel_7_MT5', 'nivel_8_MT5',  
        'nivel_9_MT5',  'nivel_10_MT5', 'nivel_0_LP9',  'nivel_1_LP9', 'nivel_2_LP9',  
        'nivel_3_LP9',  'nivel_4_LP9',  'nivel_5_LP9',  'nivel_6_LP9', 'nivel_7_LP9',  
        'nivel_8_LP9',  'nivel_0_MT9',  'nivel_1_MT9',  'nivel_2_MT9', 'nivel_3_MT9',  
        'nivel_4_MT9',  'nivel_5_MT9',  'nivel_6_MT9',  'nivel_7_MT9', 'nivel_8_MT9',  
        'nivel_9_MT9',  'nivel_0_LP12', 'nivel_1_LP12', 'nivel_2_LP12',  
        'nivel_3_LP12', 'nivel_4_LP12', 'nivel_5_LP12', 'nivel_6_LP12',  
        'nivel_7_LP12', 'nivel_8_LP12', 'nivel_0_MT12', 'nivel_1_MT12',  
        'nivel_2_MT12', 'nivel_3_MT12', 'nivel_4_MT12', 'nivel_5_MT12',  
        'nivel_6_MT12', 'nivel_7_MT12', 'nivel_8_MT12', 'nivel_9_MT12',   
        'nivel_10_MT12'
   ],
}


class handleSaeb(handleDatabase):
    def __init__(self, year, agg_level, medium=requests):
        if (year < FIRST_YEAR
            or year > LAST_YEAR):
            print_error(f'Não há dados disponíveis para o ano {year}')
            raise ValueError
        if agg_level not in AGG_LEVEL:
            print_error('As opções de nível de agregação são:'
                       f'{AGG_LEVEL}')
            raise ValueError
        if year == 2015 and agg_level == 'brasil':
            print_error('Não há informações sobre os resultados agregados'
                        'do país do saeb no ano de 2015')
            raise ValueError

        super().__init__(year, medium)
        self.agg_level = agg_level
        self.name = NAME
        self.path = os.path.join(self.root, PATH, self.agg_level)
        if not os.path.isdir(self.path):
            os.makedirs(self.path)
        self.raw_files_path = os.path.join(os.path.dirname(self.path), RAW_FILES_PATH)
        if not os.path.isdir(self.raw_files_path):
            os.makedirs(self.raw_files_path)
        if self.agg_level in ('brasil', 'uf'):
            self.url = os.path.normcase(os.path.join(URL_RESULTADOS, str(self.year)))
        if self.agg_level == 'uf':
            self.agg_finder = 'estad'
        else:
            self.agg_finder = self.agg_level
        self.is_zipped = True
        self.filename = f'{self.year}-{self.agg_level}-{self.name}'
        self.expr_filter = EXPR_FILTER_RESULTADOS
        self.cert_path = os.path.join(os.path.dirname(__file__), CERT_PATH, CERT)
        if not os.path.isfile(self.cert_path):
            self.cert_path = False

    def get_url(self):
        match self.agg_level:
            case 'brasil' | 'uf' | 'municipio':
                self.file_urls = [super().get_url(unique=True)]

    def get_save_raw_database(self):
        self.get_url()
        self.filepaths = []
        for file_url in self.file_urls:
            filepath = super().get_save_raw_database(file_url)
            self.filepaths.append(filepath)

    def unzip(self):
        if not hasattr(self, 'filepaths'):
            self.get_save_raw_database()

        if self.agg_level in ('brasil', 'uf'):
            skiprows = 0
            header = 0
            if self.agg_level == 'brasil':
                    sheet_name = 0
                    fn_2017 = 'TS_BRASIL'
            elif self.agg_level == 'uf':
                    sheet_name = 1
                    fn_2017 = 'TS_UF'
                    if self.year == 2015:
                        sheet_name = 0
                        skiprows = 3
                        header = None
                    elif self.year == 2009:
                        sheet_name = 2

            if self.year == 2017:
                with zipfile.ZipFile(self.filepaths[0], 'r') as zf:
                    fp = [fp for fp in zf.namelist() if fn_2017 in fp][0]
                    with zf.open(fp) as f:
                        self.dfs = pd.read_excel(f, dtype='string')
            else:
                self.dfs = pd.read_excel(self.filepaths[0],
                                         sheet_name=sheet_name,
                                         skiprows=skiprows,
                                         header=header,
                                         dtype='string')

            if self.year == 2021:
                self.dfs.drop(index=[0], inplace=True)
                self.dfs.reset_index(drop=True, inplace=True)

        elif self.agg_level == 'municipio':
            #TODO
            pass
        elif self.agg_level in ('escola', 'aluno'):
            #TODO
            pass
            #self.dfs = []
            #with zipfile.ZipFile(self.filepath, 'r') as zf:
            #    for filepath in zf.namelist():
            #        filename = os.path.split(filepath)[-1]
            #        if ('xls' in filename.lower() 
            #             and not filename.startswith('~')):
            #            print_info(f'Convertendo em df o arquivo {filename}')
            #            with zf.open(filepath) as f:
            #                df_sheet_dict = pd.read_excel(f, header=None,
            #                                              na_values='--',
            #                                              sheet_name=None)
            #                for df in df_sheet_dict.values():
            #                    self.dfs.append(df)
        return self.dfs

    def preprocess_df(self):
        if not hasattr(self, 'dfs'):
            self.unzip()
        match self.agg_level:
            case 'brasil' | 'uf':
                self.df = self.preprocess_brasil_uf()
        return self.df
                
    def preprocess_brasil_uf(self):
        df = self.dfs
        if self.year < 2011:
            df.columns = COLUMNS_LABELS_HEADER[self.agg_level][2011] + COLUMNS_LABELS_BODY[2011]
        elif self.year == 2015:
            df.columns = COLUMNS_LABELS_HEADER[self.agg_level][2015] + COLUMNS_LABELS_BODY[2015]
        if self.year in (2015, 2017, 2019):
            df.insert(0, 'ANO_SAEB', str(self.year))
        return df

    def otimize_df(self):
        if not hasattr(self, 'df'):
            self.preprocess_df()
        match self.agg_level:
            case 'brasil' | 'uf':
                self.df = self.otimize_brasil_uf()
        return self.df

    def otimize_brasil_uf(self):
        cols = ['ID', 'CO_UF', 'NO_UF', 'DEPENDENCIA_ADM', 'LOCALIZACAO', 'CAPITAL']
        for col in cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype('category')
        self.df['ANO_SAEB'] = self.df['ANO_SAEB'].astype('UInt8')
        for col in self.df.columns:
            if col in cols or col == 'ANO_SAEB':
                continue
            self.df[col] = self.df[col].astype('Float64')
        return self.df

    def basic_names(self):
        return [f'Base de dados = "{self.name}"',
                f'Ano = "{self.year}"',
                f'Agg_level = "{self.agg_level}"']
