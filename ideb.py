from enum import Enum
import os
import zipfile

import pandas as pd
import requests

from .common import handleDatabase, print_error
from .definitions import RAW_FILES_PATH, CERT_PATH, REGIOES

NAME = 'ideb'
PATH = 'ideb'
URL = 'https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados'
EXPR_FILTER = '[a["href"] for a in soup.find("div", id="parent-fieldname-text").find_all("a") if self.agg_level in a["href"]]'
CERT = 'inep-gov-br-chain.pem'
FIRST_YEAR = 2005
LAST_YEAR = 2021
AGG_LEVEL = (
    'brasil',
    'regioes',
    'ufs',
    'municipios',
    'escolas',
)

COLUMNS_LABELS_HEADER = {
    'brasil': ['ID', 'DEPENDENCIA_ADM'],
    'regioes': ['REGIAO', 'DEPENDENCIA_ADM'],
    'ufs': ['UF', 'DEPENDENCIA_ADM'],
}

COLUMNS_LABELS_APROVACAO = {
    'EF_AI': ['taxa_aprovacao_EF_AI', 'taxa_aprovacao_EF_1', 'taxa_aprovacao_EF_2',
              'taxa_aprovacao_EF_3',  'taxa_aprovacao_EF_4', 'taxa_aprovacao_EF_5',
              'P_EF_AI'],

    'EF_AF': ['taxa_aprovacao_EF_AF', 'taxa_aprovacao_EF_6', 'taxa_aprovacao_EF_7',  
              'taxa_aprovacao_EF_8',  'taxa_aprovacao_EF_9', 'P_EF_AF'],

    'EM':    ['taxa_aprovacao_EM',    'taxa_aprovacao_EM_1', 'taxa_aprovacao_EM_2',  
              'taxa_aprovacao_EM_3',  'taxa_aprovacao_EM_4', 'P_EM'],
}

COLUMNS_LABELS_SAEB = {
    'EF_AI': ['MEDIA_EF_AI_MT',  'MEDIA_EF_AI',  'N_EF_AI'],
    'EF_AF': ['MEDIA_EF_AF_MT',  'MEDIA_EF_AF',  'N_EF_AF'],
    'EM':    ['MEDIA_EM_MT',     'MEDIA_EM_LP',  'N_EM'],
}

COLUMNS_LABELS_TAIL = {
    'EF_AI': ['IDEB_EF_AI'],
    'EF_AF': ['IDEB_EF_AF'],
    'EM':    ['IDEB_EM'],
}


class handleIdeb(handleDatabase):
    def __init__(self, year, agg_level, medium=requests):
        if (year < FIRST_YEAR
            or year > LAST_YEAR):
            print_error(f'Não há dados disponíveis para o ano {year}')
            raise ValueError
        if agg_level not in AGG_LEVEL:
            print_error('As opções de nível de agregação são: '
                       f'{AGG_LEVEL}')
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
        if self.year == 2021:
            self.url = os.path.normcase(os.path.join(URL, str(self.year)))
        else:
            self.url = os.path.normcase(os.path.join(URL, 'anos-anteriores'))
        self.is_zipped = True
        self.filename = f'{self.year}-{self.agg_level}-{self.name}'
        self.expr_filter = EXPR_FILTER
        self.cert_path = os.path.join(os.path.dirname(__file__), CERT_PATH, CERT)
        if not os.path.isfile(self.cert_path):
            self.cert_path = False

    def get_url(self):
        match self.agg_level:
            case 'brasil' | 'regioes' | 'ufs':
                self.file_urls = [super().get_url(unique=True)]
            case 'municipios' | 'escolas':
                self.file_urls = super().get_url(unique=False)
        return self.file_urls

    def get_save_raw_database(self):
        self.get_url()
        self.filepaths = []
        for file_url in self.file_urls:
            filepath = super().get_save_raw_database(file_url)
            self.filepaths.append(filepath)

    def unzip(self):
        if not hasattr(self, 'filepaths'):
            self.get_save_raw_database()
        
        if self.agg_level in ('brasil', 'regioes', 'ufs'):
            with zipfile.ZipFile(self.filepaths[0], 'r') as zf:
                fp = [fp for fp in zf.namelist() if '.xlsx' in fp][0]
                with zf.open(fp) as f:
                    dfs = pd.read_excel(f,
                                        sheet_name=None,
                                        skiprows=10,
                                        header=None,
                                        dtype='string',
                                        na_values=['-'])
                    
        elif self.agg_level == 'municipios':
            #TODO
            pass
        elif self.agg_level in ('escolas', 'alunos'):
            #TODO
            pass
        self.dfs = list(dfs.values())
        return self.dfs

    def preprocess_df(self):
        if not hasattr(self, 'dfs'):
            self.unzip()
        match self.agg_level:
            case 'brasil' | 'regioes' | 'ufs':
                self.df = self.preprocess_brasil_uf()
        return self.df
                
    def preprocess_helper_header_body(self, level):
        year = max(2019, self.year)
        header = COLUMNS_LABELS_HEADER[self.agg_level] 
        aprovacao = COLUMNS_LABELS_APROVACAO[level]
        saeb = COLUMNS_LABELS_SAEB[level]
        tail = COLUMNS_LABELS_TAIL[level]
        body = aprovacao + saeb + tail
        return header, body, aprovacao, saeb, tail

    def preprocess_helper_year(self, df, level):
        header, body, *_ = self.preprocess_helper_header_body(level)
        df.columns = header + body 
        df.dropna(subset='DEPENDENCIA_ADM', inplace=True)
        df.set_index(header, inplace=True)
        return df

    def preprocess_brasil_uf(self):
        dfs = []
        for df, level in zip(self.dfs, ('EF_AI', 'EF_AF', 'EM')):
            if self.year <= 2019:
                header, body, aprovacao, saeb, tail = self.preprocess_helper_header_body(level)
                pad_aprovacao = (self.year - 2005) // 2 * len(aprovacao) + len(header)
                pad_saeb = (2021 - 2005) // 2 * len(aprovacao) + len(header)
                pad_tail = pad_saeb + (2021 - 2005) // 2 * len(saeb)

                pad_saeb += (self.year - 2005) //2 * len(saeb)
                pad_tail += (self.year - 2005) // 2 * len(tail)

                df = pd.concat([df.iloc[:, 0:len(header)],
                                df.iloc[:, pad_aprovacao:pad_aprovacao+len(aprovacao)],
                                df.iloc[:, pad_saeb:pad_saeb+len(saeb)],
                                df.iloc[:, pad_tail:pad_tail+len(tail)]], axis=1)
                df.iloc[:, 1] = df.iloc[:, 1].str.replace(r' ?\(\d\)', '', regex=True)
                df = self.preprocess_helper_year(df, level)
            elif self.year == 2021:
                df = self.preprocess_helper_year(df, level)
            
            dfs.append(df)

        self.df = pd.concat(dfs, axis=1)
        self.df.reset_index(inplace=True)
        if self.agg_level == 'regioes':
            self.df = self.df[self.df.iloc[:, 0].isin(REGIOES)]
        elif self.agg_level == 'ufs':
            self.df = self.df[~self.df.iloc[:, 0].isin(REGIOES)]

        self.df.insert(0, 'IDEB_ANO', self.year)

        return self.df


    def otimize_df(self):
        if not hasattr(self, 'df'):
            self.preprocess_df()
        match self.agg_level:
            case 'brasil' | 'regioes' | 'ufs':
                self.df = self.otimize_brasil_uf()
        return self.df

    def otimize_brasil_uf(self):
        header = COLUMNS_LABELS_HEADER[self.agg_level]
        for col in self.df.columns:
            if col == 'IDEB_ANO':
                self.df[col] = self.df[col].astype('UInt16')
            elif col in header:
                self.df[col] = self.df[col].astype('category')
            else:
                self.df[col] = self.df[col].astype('Float64')
        return self.df

    def basic_names(self):
        return [f'Base de dados = "{self.name}"',
                f'Ano = "{self.year}"',
                f'Agg_level = "{self.agg_level}"']
