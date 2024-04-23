import os
from urllib.parse import unquote_plus

import pandas as pd
import requests

from .edubr import handleDatabase, RAW_FILES_PATH, CERT_PATH, print_info,\
        print_error


class handleSaresp(handleDatabase):
    SARESP_PATH = 'saresp'
    SARESP_URL = 'https://dados.educacao.sp.gov.br/dataset/microdados-de-alunos-do-sistema-de-avalia%C3%A7%C3%A3o-de-rendimento-escolar-do-estado-de-s%C3%A3o-paulo'
    SARESP_CRITERION = ('[a["href"] for a in soup.find("div", id="data-and-resources")'
                                                '.find_all("a")'
                       ' if "saresp" in a["href"].lower()'
                       ' and str(self.year) in unquote_plus(a["href"])]')
    SARESP_CERT = 'educacao-sp-gov-br-chain.pem'
    SARESP_FIRST_YEAR = 2007
    SARESP_LAST_YEAR = 2022

    SARESP_COLUMNS_2007a2014 = ['CD_ALUNO', 'NOMEDEP', 'NomeDepBol', 'RegiaoMetropolitana',
                                'CDREDE', 'DE', 'CODMUN', 'MUN', 'CODESC', 'TIPOCLASSE',
                                'SERIE_ANO', 'TURMA', 'CLASSE', 'SEXO', 'DT_NASCIMENTO',
                                'PERIODO', 'NEC_ESP_1', 'NEC_ESP_2', 'NEC_ESP_3', 'NEC_ESP_4',
                                'NEC_ESP_5', 'Tip_PROVA', 'Tem_Nec', 'cad_prova_mat',
                                'cad_prova_lp', 'particip_lp', 'particip_mat', 'TOTAL_PONTO_LP',
                                'TOTAL_PONTO_MAT', 'porc_ACERT_lp', 'porc_ACERT_MAT',
                                'profic_lp', 'profic_mat', 'nivel_profic_lp', 'nivel_profic_mat',
                                'classific_lp', 'classific_mat', 'validade']
    

    SARESP_COLUMNS_2015a2020 = ['CD_ALUNO', 'NOMEDEP', 'NomeDepBol', 'RegiaoMetropolitana',
                                'CDREDE', 'DE', 'CODMUN', 'MUN', 'CODESC', 'TIPOCLASSE',
                                'SERIE_ANO', 'TURMA', 'CLASSE', 'SEXO', 'DT_NASCIMENTO',
                                'PERIODO', 'NEC_ESP_1', 'NEC_ESP_2', 'NEC_ESP_3', 'NEC_ESP_4',
                                'NEC_ESP_5', 'Tip_PROVA', 'Tem_Nec', 'cad_prova_lp',
                                'cad_prova_mat', 'particip_lp', 'particip_mat', 'TOTAL_PONTO_LP',
                                'TOTAL_PONTO_MAT', 'porc_ACERT_lp', 'porc_ACERT_MAT',
                                'profic_lp', 'profic_mat', 'nivel_profic_lp', 'nivel_profic_mat',
                                'classific_lp', 'classific_mat', 'validade']
                        
    SARESP_DTYPES = {'CD_ALUNO': 'UInt32',
                     'CODESC': 'UInt32',
                     'CLASSE': 'UInt32',
                     'profic_lp': 'Float64',
                     'profic_mat': 'Float64',
                     'profic_cie': 'Float64',
                     'DT_NASCMTO': 'datetime64[ns]',
                     'NOMEDEP': 'category',
                     'NomeDepBol': 'category',
                     'RegiaoMetropolitana': 'category',
                     'CDREDE': 'category',
                     'DE': 'category',
                     'CODMUN': 'category',
                     'MUN': 'category',
                     'TIPOCLASSE': 'category',
                     'SERIE_ANO': 'category',
                     'TURMA': 'category',
                     'SEXO': 'category',
                     'PERIODO': 'category',
                     'DEF1': 'category',
                     'DEF2': 'category',
                     'DEF3': 'category',
                     'DEF4': 'category',
                     'DEF5': 'category',
                     'Tip_PROVA': 'category',
                     'Tem_Nec': 'category',
                     'cad_prova_lp': 'category',
                     'cad_prova_mat': 'category',
                     'cad_prova_cie': 'category',
                     'particip_lp': 'category',
                     'particip_mat': 'category',
                     'particip_cie': 'category',
                     'TOTAL_PONTO_LP': 'category',
                     'TOTAL_PONTO_MAT': 'category',
                     'TOTAL_PONTO_CIE': 'category',
                     'porc_ACERT_lp': 'category',
                     'porc_ACERT_MAT': 'category',
                     'porc_CIE': 'category',
                     'nivel_profic_lp': 'category',
                     'nivel_profic_mat': 'category',
                     'nivel_profic_cie': 'category',
                     'classific_lp': 'category',
                     'classific_mat': 'category',
                     'classific_cie': 'category',
                     'validade': 'category'}

    def __init__(self, medium, year):
        if (year < self.SARESP_FIRST_YEAR
            or year > self.SARESP_LAST_YEAR
            or year == 2020): # Não houve saresp neste ano
            print_error(f'Não há dados disponíveis para o ano {year}')
            raise ValueError
        super().__init__(medium, year)
        self.name = 'saresp'
        self.filename = f'{self.year}-saresp'
        self.path = os.path.join(self.root, self.SARESP_PATH)
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.raw_files_path = os.path.join(self.path, RAW_FILES_PATH)
        if not os.path.isdir(self.raw_files_path):
            os.mkdir(self.raw_files_path)
        self.url = unquote_plus(self.SARESP_URL)
        self.is_zipped = False

    def get_url(self):
        cert = os.path.join('.', CERT_PATH, self.SARESP_CERT)
        if not os.path.isfile(cert):
            cert = False
        criterion = self.SARESP_CRITERION
        file_url = super().get_url(criterion, cert=cert)
        return self.file_url

    def get_save_raw_database(self):
        cert = os.path.join('.', CERT_PATH, self.SARESP_CERT)
        if not os.path.isfile(cert):
            cert = False
        self.get_url()
        super().get_save_raw_database(cert)

    def preprocess_df(self):
        self.df = pd.read_csv(self.filepath, sep=';', decimal=',', low_memory=False)
        if self.year < 2015:
            self.df.columns = self.SARESP_COLUMNS_2007a2014
            self.df = self.df[self.SARESP_COLUMNS_2015a2020]
        for col, dtype in self.SARESP_DTYPES.items():
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(dtype)
        return self.df


if __name__ == '__main__':
    with requests.Session() as s:
        for year in range(2007, 2023):
            if year == 2020:
                continue
            sarespDB = handleSaresp(s, year)
            sarespDB.get_df('parquet')
