import ftplib
import io
import os
import zipfile

import pandas as pd
import py7zr
import requests

from .common import handleDatabase, print_info, print_error
from .definitions import FILETYPES_PATH, RAW_FILES_PATH, DOC_PATH, SUPPORTED_FTs

PATH = 'rais'

# URL = 'ftp.mtps.gov.br'
URL = '189.9.32.26'
URL_DIR = '/pdet/microdados/RAIS/'

DTYPES = {
    'Bairros SP':                'string',
    'Bairros Fortaleza':         'string',
    'Bairros RJ':                'string',
    'Causa Afastamento 1':       'string',
    'Causa Afastamento 2':       'string',
    'Causa Afastamento 3':       'string',
    'Motivo Desligamento':       'string',
    'CBO Ocupação 2002':         'string',
    'CNAE 2.0 Classe':           'string',
    'CNAE 95 Classe':            'string',
    'Distritos SP':              'string',
    'Vínculo Ativo 31/12':       'string',
    'Faixa Etária':              'string',
    'Faixa Hora Contrat':        'string',
    'Faixa Remun Dezem (SM)':    'string',
    'Faixa Remun Média (SM)':    'string',
    'Faixa Tempo Emprego':       'string',
    'Escolaridade após 2005':    'string',
    'Qtd Hora Contr':            'UInt8',
    'Idade':                     'UInt16',
    'Ind CEI Vinculado':         'string',
    'Ind Simples':               'string',
    'Mês Admissão':              'string',
    'Mês Desligamento':          'string',
    'Mun Trab':                  'string',
    'Município':                 'string',
    'Nacionalidade':             'string',
    'Natureza Jurídica':         'string',
    'Ind Portador Defic':        'string',
    'Qtd Dias Afastamento':      'UInt16',
    'Raça Cor':                  'string',
    'Regiões Adm DF':            'string',
    'Vl Remun Dezembro Nom':     'float',
    'Vl Remun Dezembro (SM)':    'float',
    'Vl Remun Média Nom':        'float',
    'Vl Remun Média (SM)':       'float',
    'CNAE 2.0 Subclasse':        'string',
    'Sexo Trabalhador':          'string',
    'Tamanho Estabelecimento':   'string',
    'Tempo Emprego':             'float',
    'Tipo Admissão':             'string',
    'Tipo Estab':                'string',
    'Tipo Estab.1':              'string',
    'Tipo Defic':                'string',
    'Tipo Vínculo':              'string',
    'IBGE Subsetor':             'string',
    'Vl Rem Janeiro SC':         'float',
    'Vl Rem Fevereiro SC':       'float',
    'Vl Rem Março SC':           'float',
    'Vl Rem Abril SC':           'float',
    'Vl Rem Maio SC':            'float',
    'Vl Rem Junho SC':           'float',
    'Vl Rem Julho SC':           'float',
    'Vl Rem Agosto SC':          'float',
    'Vl Rem Setembro SC':        'float',
    'Vl Rem Outubro SC':         'float',
    'Vl Rem Novembro SC':        'float',
    'Ano Chegada Brasil':        'UInt16',
    'Ind Trab Intermitente':     'string',
    'Ind Trab Parcial':          'string',

    'CEP Estab':                 'string',
    'Ind Atividade Ano':         'string',
    'Ind Estab Participa PAT':   'string',
    'Ind Rais Negativa':         'string',
    'Qtd Vínculos Ativos':       'UInt32',
    'Qtd Vínculos CLT':          'UInt32',
    'Qtd Vínculos Estatutários': 'UInt32',
    'UF':                        'string',
}

NA_DICT = {
    'Bairros SP':        9000,
    'Bairros Fortaleza': 9000,
    'Bairros RJ':        9000,
    'Distritos SP':        98,
    'Regiões Adm DF':      20,
}

KINDS = ('vinculos', 'estabelecimentos')


#checagens de dados https://bi.mte.gov.br/bgcaged/caged_anuario_rais/anuario.htm
#https://cnae.ibge.gov.br/classificacoes/download-concla.html
class handleRais(handleDatabase):
    def __init__(self, year, kind, uf=None, type_db='parquet'):
        self.type_df = type_db
        self.uf = uf
        if kind not in KINDS:
            print_error(f'O tipo {kind} não é válido. São tipos válidos {KINDS}')
            raise ValueError
        if kind == 'vinculos':
            assert uf is not None
        self.kind = kind
        super().__init__(year, '')
        if kind == 'estabelecimentos':
            self.filename = f'{self.year}-{self.kind}-rais'
        else:
            self.filename = f'{self.year}-{self.kind}-{self.uf}-rais'
        self.name = 'RAIS'
        self.doc_filename = ''
        self.path = os.path.join(self.root, PATH)
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.path = os.path.join(self.path)
        self.raw_files_path = os.path.join(self.path, RAW_FILES_PATH)
        if not os.path.isdir(self.raw_files_path):
            os.mkdir(self.raw_files_path)
        self.doc_path = os.path.join(self.path, DOC_PATH)
        if not os.path.isdir(self.doc_path):
            os.mkdir(self.doc_path)
        self.url = URL
        self.url_dir = URL_DIR
        self.is_zipped = True

    def get_url(self):
        with ftplib.FTP(self.url) as ftp:
                   ftp.login()
                   ftp.cwd(self.url_dir)
                   folders = ftp.nlst()
                   for f in folders:
                       if str(self.year) in f:
                           break
                   else:
                       print_error(f'Não foi encontrado o ano {self.year} no ftp {self.url}{self.url_dir}')
                       raise ValueError
                   ftp.cwd(str(self.year))
                   files = ftp.nlst()
                   if self.kind == 'vinculos':
                       for f in files:
                           if self.uf in f:
                               filename = f
                               break
                       else:
                           print_error(f'Não foi encontrado a UF={self.uf} no ftp {self.url}{self.url_dir}/{self.year}. Os arquivos disponíveis neste endereço são: {files}')
                   elif self.kind == 'estabelecimentos':
                       for f in files:
                           if 'ESTAB' in f.upper() or 'ESTB' in f.upper():
                               filename = f
                               break
                       else:
                           print_error(f'Não foi encontrado dados dos estabelecimentos no ftp {self.url}{self.url_dir}/{self.year}. Os arquivos disponíveis neste endereço são: {files}')
        self.file_url = os.path.join(self.url_dir, str(self.year), filename)
        print_info(f'Endereço(s) {self.file_url} obtido[s] com sucesso!')
        return self.file_url

    def get_save_raw_database(self):
        if not hasattr(self, 'file_url'):
            self.get_url()
        filename = os.path.basename(self.file_url)
        self.raw_filepath = os.path.join(self.raw_files_path, f'{self.year}-{filename}')
        if os.path.isfile(self.raw_filepath) and os.path.getsize(self.raw_filepath):
            print_info(f'{self.raw_filepath} já existente.')
            return self.raw_filepath
        print_info(f'{self.raw_filepath} não existente. Fazendo download do url {self.file_url}')
        with ftplib.FTP(self.url, user='anonymous', passwd='') as ftp:
            with open(self.raw_filepath, 'wb') as f:
                ftp.retrbinary(f'RETR {self.file_url}', f.write)
        if os.path.getsize(self.raw_filepath):
            print_info('Arquivo gravado com sucesso!')
        else:
            print_error(f'Não foi possível salvar o arquivo {self.raw_filepath}')
            raise ValueError
        return self.raw_filepath

    def unzip(self):
        if not hasattr(self, 'raw_filepath'):
            self.get_save_raw_database()
        ext = os.path.splitext(self.raw_filepath)[1]
        try:
            if ext == '.zip':
                with zipfile.ZipFile(self.raw_filepath, 'r') as zf:
                    fns = [fn for fn in zf.namelist()]
                    if len(fns) > 1:
                        print_error('Mais de um arquivo .txt')
                        raise ValueError
                    filename = fns[0]
                    content = zf.open(filename)
                    print_info('Descompressão concluída!')
            elif ext == '.7z':
                with py7zr.SevenZipFile(self.raw_filepath) as zf:
                    target = [f for f in zf.getnames() if os.path.splitext(f)[-1] == '.txt'][0]
                    content = zf.read([target]).popitem()[1]
                    print_info('Descompressão concluída!')
            else:
                print_error(f'Formato {ext} não reconhecido')
                raise TypeError
            print_info('Carregando DataFrame...')
            self.df = pd.read_csv(content, sep=';', decimal=',', encoding='windows-1252', low_memory=False, dtype=DTYPES)
            print_info('DataFrame carregado com sucesso!')
        finally:
            content.close()

        if 'UF' not in self.df.columns and self.kind == 'estabelecimentos':
            filename_ibge_mun = 'RELATORIO_DTB_BRASIL_MUNICIPIO.xls'
            path_ibge_mun = os.path.join(self.doc_path, filename_ibge_mun)
            if not os.path.exists(path_ibge_mun):
                url_ibge_mun = 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/divisao_territorial/2022/DTB_2022.zip'
                file_content = zipfile.ZipFile(io.BytesIO(requests.get(url_ibge_mun).content)).read(filename_ibge_mun)
                with open(path_ibge_mun, 'wb') as f:
                    f.write(file_content)

            df_mun = pd.read_excel(path_ibge_mun, skiprows=6)
            cod_mun_dict = {str(cod_mun)[:-1]: str(uf) for cod_mun, uf in df_mun[['Código Município Completo', 'UF']].values}
            self.df['UF'] = self.df.Município.map(cod_mun_dict).astype('string')

        for c in self.df.select_dtypes('string'):
            self.df[c] = self.df[c].str.strip()
            if c in NA_DICT.keys():
                self.df[c] = pd.to_numeric(self.df[c], errors='coerce')
                self.df.loc[~self.df[c].between(1, NA_DICT[c]), c] = pd.NA
                self.df[c] = self.df[c].astype('string')
            self.df[c] = self.df[c].astype('category')
        for c in self.df.select_dtypes('float'):
            self.df[c] = self.df[c].astype('Float64')
        return self.df

