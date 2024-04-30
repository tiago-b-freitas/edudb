import collections
import glob
import os
import re
import zipfile

from urllib.parse import unquote_plus

from bs4 import BeautifulSoup
import docx
from statsmodels.stats.weightstats import DescrStatsW
import pandas as pd
import requests

from .definitions import FILETYPES_PATH

def get_dtype(series, df):
    if pd.notna(series.Categoria):
        return 'category'
    elif series.Tipo == 'Char':
        return 'string'
    elif series.Tipo == 'Data':
        return 'datetime'
    elif series.Tipo == 'Num':
        max_ = df[series['Nome da Variável']].max()
        if isinstance(max_, str):
            try:
                max_ = int(max_)
            except ValueError:
                return 'string'
        if max_ >= 2**32:
            return 'UInt64'
        elif max_ >= 2**16:
            return 'UInt32'
        elif max_ >= 2**8:
            return 'UInt16'
        else:
            return 'UInt8'


def print_info(*args):
    logging('INFO', args)


def print_error(*args):
    logging('ERROR', args)


def logging(type_, args):
    print(*[f'[{type_}] {msg}' for msg in args], sep='\n')


def weight_stats(s, weights, threshold):
    s.dropna(inplace=True)
    if len(s) <= threshold:
        return False
    return DescrStatsW(s, weights=weights[s.index])


def mean_weight(s, weights, threshold):
    ret = weight_stats(s, weights, threshold)
    if ret:
        return ret.mean
    return pd.NA


def std_weight(s, weights, threshold):
    ret = weight_stats(s, weights, threshold)
    if ret:
        return ret.std
    return pd.NA


def median_weight(s, weights, threshold):
    ret = weight_stats(s, weights, threshold)
    if ret:
        return ret.quantile(.5)
    return pd.NA


def parse_sas(self, f, encoding, ignore=False):
    db_dict = collections.defaultdict(list)
    for line in f.readlines():
        if not line.startswith(b'@'):
            continue

        l = line.decode(encoding)
        print(line)

        pos, key, type_, desc = l.split(maxsplit=3)

        pos = int(pos[1:]) - 1
        desc = desc.strip('/*\t\r\n" ')
        if ignore and desc.startswith(ignore):
            continue
        fraction = pd.NA
        if type_[0] == '$':
            try:
                size = int(type_[1:-1])
            except ValueError:
                size, _ = type_[1:].split('.')
                size = int(size)
            type_ = 'category'

        else:
            size, fraction = type_.split('.')
            size = int(size)
            type_ = 'integer'
            if fraction:
                fraction = int(fraction)
                type_ = 'float'

        db_dict['pos'].append(pos)
        db_dict['key'].append(key)
        db_dict['type'].append(type_)
        db_dict['size'].append(size)
        db_dict['desc'].append(desc)
        db_dict['fraction'].append(fraction)
    
        df_dict = pd.DataFrame(db_dict)
        self.colspecs = [(pos, pos+size) for pos, size in
                   df_dict[['pos', 'size']].itertuples(index=False, name=None)]

        self.dtypes = {key: type_ for key, type_ in
                    df_dict[['key', 'type']].itertuples(index=False, name=None)
                       if type_}

        self.df_dict = df_dict


class handleDatabase:
    def __init__(self, medium, year):
        self.medium = medium
        self.year = year
        with open('root.txt', 'r', encoding='utf-8') as f:
            self.root = f.read().strip()
        if not os.path.isdir(self.root):
            os.mkdir(self.root)
        self.is_zipped = False
        self.is_preprocessed = False
        self.is_otimized = False
        self.is_stardardized = False
        self.SUPPORTED_FTs = ('feather', 'parquet')

    def get_database(self, medium, url, cert=True):
        r = medium.get(url, verify=cert)
        return r

    def save_database(self, content, filename):
        if isinstance(content, str):
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
        else:
            with open(filename, 'wb') as f:
                f.write(content)

    def get_url(self, criterion, unique=True, cert=True):
        if hasattr(self, 'file_url'):
            print_info('Endereço já existente.',
                       *self.basic_names(),
                       f'Endereço={self.file_url}'
            )
            return self.file_url
        print_info('Obtendo endereço para extração da Base de dados.',
                   *self.basic_names(),
                   f'Endereço da busca = {self.url}')
        r = self.medium.get(self.url, verify=cert)
        soup = BeautifulSoup(r.text, 'html.parser')
        file_urls = eval(criterion, {'self': self, 'unquote_plus': unquote_plus},
                                    {'soup': soup})
        if unique:
            self.assert_url(file_urls)
            self.file_url = unquote_plus(file_urls[0])
        else:
            if not file_urls:
                print_error('Não foi encontrado nenhum endereço!')
                raise ValueError
            self.file_url = file_urls
        print_info(f'Endereço(s) {self.file_url} obtido com sucesso!')
        return self.file_url

    def get_save_raw_database(self, cert=True):
        if not hasattr(self, 'file_url'):
            self.get_url()
        filename = os.path.split(self.file_url)[-1]
        self.filepath = os.path.join(self.raw_files_path, filename)
        if os.path.isfile(self.filepath):
            print_info(f'{self.filepath} já existente.')
            return
        print_info(f'{self.filepath} não existente. Fazendo download.')
        r = self.medium.get(self.file_url, verify=cert)
        print_info('Download concluído!',
                  f'Gravando arquivo.')
        with open(self.filepath, 'wb') as f:
            f.write(r.content)
        print_info('Arquivo gravado com sucesso!')

    def assert_url(self, file_urls):
        if len(file_urls) == 0:
            print_error('Não foi encontrado nenhum endereço.')
        elif len(file_urls) > 1:
            print_error('Mais de um link para extração da base de dados.')
        else:
            return
        print_error(f'File_urls={file_urls}')
        raise ValueError

    def basic_names(self):
        return [f'Base de dados = "{self.name}"', f'Ano = "{self.year}"']

    def unzip(self):
        pass

    def wraper_unzip(self, func):
        print_info('Descomprimindo arquivo...')
        func()
        print_info('Descompressão concluída!')

    def preprocess_df(self):
        pass

    def wraper_preprocess_df(self, func):
        print_info('Preprocessamendo dataframe...')
        func()
        print_info('Preprocessamento concluído!')

    def otimize_df(self):
        pass
    
    def wraper_otimize_df(self, func):
        print_info('Otimizando base de dados...')
        func()
        print_info('Otimização concluída!')

    def standard_df(self):
        pass

    def wraper_standard_df(self, func): 
        print_info('Padronizando base de dados...')
        func()
        print_info('Padronização conluída!')

    def get_df(self, filetype, **kwargs):
        if filetype not in self.SUPPORTED_FTs:
            raise ValueError

        self.dir_path = os.path.join(self.path, FILETYPES_PATH[filetype])

        if not os.path.isdir(self.dir_path):
            os.mkdir(self.dir_path)
            
        self.dest_filepath = os.path.join(self.dir_path,
                                          f'{self.filename}.{filetype}')
        if os.path.isfile(self.dest_filepath):
            print_info(f'Arquivo {self.dest_filepath} já existente')
            read_fun = getattr(pd, f'read_{filetype}')
            self.df = read_fun(self.dest_filepath,
                               **kwargs)

            return self.df

        if not hasattr(self, 'filepath'):
            self.get_save_raw_database()
        if not hasattr(self, 'df') and self.is_zipped:
            self.wraper_unzip(self.unzip)
        if not self.is_preprocessed:
            self.wraper_preprocess_df(self.preprocess_df)
        if not self.is_otimized:
            self.wraper_otimize_df(self.otimize_df)
        if not self.is_stardardized:
            self.wraper_standard_df(self.standard_df)

        self.save(filetype)   
        return self.df
                
    def save(self, filetype):
        success = False
        print_info(f'Salvando no formato {filetype}...')
        save_fun = getattr(self.df, f'to_{filetype}')
        save_fun(self.dest_filepath)
        print_info('Arquivo salvo com sucesso!')
