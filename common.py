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

from .definitions import FILETYPES_PATH, SUPPORTED_FTs

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


def weight_stats(df, threshold):
    df.dropna(inplace=True)
    if len(df) <= threshold:
        return False
    return DescrStatsW(df.iloc[:, 0], df.iloc[:, 1])


def mean_weight(df, threshold):
    ret = weight_stats(df, threshold)
    if ret:
        return ret.mean
    return pd.NA


def std_weight(df, threshold):
    ret = weight_stats(df, threshold)
    if ret:
        return ret.std
    return pd.NA


def median_weight(df, threshold):
    ret = weight_stats(df, threshold)
    if ret:
        return ret.quantile(.5).squeeze(0)
    return pd.NA


def parse_sas(self, f, encoding, ignore=False):
    db_dict = collections.defaultdict(list)
    for line in f.readlines():
        if not line.startswith(b'@'):
            continue

        l = line.decode(encoding)
        pos, key, type_, name = l.split(maxsplit=3)

        pos = int(pos[1:]) - 1
        name = name.strip('/*\t\r\n" ')
        if ignore and name.lower().startswith(ignore.lower()):
            continue
        frac_part = pd.NA
        if type_[0] == '$':
            try:
                size = int(type_[1:-1])
            except ValueError:
                size, _ = type_[1:].split('.')
                size = int(size)
            type_ = 'category'

        else:
            size, frac_part = type_.split('.')
            size = int(size)
            type_ = 'string'
            frac_part = int(frac_part) if frac_part else pd.NA

        int_part = size - (frac_part if pd.notna(frac_part) else 0)

        db_dict['pos'].append(pos)
        db_dict['key'].append(key)
        db_dict['type'].append(type_)
        db_dict['int_part'].append(int_part)
        db_dict['name'].append(name)
        db_dict['frac_part'].append(frac_part)
        db_dict['size'].append(size)
    
    df_dict = pd.DataFrame(db_dict)
    self.colspecs = [(pos, pos+size) for pos, size in
               df_dict[['pos', 'size']].itertuples(index=False, name=None)]

    self.dtypes = {key: type_ for key, type_ in
                df_dict[['key', 'type']].itertuples(index=False, name=None)
                   if type_}

    self.df_dict = df_dict


class handleDatabase:
    def __init__(self, year, medium=requests):
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

    def get_cert(self):
        if hasattr(self, 'cert_path'):
            cert = self.cert_path
        else:
            cert = True
        return cert

    def get_url(self, unique=True):
        if hasattr(self, 'file_url'):
            print_info('Endereço já existente.',
                       *self.basic_names(),
                       f'Endereço={self.file_url}'
            )
            return self.file_url
        print_info('Obtendo endereço para extração da Base de dados.',
                   *self.basic_names(),
                   f'Endereço da busca = {self.url}')
        r = self.medium.get(self.url, verify=self.get_cert())
        soup = BeautifulSoup(r.text, 'html.parser')
        file_urls = eval(self.expr_filter, {'self': self,
                                            'unquote_plus': unquote_plus},
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

    def get_save_raw_database(self, file_url=None):
        if not hasattr(self, 'file_url'):
            self.get_url()
        if file_url is not None:
            filename = os.path.basename(file_url)
        else:
            file_url = self.file_url
            if hasattr(self, 'raw_filename'):
                filename = self.raw_filename
            else:
                filename = os.path.basename(self.file_url)
        filepath = os.path.join(self.raw_files_path, filename)
        if os.path.isfile(filepath):
            print_info(f'{filepath} já existente.')
            return filepath
        print_info(f'{filepath} não existente. Fazendo download.')
        r = self.medium.get(file_url, verify=self.get_cert())
        print_info('Download concluído!',
                  f'Gravando arquivo.')
        with open(filepath, 'wb') as f:
            f.write(r.content)
        print_info('Arquivo gravado com sucesso!')
        return filepath

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
        print_info('Preprocessamento dataframe...')
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
        if filetype not in SUPPORTED_FTs:
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

        if not hasattr(self, 'filepath') and not hasattr(self, 'filepaths'):
            self.filepath = self.get_save_raw_database()
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

    def get_min_int_dtype(self):
        if self.df[col].dtype == 'object':
            self.df[col] = self.df[col].astype(float)
        max_ = self.df[col].max()
        if max_ >= 2**32:
            dtype = 'UInt64'
        elif max_ >= 2**16:
            dtype = 'UInt32'
        elif max_ >= 2**8:
            dtype = 'UInt16'
        else:
            dtype = 'UInt8'
        return dtype
    
    def get_map_var(self, var):
        if not hasattr(self, 'map_dict_vars'):
            if not os.path.isfile(f'{self.path_dict}.pickle'):
                print_info('Dicionário da base não existente. Construindo...')
                self.make_map_dict()
                print_info('Dicionário concluído com sucesso!')
            else:
                self.map_dict_vars = pd.read_pickle(f'{self.path_dict}.pickle')

        if var == 'all':
            return self.map_dict_vars

        df = self.map_dict_vars
        nome = df.loc[df.COD_VAR == var, 'NOME_VAR'].values[0]
        vars_cod = df.loc[df.COD_VAR == var, 'MAP_VAR'].values[0]
        return nome, vars_cod

    def crosstab(self,
                 index_vars,
                 columns_vars=None,
                 values=None,
                 aggfunc='mean',
                 threshold=0,
                 normalize=False,
                 margins=False,
                 margins_name='All',
                 filter_=None):

        if filter_ is None:
            df = self.df
        else:
            df = self.df[filter_]

        if not isinstance(index_vars, list):
            index_vars = [index_vars]
        if not isinstance(columns_vars, list):
            columns_vars = [columns_vars]
        vars_g = [*index_vars, *columns_vars] if columns_vars[0] is not None else index_vars
        if values is None:
            if self.weight_var is None:
                table = df.groupby(vars_g, observed=False).size()
            else:
                table = df.groupby(vars_g, observed=False)[self.weight_var].sum()
        else:
            match aggfunc:
                case 'mean':
                    aggfunc = mean_weight
                case 'median':
                    aggfunc = median_weight
                case 'std':
                    aggfunc = std_weight
            table = (df.groupby(vars_g, observed=False)[[values, self.weight_var]]
                            .apply(lambda g:
                                aggfunc(g, threshold)))
        
        index_mapper = [self.get_map_var(v) for v in index_vars]
        columns_mapper = [self.get_map_var(v) for v in columns_vars if v is not None]

        if columns_mapper:
            table = table.unstack(list(range(-1, -len(columns_mapper)-1, -1)))
            iter_levels = []
            names = []
            for level, (name, map_var) in enumerate(columns_mapper):
                iter_levels.append(table.columns.get_level_values(level).map(map_var))
                names.append(name)
            new_columns = pd.MultiIndex.from_arrays(iter_levels, names=names)
            table.columns = new_columns
            table = table[table.columns.dropna()]

        iter_levels = []
        names = []
        for level, (name, map_var) in enumerate(index_mapper):
            iter_levels.append(table.index.get_level_values(level).map(map_var))
            names.append(name)
        new_index = pd.MultiIndex.from_arrays(iter_levels, names=names)
        table.index = new_index
        table = table.loc[table.index.dropna()]
        
        if values is not None:
            return table
        else:
            n = str(normalize)
            if n in ('index', '0'):
                return table.div(table.sum(axis=normalize))
            elif n in ('columns', '1'):     
                return table.div(table.sum(axis=normalize), axis='index')
            else:
                return table.astype('UInt64')

