import collections
import glob
import os
import shutil
import re
import zipfile

import pandas as pd
import pyreadstat
import py7zr
import requests

from .common import handleDatabase, mean_weight, std_weight, median_weight,\
                    print_info, print_error, parse_sas
from .definitions import FILETYPES_PATH, RAW_FILES_PATH, UF_SIGLA_NOME, SUPPORTED_FTs

PATH = 'censo-demografico'

URL = {
    1960: 'https://drive.google.com/uc?export=download&id=1ehlPo10QweI9xCj_3L6QnYRNfEnGP1nv',
    1970: 'https://drive.usercontent.google.com/download?id=1lcvKVIuBYczyx31CB7tRhyHxHcdjqb4A&export=download&authuser=0&confirm=t&uuid=a3100044-2435-4d25-85ce-9fb5b781c30b&at=APZUnTV0fb2psgi8DTGdzjna9YcP%3A1714616660467',
    1980: 'https://drive.usercontent.google.com/download?id=1gOCtxn9rbGTzvBzHDIf7iseljTnBgBH2&export=download&authuser=0&confirm=t&uuid=a919e85e-a488-445a-936c-3e9458d53e96&at=APZUnTWPnlNZOt3fnD61ovUYjP1j%3A1714687784460',
    1991: 'https://drive.usercontent.google.com/download?id=1T3yAwWwkqDZ4K-macO0bDg42YKh2juKF&export=download&authuser=0&confirm=t&uuid=3c130385-83bd-4130-a70e-cb79250c6194&at=APZUnTXmK7duBJ3DOcDagyXGaSQ2%3A1714688091118',
    2000: 'https://ftp.ibge.gov.br/Censos/Censo_Demografico_2000/Microdados',
    2010: 'https://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_Gerais_da_Amostra/Microdados'}

TYPES = ('PESS', 'DOMI')

EXPR_FILTER = {
    'ALL': ('[file_url["href"] for file_url in soup.find_all("a")'
                               ' if "zip" in file_url["href"]]'),
    '_' :('[file_url["href"] for file_url in soup.find_all("a")'
                               ' if "zip" in file_url["href"]' 
                               ' and f"{self.uf}" in file_url["href"]]')
}

DOCUMENTACAO = {
    'PESS': {1960: '', #TODO
             1970: 'Censo 1970/Documentação/Amostra 1970 vol03.doc',
             1980: '', #TODO
             1991: '', #TODO
             2000: 'LE PESSOAS.sas',
             2010: 'Layout_microdados_Amostra.xls'},
    'DOMI': {1970: 'Censo 1970/Documentação/Amostra 1970 vol03.doc',
             1980: '', #TODO
             1991: '', #TODO
             2000: 'LE DOMIC.sas',
             2010: 'Layout_microdados_Amostra.xls'}
}

WEIGHTS = {
    1960: None,
    1970: 'V054',
    1980: '', #TODO
    1991: '', #TODO
    2000: 'P001',
    2010: 'V0010',
}

RAW_FILENAME = {
    1960: 'Censo Demográfico de 1960.7z',
    1970: 'Censo Demográfico de 1970.7z',
    1980: 'Censo Demográfico de 1980.7z',
    1991: 'Censo Demográfico de 1991.7z',
}


class handleCensoDemografico(handleDatabase):
    def __init__(self, year, uf, type_db, medium=requests):
        if year not in (1960, 1970, 1980, 1991, 2000, 2010):
            print_error(f'Ano {year} não implementado.')
            raise ValueError 
        if uf not in UF_SIGLA_NOME and uf.upper() != 'ALL':
            print_error(f'UF {uf} não implementada. As opções válidas são'
                        f'{UF_SIGLA_NOME.keys()} e "all"')
            raise ValueError
        if type_db not in TYPES:
            print_error(f'Tipo {type_db} não existente. As opções válidas são'
                        f'{TYPES.keys()}')
            raise ValueError

        self.type_db = type_db
        self.uf = uf.upper()
        super().__init__(medium, year)
        self.filename = f'{self.year}-{self.type_db}-{self.uf}-censo-demografico'
        self.name = 'Censo Demográfico'
        self.doc_filename = DOCUMENTACAO[self.type_db][self.year]
        self.weight_var = WEIGHTS[self.year] 
        self.path = os.path.join(self.root, PATH)
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.path = os.path.join(self.path, str(year))
        self.path_dict = os.path.join(self.path, f'{self.type_db}-dicionario') 
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.raw_files_path = os.path.join(self.path, RAW_FILES_PATH)
        if not os.path.isdir(self.raw_files_path):
            os.mkdir(self.raw_files_path)
        if self.year < 2000:
            self.raw_filename = RAW_FILENAME[self.year]
            self.raw_filepath = os.path.join(self.raw_files_path, self.raw_filename)
        self.url = URL[year]
        self.expr_filter = EXPR_FILTER[self.uf if self.uf == 'ALL' else '_']
        self.is_zipped = True

    def get_url(self):
        if self.year < 2000:
            self.file_urls = [self.url]
        else:
            file_urls = super().get_url(criterion, unique=False)
            self.file_urls = [os.path.join(self.url, file_url)
                              for file_url in file_urls]
        return self.file_urls

    def get_save_raw_database(self):
        self.get_url()
        self.filepaths = []
        for file_url in self.file_urls:
            self.file_url = file_url
            filepath = super().get_save_raw_database()
            self.filepaths.append(filepath)

    def make_database_dict(self):
        docpath = glob.glob(f'{self.raw_files_path}/*[Dd]oc*.zip')[0]

        with zipfile.ZipFile(docpath, metadata_encoding='cp850') as zf:
            for file_path in zf.namelist():
                filename = os.path.split(file_path)[-1]
                if filename == self.doc_filename:
                    fp = file_path
                    break
            try:
                print_info(f'Extraíndo informações do arquivo {fp}')
            except UnboundLocalError:
                print_error(f'Não foi possível encontrar o arquivo {self.doc_filename}')
                raise UnboundLocalError
            with zf.open(fp) as f:
                match self.year:
                    case 2000:
                        parse_sas(self, f, encoding='latin-1')
                    case 2010:
                        self.df_dict = pd.read_excel(f,
                                               sheet_name=self.type_df,
                                               skiprows=1)

                        self.colspecs = {}
                        self.dtypes = {}
                        self.colspecs = [(start - 1, end) for start, end in 
                                       zip(self.df_dict['POSIÇÃO INICIAL'],
                                                self.df_dict['POSIÇÃO FINAL'])]
                        self.dtypes  = {}
                        for type_, var in zip(df.TIPO, df.VAR):
                            type_ = type_.strip()
                            dtype = 'string'
                            if tipo == 'C':
                                dtype = 'category'
                            self.dtypes[var] = dtype

                        self.df_dict.columns = ['var', 'name', 'pos', 'end_pos',
                                                'int_part', 'frac_part', 'type']
    def unzip_1960a1991(self):
        with py7zr.SevenZipFile(self.raw_filepath) as zf:
            targets_ = [f for f in zf.getnames() if os.path.splitext(f)[-1] == '.sav'][0]
        dir_path, filename = os.path.split(targets_)
        targets = [dir_path, targets_]
        tmp_path = os.path.join(self.raw_files_path, 'tmp')
        with py7zr.SevenZipFile(self.raw_filepath) as zf:
            zf.extract(path=tmp_path, targets=targets)

        file_path = os.path.join(tmp_path, targets_)
        self.df, self.meta = pyreadstat.read_sav(file_path)
        shutil.rmtree(tmp_path)

    def unzip_2000e2010(self):
        if not hasattr(self, 'database_dict'):
            self.make_database_dict()
        match self.type_db:
            case 'PESS':
                criterion = 'PES'
            case 'DOMI':
                criterion = 'DOM'
        self.df = pd.DataFrame()
        for filepath in self.filepaths:
            with zipfile.ZipFile(filepath, metadata_encoding='cp850') as zf:
                fn = [fn for fn in zf.namelist() if criterion in fn.upper()][0]
                if os.path.splitext(fn)[-1] == '.zip':
                    with zf.open(fn) as f:
                        with zipfile.ZipFile(f, metadata_encoding='cp850') as zf1:
                            fn = [fn for fn in zf1.namelist() if criterion in fn.upper()][0]
                            with zf1.open(fn) as f1:
                                df = pd.read_fwf(f1,
                                                 names=self.df_dict['var'],
                                                 colspecs=self.colspecs,
                                                     dtype=self.dtypes)
                else:
                    with zf.open(fn) as f:
                        df = pd.read_fwf(f,
                                         names=self.df_dict['var'],
                                         colspecs=self.colspecs,
                                             dtype=self.dtypes)

                self.df = pd.concat([self.df, df], ignore_index=True)
        
        if self.uf == 'SP' and self.year == 2010:
            for col, dtype in self.dtypes.items():
                if self.df[col].dtype != dtype:
                    self.df[col] = self.df[col].astype(dtype)

    def unzip(self):
        if not hasattr(self, 'filepaths'):
            self.get_save_raw_database()

        if self.year < 2000:
            self.unzip_1960a1991()
        else:
            self.unzip_2000e2010()

        return self.df

    def str_to_float(self, s, int_part, frac_part):
        if pd.isna(s):
            return pd.NA
        assert(int_part + frac_part == len(s))
        return float(s[:int_part] + '.' + s[int_part:])

    def preprocess_df(self):
        if self.year < 2000: 
            self.preprocess_df_1960a1991()
        else:
            self.preprocess_df_2000e2010()
        return self.df

    def preprocess_df_1960a1991(self):
        if self.year == 1980:
            self.df.drop(columns=['D_R'], inplace=True) #coluna vazia

        for col in self.df.columns:
            dtype = self.get_min_int_dtype()
            try:
                self.df[col] = self.df[col].abs().astype(dtype)
            except TypeError:
                self.df[col] = self.df[col].abs().astype('Float64')
        for col in self.meta.variable_value_labels.keys():
            self.df[col] = self.df[col].astype('string')
            self.df[col] = self.df[col].astype('category')


    def preprocess_df_2000e2010(self):
        if not hasattr(self, 'df'):
            self.unzip()
        float_vars = self.df_dict.loc[self.df_dict.frac_part.notna(),
                                       ['var', 'int_part', 'frac_part', 'size']]
        for var, int_part, frac_part, size in float_vars.itertuples(False, None):
            if self.year == 2000:
                self.df[var]  = self.df[var].str.zfill(size)
            self.df[var] = self.df[var].apply(self.str_to_float,
                                                    args=(int_part, frac_part))
            self.df[var] = self.df[var].astype('Float64')

        for col in self.df.select_dtypes('string').columns:
            if self.year in (2000, 2010) and col == 'V0300':
                continue
            #TODO Refactorization use get_min_int_dtype
            tmp =  pd.to_numeric(self.df[col])
            max_ = tmp.max()
            if max_ >= 2**32:
                dtype = 'UInt64'
            elif max_ >= 2**16:
                dtype = 'UInt32'
            elif max_ >= 2**8:
                dtype = 'UInt16'
            else:
                dtype = 'UInt8'
            self.df[col] = self.df[col].astype(dtype)

        return self.df

    def dict_educacao_superior(self, zf, var, filename, dict_vars, missing_values):
        path = 'Documentação/Anexos Auxiliares'
        pat = re.compile(r'\d{3}')
        with zf.open(os.path.join(path, filename)) as f:
            df = pd.read_excel(f, skiprows=1, dtype='string')
        dict_vars[var] = {key.strip(): value.strip() for key, value
                            in df.iloc[:, :2].dropna().itertuples(index=False, name=None)
                            if pat.search(key)}
        dict_vars[var].update(missing_values)

    def make_map_dict(self):
        if self.year < 2000:
            self.map_dict_vars = self.make_map_dict_1960a1991()
        elif self.year == 2000:
            self.map_dict_vars = self.make_map_dict_2000()
        elif self.year == 2010:
            self.map_dict_vars = self.make_map_dict_2010()
        return self.map_dict_vars

    def make_map_dict_1960a1991(self):
        if not hasattr(self, 'meta'):
            with py7zr.SevenZipFile(self.raw_filepath) as zf:
                targets_ = [f for f in zf.getnames() if os.path.splitext(f)[-1] == '.sav'][0]
            dir_path, filename = os.path.split(targets_)
            targets = [dir_path, targets_]
            tmp_path = os.path.join(self.raw_files_path, 'tmp')
            with py7zr.SevenZipFile(self.raw_filepath) as zf:
                zf.extract(path=tmp_path, targets=targets)

            file_path = os.path.join(tmp_path, targets_)
            _, self.meta = pyreadstat.read_sav(file_path, metadataonly=True)
            shutil.rmtree(tmp_path)

        
        map_var_dict = {}
        for key in self.meta.column_names:
            dict_map = self.meta.variable_value_labels.get(key, pd.NA)
            if pd.notna(dict_map):
                map_var_dict[key] = {str(int(k)): v for k, v in dict_map.items()}

        df = pd.DataFrame({'COD_VAR': [key for key in self.meta.column_names],
                           'NOME_VAR': [self.meta.column_names_to_labels.get(key, pd.NA) 
                                        for key in self.meta.column_names],
                           'MAP_VAR': [map_var_dict.get(key, pd.NA)
                                       for key in self.meta.column_names]})
        df.to_excel(f'{self.path_dict}.xlsx', index=False)
        df.to_pickle(f'{self.path_dict}.pickle')
        return df

    def make_map_dict_2000(self):
        path = 'Arquivos Auxiliares'
        external_vars = dict() 
        docpath = glob.glob(f'{self.raw_files_path}/*[Dd]oc*.zip')[0]
        with zipfile.ZipFile(docpath) as zf:

            #V4250
            with zf.open(os.path.join(path, 'Municipios-V4250.xls')) as f:
                df_ = pd.read_excel(f, dtype='string')
            external_vars['V4250'] = {key.strip(): value.strip() for key, value
                                      in df_.dropna().itertuples(False, None)}

            #V4276
            with zf.open(os.path.join(path, 'Municipios e Pais Estrangeiro - V4276.xls')) as f:
                df_ = pd.read_excel(f, dtype='string')
            external_vars['V4276'] = {key.strip(): value.strip() for key, value
                                      in df_.dropna().itertuples(False, None)}

            #V4279
            with zf.open(os.path.join(path, 'Estrutura ONU V4279.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=3, na_values=[' '])
            external_vars['V4279'] = {key.strip(): value.strip() for value, key
                                      in df_.dropna(subset='CODIGO').itertuples(False, None)}

            #V4239
            with zf.open(os.path.join(path, 'Estrutura ONU V4239.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=3, na_values=[' '])
            external_vars['V4239'] = {key.strip(): value.strip() for value, key
                                      in df_.dropna(subset='CODIGO').itertuples(False, None)}

            #V4219 e V4269
            with zf.open(os.path.join(path, 'Estrutura ONU V4219, V4269.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=3, na_values=[' '])
            var_ext_tmp = {key.strip(): value.strip() for value, key
                           in df_.dropna(subset='CODIGO').itertuples(False, None)
                           if key.isdigit()}
            external_vars['V4219'] = var_ext_tmp 
            external_vars['V4269'] = var_ext_tmp 

            #V4230
            with zf.open(os.path.join(path, 'Estrutura Migracao V4230.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=2, na_values=[' '])
            external_vars['V4230'] = {key.strip(): value.strip() for key, value
                                      in df_.dropna(subset='CODIGOS').itertuples(False, None)
                                      if key.isdigit()}

            #V4210 e V4260
            with zf.open(os.path.join(path, 'Estrutura Migracao V4210, V4260.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=2, na_values=[' '])
            var_ext_tmp = {key.strip(): value.strip() for key, value
                           in df_.dropna(subset='CODIGO').itertuples(False, None)
                           if key.isdigit()}
            external_vars['V4210'] = var_ext_tmp
            external_vars['V4260'] = var_ext_tmp

            #V4355 e area_de_conhecimento
            with zf.open(os.path.join(path, 'Cursos Superiores - Estrutura V4535.xls')) as f: #Houve algum erro de digitação, pois a variável correta é V4355, apesar de o arquivo se referir à variável V4535, a documentação também se refere ao documento com o mesmo nome que ele se encontra.
                df_ = pd.read_excel(f, dtype='string', skiprows=5, na_values=[' '])
            external_vars['V4355'] = {key.strip(): value.strip() for key, value
                                      in df_.iloc[:, 1:].dropna(subset='Código').itertuples(False, None)
                                      if key.isdigit()}
            external_vars['V4355']['02'] = 'Não Superior'
            external_vars['area_de_conhecimento'] = {}
            areas = []
            new_area = None
            for e in df_.iloc[:, 0].dropna():
                if e[0].isdigit():
                    if new_area is not None:
                        areas.append(new_area.strip())
                    new_area = e
                else:
                    new_area += e
            areas.append(new_area)
            for e in areas:
                key, value = e.split('-')
                for k in re.findall(r'\d', key):
                    external_vars['area_de_conhecimento'][k] = value.strip()

        print(external_vars['area_de_conhecimento'])

    def make_map_dict_2010(self):
        path = 'Documentação/Anexos Auxiliares'
        path_regioes = 'Documentação/Divisão Territorial do Brasil/'
        external_vars = collections.defaultdict(dict)
        docpath = glob.glob(f'{self.raw_files_path}/*[Dd]oc*.zip')[0]
        with zipfile.ZipFile(docpath, metadata_encoding='cp850') as zf:
            with zf.open(os.path.join(path, 'Atividade CNAE_DOM 2.0 2010.xls')) as f:
                df_ = pd.read_excel(f, skiprows=1, dtype='string')
            external_vars['V6471'] = {key.strip(): value.strip() for key, value
                                      in df_.iloc[:, 2:4]
                                            .dropna()
                                            .itertuples(index=False, name=None)}

            self.dict_educacao_superior(zf,
                                        'V6356',
                                        'Cursos Doutorado_Estrutura 2010.xls',
                                        external_vars,
                                        {'097': 'NÃO SABE E DOUTORADO NÃO ESPECIFICADO'})
            self.dict_educacao_superior(zf,
                                        'V6354',
                                        'Cursos Mestrado_Estrutura 2010.xls',
                                        external_vars,
                                        {'097': 'NÃO SABE E MESTRADO NÃO ESPECIFICADO'})
            self.dict_educacao_superior(zf,
                                        'V6352',
                                        'Cursos Superiores_Estrutura 2010.xls',
                                        external_vars,
                                        {'085': 'NÃO SABE E SUPERIOR NÃO ESPECIFICADO'})

            pat = re.compile(r'\d{4}')
            with zf.open(os.path.join(path, 'Ocupação COD 2010.xls')) as f:
                df_ = pd.read_excel(f, skiprows=1, dtype='string')
            external_vars['V6461'] = {key: value for key, value
                                      in df_.iloc[:, 0:2]
                                            .dropna()
                                            .itertuples(index=False, name=None)
                                      if pat.search(key)}

            with zf.open(os.path.join(path, 'Migração e deslocamento _Unidades da Federação.xls')) as f:
                df_ = pd.read_excel(f, skiprows=5, dtype='string')
            dict_mig_uf = {key.strip(): value.strip() for value, key
                           in df_.dropna().itertuples(index=False, name=None)}
            for var in ('V6222', 'V6252', 'V6262', 'V6362', 'V6602'):
                external_vars[var] = dict_mig_uf

            with zf.open(os.path.join(path, 'Migração e deslocamento _Municípios.xls')) as f:
                df_ = pd.read_excel(f, skiprows=6, dtype='string')
            dict_mig_mun = {key.strip(): value.strip() for value, key
                           in df_.iloc[:, 1:3]
                                  .dropna()
                                  .itertuples(index=False, name=None)}
            for var in ('V6254', 'V6264', 'V6364', 'V6604'):
                external_vars[var] = dict_mig_mun

            with zf.open(os.path.join(path, 'Migração e Deslocamento_Paises estrangeiros.xls')) as f:
                df_ = pd.read_excel(f, skiprows=7, dtype='string')
            dict_mig_pais = {key.strip(): value.strip() for value, key
                           in df_.iloc[:, 1:3]
                                   .dropna()
                                   .itertuples(index=False, name=None)}
            for var in ('V3061', 'V6224', 'V6256', 'V6266', 'V6366', 'V6606'):
                external_vars[var] = dict_mig_pais

            pat = re.compile(r'\d{3}')
            with zf.open(os.path.join(path, 'Religião 2010.txt')) as f:
                for line in f.readlines():
                    line = line.decode('latin_1').strip()
                    if pat.search(line):
                        key, value = line.split(maxsplit=1)
                        external_vars['V6121'][key] = value

            with zf.open(os.path.join(path, 'Estrutura atividade CD2000.xls')) as f:
                df_ = pd.read_excel(f, dtype='string')

            pat = re.compile(r'\d{5}')
            dict_ativ_2000 = {key.strip(): value.strip() for key, value
                                 in df_.dropna().itertuples(index=False, name=None)
                                 if pat.search(key)}
            external_vars['V6472'] = dict_ativ_2000

            with zf.open(os.path.join(path_regioes,
                                      'Unidades da Federação, Mesorregiões, microrregiões e municípios 2010.xls')) as f:
                df_ = pd.read_excel(f, skiprows=2, dtype='string')
                for _, _, meso, nome_meso, micro, nome_micro, mun, nome_mun in df_.itertuples(index=False, name=None):
                    external_vars['V1002'][meso.strip()] = nome_meso.strip()
                    external_vars['V1003'][micro.strip()] = nome_micro.strip()
                    external_vars['V0002'][mun.strip()] = nome_mun.strip()

        wordDoc = docx.Document(os.path.join(self.raw_files_path, 'estrutura ocupacao CD2000.docx'))
        for table in wordDoc.tables:
            rows_ = []
            for row in table.rows:
                row_ = []
                for cell in row.cells:
                    row_.append(cell.text)
                rows_.append(row_)
        dict_ocu_2000 = {key: value for key, value
                         in pd.DataFrame(data=rows_[2:], columns=rows_[1])
                         .iloc[:, 3:5].itertuples(index=False, name=None) if key}
        dict_ocu_2000['0000']= 'OCUPAÇÕES MAL ESPECIFICADAS'
        external_vars['V6462'] = dict_ocu_2000

        cod_vars_dict = collections.defaultdict(dict)
        pat = re.compile(r'\d+\s*-')
        names = {}
        if not hasattr(self, 'df_dict'):
            self.make_database_dict()
        for var, e in zip(self.df_dict[self.type_db].VAR,
                          self.df_dict[self.type_db].NOME):
            flag = False
            for i, line in enumerate(e.split('\n')):
                if not i:
                    names[var] = line.strip(' :')
                if pat.search(line):
                    key, value = [n.strip() for n in line.split('-', 1)]
                    if key not in cod_vars_dict[var]:
                        cod_vars_dict[var][key] = value
                    flag = True

        missing_vars = {
            'V5110': {'1': 'Contribuintes',
                      '2': 'Não contribuintes'},
            'V5120': {'1': 'Contribuintes',
                      '2': 'Não contribuintes'},
        }

        cod_vars_dict.update(missing_vars)
        cod_vars_dict.update(external_vars)

        df = pd.DataFrame({'COD_VAR': [key for key in names],
              'NOME_VAR': [nome for nome in names.values()],
              'MAP_VAR': [cod_vars_dict.get(key, pd.NA) for key in names]})
        df.to_excel(f'{self.path_dict}.xlsx', index=False)
        df.to_pickle(f'{self.path_dict}.pickle')
        return df

    def get_coded_var(self, var):
        if self.year == 2010:
            if var == 'V0002':
                col = self.cod_mun
            elif var == 'V1002':
                col = self.cod_meso
            elif var == 'V1003':
                col = self.cod_micro
            else:
                col = self.df[var]
        else:
            col = self.df[var]
        return col.map(self.get_map_var(var)[1])

    def get_df(self, filetype='parquet', **kwargs):
        if filetype not in SUPPORTED_FTs:
            raise ValueError
        if self.uf == 'ALL' and self.year >= 2000:
            self.dir_path = os.path.join(self.path, FILETYPES_PATH[filetype])
            if not os.path.isdir(self.dir_path):
                os.mkdir(self.dir_path)
                
            self.dest_filepath = os.path.join(self.dir_path,
                                              f'{self.filename}.{filetype}')
            if os.path.isfile(self.dest_filepath):
                print_info(f'Arquivo {self.dest_filepath} já existente')
                read_fun = getattr(pd, f'read_{filetype}')
                self.df = read_fun(self.dest_filepath, **kwargs)
                return self.df
            all_ufs = [f for f in glob.glob(f'{self.dir_path}/*.{filetype}')
                          if 'ALL' not in f]
            is_complete = True
            for uf in UF_SIGLA_NOME:
                has_processed = False
                for f in all_ufs:
                    if uf in f:
                        has_processed = True
                if not has_processed:
                    print_error(f'A uf {uf} não foi ainda processada')
                    is_complete = False
            if not is_complete:
                print_error('É preciso processar todas as ufs antes de juntá-las todas')
                raise ValueError
            print_info('Todas as ufs já foram processadas, preparando para juntá-las')
            self.df = pd.DataFrame()
            for f in all_ufs:
                print_info(f'Anexando o arquivo {f} no DataFrame')
                df_tmp = pd.read_parquet(f)
                self.df = pd.concat([self.df, df_tmp], ignore_index=True)

            for col in self.df.select_dtypes(object).columns:
                if self.year in (2000, 2010) and col == 'V0300':
                    continue
                self.df[col] = self.df[col].astype('category')

            self.save(filetype=self.filetype)
            return self.df
        else:
            self.df = super().get_df(filetype, **kwargs)
            if self.year == 2010:
                self.cod_mun = (self.df.V0001.astype('string')
                                + self.df.V0002.astype('string')).astype('category')
                self.cod_meso = (self.df.V0001.astype('string')
                                + self.df.V1002.astype('string')).astype('category')
                self.cod_micro = (self.df.V0001.astype('string')
                                + self.df.V1003.astype('string')).astype('category')
            return self.df

