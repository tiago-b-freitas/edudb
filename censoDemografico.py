import collections
import glob
import io
import os
import subprocess
import shutil
import re
import zipfile

import docx
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
    1980: 'V604',
    1991: 'V7301',
    2000: 'P001',
    2010: 'V0010',
}

RAW_FILENAME = {
    1960: 'Censo Demográfico de 1960.7z',
    1970: 'Censo Demográfico de 1970.7z',
    1980: 'Censo Demográfico de 1980.7z',
    1991: 'Censo Demográfico de 1991.7z',
}

VARS = {
    'sexo': {
             1960: None,
             1970: 'V054',
             1980: '', #TODO
             1991: '', #TODO
             2000: 'P001',
             2010: 'V0010',
    },
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
        super().__init__(year, medium)
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
            filepath = super().get_save_raw_database(file_url)
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

    def doc2docx(self, zf, path, filename):
            tmp_filepath0 = os.path.join(self.raw_files_path, '~tmp.doc')
            tmp_filepath1 = os.path.join(self.raw_files_path, '~tmp.docx')
            with zf.open(os.path.join(path, filename)) as f:
                with open(tmp_filepath0, 'wb') as f_tmp:
                    f_tmp.write(f.read())
            subprocess.run(['lowriter', '--convert-to', 'docx', tmp_filepath0, '--outdir', self.raw_files_path])
            wordDoc = docx.Document(tmp_filepath1)
            os.remove(tmp_filepath0)
            os.remove(tmp_filepath1)
            return wordDoc

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

        if self.year == 1970:
            #correção de um erro na codificação do SPSS
            #variável de V035 alfabetização e V036 frequenta a escola
            map_var_dict['V035'] = {'0': 'Sem declaração',
                                    '1': 'Sim',
                                    '2': 'Não'}
            map_var_dict['V036'] = {'0': 'Sem declaração',
                                    '1': 'Sim',
                                    '2': 'Não'}

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
        with zipfile.ZipFile(docpath, metadata_encoding='cp850') as zf:

            #V4250 = Municípios
            with zf.open(os.path.join(path, 'Municipios-V4250.xls')) as f:
                df_ = pd.read_excel(f, dtype='string')
            external_vars['V4250'] = {key.strip(): value.strip() for key, value
                                      in df_.dropna().itertuples(False, None)}

            #V4276 = Municípios e países estrangeiros
            with zf.open(os.path.join(path, 'Municipios e Pais Estrangeiro - V4276.xls')) as f:
                df_ = pd.read_excel(f, dtype='string')
            external_vars['V4276'] = {key.strip(): value.strip() for key, value
                                      in df_.dropna().itertuples(False, None)}

            #V4279 = Países estrangeiros
            with zf.open(os.path.join(path, 'Estrutura ONU V4279.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=3, na_values=[' '])
            external_vars['V4279'] = {key.strip(): value.strip() for value, key
                                      in df_.dropna(subset='CODIGO').itertuples(False, None)}

            #V4239 = Países estrangeiros
            with zf.open(os.path.join(path, 'Estrutura ONU V4239.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=3, na_values=[' '])
            external_vars['V4239'] = {key.strip(): value.strip() for value, key
                                      in df_.dropna(subset='CODIGO').itertuples(False, None)}

            #V4219 e V4269 = Países estrangeiros e UFs
            with zf.open(os.path.join(path, 'Estrutura ONU V4219, V4269.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=3, na_values=[' '])
            var_ext_tmp = {key.strip(): value.strip() for value, key
                           in df_.dropna(subset='CODIGO').itertuples(False, None)
                           if key.isdigit()}
            external_vars['V4219'] = var_ext_tmp 
            external_vars['V4269'] = var_ext_tmp 

            #V4230 = Países estrangeiros e UFs
            with zf.open(os.path.join(path, 'Estrutura Migracao V4230.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=2, na_values=[' '])
            external_vars['V4230'] = {key.strip(): value.strip() for key, value
                                      in df_.dropna(subset='CODIGOS').itertuples(False, None)
                                      if key.isdigit()}

            #V4210 e V4260 = Países estrangeiros e UFs
            with zf.open(os.path.join(path, 'Estrutura Migracao V4210, V4260.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=2, na_values=[' '])
            var_ext_tmp = {key.strip(): value.strip() for key, value
                           in df_.dropna(subset='CODIGO').itertuples(False, None)
                           if key.isdigit()}
            external_vars['V4210'] = var_ext_tmp
            external_vars['V4260'] = var_ext_tmp

            #V4355 = Cursos Superiores; e area_de_conhecimento
            with zf.open(os.path.join(path, 'Cursos Superiores - Estrutura V4535.xls')) as f: #Houve algum erro de digitação, pois a variável correta é V4355, apesar de o arquivo se referir à variável V4535, a documentação também se refere ao documento com o nome da variável errado.
                df_ = pd.read_excel(f, dtype='string', skiprows=5, na_values=[' '])
            external_vars['V4355'] = {key.strip(): value.strip() for key, value
                                      in df_.iloc[:, 1:].dropna(subset='Código').itertuples(False, None)
                                      if key.isdigit()}
            external_vars['V4355']['02'] = 'Não Superior'
            external_vars['cursos_superiores_area_de_conhecimento'] = {}
            areas = []
            new_area = None
            for line in df_.iloc[:, 0].dropna():
                if line[0].isdigit():
                    if new_area is not None:
                        areas.append(new_area.strip())
                    new_area = line
                else:
                    new_area += line
            areas.append(new_area)
            for area in areas:
                key, value = area.split('-')
                for k in re.findall(r'\d', key):
                    external_vars['cursos_superiores_area_de_conhecimento'][k] = value.strip()

            #V4354 = Cursos Superiores; areas_especificas e areas_gerais
            with zf.open(os.path.join(path, 'Cursos Superiores - Estrutura V4534.xls')) as f: #Mesmo caso do erro da V4355
                df_ = pd.read_excel(f, dtype='string', skiprows=4, na_values=[' '])
            external_vars['V4354'] = {}
            external_vars['cursos_superiores_areas_especificas'] = {}
            external_vars['cursos_superiores_areas_gerais'] = {}
            for line in df_.iloc[:, 2].dropna():
                key, value = line.split(maxsplit=1)
                external_vars['V4354'][key.strip()] = value.strip()
            for line in df_.iloc[:, 1].dropna():
                key, value = line.split(maxsplit=1)
                external_vars['cursos_superiores_areas_especificas'][key.strip()] = value.strip()
            for line in df_.iloc[:, 0].dropna():
                key, value = line.split(maxsplit=1)
                external_vars['cursos_superiores_areas_gerais'][key.strip()] = value.strip()

            #V1004 = Região Metropolitana
            external_vars['V1004'] = {}
            with zf.open(os.path.join(path, 'V1004.txt')) as f:
                for line in f.readlines():
                    line = line.decode('windows-1252')
                    if line[0].isdigit():
                        key, value = line.split('-', 1)
                        external_vars['V1004'][key.strip()] = value.strip()

            #V4451 = Código antigo da ocupação, relativo a 91
            with zf.open(os.path.join(path, 'Ocupacao91-Estrutura.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=2, header=None, names=['key', 'value'], na_values=[' '])
            external_vars['V4451'] = {key.strip(): value.strip() for key, value
                                      in df_.dropna(subset='key').itertuples(False, None)
                                      if key.isdigit()}
            external_vars['V4451']['927'] = 'OUTRAS OCUPACOES OU OCUPACOES MAL DEFINIDAS'
            external_vars['V4451']['000'] = 'OUTRAS OCUPACOES OU OCUPACOES MAL DEFINIDAS'

            df_.loc[df_['key'].notna(), 'value'] = pd.NA
            df_['value'] = df_['value'].ffill()
            external_vars['ocupacao_91_gg'] = {key.strip(): value.strip() for key, value
                                               in df_.dropna(subset='key').itertuples(False, None)
                                               if key.isdigit()}
            external_vars['ocupacao_91_gg']['927'] = 'OUTRAS OCUPACOES OU OCUPACOES MAL DEFINIDAS'
            external_vars['ocupacao_91_gg']['000'] = 'OUTRAS OCUPACOES OU OCUPACOES MAL DEFINIDAS'
            external_vars['ocupacao_91_gg']['999'] = 'OUTRAS OCUPACOES OU OCUPACOES MAL DEFINIDAS'


            #V4452 = Código novo da ocupação, relativo a 2000
            wordDoc = self.doc2docx(zf, path, 'Ocupacao-Estrutura.doc')
            rows_ = []
            for i, table in enumerate(wordDoc.tables):
                for row in table.rows:
                    row_ = []
                    for j, cell in enumerate(row.cells):
                        if i >= 1 and j == 0 and cell.text.startswith(('C', 'G')):
                            break
                        row_.append(cell.text)
                    if row_:
                        rows_.append(row_)
            df_ = pd.DataFrame(rows_[2:], columns=rows_[1])
            external_vars['V4452'] = {key.strip(): value.strip() for key, value
                                      in df_[['Grupo de base', 'Titulação']]
                                            .dropna(subset='Grupo de base')
                                            .itertuples(False, None)
                                      if key.isdigit()}
            external_vars['ocupacao_2000_sg'] = {key.strip()[:3]: value.strip() for key, value
                                      in df_[['Subgrupo', 'Titulação']]
                                            .dropna(subset='Subgrupo')
                                            .itertuples(False, None)
                                      if key.isdigit()}
            external_vars['ocupacao_2000_sgp'] = {key.strip()[:2]: value.strip() for key, value
                                      in df_[['Subgrupo principal', 'Titulação']]
                                            .dropna(subset='Subgrupo principal')
                                            .itertuples(False, None)
                                      if key.isdigit()}
            external_vars['ocupacao_2000_gg'] = {key.strip()[:1]: value.strip() for key, value
                                      in df_[['Grande Grupo', 'Titulação']]
                                            .dropna(subset='Grande Grupo')
                                            .itertuples(False, None)
                                      if key.isdigit()}
            external_vars['V4452']['0000'] = 'OCUPAÇÕES MAL ESPECIFICADAS'
            external_vars['ocupacao_2000_sg']['000'] = 'OCUPAÇÕES MAL ESPECIFICADAS' 
            external_vars['ocupacao_2000_sgp']['00'] = 'OCUPAÇÕES MAL ESPECIFICADAS'
            external_vars['ocupacao_2000_gg']['0'] = 'OCUPAÇÕES MAL ESPECIFICADAS' 

            #V4090 = Estrutura de Religião e grandes grupos de religião
            wordDoc = self.doc2docx(zf, path, 'Estrutura de Religiao - V4090.doc')
            rows_ = []
            for table in wordDoc.tables:
                for row in table.rows:
                    row_ = []
                    for j, cell in enumerate(row.cells):
                        if (j == 0 and cell.text.startswith('R')) or j >= 2 or cell.text == '':
                            break
                        row_.append(cell.text)
                    if row_:
                        rows_.append(row_)
            df_ = pd.DataFrame(rows_, columns=['key', 'value'])
            external_vars['V4090'] = {key.strip(): value.strip() for key, value
                                      in df_.itertuples(False, None)
                                      if key.isdigit()}
            external_vars['religiao_gg'] = {line.split(maxsplit=1)[0].strip(): line.split(maxsplit=1)[1].strip() for line
                                            in df_['key'] if not line.isdigit()}
            external_vars['religiao_gg']['00'] = 'SEM RELIGIÂO'
            external_vars['religiao_gg']['99'] = 'SEM DECLARAÇÃO'

            #V4461 = Código antigo da atividade de trabalho relativo a 91
            with zf.open(os.path.join(path, 'Atividade91-Estrutura.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', header=None, names=['key', 'value'], na_values=[' '])
            external_vars['V4461'] = {key.strip(): value.strip() for key, value
                                      in df_.dropna(subset='key').itertuples(False, None)
                                      if key.isdigit()}

            df_.loc[df_['key'].notna(), 'value'] = pd.NA
            df_['value'] = df_['value'].ffill()
            external_vars['atividade_91_gg'] = {key.strip(): value.strip() for key, value
                                                in df_.dropna(subset='key').itertuples(False, None)
                                                if key.isdigit()}

            #V4462 = Código novo da atividade
            with zf.open(os.path.join(path, 'CnaeDom-Estrutura.xls')) as f:
                df_ = pd.read_excel(f, dtype='string', skiprows=2, header=None, names=['key', 'value'], na_values=[' '])
            external_vars['V4462'] = {key.strip(): value.strip() for key, value
                                      in df_.dropna(subset='key').itertuples(False, None)
                                      if re.match(r'\d{5}', key)}
            external_vars['atividade-sgp'] = {key.strip(): value.strip() for key, value
                                              in df_.dropna(subset='key').itertuples(False, None)
                                              if re.match(r'\b\d{2}\b', key)}
            df_.loc[df_['key'].notna(), 'value'] = pd.NA
            df_['value'] = df_['value'].ffill()
            external_vars['atividade-gg'] = {key.strip(): value.split('-', 1)[-1].strip() for key, value
                                             in df_.dropna(subset='key').itertuples(False, None)
                                             if re.match(r'\d{5}', key)}

            #Divisão territorial brasileira
            with zf.open(os.path.join(path, 'Divisao Territorial Brasileira.xls')) as f:
                dfs_ = pd.read_excel(f, dtype='string', skiprows=1, header=None, names=['key', 'value'], na_values=[' '], 
                                     sheet_name=['Mesorregião', 'Microrregião', 'Município', 'Distrito', 'Subdistrito'])

            #V1002 = Mesorregião; V1003 = Microrregião; V0103 e V1103 = Municipio; V0104 = Distrito; V0105 = Subdistrito
            vars_ = {'V1002': 'Mesorregião',
                     'V1003': 'Microrregião',
                     'V0103': 'Município',
                     'V1103': 'Município',
                     'V0104': 'Distrito', 
                     'V0105': 'Subdistrito'}
            for k, v in vars_.items():
                external_vars[k] = {key.strip(): value.strip() for key, value
                                    in dfs_[v].dropna(subset='key').itertuples(False, None)
                                    if key.isdigit()}

            wordDoc = self.doc2docx(zf, 'Documentacao', 'Documentação.doc')
            rows_ = []
            for table in wordDoc.tables:
                for row in table.rows:
                    row_ = []
                    for j, cell in enumerate(row.cells):
                        if (j == 0 and (cell.text.startswith('VA') or not cell.text.startswith(('V', 'M')))) or j >= 2:
                            break
                        if j == 0:
                            row_.append(cell.text)
                        else:
                            var_dict = {}
                            for i, e in enumerate(cell.text.split('\n')):
                                if i == 0:
                                    row_.append(e)
                                elif e[0].isdigit() and e.find('-') != -1:
                                    k, v = e.split('-', 1)
                                    var_dict[k.strip()] = v.strip()
                            row_.append(var_dict)
                    if row_:
                        rows_.append(row_)
            df = pd.DataFrame(rows_, columns=['COD_VAR', 'NOME_VAR', 'MAP_VAR'])

        #Ajustes manuais
        df.loc[df.COD_VAR == 'V0104', 'NOME_VAR'] = 'CÓDIGO DO DISTRITO'
        df.loc[df.COD_VAR == 'V0300', 'NOME_VAR'] = 'IDENTIFICAÇÃO DO DOMICÍLIO'
        df.loc[df.COD_VAR == 'V0400', 'NOME_VAR'] = 'NÚMERO DE ORDEM DA PESSOA RECENSEADA'

        df = df.drop_duplicates(subset=['COD_VAR', 'NOME_VAR'])

        extra_vars = []
        for k, v in external_vars.items():
            filter_ = df.COD_VAR == k
            if filter_.sum():
                df.loc[df.COD_VAR == k, 'MAP_VAR'] = [v] #Hack(?) para o pandas aceitar um elemento de tipo dicionário
            else:
                extra_vars.append([k, k, v])

        df_extra = pd.DataFrame(extra_vars, columns=['COD_VAR', 'NOME_VAR', 'MAP_VAR'])
        df = pd.concat([df, df_extra], ignore_index=True)
        
        df.to_excel(f'{self.path_dict}.xlsx', index=False)
        df.to_pickle(f'{self.path_dict}.pickle')

        #TODO indicar nas variáveis extras o quanto se deve fazer slice (self.df[var].str.slice(stop=x).astype('category')
        #TODO indicar também naquelas em que não é possível realizar o slice e aplicar o map antes do agrupamento na função crosstable

        return df


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

    def educ(self):
        match self.year:
            case 1960:
                educacao_1960(self.df)
            case 1970:
                educacao_1970(self.df)
            case other:
                #TODO
                print_error('Ainda não implementado')

NAO_FREQUENTA = 'Não frequenta'
NAO_CONCLUIU_ANALF = 'Não concluiu e não alfabetizado'
NAO_CONCLUIU_ALFA = 'Não concluiu e alfabetizado'
EF_AI = 'Ensino Fundamental - Anos Iniciais'
EF_AF = 'Ensino Fundamental - Anos Finais'
EM = 'Ensino Médio'
ES = 'Educação Superior'


MAX_EF_AI = 4
MAX_EF_AF = 8
MAX_EM    = 11
MAX_ES    = 15

C_ANOS_ESC   = 'anos_esc'
C_ETAPA_CONC = 'etapa_concluida'


#As funções de harmonização das informações educacionais são, em parte, uma conversão dos scripts
# em R elaborados por @antrologos, disponível em https://github.com/antrologos/VariaveisHarmonizadasDataCEM/
def educacao_1960(df):
    '''
    V211 - Alfabetização
    ====================
    0	 Lê e Freqüenta Escola
    1	 Lê e não Freqüenta Escola
    2	 não Lê e Freqüenta Escola
    3	 não Lê e não Freqüenta Escola
    4	 Ignorada
         Não aplicável (4 anos de idade ou menos) ou Informação Faltante (Registro Corrompido)

    V212 - Última série concluída
    =============================
    4    Primeira Série
    5    Segunda Série
    6    Terceira Série
    7    Quarta Série
    8    Quinta Série
    9    Sexta Série
    0    Esta cursando o Primeiro ano do Elementar (não possui série concluída)
    1    Nunca Frequentou Escola
    2    Ignorado
         Não aplicável (4 anos de idade ou menos) ou Informação Faltante (Registro Corrompido)

    V213 - Grau do curso
    ====================
    2    Elementar
    3    Médio Primeiro Ciclo
    4    Médio Segundo Ciclo
    5    Superior
    6    Ignorado
    1    Nunca Frequentou Escola
    0    Esta cursando o Primeiro ano do Elementar (não possui série concluída)
         Não aplicável (4 anos de idade ou menos) ou Informação Faltante (Registro Corrompido)

    V214 - Curso completo 
    =====================
    0    Sem Curso Completo
    10   Primário/Elementar
    11   Comercial - Elementar
    13   Saúde e Serviços Sanitários - Elementar
    14   Militar Elementar - Elementar
    15   Agricultura e Pecuária - Elementar
    16   Emendativo - Elementar
    17   Industrial - Elementar
    19   Outros - Elementar
    20   Ginasial
    21   Comercial - 1º Grau / Médio 1º Ciclo
    22   Normal/Pedagógico - 1º Grau / Médio 1º Ciclo
    24   Militar - 1º Grau / Médio 1º Ciclo
    25   Agricultura e Pecuária - 1º Grau / Médio 1º Ciclo
    26   Emendativo - 1º Grau / Médio 1º Ciclo
    27   Industrial - 1º Grau / Médio 1º Ciclo
    29   Outros - 1º Grau / Médio 1º Ciclo
    33   Serviços Sanitários - 1º Grau / Médio 1º Ciclo
    34   Militar Médio - 1º Grau / Médio 1º Ciclo
    36   Educação Física - 1º Grau / Médio 1º Ciclo
    38   Eclesiástico - 1º Grau / Médio 1º Ciclo
    39   Outros Níveis Médios (1º Grau / Médio 1º Ciclo)
    40   Colegial/Científico
    41   Comercial - 2º Grau
    42   Normal/Pedagógico - 2º Grau
    44   Militar - 2º Grau
    45   Agricultura e Pecuária - 2º Grau
    47   Industrial - 2º Grau
    49   Outros - 2º Grau
    50   Geografia e História - Superior
    51   História Natural - Superior
    52   Letras - Superior
    53   Matemática, Física, Química, Desenho - Superior
    54   Outros Cursos Superiores (Pedagogia, Filosofia, Ciências Sociais, Teologia)
    57   Belas Artes - Superior
    60   Medicina - Superior
    61   Farmácia - Superior
    62   Odontologia - Superior
    63   Veterinária - Superior
    64   Engenharia - Superior
    65   Arquitetura - Superior
    66   Química Industrial - Superior
    67   Direito - Superior
    68   Agronomia - Superior
    70   Ciências Econômicas - Superior
    71   Estatística  - Superior
    72   Artes Domésticas  - Superior
    73   Saúde, Enfermagem e Serviços Sanitários - Superior
    74   Militar - Superior
    76   Educação Física - Superior
    78   Eclesiástico - Superior (Teologia e Filosofia para formação eclesiástica)
    79   Outros - Nível Superior (Adm. Pública, Música, Jornalismo, Museologia etc)
    89   Curso com grau não especificado
    99   Ignorado 
         Não aplicável (9 anos de idade ou menos) ou Informação Faltante (Registro Corrompido)

    '''
    
    #Anos base de estudo para cada grau
    yearsStage = {
        '2': 0,
        '3': 4,
        '4': 8,
        '5': 11
    }
    #Anos de estuda para as séries
    yearsSeries = {
        '4': 1,
        '5': 2,
        '6': 3,
        '7': 4,
        '8': 5,
        '9': 6
    }
    
    #Coluna de anos de escolaridade
    df[C_ANOS_ESC] = df.V213.map(yearsStage) + df.V212.map(yearsSeries)
    df.loc[df.V213.isin({'0', '1'}), 'C_ANOS_ESC'] = 0
    #Aplicar teto para os graus de escolaridade
    #Elementar: 4 anos; Médio 1º Ciclo: 8; Médio 2º Ciclo: 11; Superior: 15
    df.loc[(df[C_ANOS_ESC] > MAX_EF_AI) & (df.V213 == '2'), C_ANOS_ESC] = MAX_EF_AI
    df.loc[(df[C_ANOS_ESC] > MAX_EF_AF) & (df.V213 == '3'), C_ANOS_ESC] = MAX_EF_AF
    df.loc[(df[C_ANOS_ESC] > MAX_EM)    & (df.V213 == '4'), C_ANOS_ESC] = MAX_EM
    df.loc[(df[C_ANOS_ESC] > MAX_ES)    & (df.V213 == '5'), C_ANOS_ESC] = MAX_ES
    df[C_ANOS_ESC] = df[C_ANOS_ESC].fillna(0)
    df[C_ANOS_ESC] = df[C_ANOS_ESC].astype('UInt8')

    #Coluna de conclusão de etapa

    #Preparar algumas variáveis
    V214i = df.V214.astype(int)
    filter_no_grade = (V214i == 0) | (df.V212 == '1') | (df.V213 == '1')
    df[C_ETAPA_CONC] = pd.NA

    df.loc[filter_no_grade & df.V211.isin({'2', '3'}), C_ETAPA_CONC] = NAO_CONCLUIU_ANALF
    df.loc[filter_no_grade & df.V211.isin({'0', '1'}), C_ETAPA_CONC] = NAO_CONCLUIU_ALFA
    df.loc[V214i.between(10, 19), C_ETAPA_CONC] = EF_AI
    df.loc[V214i.between(20, 29), C_ETAPA_CONC] = EF_AF
    df.loc[V214i.between(30, 49), C_ETAPA_CONC] = EM
    df.loc[V214i.between(50, 79), C_ETAPA_CONC] = ES
    df[C_ETAPA_CONC] = df[C_ETAPA_CONC].astype('category')

def educacao_1970(df):
    '''
    V035 - Alfabetização
    ====================
    0     Sem declaração
    1     Sim
    2     Não
    --------------------
    Nota: O arquivo .sav disponibilizado pelo CEM não segue os valores
    da documentação, por essa razão, realizei essa compatibilização.
    
    V039 - Espécie de curso concluído
    =================================
    0     sem declaração
    10    primário
    11    agrícola elementar
    12    comercial elementar
    19    industrial elementar
    21    militar elementar
    22    normal elementar
    27    outros elementar
    28    emendativo elementar
    30    ginasial
    31    agrícola 1º ciclo
    32    comercial 2º ciclo
    34    eclesiástico 1º ciclo
    35    educação física 1º ciclo
    36    enfermagem 1º ciclo
    39    industrial 1º ciclo
    41    militar 1º ciclo
    42    normal 1º ciclo
    47    outros 1º ciclo
    48    emendativo 1º ciclo
    50    colegial
    51    agrícola 2º ciclo
    52    comercial 2º ciclo
    53    belas artes 2º ciclo
    54    eclesiástico 2º ciclo
    55    educação física 2º ciclo
    56    enfermagem 2º ciclo
    58    estatística 2º ciclo
    59    industrial 2º ciclo
    61    militar 2º ciclo
    62    normal 2º ciclo
    65    serviço social 2º ciclo
    67    outros 2º ciclo
    70    administração
    71    agronomia
    72    arquitetura
    73    belas artes superior
    74    ciências sociais
    75    filosofia
    76    geografia ou história
    77    história natural
    78    letras
    79    matemática, física e química
    80    pedagogia
    81    contabilidade ou atuária
    82    economia
    83    direito
    84    eclesiástico superior
    85    educação física superior
    86    enfermagem superior
    87    engenharia
    88    estatística superior
    89    farmácia ou bioquímica
    90    medicina
    91    militar superior
    92    odontologia
    93    psicologia
    94    química industrial
    95    serviço social superior
    96    veterinária
    97    outros superiores
    98    grau indeterminado
    99    nenhum
    '''

    #Coluna de conclusão de etapas
    #Preparar algumas variáveis
    V039i = df.V039.astype('UInt16')
    df[C_ETAPA_CONC] = pd.NA
    
    df.loc[(V039i == 99) & (df.V035 == '2'), C_ETAPA_CONC] = NAO_CONCLUIU_ANALF
    df.loc[(V039i == 99) & (df.V035 == '1'), C_ETAPA_CONC] = NAO_CONCLUIU_ALFA
    df.loc[V039i.between(10, 28), C_ETAPA_CONC] = EF_AI
    df.loc[V039i.between(30, 49), C_ETAPA_CONC] = EF_AF
    df.loc[V039i.between(50, 69), C_ETAPA_CONC] = EM
    df.loc[V039i.between(70, 98), C_ETAPA_CONC] = ES

    #TODO tratar os casos de não declarados
    #df.V039 == 0

    df[C_ETAPA_CONC] = df[C_ETAPA_CONC].astype('category')
