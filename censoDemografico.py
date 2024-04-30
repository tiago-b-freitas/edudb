import collections
import glob
import os
import zipfile

import pandas as pd
import requests

from .common import handleDatabase, mean_weight, std_weight, median_weight,\
                    print_info, print_error, parse_sas
from .definitions import FILETYPES_PATH, RAW_FILES_PATH, UF_SIGLA_NOME

PATH = 'censo-demografico'

URL = {
    2000: 'https://ftp.ibge.gov.br/Censos/Censo_Demografico_2000/Microdados',
    2010: 'https://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_Gerais_da_Amostra/Microdados'}

TYPES = ('PESS', 'DOMI')

CRITERION_ALL = ('[file_url["href"] for file_url in soup.find_all("a")'
                               ' if "zip" in file_url["href"]]')
CRITERION = ('[file_url["href"] for file_url in soup.find_all("a")'
                               ' if "zip" in file_url["href"]' 
                               ' and f"{self.uf}" in file_url["href"]]')

DOCUMENTACAO = {
    'PESS': {2000: 'LE PESSOAS.sas',
             2010: 'Layout_microdados_Amostra.xls'},
    'DOMI': {2000: 'LE FAMILIAS.sas',
             2010: 'Layout_microdados_Amostra.xls'}
}

WEIGHTS = {
    2000: 'P001',
    2010: 'V0010',
}

class handleCensoDemografico(handleDatabase):
    def __init__(self, year, uf, type_db, medium=requests):
        if year not in (2000, 2010):
            print_error(f'Ano {year} não implementado.')
            raise ValueError 
        if uf not in UF_SIGLA_NOME and uf != 'all':
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
        self.name = 'Censo Demográfico'
        self.filename = f'{year}-{type_db}-{uf}-censo-demografico'
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
        self.url = URL[year]
        self.doc_filename = DOCUMENTACAO[self.type_db][self.year]
        self.is_zipped = True
        self.weight_var = WEIGHTS[self.year] 

    def get_url(self):
        criterion = CRITERION_ALL if self.uf == 'ALL' else CRITERION
        file_urls = super().get_url(criterion, unique=False)
        self.file_urls = [os.path.join(self.url, file_url)
                          for file_url in file_urls]
        return self.file_urls

    def get_save_raw_database(self):
        self.get_url()
        for file_url in self.file_urls:
            self.file_url = file_url
            super().get_save_raw_database()

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
                                               sheet_name=['DOMI', 'PESS'],
                                               skiprows=1)

                        self.colspecs = {}
                        self.dtypes = {}
                        for df_name in ('DOMI', 'PESS'):
                            df = self.df_dict[df_name]
                            self.df_dict[df_name]['colspecs'] = [
                                (inicial - 1, final) for inicial, final in 
                            zip(df['POSIÇÃO INICIAL'], df['POSIÇÃO FINAL'])
                            ]
                            self.dtypes[df_name]   = {}
                            for tipo, var in zip(df.TIPO, df.VAR):
                                tipo = tipo.strip()
                                dtype = 'string'
                                if tipo == 'C':
                                    dtype = 'category'
                                self.dtypes[df_name][var] = dtype

    def unzip(self):
        if not hasattr(self, 'filepath'):
            self.get_save_raw_database()
        if not hasattr(self, 'database_dict'):
            self.make_database_dict()

        if self.type_db == 'PESS':
            criterion = 'Amostra_Pessoas'
        self.df = pd.DataFrame()
        for filepath in os.listdir(self.raw_files_path):
            if 'documentacao' in filepath.lower() or self.uf not in filepath.upper():
                continue
            with zipfile.ZipFile(os.path.join(self.raw_files_path, filepath), metadata_encoding='cp850') as zf:
                for fn in zf.namelist():
                    if criterion in fn:
                        with zf.open(fn) as f:
                            df = pd.read_fwf(f,
                                             names=self.df_dict[self.type_db].VAR,
                                             colspecs=self.df_dict[self.type_db].colspecs.to_list(),
                                             dtype=self.dtypes[self.type_db])
                self.df = pd.concat([self.df, df], ignore_index=True)
        
        if self.uf == 'SP':
            for col, dtype in self.dtypes[self.type_db].items():
                if self.df[col].dtype != dtype:
                    self.df[col] = self.df[col].astype(dtype)
        return self.df

    def str_to_float(self, s, first, last):
        if pd.isna(s):
            return pd.NA
        assert(first + last == len(s))
        return float(s[:first] + '.' + s[first:])

    def preprocess_df(self):
        if not hasattr(self, 'df'):
            self.unzip()
        float_vars = self.df_dict[self.type_db].loc[self.df_dict[self.type_db].DEC.notna(),
                                               ['VAR', 'INT', 'DEC']]
        for var, first, last in float_vars.itertuples(index=False, name=None):
            self.df[var] = self.df[var].apply(self.str_to_float, args=(first, last))
            self.df[var] = self.df[var].astype('Float64')

        for col in self.df.select_dtypes(object).columns:
            if col == 'V0300':
                continue
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
        match self.year:
            case 2000:
                df = self.make_map_dict_2010()
            case 2010:
                df = self.make_map_dict_2000()
        return df

    def make_map_dict_2000(self):
        ...

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
            with zf.open(os.path.join(path, 'Ocupaç╞o COD 2010.xls')) as f:
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


    def get_map_var(self, var):
        if not os.path.isfile(f'{self.path_dict}.pickle'):
            print_info('Dicionário da base não existente. Construindo...')
            df = self.make_map_dict()
            print_info('Dicionário concluído com sucesso!')
        else:
            df = pd.read_pickle(f'{self.path_dict}.pickle')

        if var == 'all':
            return df

        nome = df.loc[df.COD_VAR == var, 'NOME_VAR'].values[0]
        vars_cod = df.loc[df.COD_VAR == var, 'MAP_VAR'].values[0]
        return nome, vars_cod

    def get_coded_var(self, var):
        if var == 'V0002':
            col = self.cod_mun
        elif var == 'V1002':
            col = self.cod_meso
        elif var == 'V1003':
            col = self.cod_micro
        else:
            col = self.df[var]
        return col.map(self.get_map_var(var)[1])

    def crosstab(self,
                 index_vars,
                 columns_vars,
                 values=None,
                 aggfunc='mean',
                 threshold=0,
                 normalize=False,
                 margins=False,
                 margins_name='All'):
        index = [self.get_coded_var(var) for var in index_vars]
        columns = [self.get_coded_var(var) for var in columns_vars]
        if values is not None:
            if aggfunc == 'mean':
                aggfunc = mean_weight
            elif aggfunc == 'median':
                aggfunc = median_weight
            elif aggfunc == 'std':
                aggfunc = std_weight

            return pd.crosstab(index=index,
                           columns=columns,
                           values=self.df[values],
                           aggfunc=lambda s: aggfunc(s,
                                                     self.df[self.weight_var],
                                                     threshold))
        else:
            return pd.crosstab(index=index,
                               columns=columns,
                               values=self.df[self.weight_var],
                               aggfunc='sum',
                               normalize=normalize,
                               margins=margins,
                               margins_name=margins_name)

    def get_df(self, filetype, **kwargs):
        df = super().get_df(filetype, **kwargs)
        self.cod_mun = (self.df.V0001.astype('string')
                        + self.df.V0002.astype('string')).astype('category')
        self.cod_meso = (self.df.V0001.astype('string')
                        + self.df.V1002.astype('string')).astype('category')
        self.cod_micro = (self.df.V0001.astype('string')
                        + self.df.V1003.astype('string')).astype('category')
        return df

