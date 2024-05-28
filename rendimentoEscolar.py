import os
import zipfile

from .common import handleDatabase, RAW_FILES_PATH

PATH = 'rendimento-escolar'
URL = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/taxas-de-rendimento-escolar'
EXPR_FILTER = ('[a["href"] for a in soup.find("div",'
                                ' id="parent-fieldname-text").find_all("a")'
                                ' if self.agg_level in a["href"].lower()]')
CERT = 'inep-gov-br-chain.pem'
FIRST_YEAR = 2007
LAST_YEAR = 2022
AGG_LEVEL_REN = (
    'brasil',
    'regioes',
    'ufs',
    'municipios',
    'escola',
)

REN_REGIOES = {
    'Centro - Oeste': 'Centro-Oeste',
    'Centro_Oeste': 'Centro-Oeste',
}
COLUMN_SIZE_REN = 58

COLUMNS_LABELS_REN = {
    2007: [
    'NU_ANO_CENSO', 'UNIDGEO', 'NO_CATEGORIA', 'NO_DEPENDENCIA',   
    
    'APROVACAO_EF_01', 'APROVACAO_EF_02', 'APROVACAO_EF_03',
    'APROVACAO_EF_04', 'APROVACAO_EF_05', 'APROVACAO_EF_06',
    'APROVACAO_EF_07', 'APROVACAO_EF_08', 'APROVACAO_EF_09',
    'APROVACAO_EF',    'APROVACAO_EF_AI', 'APROVACAO_EF_AF',
    'APROVACAO_EM_01', 'APROVACAO_EM_02', 'APROVACAO_EM_03',
    'APROVACAO_EM_04', 'APROVACAO_EM_NS', 'APROVACAO_EM',

    'REPROVACAO_EF_01', 'REPROVACAO_EF_02', 'REPROVACAO_EF_03',
    'REPROVACAO_EF_04', 'REPROVACAO_EF_05', 'REPROVACAO_EF_06',
    'REPROVACAO_EF_07', 'REPROVACAO_EF_08', 'REPROVACAO_EF_09',
    'REPROVACAO_EF',    'REPROVACAO_EF_AI', 'REPROVACAO_EF_AF',
    'REPROVACAO_EM_01', 'REPROVACAO_EM_02', 'REPROVACAO_EM_03',
    'REPROVACAO_EM_04', 'REPROVACAO_EM_NS', 'REPROVACAO_EM',

    'ABANDONO_EF_01', 'ABANDONO_EF_02', 'ABANDONO_EF_03',
    'ABANDONO_EF_04', 'ABANDONO_EF_05', 'ABANDONO_EF_06',
    'ABANDONO_EF_07', 'ABANDONO_EF_08', 'ABANDONO_EF_09',
    'ABANDONO_EF',    'ABANDONO_EF_AI', 'ABANDONO_EF_AF',
    'ABANDONO_EM_01', 'ABANDONO_EM_02', 'ABANDONO_EM_03',
    'ABANDONO_EM_04', 'ABANDONO_EM_NS', 'ABANDONO_EM', 
    ],

    2011: [
    'NU_ANO_CENSO', 'UNIDGEO', 'NO_CATEGORIA', 'NO_DEPENDENCIA',   
    
    'APROVACAO_EF',    'APROVACAO_EF_AI', 'APROVACAO_EF_AF',
    'APROVACAO_EF_01', 'APROVACAO_EF_02', 'APROVACAO_EF_03',
    'APROVACAO_EF_04', 'APROVACAO_EF_05', 'APROVACAO_EF_06',
    'APROVACAO_EF_07', 'APROVACAO_EF_08', 'APROVACAO_EF_09',
    'APROVACAO_EM',    'APROVACAO_EM_01', 'APROVACAO_EM_02',
    'APROVACAO_EM_03', 'APROVACAO_EM_04', 'APROVACAO_EM_NS',

    'REPROVACAO_EF',    'REPROVACAO_EF_AI', 'REPROVACAO_EF_AF',
    'REPROVACAO_EF_01', 'REPROVACAO_EF_02', 'REPROVACAO_EF_03',
    'REPROVACAO_EF_04', 'REPROVACAO_EF_05', 'REPROVACAO_EF_06',
    'REPROVACAO_EF_07', 'REPROVACAO_EF_08', 'REPROVACAO_EF_09',
    'REPROVACAO_EM',    'REPROVACAO_EM_01', 'REPROVACAO_EM_02',
    'REPROVACAO_EM_03', 'REPROVACAO_EM_04', 'REPROVACAO_EM_NS',

    'ABANDONO_EF',    'ABANDONO_EF_AI', 'ABANDONO_EF_AF',
    'ABANDONO_EF_01', 'ABANDONO_EF_02', 'ABANDONO_EF_03',
    'ABANDONO_EF_04', 'ABANDONO_EF_05', 'ABANDONO_EF_06',
    'ABANDONO_EF_07', 'ABANDONO_EF_08', 'ABANDONO_EF_09',
    'ABANDONO_EM',    'ABANDONO_EM_01', 'ABANDONO_EM_02',
    'ABANDONO_EM_03', 'ABANDONO_EM_04', 'ABANDONO_EM_NS',
    ],
}

class handleRendimentoEscolar(handleDatabase):
    def __init__(self, year, agg_level, medium=requests):
        if (year < FIRST_YEAR
            or year > LAST_YEAR):
            print_error(f'Não há dados disponíveis para o ano {year}')
            raise ValueError
        if agg_level not in AGG_LEVEL_REN:
            print_error('As opções de nível de agregação são:'
                       f'{AGG_LEVEL_REN}')
            raise ValueError

        super().__init__(year, medium)
        self.agg_level = agg_level
        self.name = 'Rendimento Escolar'
        self.path = os.path.join(self.root, PATH, self.agg_level)
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.raw_files_path = os.path.join(os.path.split(self.path)[0],
                                           RAW_FILES_PATH)
        if not os.path.isdir(self.raw_files_path):
            os.mkdir(self.raw_files_path)
        self.url = f'{URL}/{year}'
        self.is_zipped = True
        self.filename = f'{self.year}-{self.agg_level}-rendimento-escolar'
        self.expr_filter = EXPR_FILTER

    def get_save_raw_database(self):
        cert = os.path.join('.', CERT_PATH, CERT)
        if not os.path.isfile(cert):
            cert = False
        self.get_url()
        super().get_save_raw_database(cert)

    def unzip(self):
        if not hasattr(self, 'filepath'):
            self.get_save_raw_database()
        self.dfs = []
        with zipfile.ZipFile(self.filepath, 'r') as zf:
            for filepath in zf.namelist():
                filename = os.path.split(filepath)[-1]
                if ('xls' in filename.lower() 
                     and not filename.startswith('~')):
                    print_info(f'Convertendo em df o arquivo {filename}')
                    with zf.open(filepath) as f:
                        df_sheet_dict = pd.read_excel(f, header=None,
                                                      na_values='--',
                                                      sheet_name=None)
                        for df in df_sheet_dict.values():
                            self.dfs.append(df)
        return self.dfs

    def preprocess_df(self):
        if not hasattr(self, 'dfs'):
            self.unzip()
        dfs = []
        for df in self.dfs:
            for i_start, e in enumerate(df.iloc[:, 0]):
                if pd.isnull(e) or pd.isna(e):
                    continue
                if str(self.year) == str(e).strip():
                    break
            for i_end, e in enumerate(df.iloc[::-1, 0]):
                if pd.isnull(e) or pd.isna(e):
                    continue
                if str(self.year) == str(e).strip():
                    i_end = None if i_end == 0 else -i_end
                    break
            
            match self.agg_level:
                case 'brasil' | 'regioes' | 'ufs':
                    flag0 = False
                    flag1 = False
                    for e in df.iloc[:i_start, 1]:
                        if pd.isnull(e) or pd.isna(e):
                            continue
                        if str(e).strip().lower() == 'região':
                            flag0 = True
                    for e in df.iloc[:i_start, 2]:
                        if pd.isnull(e) or pd.isna(e):
                            continue
                        if str(e).strip().lower() == 'uf':
                            flag1 = True

                    if flag0 and flag1:
                        df.drop(columns=1, inplace=True)
                        df.columns = range(COLUMN_SIZE_REN)
                    assert len(df.columns) == COLUMN_SIZE_REN, \
                             len(df.columns)
                
                case 'municipios':
                    df.drop(columns=[1, 2, 4], inplace=True)

            dfs.append(df.iloc[i_start:i_end].reset_index(drop=True))

        df = pd.concat(dfs, ignore_index=True)

        if self.agg_level == 'escola':
            filename_esc = os.path.join(self.path,
                                        'escolas-todas.pickle')
            if not os.path.isfile(filename_esc):
                assert self.year == 2007, \
                 'O ano deve ser 2007, para iniciar a base das escolas'

                df_esc = pd.DataFrame({
                    'ANO_INCLUSÃO': self.year,
                    'CO_ESCOLA': df.iloc[:, 5],
                    'NO_ESCOLA': df.iloc[:, 6],
                    'CO_MUNICIPIO': df.iloc[:, 3],
                    'NO_CATEGORIA': df.iloc[:, 7],
                    'NO_DEPENDENCIA': df.iloc[:, 8],
                    'DUPLICADO': False
                })

                df_esc.to_pickle(filename_esc)

            else:
                df_esc = pd.read_pickle(filename_esc)
                df_esc = pd.concat([df_esc, pd.DataFrame({
                    'ANO_INCLUSÃO': self.year,
                    'CO_ESCOLA': df.iloc[:, 5],
                    'NO_ESCOLA': df.iloc[:, 6],
                    'CO_MUNICIPIO': df.iloc[:, 3],
                    'NO_CATEGORIA': df.iloc[:, 7],
                    'NO_DEPENDENCIA': df.iloc[:, 8],
                    'DUPLICADO': False
                    })], ignore_index=True)
                df_esc.drop_duplicates(subset=['CO_ESCOLA',
                                               'NO_ESCOLA',
                                               'CO_MUNICIPIO',
                                               'NO_CATEGORIA',
                                               'NO_DEPENDENCIA'],
                                       inplace=True,
                                       ignore_index=True)
                
                filter_ = (df_esc.CO_ESCOLA.value_counts() > 2).index
                df_esc.loc[df_esc.CO_ESCOLA.isin(filter_),
                                                 'DUPLICADO'] = True
                df_esc.to_pickle(filename_esc)
            
            df.drop(columns=[1, 2, 3, 4, 6], inplace=True)

        if self.year < 2011:
            columns = COLUMNS_LABELS_REN[2007]
        elif self.year < 2023:
            columns = COLUMNS_LABELS_REN[2011]
        else:
            raise ValueError
        df.columns = columns 
        
        self.df = df[COLUMNS_LABELS_REN[2011]]

        match self.agg_level:
            case 'brasil' | 'regioes' | 'ufs':
                self.df = self.preprocess_br()
            case 'municipios':
                self.df = self.preprocess_mun()
            case 'escola':
                self.df = self.preprocess_esc()
        return self.df

    def preprocess_br(self):
        self.df.UNIDGEO = (self.df.UNIDGEO.str.strip()
                                          .str.title()
                                          .str.replace(' Do ', ' do ')
                                          .str.replace(' De ', ' de '))
        self.df.UNIDGEO = self.df.UNIDGEO.map(
                lambda e: UF_SIGLA_NOME.get(e.upper(), e)) 

        mapping = self.df.UNIDGEO.map(MAP_BRASIL_REGIOES_UFS)

        if any(mapping.isna()):
            self.df['tmp'] = mapping
            print_error('O mapeamento não foi completo',
                        self.df.UNIDGEO.unique(),
                        self.df[self.df.tmp.isna()])
            raise ValueError

        filter_ = mapping == self.agg_level
        self.df = self.df[filter_].reset_index(drop=True)
        return self.df

    def preprocess_mun(self):
        return self.df

    def preprocess_esc(self):
        self.df.drop(columns=['NO_CATEGORIA', 'NO_DEPENDENCIA'], inplace=True)
        return self.df

    def otimize_df(self):
        if not hasattr(self, 'df'):
            self.preprocess_df()

        self.df['NU_ANO_CENSO'] = pd.to_numeric(self.df['NU_ANO_CENSO'],
                                                downcast='unsigned')
        if self.agg_level != 'escola':
            for col in ('NO_CATEGORIA', 'NO_DEPENDENCIA'):
                self.df[col] = self.df[col].astype('category')

        self.df.UNIDGEO = self.df.UNIDGEO.astype('string')

        for col in self.df.columns[self.df.columns.str.match('^APR|REP|ABA\w+')]:
            self.df[col] = self.df[col].astype('Float32')
        
        return self.df

    def basic_names(self):
        return [f'Base de dados = "{self.name}"',
                f'Ano = "{self.year}"',
                f'Agg_level = "{self.agg_level}"']
