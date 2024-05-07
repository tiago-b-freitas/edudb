import os
import zipfile

from .common import handleDatabase, RAW_FILES_PATH
PATH = 'censo-escolar'
URL = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dadosabertos/microdados/censo-escolar'
EXPR_FILTER = ('[file_url["href"] for file_url in soup.find("div",'
                           ' id="content-core").find_all("a")'
                           ' if str(self.year) in file_url["href"]]')
CERT = 'inep-gov-br-chain.pem'


class handleCensoEscolar(handleDatabase):
    def __init__(self, year, medium=requests):
        super().__init__(medium, year)
        self.name = 'censo escolar'
        self.filename = 'censo-escolar'
        self.path = os.path.join(self.root, PATH)
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.raw_files_path = os.path.join(self.path, RAW_FILES_PATH)
        if not os.path.isdir(self.raw_files_path):
            os.mkdir(self.raw_files_path)
        self.url = URL
        self.is_zipped = True
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
        match self.year:
            case 2022 | 2023:
                selections = ['microdados', '.csv', '~suplemento']
        with zipfile.ZipFile(self.filepath, 'r') as zf:
            for filename in zf.namelist():
                correct_file = True
                for sel in selections:
                    if sel[0] == '~':
                        correct_file &= not sel in filename.lower()
                    else:
                        correct_file &= sel in filename.lower()
                    if not correct_file:
                        break
                if correct_file:
                    print_info(f'Convertendo em df o arquivo {filename}')
                    with zf.open(filename) as f:
                        self.df = pd.read_csv(f,
                                         sep=';',
                                         decimal='.',
                                         encoding='windows-1252',
                                         low_memory=False)
                        return self.df
                    
    def make_database_dict(self):
        with zipfile.ZipFile(self.filepath) as zf:
            for fn in zf.namelist():
                if 'xlsx' in fn and 'dicion' in fn and '~' not in fn:
                    df_dict_tmp = pd.read_excel(zf.open(fn), header=None)
        df_dict_tmp = (df_dict_tmp[df_dict_tmp[0].notna() &
                            (df_dict_tmp.iloc[:, self.year % 2000 - 1] != 'n')]
                             .reset_index(drop=True))
        df_dict = df_dict_tmp[df_dict_tmp[0].astype(str).str.isdecimal()]
        header_index = df_dict.index[0] - 1
        df_dict = df_dict.set_axis(df_dict_tmp.iloc[header_index, :], axis=1)
        
        df_dict['dtype'] = df_dict.apply(get_dtype, axis=1, df=self.df)

        dtype_dict = {nome: dtype for nome, dtype
                                  in df_dict[['Nome da Variável', 'dtype']]
                                            .itertuples(index=False, name=None)
                                  if dtype != 'datetime'}

        self.database_dict = dtype_dict
        return self.database_dict

    def otimize_df(self):
        self.make_database_dict()
        for col, dtype in self.database_dict.items():
            try:
                self.df[col] = self.df[col].astype(dtype)
            except TypeError:
                print_error(f"TypeError: {col}")
                self.df[col] = self.df[col].astype('string')
            except ValueError:
                print_error(f"ValueError: {col}")
                self.df[col] = self.df[col].astype('string')

        match self.year:
            case 2022:
                format_ = '%d%b%Y:%X'
            case 2023:
                format_ = '%d%b%y:%X'

        for col in self.df.select_dtypes('O').columns:
           self.df[col] = pd.to_datetime(self.df[col], format=format_)

        self.is_otimized = True
        return self.df
