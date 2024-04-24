CERT_PATH = 'certificados'

FILETYPES_PATH = {
    'feather': 'feathers',
    'parquet': 'parquets',
}
RAW_FILES_PATH = 'raw-files'

UF_SIGLA_NOME = {
    'RO': 'Rondônia',
    'AC': 'Acre',
    'AM': 'Amazonas',
    'RR': 'Roraima',
    'PA': 'Pará',
    'AP': 'Amapá',
    'TO': 'Tocantins',
    'MA': 'Maranhão',
    'PI': 'Piauí',
    'CE': 'Ceará',
    'RN': 'Rio Grande do Norte',
    'PB': 'Paraíba',
    'PE': 'Pernambuco',
    'AL': 'Alagoas',
    'SE': 'Sergipe',
    'BA': 'Bahia',
    'MG': 'Minas Gerais',
    'ES': 'Espírito Santo',
    'RJ': 'Rio de Janeiro',
    'SP': 'São Paulo',
    'PR': 'Paraná',
    'SC': 'Santa Catarina',
    'RS': 'Rio Grande do Sul',
    'MS': 'Mato Grosso do Sul',
    'MT': 'Mato Grosso',
    'GO': 'Goiás',
    'DF': 'Distrito Federal',
}

UF_NOME_SIGLA = {value: key for key, value in UF_SIGLA_NOME.items()}

UF_COD_NOME = {
    11:	'Rondônia',
    12:	'Acre',
    13:	'Amazonas',
    14:	'Roraima',
    15:	'Pará',
    16:	'Amapá',
    17:	'Tocantins',
    21:	'Maranhão',
    22:	'Piauí',
    23:	'Ceará',
    24:	'Rio Grande do Norte',
    25:	'Paraíba',
    26:	'Pernambuco',
    27:	'Alagoas',
    28:	'Sergipe',
    29:	'Bahia',
    31:	'Minas Gerais',
    32:	'Espírito Santo',
    33:	'Rio de Janeiro',
    35:	'São Paulo',
    41:	'Paraná',
    42:	'Santa Catarina',
    43:	'Rio Grande do Sul',
    50:	'Mato Grosso do Sul',
    51:	'Mato Grosso',
    52:	'Goiás',
    53:	'Distrito Federal',
}

UF_NOME_COD = {value: key for key, value in UF_COD_NOME.items()}

MAP_BRASIL_REGIOES_UFS = {key: 'ufs' for key in UF_NOME_COD.keys()}

MAP_BRASIL_REGIOES_UFS.update({
    'Norte': 'regioes',
    'Nordeste': 'regioes',
    'Sudeste': 'regioes',
    'Sul': 'regioes',
    'Centro_Oeste': 'regioes',
    'Centro - Oeste': 'regioes',
    'Centro-Oeste': 'regioes',
})

MAP_BRASIL_REGIOES_UFS.update({'Brasil': 'brasil'})
