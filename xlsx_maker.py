import warnings
from collections import namedtuple, defaultdict

import pandas as pd

TAMANHO_PADRAO = 10
HEADER_PADRAO = 'h1'

worksheet_column = namedtuple('Coluna', ['nome', 'title', 'tamanho', 'estilo', 'm_header', 'herdar_estilo', 'estilo_filho'],
                    defaults=['', '', TAMANHO_PADRAO, HEADER_PADRAO, False, False, None])


def contar_rec(est):
    if isinstance(est, list):
        return len(est)
    elif isinstance(est, dict):
        return sum(contar_rec(est[e]) for e in est)
    else:
        return 1


def ordenar_colunas(estrutura, sufixo=''):
    if isinstance(estrutura, list):
        return [val.nome+sufixo for val in estrutura]
    ordem_colunas = []
    for val in estrutura:
        ordem_colunas.extend(ordenar_colunas(estrutura[val], sufixo+'_'+val.nome if val.nome else sufixo))
    return ordem_colunas


def contar_header(estrutura, n=1):
    if isinstance(estrutura, list):
        return n
    for val in estrutura:
        return contar_header(estrutura[val], n+1)


def parse_estrutura(worksheet, estrutura, coluna_offset, estilos, estilo_pai=None, linha_inicial=0):
    col_nivel0 = coluna_offset
    col_nivel1 = coluna_offset
    
    if isinstance(estrutura, list):
        col = coluna_offset
        for val in estrutura:
            worksheet.write(linha_inicial,
                            col,
                            val.title,
                            estilos[HEADER_PADRAO])
            estilo = estilo_pai if val.herdar_estilo else val.estilo
            worksheet.set_column(col, col, val.tamanho, estilos[estilo])
            col += 1
        return col

    for val in estrutura:
        n_colunas = contar_rec(estrutura[val]) 
        col_nivel0_futura = col_nivel0 + n_colunas
        worksheet.merge_range(linha_inicial,
                              col_nivel0,
                              linha_inicial,
                              col_nivel0_futura - 1,
                              val.title,
                              estilos[HEADER_PADRAO])
        col_nivel0 = col_nivel0_futura
        col_nivel1 = parse_estrutura(worksheet,
                        estrutura[val],
                        col_nivel1,
                        estilos,
                        estilo_pai=val.estilo_filho,
                        linha_inicial=linha_inicial+1)
    return col_nivel1


def workbook_setup(workbook, title, author):

    workbook.set_properties(
        {
            'title': title,
            'author': author,
        }
    )

    # https://xlsxwriter.readthedocs.io/format.html
    normal = {'font_name': 'Arial', 'font_size': 10}
    header = {'text_wrap': True, 'valign': 'vcenter', 'align': 'center', 'bold': True}
    format_white = workbook.add_format({'bg_color': 'white', 'pattern': 1})
    format_int  = workbook.add_format({**normal, 'num_format': '#,##0'})
    format_float  = workbook.add_format({**normal, 'num_format': '0.00'})
    format_float_1  = workbook.add_format({**normal, 'num_format': '0.0'})
    format_perc = workbook.add_format({**normal, 'num_format': '0.0%'})
    format_header = workbook.add_format({**normal,
                                         **header})
    format_h1 = workbook.add_format({**normal,
                                     **header,
                                     'bottom': True,
                                     'left': True})
    format_hleft = workbook.add_format({**normal,
                                        **header,
                                        'text_wrap': False,
                                        'align': 'left'})
    format_hcenter = workbook.add_format({**normal,
                                        **header,
                                        'text_wrap': False,
                                        'align': 'center'})
    format_top = workbook.add_format({**normal, 'top': True})
    format_fonte = workbook.add_format({**normal, 'top': True, 'font_size': 8})

    estilos = {'int': format_int,
               'float': format_float,
               'float_1': format_float_1,
               '%':   format_perc,
               'header': format_header,
               'h1': format_h1,
               'h_left': format_hleft,
               'h_center': format_hcenter,
               'top': format_top,
               'fonte': format_fonte,
               'white': format_white,}

    return estilos


def to_worksheet(writer,
                 df,
                 estrutura,
                 estrutura_header,
                 sheet_name,
                 fonte,
                 estilos,
                 header_height_size=60):

    ordem_colunas = ordenar_colunas(estrutura)
    df = df.reset_index()[[v.nome for v in estrutura_header]+ordem_colunas]
    header_size = contar_header(estrutura)
    df.to_excel(writer, sheet_name=sheet_name, startrow=header_size,  header=False, index=False)

    worksheet = writer.sheets[sheet_name]

    for i in range(header_size):
        size = 20 if i != header_size-1 else header_height_size
        worksheet.set_row(i, size, estilos['header'])
        
    coluna_offset = len(estrutura_header)
    
    for i, val in enumerate(estrutura_header):
        assert header_size > 0
        if header_size == 1:
            worksheet.write(header_size-1, i, val.title, estilos[HEADER_PADRAO]) 
        else:
            worksheet.merge_range(0, i, header_size-1, i, val.title, estilos[HEADER_PADRAO])
        worksheet.set_column(i, i, val.tamanho, estilos[val.estilo])

    for i in range(df.shape[1]):
        if i == 0:
            worksheet.write(df.shape[0]+header_size, i, fonte, estilos['fonte'])
        else:
            worksheet.write_blank(df.shape[0]+header_size, i, '', estilos['fonte'])

    parse_estrutura(worksheet, estrutura, coluna_offset, estilos)

    for i_col, h in enumerate(estrutura_header):
        if h.m_header == True:
            v_counts = df[h.nome].value_counts()
            i_row_start = header_size
            for n_row, val in zip(v_counts, v_counts.index):
                i_row_end = i_row_start + n_row - 1
                if i_row_start != i_row_end:
                    worksheet.merge_range(i_row_start, i_col, i_row_end, i_col, val, estilos[HEADER_PADRAO])
                i_row_start = i_row_end + 1

    return worksheet
