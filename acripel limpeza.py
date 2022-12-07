import pandas as pd
import re
import numpy as np
from sqlalchemy import create_engine
import os
import glob
import time


os.chdir(r'D:\Guilherme Pontual\Downloads\acripel_romaneios')

engine = create_engine('postgresql://postgres:teste@localhost/postgres')

for file in glob.glob("*.TXT"):
    # lendo o romaneio
    df = pd.read_csv(r'D:\Guilherme Pontual\Downloads\acripel_romaneios\%s' % file, delimiter=';', header=None,
                     dtype='unicode',
                     names=['teste', 'romaneio', 'estabelecimento', 'transportadora', 'nt_fiscal', 'pedido', 'emissao',
                            'cidade', 'cliente', 'vlr_liq_nota', 'vlr_nota', 'vol'])

    # iniciando tratamento dos dados do romaneio
    df['teste'] = df['teste'].str.replace(r'TOTAL GERAL DE NOTAS.+', '', regex=True)
    df['teste'] = df['teste'].str.replace(r'Assinatura.+', '', regex=True)
    df['teste'] = df['teste'].str.replace(r'=', '', regex=True)
    df['teste'] = df['teste'].str.replace(r'-', '', regex=True)
    df['teste'] = df['teste'].str.replace(r'_', '', regex=True)
    df['teste'] = df['teste'].str.replace(r'Relacao de Manifesto por Nota Fiscal', '', regex=True)
    df['teste'] = df['teste'].str.replace(r'Nt Fiscal CFOP.+', '', regex=True)

    df.iloc[[0]]
    df['estabelecimento'] = df.iloc[0, 0]
    df['estabelecimento'] = df['estabelecimento'].str.replace(r"Pagina.+", "", regex=True)
    df['estabelecimento'] = df['estabelecimento'].str.strip()

    df['transportadora'] = df.iloc[6, 0]
    df['transportadora'] = df['transportadora'].str.replace(r'ROMANEIO: ', '')
    df['transportadora'] = df['transportadora'].str.replace(r'([0-9]+)', '', regex=True)
    df['transportadora'] = df['transportadora'].str.replace(r'-', '', regex=True)
    df['transportadora'] = df['transportadora'].str.replace(r'/ PE.+', '', regex=True)
    df['transportadora'] = df['transportadora'].str.replace(r'^ ', '', regex=True)

    df['cidade'] = df['cidade'].fillna(df['teste'])
    index_cliente = df[df['cidade'].str.contains('200')].index
    df.loc[index_cliente, 'cidade'] = df.loc[index_cliente, 'cidade'].str.replace(r'[0-9]', '', regex=True)

    df['cidade'] = df['cidade'].str.replace(r'ROMANEIO.+', '', regex=True)
    df['cidade'] = df['cidade'].str.replace(r'//', '', regex=True)
    df['cidade'] = df['cidade'].str.replace(r',', '', regex=True)
    df['cidade'] = df['cidade'].str.replace(r'PE\s.+', '', regex=True)
    df['cidade'] = df['cidade'].str.replace(r' {1,}', '', regex=True)

    df['romaneio'] = df.iloc[6, 0]

    df['romaneio'] = df['romaneio'].str.replace(r'ROMANEIO: ', '')
    df['romaneio'] = df['romaneio'].str.replace(r'[A-Z]', '', regex=True)
    df['romaneio'] = df['romaneio'].str.replace(r'-', '', regex=True)
    df['romaneio'] = df['romaneio'].str.replace(r'[a-z]', '', regex=True)
    df['romaneio'] = df['romaneio'].str.replace(r'[a-z]', '', regex=True)
    df['romaneio'] = df['romaneio'].str.replace(r' /.+', '', regex=True)
    df['romaneio'] = df['romaneio'].fillna(df['teste'])
    index_cliente = df[df['romaneio'].str.contains('RECIFEPE')].index
    df.loc[index_cliente, 'romaneio'] = df.loc[index_cliente, 'romaneio'].str.replace(r'[A-Z]', '', regex=True)
    df.loc[index_cliente, 'romaneio'] = df.loc[index_cliente, 'romaneio'].str.replace(r'\D\d\d\d\d\s\d{4}.+', '',
                                                                                      regex=True)

    data = df['emissao'] = df.iloc[6, 0]
    emissao = re.findall('[Data:]{5}[ /0-9]{11}', data)


    def split_it(data):
        return re.findall('[Data:]{5}[ /0-9]{11}', data)


    df['data'] = df['emissao'].apply(split_it)
    df['data'] = df['data'].str[0]
    df['data'] = df['data'].astype(str)
    df['data'] = df['data'].str.replace(r'Data: ', '', regex=True)
    df['data'] = df['data'].str.replace(r'\s', '', regex=True)

    df['cliente'] = df['cliente'].fillna(df['teste'])
    index_cliente = df[df['cliente'].str.contains('PE')].index
    df.loc[index_cliente, 'cliente'] = df.loc[index_cliente, 'cliente'].str.replace(r'[0-9]', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r' +$', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r'^ +|    ', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r'RECIFEPE', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r',.+', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r'ACRIPEL.+', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r'Emissao.+', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r'Usuario.+', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r'ROMANEIO.+', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r'Nome.+', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r'Total.+', '', regex=True)
    df['cliente'] = df['cliente'].str.replace(r'//', '', regex=True)

    df['vlr_liq_nota'] = df['vlr_liq_nota'].fillna(df['teste'])
    index_cliente = df[df['vlr_liq_nota'].str.contains('PE')].index
    df.loc[index_cliente, 'vlr_liq_nota'] = df.loc[index_cliente, 'vlr_liq_nota'].str.replace(r'[A-Z.]', '', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'{1,4}\d\d\d\d\d+ \d\d\d\d \d\d\d\d\d\d\d\d  \d\d/\d\d/\d\d '
                                                        r'{14}', '', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'^[0-9]{3,6} ', '', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'\b\d\b.+', '', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'^ {1,}', '', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'\s{1,}\d{2,}.+', '', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'ina.+', 'none', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'ssao.+', 'none', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'o:.+', 'none', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'ROMANEIO.+', 'none', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r'torista.+', 'none', regex=True)
    df['vlr_liq_nota'] = df['vlr_liq_nota'].str.replace(r',', '.', regex=True)

    df = df[df['vlr_liq_nota'].notna()]
    df = df[~df.vlr_liq_nota.str.contains("none")]
    df['vlr_liq_nota'].replace('', np.nan, inplace=True)
    df = df[df['vlr_liq_nota'].notna()]

    df['cliente'].replace('', np.nan, inplace=True)
    df = df[df['cliente'].notna()]

    df['pedido'] = df['pedido'].fillna(df['teste'])
    df['pedido'] = df['pedido'].str.replace(r'[0-9]{8}\s\d{4}\s', '', regex=True)
    df['pedido'] = df['pedido'].str.replace(r'\s\s.+', '', regex=True)
    df['pedido'] = df['pedido'].str.replace(r'\D\d.', '', regex=True)
    df['pedido'] = df['pedido'].str.replace(r'[A-Z]', '', regex=True)

    df['nt_fiscal'] = df['nt_fiscal'].fillna(df['teste'])
    index_cliente = df[df['nt_fiscal'].str.contains('RECIFEPE')].index
    df.loc[index_cliente, 'nt_fiscal'] = df.loc[index_cliente, 'nt_fiscal'].str.replace(r'[A-Z]', '', regex=True)
    df.loc[index_cliente, 'nt_fiscal'] = df.loc[index_cliente, 'nt_fiscal'].str.replace(r'\D\d\d\d\d\s\d{4}.+', '',
                                                                                        regex=True)
    df.loc[index_cliente, 'nt_fiscal'] = df.loc[index_cliente, 'nt_fiscal'].str.replace(r'^ ', '', regex=True)

    df['vlr_nota'] = df['vlr_nota'].fillna(df['teste'])
    df['vlr_nota'] = df['vlr_nota'].str.replace(r'[A-Z]', '', regex=True)
    df['vlr_nota'] = df['vlr_nota'].str.replace(r'.', '', regex=True)
    df['vlr_nota'] = df['vlr_nota'].str.replace(r'RECIFEPE', '')
    df['vlr_nota'] = df['vlr_nota'].str.replace(r'(\d\d\d\d\d+ \d\d\d\d \d\d\d\d\d\d\d\d  \d\d/\d\d/\d\d)', '',
                                                regex=True)
    df['vlr_nota'] = df['vlr_nota'].str.replace(r'[0-9]{3,6}\s', '', regex=True)
    df['vlr_nota'] = df['vlr_nota'].str.replace(r'\b\d\b.+', '', regex=True)
    df['vlr_nota'] = df['vlr_nota'].str.replace(r'\s', '', regex=True)
    df['vlr_nota'] = df['vlr_nota'].str.replace(r'^\b\d{2,}\b\b\D\d\d', '', regex=True)
    df['vlr_nota'] = df['vlr_nota'].str.replace(r',', '.', regex=True)
    df['vlr_nota'] = pd.to_numeric(df['vlr_nota'])

    df['vol'] = df['vol'].fillna(df['teste'])
    df['vol'] = df['vol'].str.replace(r'[A-Z]', '', regex=True)
    df['vol'] = df['vol'].str.replace(r'.', '', regex=True)
    df['vol'] = df['vol'].str.replace(r'RECIFEPE', '')
    df['vol'] = df['vol'].str.replace(r'(\d\d\d\d\d+ \d\d\d\d \d\d\d\d\d\d\d\d  \d\d/\d\d/\d\d)', '', regex=True)
    df['vol'] = df['vol'].str.replace(r'[0-9]{3,6}\s', '', regex=True)
    df['vol'] = df['vol'].str.replace(r'\b\d{1},.+', '', regex=True)
    df['vol'] = df['vol'].str.replace(r'\b\d{2,},[0-9]{2}', '', regex=True)
    df['vol'] = df['vol'].str.replace(r'\s', '', regex=True)

    del df['emissao']
    del df['teste']

    df['vlr_liq_nota'] = pd.to_numeric(df['vlr_liq_nota'])
    df['vol'] = pd.to_numeric(df['vol'])
    df.loc["Total", "vlr_liq_nota"] = df.vlr_liq_nota.sum()
    df.loc["Total", "vlr_nota"] = df.vlr_nota.sum()
    df.loc["Total", "vol"] = df.vol.sum()

    # subindo os dados dos romaneios pro banco de dados
    df.to_sql('teste_acripel', engine, if_exists='replace', index=False)
