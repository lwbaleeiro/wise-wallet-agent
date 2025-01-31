import re
import pandas as pd

from datetime import datetime

# Mapeamento de categorias aprimorado
CATEGORY_RULES = {
    'ALIMENTAÇÃO': {
        'keywords': ['OK SORVETES', 'SUBWAY', 'IFOOD', 'RESTAURANTE', 'PIZZARIA'],
        'exceptions': []
    },
    'TRANSPORTE': {
        'keywords': ['UBER', 'TAXI', 'POSTO DE GASOLINA', 'PEDAGIO'],
        'exceptions': []
    },
    'CONVENIÊNCIA': {
        'keywords': ['ESTACAO CONVENIENCIA', 'MERCADINHO', 'DROGARIA'],
        'exceptions': []
    },
    'INVESTIMENTOS': {
        'keywords': ['APLICAÇÃO RDB', 'CDB', 'TESOURO DIRETO'],
        'exceptions': []
    },
    'RECEITAS': {
        'keywords': ['TRANSFERÊNCIA RECEBIDA', 'DEPÓSITO', 'SALÁRIO'],
        'exceptions': ['-']  # Só considera se não for débito
    },
    'PAGAMENTOS': {
        'keywords': ['PAGAMENTO DE FATURA', 'BOLETO'],
        'exceptions': []
    }
}

def parse_nubank_csv(file_path):
    try:
        df = pd.read_csv(
            file_path,
            encoding='utf-8',
            header=0,
            sep=',',
            thousands='.',
            decimal=',',
            skip_blank_lines=True,
            na_filter=True,    
            converters={
                'Data': lambda x: datetime.strptime(x, '%d/%m/%Y'),
            }
            #'Valor': lambda x: float(x.replace('.', '').replace(',', '.'))            
        )

        df = df.dropna(how='all')

    except Exception as e:
        raise Exception(f"Error parsing Nubank CSV: {e}")
    
    df = df.rename(columns={
        'Data': 'data', 
        'Valor': 'valor', 
        'Identificador': 'id_transacao', 
        'Descrição': 'descricao'})
    
    df['categoria'] = df.apply(categorizar_transacao, axis=1)

    df = df[['data', 'valor', 'categoria', 'descricao', 'id_transacao']].drop_duplicates('id_transacao')

    if df.empty:
        raise ValueError("Nenhuma transação valida encontrada após o processamento")
    
    return df.sort_values('data', ascending=False)
    
def categorizar_transacao(row):
    desc = row['descricao'].upper()
    valor = row['valor']

    # 1. Verificar pagamentos primeiro
    for keyword in CATEGORY_RULES['PAGAMENTOS']['keywords']:
        if keyword in desc:
            return 'PAGAMENTOS'
    
    # 2. Verificar receitas (valores positivos)
    if valor > 0:
        for keyword in CATEGORY_RULES['RECEITAS']['keywords']:
            if keyword in desc:
                return 'RECEITAS'
        return 'RECEITAS NÃO CATEGORIZADAS'
    
    # 3. Verificar outras categorias para débitos
    for category, rules in CATEGORY_RULES.items():
        if category in ['PAGAMENTOS', 'RECEITAS']:
            continue  # Já verificados
            
        for keyword in rules['keywords']:
            if keyword in desc and not any(exc in desc for exc in rules['exceptions']):
                return category
    
    # 4. Categoria padrão para débitos não identificados
    return 'OUTROS GASTOS' if valor < 0 else 'RECEITAS NÃO CATEGORIZADAS'

def validate_transactions(df):

    # Verificar linhas com valores ausentes
    if df.isnull().values.any():
        raise ValueError("Dados corrompidos: valores ausentes detectados")

    if df.empty:
        raise ValueError("DataFrame vazio - nenhuma transação processada")
    
    required_columns = ['data', 'valor', 'descricao', 'id_transacao']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Colunas faltando no DataFrame: {missing}")
    
    max_date = datetime.now() + pd.DateOffset(days=1)
    if (df['data'] > max_date).any():
        raise ValueError("Data futura encontrada nas transações")
    
    return True