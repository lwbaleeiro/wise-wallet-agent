# import os
# import requests
# import json
import ollama

import pandas as pd

# from dotenv import load_dotenv

# load_dotenv()

def generate_financial_insights(df):
    try:
        monthly_summary = df.groupby(pd.Grouper(key='data', freq='ME')).agg(
            {'valor': ['sum', 'mean', 'max', 'min']}).reset_index()
        
        prompt = f"""
            Analise estas transações bancárias e responda em portugês brasileiro:
            {monthly_summary.to_markdown()}

            Principais solicitações:
            1. Identifique os 3 maiores gastos do mês.
            2. Compare com o mês anterior (% de variação).
            3. Sugira categorias para otimização de custos.
            4. Destaque padrões incomuns."""
        
        response = ollama.chat(
            model="deepseek-r1:7b",
            messages=[{
                "role": "user", 
                "content": prompt
                }])
        
        if response.done:
            return response.message.content
        else:
            return f"Erro na API: {response}"

    except Exception as e:
        raise Exception(f"Error generating financial insights: {e}")