import pandas as pd
import random

# Função para filtrar e selecionar 250 mil linhas do arquivo do dataset
def filter_and_select(input, output, linhas=250000):
    # Carrega o dataset em CSV
    df = pd.read_csv(input)
    
    # Filtra apenas as linhas do terceiro trimestre
    df_filtrado = df[df["Quarter"] == 3]
    
    # Seleciona aleatoriamente 250 mil linhas
    df_final = df_filtrado.sample(n=linhas, random_state=42)
    
    # Exporta o resultado para um novo arquivo CSV
    df_final.to_csv(output, index=False)
    print('Finalizado')

# Chama a função com os arquivos de entrada e saída
filter_and_select('Cleaned_2018_Flights.csv', 'voos_2018_q3_250k.csv')