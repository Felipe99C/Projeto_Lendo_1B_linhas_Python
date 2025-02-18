import os
import sys
import random
import time
import csv


def construir_lista_nome_estaca_climatica():
# Pega os nomes das estacoes no arquivo weather_station.csv e explode com os novos dados

    nome_estacao = set()

    with open('./data/weather_stations.csv', 'r', encoding='utf-8') as arquivo:
        conteudo_arquivo = csv.reader(arquivo, delimiter=';') #mais otimizado para leitura de csv
    
        for linha in conteudo_arquivo:
            if linha and not linha[0].startswith("#"): #ignora linhas vazias e comentarios
                nome_estacao.add(linha[0]) # usa set diretamente para evitar duplicadas
    return list(nome_estacao)


def main():
    num_rows_to_create = 100000000
    weather_station_names = []
    weather_station_names = construir_lista_nome_estaca_climatica()
    #print(estimate_file_size(weather_station_names, num_rows_to_create))
    #build_test_data(weather_station_names, num_rows_to_create)
    print("Arquivo de teste finalizado.")


if __name__ == "__main__":
    main()
exit()