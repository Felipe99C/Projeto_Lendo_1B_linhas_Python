#!/usr/bin/env python3
import argparse
import csv
import logging
from tqdm import tqdm
import random
import time
from pathlib import Path

# Configuração do logging para facilitar o debug e monitoramento da execução
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_arguments() -> int:
    """
    Utiliza o módulo argparse para tratar os argumentos de linha de comando.
    Permite mensagens de ajuda e tratamento automático de erros.
    """
    parser = argparse.ArgumentParser(
        description="Gera um arquivo de medições a partir de nomes de estações meteorológicas."
    )
    parser.add_argument(
        "num_records",
        type=str,
        help="Número de registros a serem criados (use underscore para separar milhares, ex: 1_000_000)",
    )
    args = parser.parse_args()

    try:
        # Remove underscores para permitir notação
        num = int(args.num_records.replace("_", ""))
        if num <= 0:
            raise ValueError("O número deve ser um inteiro positivo.")
    except ValueError as e:
        parser.error(f"Argumento inválido: {e}")
    return num


def build_weather_station_name_list() -> list[str]:
    """
    Lê o arquivo CSV de estações meteorológicas e retorna uma lista com nomes únicos.
    Utiliza o módulo csv para uma leitura mais robusta e ignora linhas de comentário.
    """
    station_names = set()

    file_path = Path("D:\\Lendo 1B linhas\\Lendo_1B_linhas\\data\\weather_stations.csv")

    if not file_path.exists():
        logging.error(f"Arquivo {file_path} não encontrado.")
        exit(1)

    with file_path.open("r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=";")
        for row in reader:
            if not row or row[0].startswith("#"):
                continue  # Pula linhas vazias e comentários
            station_names.add(row[0])
    return list(station_names)

def convert_bytes(num: float) -> str:
    """
    Converte bytes para um formato legível (bytes, KiB, MiB, GiB).
    """
    for x in ["bytes", "KiB", "MiB", "GiB"]:
        if num < 1024.0:
            return f"{num:3.1f} {x}"
        num /= 1024.0
    return f"{num:3.1f} GiB"

def format_elapsed_time(seconds: float) -> str:
    """
    Formata o tempo decorrido em um formato legível.
    """
    if seconds < 60:
        return f"{seconds:.3f} segundos"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutos {int(seconds)} segundos"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)} horas {int(minutes)} minutos {int(seconds)} segundos"

def estimate_file_size(weather_station_names: list[str], num_rows_to_create: int) -> str:
    """
    Estima o tamanho do arquivo com base no comprimento médio dos nomes das estações
    e no tamanho fixo da parte numérica da medição.

    Acho meio inutil, mas vale a ideia de como fazer.

    """
    if not weather_station_names:
        return "Não foi possível estimar o tamanho: lista de estações vazia."

    # Calcula o comprimento médio dos nomes das estações
    total_length = sum(len(name) for name in weather_station_names)
    avg_length = total_length / len(weather_station_names)
    numeric_part = len(",-123.4")  # Tamanho fixo da medição
    record_size = avg_length + numeric_part + 1  # +1 para a quebra de linha

    total_file_size = num_rows_to_create * record_size
    human_file_size = convert_bytes(total_file_size)
    return f"Tamanho estimado do arquivo: {human_file_size}. (Nota: o tamanho final pode ser menor)"


def build_test_data(weather_station_names: list[str], num_rows_to_create: int) -> None:
    """
    Gera e escreve os dados de teste em batches, utilizando o Path para manipulação de arquivos.
    Também trata a escrita dos registros que não completam um lote inteiro.
    """
    start_time = time.time()
    coldest_temp = -99.9
    hottest_temp = 99.9
    batch_size = 10_000

    # Garante que o diretório './data' exista
    data_dir = Path("./data")
    data_dir.mkdir(exist_ok=True)

    output_file = data_dir / "measurements.txt"
    logging.info("Iniciando a criação do arquivo. Esse processo pode demorar...\n")

    try:
        with output_file.open("w", encoding="utf-8") as file:
            full_batches = num_rows_to_create // batch_size
            leftover = num_rows_to_create % batch_size

            for batch_index in tqdm(range(full_batches), desc="Processando batches", total=full_batches):
                batch = random.choices(weather_station_names, k=batch_size)
                lines = "\n".join(
                    [f"{station};{random.uniform(coldest_temp, hottest_temp):.1f}" for station in batch]
                )
                file.write(lines + "\n")

            # Processa os registros restantes, se houver
            if leftover:
                batch = random.choices(weather_station_names, k=leftover)
                lines = "\n".join(
                    [f"{station};{random.uniform(coldest_temp, hottest_temp):.1f}" for station in batch]
                )
                file.write(lines + "\n")
    except Exception as e:
        logging.error("Erro ao escrever o arquivo", exc_info=True)
        exit(1)

        elapsed_time = time.time() - start_time
        file_size = output_file.stat().st_size
        human_file_size = convert_bytes(file_size)
        logging.info(f"\n\nArquivo escrito com sucesso em {output_file}")
        logging.info(f"Tamanho final: {human_file_size}")
        logging.info(f"Tempo decorrido: {format_elapsed_time(elapsed_time)}\n")

def main() -> None:
    print("Inciando..... \n")

    num_rows_to_create =  100_000_000 # parse_arguments()  --> qtd de linhas que deseja criar.

    station_names = build_weather_station_name_list()
    print(estimate_file_size(station_names, num_rows_to_create))
    build_test_data(station_names, num_rows_to_create)
    print("Arquivo de teste finalizado.\n")

if __name__ == "__main__":
    main()
