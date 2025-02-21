from csv import reader
from collections import defaultdict, Counter
from tqdm import tqdm
import time

NUMERO_DE_LINHAS = 100_000_000

def processar_temperaturas(caminho_do_csv):
    minimas = defaultdict(lambda: float('inf')) #Cria um dicionário que, se tentar acessar uma chave que ainda não existe, automaticamente atribui o valor "infinito" (ou seja, um número muito grande). Assim, qualquer valor lido será menor que esse valor inicial
    maximas = defaultdict(lambda: float('-inf')) # faz o mesmo só que inverso
    somas = defaultdict(float)
    medicoes = Counter() # É um tipo especial de dicionário que conta quantas vezes cada estação aparece (ou seja, quantas medições foram feitas por estação).

    with open(caminho_do_csv, 'r', encoding='utf-8') as file:
        _arquivo = reader(file,delimiter=';')
        
        # usando tqdm diretamente no iterador, isso mostrará a porcentagem de conclusão
        for linha in tqdm(_arquivo, total=NUMERO_DE_LINHAS, desc="Processando"):
            nome_da_estacao, temperatura = linha[0], float(linha[1])
            medicoes.update([nome_da_estacao]) # Conta quantas medições essa estação teve.
            minimas[nome_da_estacao] = min(minimas[nome_da_estacao], temperatura) # Compara a temperatura atual com a menor já registrada e guarda a menor.
            maximas[nome_da_estacao] = max(maximas[nome_da_estacao], temperatura)
            somas[nome_da_estacao] += temperatura # Acumula a soma das temperaturas para, depois, calcular a média.
        
    print("Dados carregados. Calculando estatísticas...")

    # Calculando os dados para cada estação
    results = {}
    for station, qtd_medicoes in medicoes.items():
        temp_media = somas[station] / qtd_medicoes
        results[station] = (minimas[station], temp_media, maximas[station])

    print ("Estatísticas calculada. Ordenando....")
    #Ordenando pelo nome da estacao
    sorted_results = dict(sorted(results.items()))

    # formatando os resultados para exibição
    resultado_formatado = {station: f"{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}"
                         for station, (min_temp, mean_temp, max_temp) in sorted_results.items()}

    return resultado_formatado


if __name__ == "__main__":
    
    caminho_do_csv = "./data/measurements.txt"

    print("Iniciando o processamento do arquivo.")
    start_time = time.time()  # Tempo de início

    resultados = processar_temperaturas(caminho_do_csv)

    end_time = time.time()  # Tempo de término

    #for station, metrics in resultados.items():
    #    print(station, metrics, sep=': ')

    print(f"\nProcessamento concluído em {end_time - start_time:.2f} segundos.")

    