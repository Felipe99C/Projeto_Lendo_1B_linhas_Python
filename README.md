# Um Bilhão de Linhas: Desafio de Processamento de Dados com Python

Esse projeto serve como base de um estudo e aprendizagem de Python, utilizando esse desafio para não só estudar como o código funciona mas reescreve-lo buscando possiveis melhorias com o que já aprendi até o momento com Python.

Esse projeto foi apresentado a mim através da do workshop da [Jornada de dados](https://suajornadadedados.com.br/). 


## Introdução

O objetivo deste projeto é demonstrar como processar eficientemente um arquivo de dados massivo contendo 1 bilhão de linhas (~14GB), especificamente para calcular estatísticas (Incluindo agregação e ordenação que são operações pesadas) utilizando Python. 

Este desafio foi inspirado no [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc), originalmente proposto para Java.

O arquivo de dados consiste em medições de temperatura de várias estações meteorológicas. Cada registro segue o formato `<string: nome da estação>;<double: medição>`, com a temperatura sendo apresentada com precisão de uma casa decimal.

Aqui estão dez linhas de exemplo do arquivo:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. Johns;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

O desafio é desenvolver um programa Python capaz de ler esse arquivo e calcular a temperatura mínima, média (arredondada para uma casa decimal) e máxima para cada estação, exibindo os resultados em uma tabela ordenada por nome da estação.

| station      | min_temperature | mean_temperature | max_temperature |
|--------------|-----------------|------------------|-----------------|
| Abha         | -31.1           | 18.0             | 66.5            |
| Abidjan      | -25.9           | 26.0             | 74.6            |
| Abéché       | -19.8           | 29.4             | 79.9            |
| Accra        | -24.8           | 26.4             | 76.3            |
| Addis Ababa  | -31.8           | 16.0             | 63.9            |
| Adelaide     | -31.8           | 17.3             | 71.5            |
| Aden         | -19.6           | 29.1             | 78.3            |
| Ahvaz        | -24.0           | 25.4             | 72.6            |
| Albuquerque  | -35.0           | 14.0             | 61.9            |
| Alexandra    | -40.1           | 11.0             | 67.9            |
| ...          | ...             | ...              | ...             |
| Yangon       | -23.6           | 27.5             | 77.3            |
| Yaoundé      | -26.2           | 23.8             | 73.4            |
| Yellowknife  | -53.4           | -4.3             | 46.7            |
| Yerevan      | -38.6           | 12.4             | 62.8            |
| Yinchuan     | -45.2           | 9.0              | 56.9            |
| Zagreb       | -39.2           | 10.7             | 58.1            |
| Zanzibar City| -26.5           | 26.0             | 75.2            |
| Zürich       | -42.0           | 9.3              | 63.6            |
| Ürümqi       | -42.1           | 7.4              | 56.7            |
| İzmir        | -34.4           | 17.9             | 67.9            |

## Dependências

Para executar os scripts deste projeto, você precisará das seguintes bibliotecas:

* Polars: `0.20.3`
* DuckDB: `0.10.0`
* Dask[complete]: `^2024.2.0`

## Resultados

Os testes foram realizados em um laptop equipado com um processador M1 da Apple e 8GB de RAM. As implementações utilizaram abordagens puramente Python, Pandas, Dask, Polars e DuckDB. Os resultados de tempo de execução para processar o arquivo de 1 bilhão de linhas são apresentados abaixo:

| Implementação | Tempo |
| --- | --- |
| Bash + awk | 25 minutos |
| Python | 20 minutos |
| Python + Pandas | 263 sec |
| Python + Dask | 155.62 sec  |
| Python + Polars | 33.86 sec |
| Python + Duckdb | 14.98 sec |

Obrigado por [Koen Vossen](https://github.com/koenvo) pela implementação em Polars e [Arthur Julião](https://github.com/ArthurJ) pela implementação em Python e Bash 