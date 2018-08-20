# Gradient-descent-Spark

Распределенная версия градиентного спуска в Apache Spark для линейной регрессии.

## Загрузка и визуализация данных

Для удобства исследования и оценки разработанного алгоритма, в качестве исходных данных были выбраны данные с одной переменной.

Источник данных: [Bike Sharing Dataset Data Set](https://archive.ics.uci.edu/ml/datasets/Bike+Sharing+Dataset)

На Python был написан скрипт для обработки и визуализации входной выборки:


```python
import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv("day.csv")
temps = data['atemp'].values
rentals = data['cnt'].values / 1000

plt.scatter(temps, rentals, marker='x', color='red')
plt.xlabel('Normalized Temperature in C')
plt.ylabel('Bike Rentals in 1000s')
plt.show()

f = open('sample.txt', 'w')

for t, r in zip(temps, rentals):
    f.write('%f %f\n' % (float(t), float(r)))
```

- atemp: Normalized feeling temperature in Celsius. The values are divided to 50 (max)
- cnt: count of total rental bikes including both casual and registered

Таким образом, формат данных файла sample.txt:
```       
0.363625 0.985000
0.353739 0.801000
0.189405 1.349000
0.212122 1.562000
0.229270 1.600000
0.233209 1.606000
0.208839 1.510000
0.162254 0.959000
0.116175 0.822000
```
Визуадизация данных:

![alt text](./data.jpg)

## Описание алгоритма

Рассмотрим алгоритм на примере линейной регрессии первого порядка:

![alt text](./formula_1.png)

Задача найти такие параметры тета, при которых функция ошибки (J) будет минимальна.

Обновление параметров происходит по следующим формулам:

![alt text](./formula_2.png)

Для первого порядка:

![alt text](./formula_3.png)







## Оценка результатов

## Комментарии

