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

## Теоретические сведения

Рассмотрим алгоритм на примере линейной регрессии первого порядка:

![alt text](./formula_1.png)

Задача найти такие параметры тета, при которых функция ошибки (J) будет минимальна.

Обновление параметров происходит по следующей формуле:

![alt text](./formula_2.png)

Таким образом, для первого порядка:

![alt text](./formula_3.png)

## Описание распределенного алгоритма

В том случае, когда m >> j (например m = 100.000.000), где j число параметров, производительность вычислений резко снижается.  
Для решения данной проблемы применяют Mini-batch gradient descent.

![alt text](./formula_5.png)

В данном случае исходная выборка разбивается на равные части и параллельно поступает на узлы машины (машин) где производится подсчет локального градиента. 

После того как закончился подсчет локальных градиентов, обновляем параметры (тета) по формуле:

![alt text](./formula_6.png)

Таким образом, алгоритм будет выглядить следующим образом:  
1. Инициализируем вектор параметров случайными значениями  
2. Пока количество итераций не привысит максимально допустимое или пока ошибка больше порогового значения  
    2.1. Для каждого элемента вектора параметров  
        2.1.1. Отправляем исходные данные на Worker'ы и считаем локальный градиент  
        2.1.2. Суммируем локальные градиенты и обновляем вектор параметров  
        2.1.3. Рассчитываем ошибку и сравниваем с предыдущим значением. Если ошибка больше предыдущего значения идем к пункту 4, иначе к пункту 2.1.  
3. Возвращаем найденный вектор параметров  
4. Конец алгоритма  

## Оценка результатов

## Комментарии

