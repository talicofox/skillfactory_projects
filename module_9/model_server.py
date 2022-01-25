# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 21:01:56 2022

UNIT 9 Модуль 6

18. Итоговое задание

- Обучить модель по примеру и сериализовать её.
- Десериализовать модель в коде сервера, загружая её только один раз при старте (это важно, иначе предсказание будет слишком долгим).
Написать функцию, которая будет принимать запрос с числом, отправлять это число в модель и выводить результат на экран.

Проверить работу сервера: http://localhost:5000/model?value=0.56

@author: Nata Lisitskaya
"""

from pathlib import Path
from sklearn.linear_model import LinearRegression
from sklearn.datasets import load_diabetes
import numpy as np
import pickle

from flask import Flask, request

name_file = 'my_model.pkl'
   
app = Flask(__name__)

def model_train():
    '''
    Тренировка модели и ее сериализация для дальнейшего использования

    Returns
    -------
    None.

    '''
    X, y = load_diabetes(return_X_y=True)
    X = X[:, 0].reshape(-1, 1) # Берём только один признак
    regressor = LinearRegression()
    regressor.fit(X,y)
    
    with open(name_file, 'wb') as output:
        pickle.dump(regressor, output) #Сохраняем
    
    return


def model_predict(value):
    '''
    
    Parameters
    ----------
    value : float
        значения для предсказания

    Returns
    -------
    TYPE
        float.
    Полученные значения

    '''
    value_to_predict = np.array([value]).reshape(-1, 1)
    
    return regressor_from_file.predict(value_to_predict)
     

@app.route('/model')
def main():
    
    value = request.args.get('value')
    
    '''
    Обработка ошибки при вводе:
    описание ошибки и ошибочное значение выводятся в окно результата    
    
    '''
    try:
        value = float(value)
        prediction = model_predict(float(value)) #Приводим к типу int
        
    except Exception as e:
        print(e, value)    
        prediction = e    
    
    
    return f'the result is {prediction}!'


if __name__ == '__main__':
    
    '''
    Проверка: существует ли файл с моделью.
    если файла модели не существует, запускается тренировка модели и сериализация
    '''
    if not Path(name_file).is_file():
        model_train()
    
    #открывается файл с моделью    
    with open(name_file, 'rb') as pkl_file:
        regressor_from_file = pickle.load(pkl_file) #Загружаем
        
    app.run('localhost', 5000)