# -*- coding: utf-8 -'
"""
Created on Sat Jan 30 22:11:39 2021

@author: Natalya Lisitskaya

"""
import numpy as np

def game_core_v3(number):
    
   ''' Для угадывания используем метод дихотомии
       x1, x2 - концы отрезка на котором расположено задуманное число
       Функция принимает загаданное число и возвращает число попыток
   '''
       
   count = 1
   x1, x2 = 1, 101
   predict = (x2-x1) // 2
   while number != predict:
       count+=1
       if number > predict: 
           x1 = predict
       
       elif number < predict: 
           x2 = predict
          
       predict = x1 + (x2-x1) // 2 
   return(count) # выход из цикла, если угадали


def score_game(game_core):
    '''Запускаем игру 1000 раз, чтобы узнать, как быстро игра угадывает число'''
    count_ls = []
    i = 1
    np.random.seed(1)  # фиксируем RANDOM SEED, чтобы ваш эксперимент был воспроизводим!
    random_array = np.random.randint(1,101, size=(1000))
    for number in random_array:
        count_ls.append(game_core(number))
        i=i+1
    score = int(np.mean(count_ls))
    print(f"Ваш алгоритм угадывает число в среднем за {score} попыток")
    return(score)

if __name__ == '__main__':
    score_game(game_core_v3)