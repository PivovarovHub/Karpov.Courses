## Описание задания

А/А-тестирование мобильного приложения. 
Необходимо посчитать результаты A/A-теста, проверяя метрику качества FPR (будем проверять на конверсии в покупку). Известно, что сплит-система сломана. Требуется проверить утверждение о поломке и найти ее причины.

 

## Описание колонок
- experimentVariant – вариант эксперимента
- version – версия приложения
- purchase – факт покупки
 

## Задача
* Запустите A/A-тест
* Посчитайте FPR на уровне альфа = 0.05 (ставьте подвыборки без возвращения объемом 1000).
* Найдите причины поломки сплит-системы, ориентируясь на результаты эксперимента
* Напишите выводы, которые можно сделать на основе анализа результатов A/A-теста
