## Создать DAG для ежедневного вывода информации о продажах игр в мире.

### Задача:

У нас есть файл с информацией о продажах игр в мире.
Нужно составить DAG из нескольких тасок, в результате которого нужно будет найти ответы на следующие вопросы:

- Какая игра была самой продаваемой в этом году во всем мире?
- Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
- На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
  Перечислить все, если их несколько
- У какого издателя самые высокие средние продажи в Японии? 
  Перечислить все, если их несколько
- Сколько игр продались лучше в Европе, чем в Японии?

Оформлять DAG можно как угодно, важно чтобы финальный таск писал в лог ответ на каждый вопрос.
Ожидается, что в DAG будет 7 тасков, по одному на каждый вопрос, таск с загрузкой данных и финальный таск который собирает все ответы. 