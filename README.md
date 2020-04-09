# Рекомендательная система небольших текстовых документов на основе хэштегов  

Реализованный плагин для фреймворка KFrame.

### Пример использования

```python
from kframe import Parent
from kframe.plugins import SQL

from mindminer import conf, Miner

# Инициализация Kframe
p = Parent(name='Mind Miner')
p.add_plugin(target=SQL, autostart=True, kwargs=conf.SQL)

# Импорт плагина РС
p.add_module(key='conf', target=conf)
p.add_plugin(target=Miner, autostart=True, dependes=['sql', 'conf'])

# Запуск Kframe
p.init()
p.start()

# использование РС
print(
  'relevance = %.4f' % p.miner.relevante(
    user_tags=('kpop', 'bts', 'love'),
    post_tags=('jimin')
  )
)
# напишет относительную оценку релевантности
```
