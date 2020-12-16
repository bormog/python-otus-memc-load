# Memcache Loader
## Что делает
Скрипт парсит файлы из заданной папки и заливает данные в memcache.

- Файлы читаются в несколько потоков через lines_producer
- line_producer складывает данные из файла построчно в очередь чанками заданного размера
- Далее lines_worker в несколько процессов преобразовывают строки в protobuff и складывают в другую очередь чанками заданного размера
- memcache_consumer в несколько потоков вычитывает чанки из своей очереди и складывает в memcache 

## Как запускать
```
python main.py
```

### Опции
- l - logfile, default = None,
- dry - тестовый прогон, default = False
- pattern - регулярка для поиска файлов, default = data/appsinstalled/*.tsv.gz

- idfa, default = 127.0.0.1:33013
- idfv, default = 127.0.0.1:33014
- adid, default = 127.0.0.1:33015
- gaid, default = 127.0.0.1:33016

- producers_count, default = 4
- workers_count, default = cpu_count()
- consumers_count, default = 2
- producer_buff_size, default = 3000
- worker_buff_size, default = 2000

### Тесты
```
python -m unittest tests -v
```