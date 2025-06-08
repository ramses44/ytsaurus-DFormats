# Описание проекта

Проект направлен на повышение эффективности работы с табличными данными в
рамках MapReduce-операций в YTsaurus путём использования различных форматов данных
и разработки универсального C++ API для работы с ними. YTsaurus – это распределённая платформа для хранения и обработки данных, в том числе посредством MapReduceопераций. Однако, из-за распределённой модели вычислений, эффективность работы с данными в таких операциях может сильно зависеть от формата сериализации, используемого
для передачи данных между узлами системы. Хотя сама по себе платформа YTsaurus поддерживает работу с табличными данными в нескольких форматах, в текущей версии C++
библиотеки YTsaurus отсутствует API для работы с большинством из них. В рамках данной
работы исследуется возможность использования альтернативных форматов для оптимизации MapReduce-операций, использующих C++ API YTsaurus. Предлагаемая проектом C++
библиотека предоставляет единый интегрированный с MapReduce API интерфейс для работы с табличными данными в форматах YSON, Skiff, Protocol Buffers и Apache Arrow. Особое
внимание уделяется возможности работы с динамически схематизированными таблицами,
так как в таких сценариях вопрос повышения производительности особенно актуален ввиду
невозможности использования предварительно скомпилированных схематизированных представлений. Также, в рамках работы проводится сравнительный анализ эффективности использования рассматриваемых форматов в различных сценариях. В ходе бенчмаркинга были
выявлены характерные сценарии, в которых использование конкретных форматов позволяет
добиться кратного увеличения производительности. На основании результатов тестирования
был составлен набор рекомендаций для разработчиков по выбору наиболее эффективного
формата в зависимости от специфики задачи.

The project aims to solve the problem of efficiently working with tabular data in MapReduce
operations in YTsaurus by using various data formats and developing a universal C++ API for
working with them. YTsaurus is a distributed platform for storing and processing data, including
through MapReduce operations. However, due to the distributed computing model, the efficiency
of working with data in such operations may strongly depend on the serialization format for data
transfer between system nodes. Despite the fact that the YTsaurus platform itself supports working
with tabular data in several formats, the current version of the YTsaurus C++ library lacks an
API for working with most of them. Thus, within the framework of this work, the possibility of
utilizing alternative formats to optimize MapReduce operations using the YTsaurus C++ API is
being investigated. The C++ library provided by the project offers a unified interface integrated
with the MapReduce API, allowing users to work with various data formats such as YSON,
Skiff, Protocol Buffers, and Apache Arrow. Special attention has been paid to the ability to work
with dynamically schematized tables, as this is an area where performance can be significantly
affected due to the inability to use precompiled schematized representations. As part of this work,
a comparative analysis of the effectiveness of each format in different scenarios. Benchmarking
identified specific cases where the use of certain formats resulted in a significant performance
boost. Based on these findings, a set of recommendations was developed to assist developers in
choosing the most appropriate format for their specific needs. The proposed software solution has
been successfully integrated into the company’s workflow, where it has proven its effectiveness.

# Рекомендации по выбору формата

![diagram](benchmarks/results/benchmarks_diagram.png)

Подробно результаты тестирования форматов описаны [тут](benchmarks/results/description.md).

Краткие выводы об эффективности выбора форматов:

• **Skiff** наиболее эффективен при работе со сложными типами данных в колонках или при активной записи
(и практически во всех сценариях с динамической схематизацией);

• **Статический Protobuf** следует использовать, когда схема таблицы известна на этапе компиляции, а в операциях
производится интенсивная работа с данными в строках;

• **Динамический Protobuf** имеет смысл использовать только на более старых версиях YT, где
его производительность была сопоставима со статической версией;

• **YSON** может быть выбран в сценариях с низкими требованиями к скорости чтения и записи данных, а также их
эффективности представления в программе;

• **Apache Arrow** подходит для данных с низкой уникальностью, где может эффективно применяться
словарное сжатие;