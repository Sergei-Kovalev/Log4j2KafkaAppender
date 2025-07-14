# 🚀 Аппендер для log4j2, который будет отправлять логи в топик Kafka.

# Деплой и запуск
- собрать в локальный репозиторий maven -install
- добавить в виде зависимости в свой проект:
~~~
<dependency>
    <groupId>com.yourcompany.log4j2</groupId>
    <artifactId>log4j2-kafka-appender</artifactId>
    <version>0.0.2</version>
</dependency>
~~~
# [Пример конфигурации:](src/main/resources/log4j2-example.xml)
