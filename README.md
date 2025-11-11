En este proyecto trabajé con Apache Kafka para entender y aplicar el modelo de comunicación basada en eventos.
Desarrollé dos microservicios con Spring Boot: un Producer y un Consumer.
El Producer expone un endpoint REST; cada vez que recibo una petición HTTP (en este caso, un POST) se genera un mensaje y se publica en un tópico de Kafka.
Por otro lado, el Consumer está suscrito a ese mismo tópico y se encarga de procesar los mensajes que recibe.
Esto me permitió simular un flujo real de eventos, donde un servicio produce información y otro la consume de forma asíncrona, sin depender directamente del primero.
Toda la infraestructura la levanté con Docker Compose, incluyendo Kafka, Zookeeper (que en esta configuración se encarga de coordinar los brokers y manejar la metadata del clúster) y los dos microservicios.
También subí las imágenes a Docker Hub para poder desplegarlas fácilmente o compartir el proyecto.
La idea principal es demostrar cómo Kafka permite desacoplar los servicios y manejar eventos en tiempo real, algo muy común en arquitecturas distribuidas modernas, por ejemplo cuando un servicio de pagos genera un evento y otros servicios (como notificaciones o auditoría) reaccionan sin necesidad de una llamada directa.
En resumen, este proyecto me sirvió para comprender el flujo productor-consumidor, el manejo de tópicos y particiones en Kafka, el rol de Zookeeper dentro del clúster, y cómo orquestar todo el entorno con contenedores Docker.
