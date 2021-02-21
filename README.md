# proyecto_spark

## En este proyecto recogemos datos de uso de datos móviles de nuestros clientes a través de un simulador:

docker run -it -e KAFKA_SERVERS=${INSTANCE_PUBLIC_IP}:9092 andresgomezfrr/data-simulator:1.1

## simulador corre a través de Kafka junto Zookeeper para coordinar procesos.

# Streaming & Batch

##ecogemos los datos y configuramos de manera que agrupe los datos en nuestro Servidor PostgreSQL de esta manera:

Total de bytes recibidos por antena.
Total de bytes transmitidos por mail de usuario.
Total de bytes transmitidos por aplicación.
Email de usuarios que han sobrepasado la cuota por hora. En concepto de Fraude vs No Fraude

##Para ello necesitamos aparte de agrupar hacer un join con una tabla de metadata de nuestros clientes.

##Recoge cada 5 minutosos agrupados para así obtener métricas históricas, útil par ala visualización de la evolución.

# ServingLayer

##A través de la aplicación nativa de visualización de apache. Logramos ver la evolución mencionada anteriormente facilitáandonos la toma de decisiones y su anterior investigación.

