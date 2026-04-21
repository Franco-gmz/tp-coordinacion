# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

## Resolución

Se implementó la sincronización de las instancias utilizando el middleware diseñado en el trabajo previo basado en RabbitMQ. A continuación se detallan los detalles de implementación asociados a las instancias de `sum`, `agg` y `join`.

### Consumo y Confirmación Manual

Se utilizó `auto_ack=False` en todos los consumidores para garantizar que los mensajes solo sean eliminados de RabbitMQ una vez procesados correctamente.

El callback interno expone dos funciones auxiliares:

* `ack()`: confirma el procesamiento correcto del mensaje.
* `nack()`: rechaza el mensaje y lo reencola.

Esto permite que, si un worker falla o se apaga antes de terminar el procesamiento, RabbitMQ vuelva a entregar el mensaje a otro consumidor.

### Shutdown Seguro

Se implementó una lógica de apagado seguro para evitar pérdida de mensajes.

El comportamiento esperado es:

1. Detener el loop de consumo usando `stop_consuming()`.
2. Permitir finalizar el callback actualmente en ejecución.
3. Hacer `ack` si el mensaje terminó correctamente.
4. Hacer `nack(requeue=True)` si el mensaje no pudo completarse.
5. Flushear datos pendientes en memoria.
6. Notificar shutdown a los workers downstream.
7. Cerrar canales y conexiones.

Gracias a `prefetch_count=1`, cada worker puede tener como máximo un único mensaje pendiente de confirmación.

Si un worker se apaga sin confirmar ese mensaje, RabbitMQ lo reencola automáticamente en la misma cola para que otro worker pueda procesarlo.

### Coordinación

Además del middleware, se implementó la lógica de coordinación entre workers para manejar mensajes de control, EOFs y shutdowns.

Entre las principales resoluciones se incluyen:

* Tracking de EOF recibidos por cliente.
* Espera de EOF de todos los workers antes de emitir el resultado final.
* Cálculo y envío del top final por cliente.
* Liberación de memoria eliminando estructuras internas una vez finalizado un cliente.
* Manejo de shutdowns de workers de sum y agregación.
* Reducción de la cantidad de workers activos restantes en caso de una caída.

### Sum Filter

El componente Sum fue implementado para recibir datos parciales, agruparlos por cliente y distribuirlos hacia los workers de agregación.

Entre las principales responsabilidades implementadas se encuentran:

* Consumo de mensajes desde la cola de entrada.
* Parseo y deserialización de mensajes internos.
* Distribución determinística de datos hacia los workers de agregación utilizando un hash basado en cliente y fruta.
* Envío de datos parciales a exchanges de agregación.
* Manejo de EOF por cliente.
* Broadcast de EOF a los workers correspondientes.
* Tracking de clientes cerrados para evitar reprocesamiento.
* Recepción y propagación de mensajes de shutdown.
* Flush de datos pendientes antes del cierre definitivo.

También se implementó coordinación entre Sum y Aggregation utilizando mensajes de control para notificar shutdowns y permitir que los workers de agregación ajusten la cantidad de sum workers activos esperados.

### Aggregation Filter

El componente Aggregation fue implementado para recibir datos parciales provenientes de los workers Sum y acumular resultados intermedios por cliente.

Las principales resoluciones implementadas fueron:

* Recepción de mensajes de tipo `PROCESS_DATA`.
* Acumulación de cantidades por fruta y cliente.
* Mantenimiento de estructuras internas por cliente.
* Recepción de EOF por cliente desde múltiples workers Sum.
* Conteo de EOF recibidos para determinar cuándo un cliente terminó completamente.
* Cálculo del top parcial de frutas por cliente.
* Envío de resultados parciales hacia Join.
* Envío de EOF hacia Join una vez finalizado un cliente.
* Manejo de mensajes de shutdown.
* Limpieza de memoria de clientes finalizados.

Además, se implementó la lógica necesaria para esperar EOF de todos los workers Sum antes de considerar finalizado un cliente.

### Join

El componente Join fue implementado para consolidar los resultados parciales provenientes de los workers de agregación y emitir el resultado final por cliente.

Entre las principales funcionalidades implementadas se encuentran:

* Recepción de resultados parciales por cliente.
* Acumulación de cantidades por fruta provenientes de múltiples aggregators.
* Mantenimiento de estructuras internas por cliente.
* Recepción de EOF desde cada worker de agregación.
* Tracking de EOF recibidos por cliente.
* Espera hasta recibir EOF de todos los aggregators antes de finalizar un cliente.
* Ordenamiento final de frutas por cantidad.
* Obtención del top final limitado por `TOP_SIZE`.
* Envío del resultado final a la cola de salida.
* Liberación de memoria eliminando los datos del cliente finalizado.
* Manejo de shutdown de aggregators y reducción de workers activos.

### Resultado

La implementación final permite:

* Procesar mensajes de forma confiable.
* Evitar pérdida de mensajes ante errores o caídas.
* Redistribuir mensajes no confirmados.
* Cerrar conexiones correctamente.
* Coordinar workers y resultados finales.
* Manejar EOFs y shutdowns de forma consistente.
* Sincronizar Sum, Aggregation y Join correctamente.
* Cumplir con todas las pruebas provistas por la cátedra.
