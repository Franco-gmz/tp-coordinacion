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

El **Sum Filter** es un componente intermedio dentro del pipeline distribuido cuya responsabilidad principal es **recibir datos parciales por cliente, agregarlos localmente y distribuirlos hacia los Aggregators (agg)** correspondientes.

---

## Responsabilidad General

- Consumir mensajes desde una cola de entrada.
- Agrupar cantidades por **cliente y tipo de fruta**.
- Determinar dinámicamente a qué **Aggregator (agg)** enviar cada resultado.
- Manejar eventos de finalización por cliente (**EOF**).
- Coordinar el cierre del sistema de forma ordenada.

---

## Flujo General

1. **Recepción de mensajes**
   - Se reciben mensajes desde RabbitMQ.
   - Se identifican por tipo (`PROCESS_DATA`, `CLIENT_EOF`, etc.).

2. **Procesamiento**
   - Se acumulan cantidades por cliente y fruta.
   - Se mantiene estado interno de agregación.

3. **Distribución**
   - Cuando corresponde (por ejemplo al recibir EOF), se envían los datos agregados a los `agg`.

4. **Cierre**
   - Se notifican eventos de finalización a otros nodos.
   - Se liberan recursos.

---

## Estructuras Clave

- `amount_by_client`  
  Diccionario que agrupa resultados:

  ```python
  {
    client_id: {
        fruit: FruitItem(amount)
    }
  }
  ```
  
---

### Métodos Principales

#### `process_data_messsage(self, message, ack, nack)` / `process_control_message(self, message, ack, nack)`

Puntos de entrada del worker.

- Deserializan los mensajes recibidos.
- Determinan su tipo.
- Delegan el procesamiento a los métodos correspondientes.

---

#### `_process_data(self, client_id, fruit, amount)`

Procesa mensajes de datos.

- Acumula cantidades por cliente y fruta.

---

#### `_dispatch_aggregated_data(self, client_id)`

Envía los datos agregados a los aggregators.

- Determina el `agg` destino mediante `_get_agg_id`.
- Serializa y envía los datos.

---

#### `_get_agg_id(self, client_id, fruit)`

Determina a qué aggregator enviar los datos.

---

#### `_notify_shutdown(self)`

Notifica a otros nodos que este worker se apagará.

---

## Distribución hacia Aggregators

El `Sum Filter` no envía todos los datos a todos los aggregators.

En cambio:
- Divide el universo de datos.
- Cada `(cliente, fruta)` es asignado a un `agg` específico.
- Esto permite paralelismo y escalabilidad.

La asignación se realiza típicamente mediante:

```python
hash(client_id + fruit) % AGGREGATION_AMOUNT
```

---

## Limitaciones Conocidas

> **Si un aggregator se cae, el sistema NO redistribuye correctamente la carga.**

### Problema

- Se detecta el fallo (por excepción al enviar).
- Se elimina el output del `agg`.
- Pero:
  - No se decrementa correctamente la cantidad de aggregators activos.
  - No se recalcula el routing (`_get_agg_id`).
  - No se reenvían los datos pendientes a otros aggregators.

### Impacto

- Posible pérdida de datos.
- Distribución inconsistente.
- Resultados incompletos.

### Estado

- Problema identificado.
- No implementado por falta de tiempo.

### Mejora Pendiente

- Recalcular dinámicamente el conjunto de aggregators activos.
- Rebalancear el hash/routing.
- Reintentar envíos fallidos.

---

## Conclusión

El **Sum Filter** actúa como una capa de **pre-agregación y distribución inteligente**, reduciendo volumen de datos y permitiendo paralelismo en los aggregators.

Si bien cumple correctamente su función en condiciones normales, **la tolerancia a fallos de aggregators es una mejora clave pendiente**.

---

### Aggregation (Agg)

El componente **Aggregation (agg)** es responsable de recibir los datos parciales provenientes de los `sum`, realizar la **agregación final** y emitir los resultados consolidados por cliente y fruta.

---

## Responsabilidad General

- Recibir datos agregados parciales desde los `sum`.
- Consolidar resultados finales por **cliente y fruta**.
- Detectar cuándo un cliente está completamente procesado.
- Emitir el resultado final downstream (por ejemplo hacia `join`).
- Detectar la finalización de clientes a partir de señales (`CLIENT_EOF`) recibidas desde los workers `sum`.
- Sincronizar la finalización por cliente en base a los EOF recibidos.
- Ajustar su lógica interna ante la caída de workers `sum` (por ejemplo, mediante `SUM_WORKER_SHUTDOWN`).

---

## Flujo General

1. **Recepción de datos**
   - Recibe mensajes `PROCESS_DATA` desde múltiples `sum`.

2. **Acumulación**
   - Suma los valores por `(client_id, fruit)`.

3. **Recepción de EOF**
   - Cada `sum` envía un `CLIENT_EOF` cuando termina un cliente.

4. **Finalización**
   - Cuando el `agg` recibe EOF de todos los `sum` para un cliente:
     - considera el cliente completo;
     - emite el resultado final.

---

## Estructuras Clave

- `fruit_top_by_client`
  ```python
  {
    client_id: {
        fruit: total_amount
    }
  }
  ```

- `eof_received`
  - Lleva cuenta de cuántos `sum` enviaron EOF por cliente.

- `sum_workers`
  - Cantidad esperada de `sum`.

---

## Métodos Principales

#### `process_message(self, message, ack, nack)`

Punto de entrada principal.

- Deserializa el mensaje.
- Determina su tipo.
- Delega el procesamiento.

---

#### `_process_data(self, client_id, fruit, amount)`

- Acumula datos recibidos.

---

#### `_process_eof(self, client_id, sum_id)`

- Registra EOF por `sum`.
- Determina cuándo un cliente está completo.

---

#### `_send_aggregated_data(self, client_id)`

- Envía resultados agregados hacia `join`.

---

#### `_handle_sum_shutdown(self, sum_id)`

- Maneja la caída de un worker `sum`.

---

## Comportamiento Distribuido

- Cada `agg` recibe solo un subconjunto de datos.
- Esto se define por el hashing realizado en `sum`.
- No todos los `agg` ven todos los clientes completos.
- Cada `agg` produce resultados parciales finales por partición.

---

## Manejo de Errores

- Si hay errores en parsing → `nack`.
- Si hay problemas en envío downstream → log + retry implícito (según middleware).

---

## Limitaciones Conocidas

> **Dependencia fuerte en la cantidad de SUM activos**

### Problema

- El `agg` espera recibir EOF de todos los `sum`.
- Si un `sum` falla y no se notifica correctamente:
  - el cliente puede quedar bloqueado indefinidamente.
- Se requiere implementar o definir algún tipo de timeout para dejar de considerar al `sum` demorado.

### Impacto

- Resultados nunca emitidos.
- Deadlock lógico por cliente.

### Mejora Pendiente

- Implementar timeout por cliente.
- Manejo robusto de fallos de `sum`.
- Reconciliación dinámica de workers activos.

---

## Conclusión

El componente **Aggregation** realiza la agregación final distribuida, asegurando que los datos provenientes de múltiples `sum` se consoliden correctamente.

Es clave para la correcta finalización por cliente, pero actualmente **requiere mejoras en tolerancia a fallos** para ser completamente robusto.

### Join

El componente **Join** es la etapa final del pipeline. Su responsabilidad es **reunir (join) los resultados provenientes de los Aggregators (`agg`) y producir la salida final por cliente**.

---

## Responsabilidad General

- Recibir resultados finales parciales desde múltiples `agg`.
- Combinar esos resultados por **cliente**.
- Detectar cuándo un cliente está completamente procesado.
- Emitir el resultado final consolidado.

---

## Flujo General

1. **Recepción de datos**
   - Recibe mensajes con resultados finales desde los `agg`.

2. **Acumulación / Unión**
   - Integra los datos por `client_id`.

3. **Recepción de EOF**
   - Cada `agg` envía `CLIENT_EOF` por cliente.

4. **Finalización**
   - Cuando recibe EOF de todos los `agg` para un cliente:
     - considera el cliente completo;
     - emite el resultado final.

---

## Estructuras Clave

- `results_by_client`
  ```python
  {
    client_id: {
        fruit: total_amount
    }
  }
  ```

- `eof_by_client`
  - Contador de EOF recibidos por cliente.

- `agg_count`
  - Cantidad esperada de aggregators.

---

## Métodos Principales

#### `process_messsage(self, message, ack, nack)`

Punto de entrada principal.

- Deserializa el mensaje.
- Determina el tipo.
- Delega el procesamiento.

---

#### `_process_partial_top(self, client_id, fruit_top)`

- Recibe datos parciales desde `agg`.
- Construye el resultado final.

---

#### `_process_eof(self, client_id, agg_id)`

- Registra EOF por `agg`.
- Determina cuándo el cliente está completo.

---

#### `_handle_agg_shutdown(self, agg_id)`

- Maneja la caída de un worker `agg`.

---

## Comportamiento Distribuido

- Cada `agg` procesa solo una partición del dataset.
- El `join` reconstruye la visión completa del cliente.
- Actúa como punto de **sincronización final**.

---

## Manejo de Errores

- Mensajes mal formados → `nack`.
- Datos inconsistentes → log + descarte o tolerancia según implementación.

---

## Limitaciones Conocidas

> ⚠️ **Dependencia de la correcta señalización de EOF desde los aggregators**

### Problema

- El `join` depende de recibir EOF de todos los `agg`.
- Si un `agg` falla y no notifica:
  - el cliente puede no cerrarse nunca.

### Impacto

- Resultados finales incompletos.
- Clientes "colgados".

### Mejora Pendiente

- Implementar timeouts por cliente.
- Detección automática de `agg` caídos.
- Finalización resiliente.

---

## Conclusión

El **Join** es el último paso del pipeline distribuido, encargado de consolidar completamente los datos por cliente.

Cumple un rol crítico de sincronización, pero actualmente **depende fuertemente de señales de EOF correctas**, lo que lo hace sensible a fallos en etapas previas.

### Resultado

La implementación desarrollada permite construir un pipeline distribuido basado en middlewares orientados a mensajes, compuesto por las etapas `Sum`, `Aggregation` y `Join`, logrando:

* Procesar y distribuir mensajes de forma desacoplada entre múltiples workers.
* Realizar agregación parcial por cliente y tipo de fruta en la etapa `Sum`.
* Consolidar resultados intermedios en la etapa `Aggregation`, respetando la partición de datos.
* Integrar los resultados finales por cliente en la etapa `Join`.
* Coordinar el procesamiento mediante mensajes de control, incluyendo señales de `CLIENT_EOF` y eventos de shutdown.
* Mantener un comportamiento consistente y sincronizado entre las distintas etapas del sistema en condiciones normales de ejecución.
* Gestionar el cierre ordenado de consumidores y conexiones al middleware.
* Cumplir satisfactoriamente con las pruebas provistas por la cátedra.

### Limitaciones

Si bien la solución cumple con los requerimientos funcionales esperados, se identificaron las siguientes limitaciones en términos de tolerancia a fallos y robustez:

* Ante la caída de un worker `Aggregation`, la etapa `Sum` no recalcula dinámicamente el conjunto de aggregators activos, lo que puede derivar en pérdida de datos, resultados incompletos y/o bloqueos.
* La etapa `Aggregation` depende de recibir correctamente los mensajes `CLIENT_EOF` de todos los workers `Sum` para poder determinar la finalización de un cliente.
* La etapa `Join` depende de recibir los `CLIENT_EOF` de todos los workers `Aggregation` esperados para emitir el resultado final consolidado.
* No se implementaron mecanismos de reintento, rebalanceo dinámico ni timeouts por cliente, lo que limita la capacidad del sistema para recuperarse automáticamente ante fallos parciales.

Estas limitaciones fueron identificadas durante el desarrollo y se consideran oportunidades de mejora para una versión futura del trabajo.