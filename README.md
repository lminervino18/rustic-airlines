> ⚠️ **Advertencia:** Este repositorio solo contiene un commit porque fue utilizado en un ámbito educativo. El repositorio original del proyecto permanece en privado.

# Taller de Programacion - Grupo "Ferrum"

![image](https://github.com/user-attachments/assets/34cde573-acef-44c8-a5fb-16f3334902ce)

## Integrantes

- **Lorenzo Minervino:** 107863
- **Federico Camurri:** 106359
- **Alen Davies:** 107084
- **Luca Lazcano:** 107044

## Introducción

Este trabajo práctico consiste en la implementación de un sistema de control de vuelos global. Para ello, se desarrolló una base de datos distribuida basada en el modelo de Cassandra, que permite el acceso concurrente para lectura y escritura por múltiples clientes distribuidos.

## Guía de Uso

A continuación, se describen los pasos para compilar, ejecutar y probar el programa.

### Compilación y Ejecución

Para compilar y ejecutar el programa, existen las siguientes opciones:

1. **Levantar 5 nodos automáticamente:**  
   Ejecutar `make run` en el directorio `node_launcher`.

2. **Levantar nodos individualmente:**  
   Ejecutar `cargo run {ip}` en el directorio `node_launcher`.

3. **Crear tablas, keyspaces y cargar datos de prueba:**  
   Ejecutar `cargo run` en el directorio `driver/examples`.

4. **Cargar datos de vuelos y aeropuertos para la interfaz gráfica:**  
   Ejecutar `cargo run --example airports` en el directorio `graphical-interface`.

5. **Ejecutar la interfaz gráfica:**  
   Una vez cargados los datos en el clúster, ejecutar `cargo run` en el directorio `graphical-interface`.

### Pruebas

Para probar el programa:

- **Probar módulos individuales:** Ejecutar `cargo test` dentro de cada módulo.
- **Probar todos los módulos a la vez:** Ejecutar `cargo test --all` desde la raíz del proyecto.

## Ejecución con Docker

### Levantar el Clúster con Docker Compose

1. Para levantar el clúster de nodos definido en `compose.yml`, ejecutar:

   ```bash
   sudo docker compose --profile "*" up --build
   ```

2. El driver utiliza la variable de entorno `NODE_ADDR` para conectarse al clúster. Por ejemplo:

   ```bash
   export NODE_ADDR="127.0.0.1:10000" && cargo run
   ```

   Esto conecta la interfaz gráfica al clúster a través del puerto `10000` mapeado al nodo correspondiente según lo definido en `compose.yml`.

3. Ejemplos específicos de ejecución:
   - Para la interfaz gráfica:
     ```bash
     cd graphical-interface && export NODE_ADDR="127.0.0.1:10001" && cargo run
     ```
   - Para el simulador de vuelos:
     ```bash
     cd flight-sim && export NODE_ADDR="127.0.0.1:10002" && cargo run
     ```

### Configuración del Nodo Semilla

- La IP del nodo semilla utilizado por un nodo puede configurarse mediante la variable de entorno `SEED`.
- Para agregar o quitar nodos, se deben ajustar las direcciones IP en `compose.yml`.

### Manejo de Perfiles en Docker Compose

1. Para levantar los nodos definidos en el perfil `initial-nodes`, ejecutar:

   ```bash
   sudo docker compose --profile initial-nodes up
   ```

2. Para simular la unión de un nuevo nodo al clúster, ejecutar:

   ```bash
   sudo docker compose --profile new-node up
   ```

3. Se pueden agregar más perfiles para simular la unión de más nodos.

### Apagar y Eliminar Nodos

Para detener y eliminar un nodo, ejecutar:

```bash
sudo docker stop nodeX && sudo docker rm nodeX
```

Reemplazar `nodeX` con el nombre del nodo a detener. Para identificar el nombre del nodo, consultar la sección `container_name` del servicio correspondiente en `compose.yml`.

---

Este sistema está diseñado para ofrecer una solución distribuida y escalable que permita la simulación y gestión de vuelos globales mediante nodos interconectados. Cada componente ha sido probado y configurado para garantizar un rendimiento óptimo.
