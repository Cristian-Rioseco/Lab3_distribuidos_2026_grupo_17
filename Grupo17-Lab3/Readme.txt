Lab 3: Sistemas Distribuidos - DistriEats

Repositorio GitHub: https://github.com/Cristian-Rioseco/Lab3_distribuidos_2026_grupo_17


Cristian Rioseco (202204507-8) - rol de creacion: nameserver,dockercompose mv1 y mv3, makefile, datanode.
Jaime Donoso (202273645-3) - rol de creacion: Extractor, cliente y broker.
Dan Gonzales (202273543-0) - rol de creacion: Procesadores, docker compose mv2 y mv4.

Distribución en Máquinas Virtuales:
VM1 (dist065): Base de Datos Legada (PostgreSQL), Cliente Emisor, Extractor Histórico.
VM2 (dist066): RabbitMQ, Broker de Logística, Procesadora 1 (Trabajadora/Líder).
VM3 (dist067): Servidor de Nombres, Procesadora 2 (Trabajadora/Líder).
VM4 (dist068): Datanode Central, Procesadora 3 (Trabajadora/Líder).

Instrucciones de Ejecución
Para el correcto funcionamiento del sistema, es estrictamente necesario levantar las máquinas en el orden indicado a continuación. Cada comando debe ejecutarse en la terminal correspondiente a su Máquina Virtual tras acceder por SSH.

Requisito para la mv2

Dado que las máquinas de la U ya tienen el rabbitmq, antes de levantar la VM2 debemos detener dicho servicio para evitar colisiones con el contenedor de Docker (ya esta desde antes en nuestras maquinas para probar el codigo):
sudo systemctl stop rabbitmq-server    (ya no es necesario)


Orden de Encendido (paso a paso, puede llegar y copiar ):

1. Levantar VM3 (aca esta el nameserver, por eso es la primera)
ssh dist@dist067.inf.santiago.usm.cl
cd Grupo17-Lab3
sudo make docker-VM3

2. Levantar VM4
ssh dist@dist068.inf.santiago.usm.cl
cd Grupo17-Lab3
sudo make docker-VM4

3. Levantar VM2
ssh dist@dist066.inf.santiago.usm.cl
cd Grupo17-Lab3
sudo make docker-VM2

4. Levantar VM1
ssh dist@dist065.inf.santiago.usm.cl
cd Grupo17-Lab3
sudo make docker-VM1

Importante:

Si se abren múltiples terminales para observar los logs en tiempo real (por ejemplo, el registro en el NameServer), es probable que la terminal se congele o deje de mostrar nuevos eventos tras un rato de no usarla. Esto es normal de los servidores de la U, cortan la conexión SSH por timeout. Si eso pasa, simplemente vuelva a conectarse a la máquina y ejecute "sudo docker compose -f docker-compose-mvX.yml logs -f" para volver a ver.

Apagado y Limpieza

para apagar y limpiar haga:

sudo make down-VMX

(Reemplazando la X por el número de la máquina correspondiente).

(IMPORTANTE: toma alrededor de 200-300 segundos por cada vm)
(sudo docker compose -f docker-compose-mv3.yml logs -f     : este para ver que esta pasando)