# Lab3_distribuidos_2026_grupo_17

Cristian Rioseco 202204507-8
Jaime Donoso 202273645-3
Dan gonzales  202273543-0


orden a ejecutar:

(VM3 → VM4 → VM2 → VM1)

Acceder a las vm (ssh) y luego ejecutar de acuerdo al orden en cada una 
de las vm.

1. entras a la vm3 y luego:
make docker-VM3
2. entras a la vm4 y luego:
make docker-VM4
3. entras a la vm2 y luego:
make docker-VM2
4.entras a la vm1 y luego:
make docker-VM1


(destacar que cada codigo se debe ejectutar cuando estes en 
la maquina virtual correspondiente, ademas cabe destacar que
si habres dos terminales, una con la tabla de nombre e intentas
ver en tiempo real el registro (si funciona), es
probable que en un punto deje "mostar que se esta registrando", esto se 
debe a que la universidad cada cierto tiempo te hecha de la conexion
, debes volver a meterte y ver denuevo la tabla que está actualizada
 ya que cada cierto tiempo te saca el ssh.)

