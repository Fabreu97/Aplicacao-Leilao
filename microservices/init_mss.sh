#!/bin/bash

echo "Inicializando os micvroserviços . . ."

python3 ms_leilao.py &
python3 ms_lance.py &
python3 ms_notificacao.py &
