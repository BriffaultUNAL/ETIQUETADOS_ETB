#!/usr/bin/python

from src.telegram_bot import enviar_mensaje
from src.utils import *
import sys
import os

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, 'src')
sys.path.append(proyect_dir)


if __name__ == "__main__":

    asyncio.run(enviar_mensaje('Etiquetado_ETB'))

    load(transform(os.path.join(act_dir, 'sql', 'df_extend_inb_172.66.7.171.sql'),
                   df_recording_log, 15, 'inbound'))

    load(transform(os.path.join(act_dir, 'sql', 'df_extend_inb_172.80.7.214.sql'),
                   df_recording_log, 15, 'inbound'))

    load(transform(os.path.join(act_dir, 'sql', 'df_extend_out_172.66.7.171.sql'),
                   df_recording_log, 45, 'outbound'))

    asyncio.run(enviar_mensaje("____________________________________"))
