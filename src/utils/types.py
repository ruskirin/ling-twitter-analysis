from typing import Literal, NewType, Union
from enum import Enum


class DbKey(Enum):
    adminDb = 'ADMIN_DB_NAME'
    adminUser = 'ADMIN_DB_USER'
    adminPwd = 'ADMIN_DB_PASS'
    adminHost = 'ADMIN_DB_HOST'
    prodDb = 'PROD_DB_NAME'
    prodUser = 'PROD_DB_USER'
    prodPwd = 'PROD_DB_PASS'
    prodHost = 'PROD_DB_HOST'
    testDb = 'TEST_DB_NAME'
    testUser = 'TEST_DB_USER'
    testPwd = 'TEST_DB_PASS'
    testHost = 'TEST_DB_HOST'


EnvKey = NewType('EnvKey', Union[DbKey, str])
Lang = Literal['es', 'pt']
Verb = Literal[
    'pensar', 'lamentar', 'rogar', 'afirmar', 'lograr', 'sentir', 'querer',
    'admitir', 'prever', 'suplicar', 'comprobar', 'recomendar', 'confesar',
    'predecir', 'entender', 'parecer', 'ojala', 'reclamar', 'esperar', 'dudar',
    'creer', 'mandar', 'mencionar', 'acordar', 'apostar', 'demostrar',
    'mostrar', 'pedir', 'saber', 'temer', 'suspirar', 'ver', 'confirmar',
    'conseguir', 'prometer', 'considerar', 'negar', 'recordar', 'adivinar',
    'contar', 'ordenar', 'jurar', 'asegurar', 'solicitar', 'responder',
    'gritar', 'desear', 'suponer', 'imaginar', 'decir'
]
MetaType = Literal['user']