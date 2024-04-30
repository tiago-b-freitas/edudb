#from .edubr import handleCensoEscolar, handleCensoDemografico, handlePNADc,\
#                   handleRendimentoEscolar
#from .edusp import handleSaresp
from .censoDemografico import handleCensoDemografico 
from .definitions import UF_SIGLA_NOME


__author__ = "Tiago Barreiros de Freitas (t036278@dac.unicamp.br)"
__license__ = "MIT"
__version__ = '0.1.0'

#__all__ = (
#    "handleCensoEscolar",
#    "handlePNADc",
#    "handleCensoDemografico",
#    "handleRendimentoEscolar",
#    "handleSaresp",
#)

__all__ = (
    "handleCensoEscolar",
    "UF_SIGLA_NOME"
)
