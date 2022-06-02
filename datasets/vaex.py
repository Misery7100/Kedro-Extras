import vaex
import os
import gc
import logging
from typing import Any, Dict
from kedro.io import AbstractDataSet
from vaex.legacy import SubspaceLocal, Subspace

from carousel_ranking_v2.extras.utils.io import safe_rmtree, _TEMPDIR
from carousel_ranking_v2.extras.utils.types import VaexDataFrame

log = logging.getLogger(__name__)

# ------------------------- #

class VaexDataSet(AbstractDataSet):

    def __init__(self, filepath: str):

        self.filepath = filepath
        _dir, _ = os.path.split(filepath)
        self._tempdir = os.path.join(_dir, _TEMPDIR)
    
    # ......................... #

    def _load(self) -> VaexDataFrame:
        return vaex.open(self.filepath)
    
    # ......................... #

    def _save(self, data: VaexDataFrame) -> None:

        if isinstance(data, (Subspace, SubspaceLocal)):
            data.df.export(
                path=self.filepath, 
                progress=True
            )
            
        else:
            data.export(
                path=self.filepath, 
                progress=True
            )
            
        safe_rmtree(self._tempdir)
        gc.collect(generation=2)
    
    # ......................... #

    def _describe(self) -> Dict[str, Any]:
        return dict(filepath=self.filepath)

# ------------------------- #