from datarepo.core import Catalog, ModuleDatabase
import neuro_tables as nt

NeuroCatalog = Catalog({
    "neuro": ModuleDatabase(nt)
})
