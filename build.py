from datarepo.export.web import export_and_generate_site
from catalog import NeuroCatalog
import shutil
import pathlib

output_dir = pathlib.Path("public")
shutil.rmtree(output_dir, ignore_errors=True)
export_and_generate_site(catalogs=[("NeuroCatalog", NeuroCatalog)], output_dir=str(output_dir))
print("Static site generated in ./public/ → open public/index.html or deploy!")
