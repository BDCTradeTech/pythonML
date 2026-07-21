"""
conftest de esta carpeta SOLAMENTE. Aislado del resto del repo a propósito:
requiere nicegui.testing, que necesita Python 3.11/3.12 (nicegui usa vbuild,
que rompe con Python 3.14 por pkgutil.find_loader removido). Los demás tests
del repo (test_auth.py, etc.) corren con el Python del sistema y no deben
verse afectados por este plugin -- por eso vive en su propia carpeta y no en
el conftest.py de la raíz.

Correr con el venv dedicado (ver README_TESTS.txt en esta carpeta si existe,
o el venv 3.11 armado para este trabajo).
"""
pytest_plugins = ["nicegui.testing.user_plugin"]
