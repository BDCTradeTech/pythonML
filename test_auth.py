"""
Tests de autenticación — estado actual SHA-256.

Propósito doble:
  1. Documentar el comportamiento actual para detectar regresiones.
  2. Servir de red de seguridad durante la migración a bcrypt:
       - TestAuthenticateUser y TestUpdateUserPassword deben pasar SIN cambios
         post-migración (son tests de comportamiento, no de implementación).
       - TestHashPassword tiene dos "canary tests" que DEBEN fallar después de
         migrar a bcrypt; cuando fallen, actualizarlos es la señal de que la
         migración está completa.

Estrategia de aislamiento:
  - La fixture `temp_db` parchea `main.DB_PATH` a un archivo SQLite temporal
    (distinto por test, cortesía de `tmp_path` de pytest).
  - `main.get_connection()` lee `DB_PATH` como global en cada llamada, por lo
    que el parche afecta a todas las funciones del módulo durante el test.
  - La base de datos real `app.db` nunca se toca.

Instalar dependencia de test (una sola vez):
  pip install pytest
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

import pytest


# ── Fixture central ───────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def temp_db(tmp_path, monkeypatch):
    """
    Redirige main.DB_PATH a un archivo SQLite temporal y crea solo la
    tabla `users` (suficiente para todas las funciones de auth).

    NO llamamos a main.init_db() porque tiene un bug de orden:
    el bloque del usuario semilla (línea ~203) intenta INSERT en
    `user_tab_permissions` antes de que esa tabla se cree (línea ~450).
    En producción el bug es invisible porque las tablas ya existen de
    arranques previos; en BD fresca da OperationalError.
    Las funciones authenticate_user / update_user_password / hash_password
    solo usan la tabla `users`, así que este setup mínimo es suficiente.
    """
    import main  # import tardío: así monkeypatch puede actuar antes de cualquier conexión

    db_file = tmp_path / "test_app.db"
    monkeypatch.setattr(main, "DB_PATH", db_file)

    # Setup mínimo: solo la tabla que usan las funciones de auth
    conn = sqlite3.connect(db_file)
    conn.executescript("""
        CREATE TABLE users (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT    UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at    TEXT NOT NULL,
            email         TEXT
        );
    """)
    conn.commit()
    conn.close()

    yield db_file


# ── Helper ────────────────────────────────────────────────────────────────────

def _crear_usuario(username: str, password: str) -> int:
    """
    Inserta un usuario de prueba directamente en la BD de test.
    Devuelve el id asignado.
    Usa main.get_connection() (que lee el DB_PATH parcheado) y
    main.hash_password() (para que el hash sea consistente con la
    implementación bajo prueba, sea SHA-256 o bcrypt).
    """
    import main

    conn = main.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (username, password_hash, created_at, email)"
            " VALUES (?, ?, ?, ?)",
            (
                username,
                main.hash_password(password),
                datetime.utcnow().isoformat(),
                username,
            ),
        )
        conn.commit()
        return cur.lastrowid  # type: ignore[return-value]
    finally:
        conn.close()


# ── TestHashPassword ──────────────────────────────────────────────────────────

class TestHashPassword:
    """
    Tests de formato e invariantes de hash_password().

    CANARY TESTS (marcados con [CANARY]):
      Fallarán intencionalmente al migrar a bcrypt.
      Cuando fallen: actualizar las aserciones para el nuevo formato
      y verificar que los tests de comportamiento siguen en verde.
    """

    def test_retorna_string(self):
        import main
        assert isinstance(main.hash_password("cualquier"), str)

    def test_no_retorna_vacio(self):
        import main
        assert main.hash_password("x") != ""

    def test_formato_bcrypt(self):
        """bcrypt produce hashes con prefijo $2b$ y longitud > 50 caracteres."""
        import main
        result = main.hash_password("test")
        assert result.startswith("$2b$")
        assert len(result) > 50

    def test_bcrypt_salt_aleatorio(self):
        """bcrypt usa salt aleatorio: mismo input produce hashes distintos, ambos válidos."""
        import bcrypt
        import main
        h1 = main.hash_password("abc123")
        h2 = main.hash_password("abc123")
        assert h1 != h2
        assert bcrypt.checkpw("abc123".encode(), h1.encode())
        assert bcrypt.checkpw("abc123".encode(), h2.encode())

    def test_distintos_inputs_dan_distintos_outputs(self):
        import main
        assert main.hash_password("password1") != main.hash_password("password2")

    def test_password_vacio_bcrypt(self):
        """bcrypt acepta password vacío: hash empieza con $2b$ y checkpw lo verifica."""
        import bcrypt
        import main
        result = main.hash_password("")
        assert result.startswith("$2b$")
        assert bcrypt.checkpw("".encode(), result.encode())

    def test_case_sensitive(self):
        import main
        assert main.hash_password("Password") != main.hash_password("password")

    def test_espacios_importan(self):
        import main
        assert main.hash_password("pass") != main.hash_password(" pass")
        assert main.hash_password("pass") != main.hash_password("pass ")


# ── TestAuthenticateUser ──────────────────────────────────────────────────────

class TestAuthenticateUser:
    """
    Tests de comportamiento de authenticate_user().
    Deben pasar SIN modificaciones después de migrar a bcrypt.
    """

    def test_login_exitoso_retorna_dict(self):
        import main
        _crear_usuario("alice@test.com", "pass1234")
        result = main.authenticate_user("alice@test.com", "pass1234")
        assert result is not None
        assert isinstance(result, dict)

    def test_login_exitoso_retorna_username_correcto(self):
        import main
        _crear_usuario("alice@test.com", "pass1234")
        result = main.authenticate_user("alice@test.com", "pass1234")
        assert result["username"] == "alice@test.com"

    def test_login_exitoso_retorna_id_correcto(self):
        import main
        uid = _crear_usuario("alice@test.com", "pass1234")
        result = main.authenticate_user("alice@test.com", "pass1234")
        assert result["id"] == uid

    def test_login_fallido_password_incorrecto(self):
        import main
        _crear_usuario("alice@test.com", "pass_correcto")
        result = main.authenticate_user("alice@test.com", "pass_malo")
        assert result is None

    def test_login_fallido_usuario_inexistente(self):
        import main
        result = main.authenticate_user("noexiste@test.com", "cualquier")
        assert result is None

    def test_login_fallido_password_vacio(self):
        import main
        _crear_usuario("alice@test.com", "pass1234")
        result = main.authenticate_user("alice@test.com", "")
        assert result is None

    def test_username_es_case_sensitive(self):
        """El username se guarda y busca tal cual; mayúsculas importan."""
        import main
        _crear_usuario("alice@test.com", "pass")
        result = main.authenticate_user("ALICE@test.com", "pass")
        assert result is None

    def test_password_es_case_sensitive(self):
        import main
        _crear_usuario("alice@test.com", "MiPassWord")
        result = main.authenticate_user("alice@test.com", "mipassword")
        assert result is None

    def test_dos_usuarios_no_se_mezclan_passwords(self):
        """El password de alice no sirve para bob y viceversa."""
        import main
        _crear_usuario("alice@test.com", "pass_alice")
        _crear_usuario("bob@test.com", "pass_bob")

        assert main.authenticate_user("alice@test.com", "pass_bob") is None
        assert main.authenticate_user("bob@test.com", "pass_alice") is None
        assert main.authenticate_user("alice@test.com", "pass_alice") is not None
        assert main.authenticate_user("bob@test.com", "pass_bob") is not None

    def test_usuario_semilla_no_interfiere(self):
        """
        init_db() inserta sanjustocentrocomputacion@gmail.com (Temp1234).
        Verificar que nuestros usuarios de test son independientes.
        """
        import main
        _crear_usuario("nuevo@test.com", "mi_pass")
        assert main.authenticate_user("nuevo@test.com", "Temp1234") is None
        assert main.authenticate_user("nuevo@test.com", "mi_pass") is not None


# ── TestUpdateUserPassword ────────────────────────────────────────────────────

class TestUpdateUserPassword:
    """
    Tests de comportamiento de update_user_password().
    Deben pasar SIN modificaciones después de migrar a bcrypt.
    """

    def test_exito_retorna_none(self):
        import main
        uid = _crear_usuario("user@test.com", "pass_viejo")
        error = main.update_user_password(uid, "pass_viejo", "pass_nuevo_ok")
        assert error is None

    def test_nuevo_password_permite_login(self):
        import main
        uid = _crear_usuario("user@test.com", "pass_viejo")
        main.update_user_password(uid, "pass_viejo", "pass_nuevo_ok")
        assert main.authenticate_user("user@test.com", "pass_nuevo_ok") is not None

    def test_password_viejo_ya_no_funciona(self):
        import main
        uid = _crear_usuario("user@test.com", "pass_viejo")
        main.update_user_password(uid, "pass_viejo", "pass_nuevo_ok")
        assert main.authenticate_user("user@test.com", "pass_viejo") is None

    def test_falla_password_actual_incorrecto(self):
        import main
        uid = _crear_usuario("user@test.com", "pass_real")
        error = main.update_user_password(uid, "equivocado", "nuevo_pass_ok")
        assert error is not None
        assert "incorrecta" in error.lower()

    def test_falla_password_actual_incorrecto_no_cambia_nada(self):
        """Un intento fallido no debe alterar la contraseña almacenada."""
        import main
        uid = _crear_usuario("user@test.com", "pass_real")
        main.update_user_password(uid, "equivocado", "nuevo_pass_ok")
        assert main.authenticate_user("user@test.com", "pass_real") is not None
        assert main.authenticate_user("user@test.com", "nuevo_pass_ok") is None

    def test_falla_usuario_inexistente(self):
        import main
        error = main.update_user_password(99999, "cualquier", "nuevo_ok_1234")
        assert error is not None
        assert "no encontrado" in error.lower()

    def test_falla_nuevo_password_muy_corto(self):
        """Menos de 4 caracteres debe ser rechazado."""
        import main
        uid = _crear_usuario("user@test.com", "pass_ok")
        error = main.update_user_password(uid, "pass_ok", "ab")  # 2 chars < 4
        assert error is not None

    def test_falla_nuevo_password_muy_corto_no_cambia_nada(self):
        import main
        uid = _crear_usuario("user@test.com", "pass_ok")
        main.update_user_password(uid, "pass_ok", "ab")
        assert main.authenticate_user("user@test.com", "pass_ok") is not None
        assert main.authenticate_user("user@test.com", "ab") is None

    def test_password_exactamente_4_chars_es_valido(self):
        """El límite es >= 4; exactamente 4 debe ser aceptado."""
        import main
        uid = _crear_usuario("user@test.com", "pass_ok")
        error = main.update_user_password(uid, "pass_ok", "abcd")  # exactamente 4
        assert error is None
        assert main.authenticate_user("user@test.com", "abcd") is not None

    def test_cambio_doble_encadenado(self):
        """Cambiar password dos veces: solo el último debe funcionar."""
        import main
        uid = _crear_usuario("user@test.com", "primero")
        main.update_user_password(uid, "primero", "segundo_ok")
        main.update_user_password(uid, "segundo_ok", "tercero_ok")
        assert main.authenticate_user("user@test.com", "primero") is None
        assert main.authenticate_user("user@test.com", "segundo_ok") is None
        assert main.authenticate_user("user@test.com", "tercero_ok") is not None
